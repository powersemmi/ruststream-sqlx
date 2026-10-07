//! A row-mode delivery lends its handler the row the driver read, run as an application through
//! `TestApp::start_live` against each stand and form: the whole row, a retried row with its attempt
//! counted, and a headers column taken into the delivery's headers.

use std::collections::BTreeMap;

use ruststream::testing::TestApp;
use ruststream_sqlx::prelude::*;
use sqlx::Pool;
use sqlx::types::Json;

use super::{POLL, SETTLED};
use crate::live;

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(POLL)
    }

    #[subscriber(InboxQueue::<Mail>::new("mail"))]
    async fn send(mail: &Mail, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
        // A mail whose subject asks for it goes back to the queue once.
        if mail.subject.as_deref() == Some("retry") && attempt == Some(1) {
            HandlerOutcome::retry()
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_takes_the_row_itself() {
        let Some(db) = database().await else { return };
        db.mail(&[Mail::queued("mail", "ops@example.com", None)]).await;
        let written: Mail = db.mails().await.remove(0);
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(send);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the row settles");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .received_values::<Mail>();
        assert_eq!(
            lent.iter().map(|mail| mail.recipient.as_str()).collect::<Vec<_>>(),
            ["ops@example.com"]
        );
        // The row as the table holds it, with the id the database generated.
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_called_once()
            .with_value(&written.leased_as(&lent[0]))
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("mail_jobs").await, 0, "the acknowledgement deleted the row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_row_comes_back_with_its_attempt_counted() {
        let Some(db) = database().await else { return };
        db.mail(&[Mail::queued("mail", "ops@example.com", Some("retry"))]).await;
        let written: Mail = db.mails().await.remove(0);
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(send);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("both deliveries settle");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .received_values::<Mail>();
        assert_eq!(lent.len(), 2, "the retry brought the row back once: {lent:?}");
        assert_eq!(
            lent[0],
            written.clone().leased_as(&lent[0]),
            "the first delivery lent the row as it was written"
        );
        // The table counted the first delivery, and the row comes back with it.
        let counted = Mail {
            attempt: written.attempt + 1,
            ..written
        };
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_called(2)
            .with_value(&counted.leased_as(&lent[1]))
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("mail_jobs").await, 0, "the second delivery acknowledged the row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Mail>::new("mail"))]
    async fn tenanted(_mail: &Mail, ctx: &mut Context<'_>) -> HandlerOutcome {
        // The delivery carries the headers the row's column held, as middleware reads them.
        if ctx.headers().get_str("x-tenant") == Some("acme") {
            HandlerOutcome::ack()
        } else {
            HandlerOutcome::drop()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_headers_column_reaches_the_delivery_and_the_row_lends_it_empty() {
        let Some(db) = database().await else { return };
        let headers = BTreeMap::from([("x-tenant".to_owned(), "acme".to_owned())]);
        db.mail(&[Mail {
            meta: Some(Json(headers)),
            ..Mail::queued("mail", "ops@example.com", None)
        }])
        .await;
        let written: Mail = db.mails().await.remove(0);
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(tenanted);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the row settles");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .received_values::<Mail>();
        // The claim took the headers into the delivery, and left the row's column empty.
        let emptied = Mail {
            meta: Some(Json(BTreeMap::new())),
            ..written
        };
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_called_once()
            .with_value(&emptied.leased_as(&lent[0]))
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
