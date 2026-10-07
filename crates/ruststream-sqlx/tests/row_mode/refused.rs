//! What a row-mode delivery has no row to lend for settles by the decode policy, on every stand and
//! in every form, and the handler does not run: a row the driver could not read, a claimed id
//! without a row, and every delivery of a handler that decodes a payload, which a row-mode table
//! does not carry.

use ruststream::testing::{Outcome, TestApp};
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::Pool;

use super::{POLL, SETTLED};
use crate::live;

/// What a handler that decodes a payload reads from a message.
#[derive(Debug, Deserialize)]
struct Email {
    to: String,
}

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(POLL)
    }

    #[subscriber(InboxQueue::<Mail>::new("mail"), on_failure(decode = drop))]
    async fn unread(_mail: &Mail) -> HandlerOutcome {
        // Reaching this would acknowledge; the decode policy drops what the driver could not read.
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_the_driver_cannot_read_settles_by_the_decode_policy() {
        let Some(db) = database().await else { return };
        db.unreadable_mail("mail").await;
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(unread);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the row settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_called_once()
            .settled(HandlerOutcome::drop())
            .assert_last_failed_to_decode();
        assert_eq!(db.count("mail_jobs").await, 0, "the policy dropped the row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<FetchedMail>::new("mail"), on_failure(decode = drop))]
    async fn fetched(_mail: &FetchedMail) -> HandlerOutcome {
        // Reaching this would acknowledge; the decode policy drops a claimed id without a row.
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_claimed_id_without_a_row_settles_by_the_decode_policy() {
        let Some(db) = database().await else { return };
        // The service's fetch leaves this mail out: the claim takes its id and finds no row.
        db.mail(&[FetchedMail::queued("mail", "ops@example.com", Some("gone"))]).await;
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(fetched);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the row settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_called_once()
            .settled(HandlerOutcome::drop())
            .assert_last_failed_to_decode();
        assert_eq!(db.count("mail_jobs").await, 0, "the policy dropped the row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Mail>::new("mail"), on_failure(decode = drop))]
    async fn decoded(email: &Email) -> HandlerOutcome {
        // Never reached: a row-mode delivery carries no payload to decode.
        if email.to.is_empty() {
            HandlerOutcome::drop()
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_that_decodes_a_payload_fails_every_delivery() {
        let Some(db) = database().await else { return };
        db.mail(&[
            Mail::queued("mail", "ops@example.com", None),
            Mail::queued("mail", "dev@example.com", None),
        ])
        .await;
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(decoded);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("both rows settle");
        assert_eq!(
            tb.broker::<SqlxBroker<Db>>().subscriber("mail").outcomes(),
            [Outcome::DecodeFailed, Outcome::DecodeFailed],
            "neither delivery carried a payload to decode"
        );
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .settled(HandlerOutcome::drop());
        assert_eq!(db.count("mail_jobs").await, 0, "the policy dropped both rows");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
