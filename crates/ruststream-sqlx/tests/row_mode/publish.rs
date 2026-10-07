//! A row-mode table is written through `Repository` over a `Publish` of the service's own, run as
//! an application through `TestApp::start_live` against each stand and form: a handler's reply
//! becomes a row, and a `&Row` handler of its group receives it.

use ruststream::testing::TestApp;
use ruststream_sqlx::Repository;
use ruststream_sqlx::prelude::*;
use sqlx::Pool;

use super::{POLL, SETTLED};
use crate::live;

/// A recipient on its way to the mail queue, as the bytes the table's `Publish` reads; no codec
/// runs on them.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "mail")]
struct Address(Vec<u8>);

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(POLL)
    }

    #[subscriber(InboxQueue::<Mail>::new("requests"), reply)]
    async fn forward(request: &Mail) -> Address {
        Address(request.recipient.clone().into_bytes())
    }

    #[subscriber(InboxQueue::<Mail>::new("mail"))]
    async fn send(_mail: &Mail) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_repository_reply_becomes_a_row_a_row_handler_receives() {
        let Some(db) = database().await else { return };
        db.mail(&[Mail::queued("requests", "ops@example.com", None)]).await;
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(forward).out_reply(Repository::<Mail>::default());
                b.include(send);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("both rows settle");
        let sent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .received_values::<Mail>();
        assert_eq!(
            sent.iter()
                .map(|mail| (mail.name.as_str(), mail.recipient.as_str(), mail.subject.as_deref()))
                .collect::<Vec<_>>(),
            [("mail", "ops@example.com", None)],
            "the reply became one mail of the group its type names"
        );
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("mail_jobs").await, 0, "both rows were acknowledged");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
