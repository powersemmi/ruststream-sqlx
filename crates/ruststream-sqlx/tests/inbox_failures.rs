//! What a subscription does when a statement fails: a failed claim reaches the stream and the next
//! one waits a second; a failed settlement reports itself and returns its row.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::InProcess;
use ruststream::{
    AckError, Broker, ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource,
};
use ruststream_sqlx::{InboxQueue, SqlxBroker, SqlxBrokerError};
use tokio::time::Instant;

live::matrix! {
    // The in-process mode keeps a paused clock still while the database answers, so the wait
    // between claims shows on the tokio clock exactly.
    #[tokio::test]
    async fn a_failed_claim_is_an_item_and_the_next_one_waits_a_second() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(Duration::from_millis(20))
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        db.drop_plain().await;
        tokio::time::pause();
        {
            let mut deliveries = pin!(subscriber.stream());
            let failed = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect_err("the claim fails");
            assert!(
                matches!(&failed, SqlxBrokerError::Sqlx { statement, table, .. }
                    if statement.contains("plain_jobs") && table == "plain_jobs"),
                "{failed:?}"
            );
            let started = Instant::now();
            let again = deliveries.next().await.expect("the stream goes on");
            assert!(again.is_err(), "the table is still gone");
            let waited = started.elapsed();
            assert!(
                waited >= Duration::from_secs(1) && waited < Duration::from_millis(1100),
                "a failed claim waits a second, not the poll interval: waited {waited:?}"
            );
        }
        tokio::time::resume();
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_settlement_reports_itself_and_returns_its_row() {
        let Some(db) = database().await else { return };
        let ids = db.fragile(&["a"]).await;
        // A reference stands, so the acknowledgement's delete fails.
        db.reference(ids[0]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(Duration::from_millis(20))
            .connect()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Fragile>::new("fragile")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let delivery = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim");
            let refused = delivery.ack().await.expect_err("the delete fails");
            let source = match &refused {
                AckError::Broker(source) => source.downcast_ref::<SqlxBrokerError>(),
                _ => None,
            };
            assert!(
                matches!(source, Some(SqlxBrokerError::Sqlx { statement: "ack", table, .. })
                    if table == "fragile_jobs"),
                "{refused:?}"
            );
            // The settlement rolled back, so the row is claimed again.
            let again = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim");
            assert_eq!(again.payload(), b"a");
        }
        assert_eq!(db.fragile_rows().await, ["a"]);
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }
}
