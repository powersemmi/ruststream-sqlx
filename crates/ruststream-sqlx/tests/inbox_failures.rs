//! What a subscription does when a statement fails: a failed claim reaches the stream and the next
//! one waits a second; a failed settlement reports itself and returns its row. A claim that took
//! no row reads none, so a fetch of the service's own is never handed an empty list. A row read by
//! name whose payload does not decode still reports its attempt.

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
    AckError, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscribe, Subscriber,
    SubscriptionSource,
};
use ruststream_sqlx::{InboxQueue, Publish, SqlxBroker, SqlxBrokerError};
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

    // The fetch refuses an empty list, as MySQL refuses an empty `IN ()`: a claim that handed it
    // one would reach the stream as a failed claim.
    #[tokio::test]
    async fn an_empty_claim_does_not_call_the_services_fetch() {
        let Some(db) = database().await else { return };
        let poll = Duration::from_millis(20);
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(poll)
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Fetched>::new("plain")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        tokio::time::pause();
        {
            let mut deliveries = pin!(subscriber.stream());
            // The paused clock stands still while the database answers, so the empty table is
            // claimed five times before the wait ends.
            match tokio::time::timeout(poll * 5, deliveries.next()).await {
                Err(_) => {}
                Ok(Some(Err(failed))) => panic!("an empty claim failed: {failed}"),
                Ok(_) => panic!("an empty table delivered a row"),
            }
            tokio::time::resume();
            db.plain(&[b"x".as_slice()]).await;
            let delivery = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim fetches the row it took");
            assert_eq!(delivery.payload(), b"x");
            delivery.ack().await.expect("the row settles");
        }
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
        // A lease of a second: a leased row whose settlement failed returns within the test.
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(Duration::from_millis(20))
            .lease(Duration::from_secs(1))
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
            // The settlement took no effect, so the row is claimed again: at once after the
            // rollback of its transaction, or once its lease runs out.
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

    // A bare name declares no cap, so the attempt a by-name delivery reports is what the runtime
    // and the handler's `Ctx<Attempt>` read of the row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_by_name_row_that_does_not_decode_reports_its_attempt() {
        let Some(db) = database().await else { return };
        let mut conn = db.pool.acquire().await.expect("a connection");
        <Unreadable as Publish<Db>>::publish(&mut conn, &OutgoingMessage::new("unreadable", b"{}"))
            .await
            .expect("the row writes");
        drop(conn);
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(Duration::from_millis(20))
            .route::<Unreadable>("unreadable")
            .connect()
            .await
            .expect("the broker connects");
        let mut subscriber = connected
            .subscribe("unreadable")
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let first = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim");
            assert!(first.payload().is_empty(), "the payload does not decode");
            assert_eq!(first.redelivery_count(), Some(1));
            first.nack(true).await.expect("the row returns");
            let second = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim");
            assert_eq!(second.redelivery_count(), Some(2), "the retry counted");
            second.ack().await.expect("the row settles");
        }
        assert_eq!(db.count("unreadable_jobs").await, 0);
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }
}
