//! A batch handler: one claim is the batch, each delivery settles on its own.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream::{AckError, BatchSubscriber, ConnectedBroker, SubscriptionSource};
use ruststream_sqlx::{InboxQueue, SqlxBroker, SqlxBrokerError};
use serde::{Deserialize, Serialize};

use crate::live;

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Line {
    n: u32,
}

live::matrix! {
    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn settle(lines: &[Line]) -> Vec<HandlerOutcome> {
        lines
            .iter()
            .map(|line| {
                if line.n % 2 == 0 {
                    HandlerOutcome::ack()
                } else {
                    HandlerOutcome::drop()
                }
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_claim_is_the_batch_and_each_delivery_settles_on_its_own() {
        let Some(db) = database().await else { return };
        // Rows written before the app starts: the first claim takes four of them.
        let lines: Vec<Vec<u8>> = (0..6_u32)
            .map(|n| serde_json::to_vec(&Line { n }).expect("json"))
            .collect();
        db.plain(&lines).await;
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(Duration::from_millis(20))
            .route::<Plain>("plain");
        let app = RustStream::new(AppInfo::new("batches", "0.0.0")).with_broker(broker, |b| {
            b.include(settle.batch(nonzero!(4)));
        });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(Duration::from_millis(300))
            .await
            .expect("the batches settle");
        let sizes: Vec<usize> = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .batches::<Line>()
            .iter()
            .map(Vec::len)
            .collect();
        assert_eq!(
            sizes,
            [4, 2],
            "a full claim is followed at once by the next"
        );
        // Every row was settled: acknowledged or dropped, each deleted by its own statement.
        assert_eq!(db.plain_rows("plain_jobs").await, Vec::<Vec<u8>>::new());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_whose_last_delivery_drops_unsettled_keeps_the_others_settlements() {
        let Some(db) = database().await else { return };
        db.fragile(&["a", "b"]).await;
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
            let mut batches = pin!(subscriber.batches(nonzero!(2_usize)));
            let mut batch = batches.next().await.expect("a batch").expect("the claim");
            // SQLite returns a batch's rows in no particular order: the payload names each one.
            batch.sort_by(|left, right| left.payload().cmp(right.payload()));
            let [a, b]: [_; 2] = batch.try_into().expect("two deliveries");
            a.ack().await.expect("the first acknowledges");
            drop(b);

            // The next claim passes over the batch's rows until the batch lets go of them (its
            // transaction ends, or the dropped delivery's release clears its lease), then finds
            // `b` alone.
            let again = batches.next().await.expect("a batch").expect("the claim");
            let payloads: Vec<&[u8]> = again.iter().map(IncomingMessage::payload).collect();
            assert_eq!(payloads, [b"b".as_slice()]);
        }
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }
}

// The form decides what a failed settlement does to the rest of its batch, so each form states it
// with rows of its own.

/// The row lock form: a batch is one transaction, on the stands whose databases lock rows.
mod in_one_transaction {
    #[allow(unused_imports)]
    use super::*;

    crate::live::row_lock_stands! {
        use crate::live::rows::row_lock;

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_failed_settlement_rolls_its_whole_batch_back() {
            let Some(db) = database().await else { return };
            let ids = db.fragile(&["a", "b", "c"]).await;
            // `b` is still referenced, so its acknowledgement fails.
            db.reference(ids[1]).await;
            let connected = SqlxBroker::new(db.pool.clone())
                .connect()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<row_lock::Fragile>::new("fragile")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let batch = pin!(subscriber.batches(nonzero!(3_usize)))
                .next()
                .await
                .expect("a batch")
                .expect("the claim");
            let [a, b, c]: [_; 3] = batch.try_into().expect("three deliveries");

            a.ack().await.expect("the first statement runs");
            b.ack()
                .await
                .expect_err("a referenced job cannot be deleted");
            let refused = c
                .ack()
                .await
                .expect_err("the batch's transaction failed already");
            let source = match &refused {
                AckError::Broker(source) => source.downcast_ref::<SqlxBrokerError>(),
                _ => None,
            };
            assert!(
                matches!(source, Some(SqlxBrokerError::BatchRolledBack { .. })),
                "{refused:?}"
            );
            // The batch's settlements become durable together, so `a` comes back with the others.
            assert_eq!(db.fragile_rows().await, ["a", "b", "c"]);
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            db.finish().await;
        }
    }
}

live::stands! {
    use crate::live::rows::lease;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_settlement_leaves_the_other_settlements_of_a_leased_batch_in_place() {
        let Some(db) = database().await else { return };
        let ids = db.fragile(&["a", "b", "c"]).await;
        // `b` is still referenced, so its acknowledgement fails.
        db.reference(ids[1]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .connect()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<lease::Fragile>::new("fragile")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        let mut batch = pin!(subscriber.batches(nonzero!(3_usize)))
            .next()
            .await
            .expect("a batch")
            .expect("the claim");
        // SQLite returns a batch's rows in no particular order: the payload names each one.
        batch.sort_by(|left, right| left.payload().cmp(right.payload()));
        let [a, b, c]: [_; 3] = batch.try_into().expect("three deliveries");

        a.ack().await.expect("the first acknowledgement commits");
        let refused = b
            .ack()
            .await
            .expect_err("a referenced job cannot be deleted");
        let source = match &refused {
            AckError::Broker(source) => source.downcast_ref::<SqlxBrokerError>(),
            _ => None,
        };
        assert!(
            matches!(
                source,
                Some(SqlxBrokerError::Sqlx {
                    statement: "ack",
                    ..
                })
            ),
            "{refused:?}"
        );
        c.ack()
            .await
            .expect("a leased delivery settles on its own, whatever its batch did");
        // Each settlement committed on its own: only the job whose acknowledgement failed stays.
        assert_eq!(db.fragile_rows().await, ["b"]);
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }
}

/// The advisory lock form: each delivery of a batch holds a session of its own and settles on it.
mod one_session_each {
    #[allow(unused_imports)]
    use super::*;

    crate::live::advisory_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_failed_settlement_leaves_the_other_settlements_of_an_advisory_batch_in_place() {
            let Some(db) = database().await else { return };
            let ids = db.fragile(&["a", "b", "c"]).await;
            // `b` is still referenced, so its acknowledgement fails.
            db.reference(ids[1]).await;
            let connected = SqlxBroker::new(db.pool.clone())
                .connect()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<Fragile>::new("fragile")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let batch = pin!(subscriber.batches(nonzero!(3_usize)))
                .next()
                .await
                .expect("a batch")
                .expect("the claim");
            let [a, b, c]: [_; 3] = batch.try_into().expect("three deliveries");

            a.ack().await.expect("the first acknowledgement settles");
            let refused = b
                .ack()
                .await
                .expect_err("a referenced job cannot be deleted");
            let source = match &refused {
                AckError::Broker(source) => source.downcast_ref::<SqlxBrokerError>(),
                _ => None,
            };
            assert!(
                matches!(
                    source,
                    Some(SqlxBrokerError::Sqlx {
                        statement: "ack",
                        ..
                    })
                ),
                "{refused:?}"
            );
            c.ack()
                .await
                .expect("a delivery settles on its own session, whatever its batch did");
            // Each settlement took effect on its own: only the job whose acknowledgement failed
            // stays.
            assert_eq!(db.fragile_rows().await, ["b"]);
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            db.finish().await;
        }
    }
}
