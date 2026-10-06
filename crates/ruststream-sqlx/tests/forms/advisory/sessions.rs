//! The sessions of an advisory subscription: it opens with its candidate claim, its lock, its
//! unlock and its take prepared; a batch takes a session per delivery, as many as the pool spares
//! at once; a delivery dropped unsettled closes its session, and a new broker takes its row at
//! once; two brokers on one table never deliver one row twice.

use std::collections::BTreeSet;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::prelude::*;
use ruststream::testing::InProcess;
use ruststream::{
    BatchSubscriber, Broker, ConnectedBroker, IncomingMessage, Subscribe, Subscriber,
    SubscriptionSource,
};
use ruststream_sqlx::{InboxQueue, SqlxBroker};
use sqlx::Pool;
use sqlx::pool::PoolOptions;

use super::{AT_ONCE, POLL, assert_no_lock, payload_id};
use crate::live;

live::advisory_stands! {
    // A batch takes a connection per delivery: what the pool spares at once, never a wait for a
    // delivery in work to settle.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_larger_than_the_pool_shrinks_instead_of_waiting() {
        let Some(db) = database().await else { return };
        db.plain(&[b"a".as_slice(), b"b", b"c"]).await;
        // A pool of two connections, one of them held: the claim has one place left. The pool
        // waits for a connection longer than the test waits for the batch.
        let small = PoolOptions::<Db>::new()
            .max_connections(2)
            .acquire_timeout(AT_ONCE * 6)
            .connect_with((*db.pool.connect_options()).clone())
            .await
            .expect("a second pool connects");
        let held = small.acquire().await.expect("a connection");
        let connected = SqlxBroker::new(small.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut batches = pin!(subscriber.batches(nonzero!(3_usize)));
            let batch = tokio::time::timeout(AT_ONCE, batches.next())
                .await
                .expect("the claim takes what the pool spares at once")
                .expect("the stream goes on")
                .expect("the claim takes a row");
            assert_eq!(batch.len(), 1, "the pool spares one connection");
            for delivery in batch {
                delivery.ack().await.expect("the row settles");
            }
        }
        drop((subscriber, held));
        connected.shutdown().await.expect("the broker shuts down");
        assert_eq!(db.count("plain_jobs").await, 2, "two rows wait for the next claim");
        small.close().await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_dropped_unsettled_closes_its_session() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let dropped = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(dropped.redelivery_count(), Some(1));
            drop(dropped);
        }
        drop(subscriber);
        // `shutdown` returns once the dropped delivery's session is closed.
        connected.shutdown().await.expect("the broker shuts down");
        assert_no_lock(&db.pool, &["plain_jobs-1"]).await;
        let again = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("a second broker connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&again)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let taken = tokio::time::timeout(AT_ONCE, deliveries.next())
                .await
                .expect("the row is claimable at once")
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(
                taken.redelivery_count(),
                Some(2),
                "the take of the dropped delivery counted its attempt"
            );
            taken.ack().await.expect("the row settles");
        }
        drop(subscriber);
        again.shutdown().await.expect("the broker shuts down");
        assert_eq!(db.count("plain_jobs").await, 0);
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_advisory_subscription_prepares_its_statements() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .route::<SendEmail>("routed")
            .connect()
            .await
            .expect("the broker connects");
        // Whole rows, ids for the service's own fetch, and the role columns a by-name
        // subscription reads: each take the dialect builds prepares.
        let rows = InboxQueue::<SendEmail>::new("emails")
            .subscribe(&connected)
            .await;
        assert!(rows.is_ok(), "{rows:?}");
        let ids = InboxQueue::<Fetched>::new("plain")
            .subscribe(&connected)
            .await;
        assert!(ids.is_ok(), "{ids:?}");
        let by_name = connected.subscribe("routed").await;
        assert!(by_name.is_ok(), "{by_name:?}");
        drop((rows, ids, by_name));
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    // Real concurrency: two brokers, each with its own claim loop, on a multi-threaded runtime.
    // Their candidate selects see the same rows; the lock, then the take that reads the row again
    // while it is still claimable, gives each row to one of them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_brokers_never_share_a_row() {
        let Some(db) = database().await else { return };
        let ids: BTreeSet<i64> = db.plain_ids(40).await;
        let claim = async move |pool: Pool<Db>| -> Vec<i64> {
            let connected = SqlxBroker::new(pool)
                .poll_interval(Duration::from_millis(10))
                .connect()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<Plain>::new("plain")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let mut taken = Vec::new();
            {
                let mut deliveries = pin!(subscriber.stream());
                while let Ok(Some(next)) =
                    tokio::time::timeout(Duration::from_millis(500), deliveries.next()).await
                {
                    let delivery = next.expect("a claim");
                    taken.push(payload_id(&delivery));
                    delivery.ack().await.expect("the acknowledgement");
                }
            }
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            taken
        };
        let (first, second) = tokio::join!(
            tokio::spawn(claim(db.pool.clone())),
            tokio::spawn(claim(db.pool.clone())),
        );
        let (first, second) = (first.expect("joins"), second.expect("joins"));
        let shared: Vec<_> = first.iter().filter(|id| second.contains(id)).collect();
        assert!(shared.is_empty(), "rows delivered twice: {shared:?}");
        let all: BTreeSet<i64> = first.into_iter().chain(second).collect();
        assert_eq!(all, ids, "every row was delivered once");
        let keys: Vec<String> = ids.iter().map(|id| format!("plain_jobs-{id}")).collect();
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        assert_no_lock(&db.pool, &keys).await;
        db.finish().await;
    }
}
