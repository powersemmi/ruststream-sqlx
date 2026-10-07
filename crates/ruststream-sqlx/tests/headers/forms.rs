//! A message assembled from a headers struct holds its row in the headers struct's form, as a flat
//! table's delivery does: the lease form extends the lease of a long handler's row, and the
//! advisory lock form frees the key once the delivery settles.

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_sqlx::prelude::*;
use sqlx::{Database, Pool};

use super::{POLL, SETTLED};
use crate::live;

/// A stand's database, and where it keeps the advisory locks the broker's sessions hold.
trait DatabaseLocks: Database {
    /// How many of the locks the broker takes on `keys` the database holds now; `None` where it
    /// keeps none, as SQLite does, whose keys in work live in the process.
    fn locks_held(pool: &Pool<Self>, keys: &[&str]) -> impl Future<Output = Option<i64>> + Send;
}

#[cfg(feature = "postgres")]
impl DatabaseLocks for sqlx::Postgres {
    async fn locks_held(pool: &Pool<Self>, _: &[&str]) -> Option<i64> {
        Some(live::postgres::advisory_locks(pool).await)
    }
}

#[cfg(feature = "mysql")]
impl DatabaseLocks for sqlx::MySql {
    async fn locks_held(pool: &Pool<Self>, keys: &[&str]) -> Option<i64> {
        let mut held = 0;
        for key in keys {
            let name = live::mysql::lock_name(pool, key).await;
            if live::mysql::lock_held(pool, &name).await {
                held += 1;
            }
        }
        Some(held)
    }
}

#[cfg(feature = "sqlite")]
impl DatabaseLocks for sqlx::Sqlite {
    fn locks_held(_: &Pool<Self>, _: &[&str]) -> impl Future<Output = Option<i64>> + Send {
        ready(None)
    }
}

live::lease_stands! {
    #[subscriber(InboxQueue::<OrderJob>::new("orders").lease(Duration::from_secs(1)))]
    async fn longer_than_the_lease(_job: &OrderJob) -> HandlerOutcome {
        // The handler's own work outlasts two leases; the keeper extends the row meanwhile.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lease_extension_keeps_a_long_handlers_row() {
        let Some(db) = database().await else { return };
        db.mail(&[OrderJob::queued("orders", "acme", None, 7).headers]).await;
        let broker = SqlxBroker::new(db.pool.clone()).poll_interval(Duration::from_millis(50));
        let app = RustStream::new(AppInfo::new("shop", "0.0.0")).with_broker(broker, |b| {
            // A second worker claims whatever is claimable: it would take the row if the lease
            // lapsed.
            b.include(longer_than_the_lease.workers(nonzero!(2)));
        });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(Duration::from_secs(3))
            .await
            .expect("the handler finishes");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(
            db.count("headed_jobs").await,
            0,
            "the long handler's acknowledgement took effect"
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}

live::advisory_stands! {
    #[subscriber(InboxQueue::<OrderJob>::new("orders"))]
    async fn acked(_job: &OrderJob) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_acks_and_frees_its_key() {
        let Some(db) = database().await else { return };
        db.mail(&[OrderJob::queued("orders", "acme", None, 7).headers]).await;
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(SqlxBroker::new(db.pool.clone()).poll_interval(POLL), |b| {
                b.include(acked);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        // The row was written before the app started, so the claim loop finds it on its own.
        tb.advance(SETTLED).await.expect("the delivery settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("headed_jobs").await, 0, "the job is done");
        // The key the headers struct's template names, over its `job_id`.
        if let Some(held) = <Db as DatabaseLocks>::locks_held(&db.pool, &["order-1"]).await {
            assert_eq!(held, 0, "the database holds advisory locks of the broker");
        }
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
