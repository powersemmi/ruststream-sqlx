//! A claim in flight when its stream drops, as the runtime drops a subscription's stream at
//! shutdown: `shutdown` returns once the server ended the claim's statement, in every form, so the
//! stopped broker holds no lock on the table.

#![cfg(feature = "postgres")]

use std::pin::pin;
use std::time::Duration;

use futures::{StreamExt, poll};
use ruststream::{Broker, ConnectedBroker, Subscriber, SubscriptionSource};
use ruststream_sqlx::{InboxQueue, SqlxBroker};
use sqlx::PgPool;

use crate::live::postgres::database;

/// The longest the suite waits for the stand: a hang becomes a failure.
const GUARD: Duration = Duration::from_secs(30);

/// The sessions of the database `pool` reaches, beside the caller's own, that wait for a lock.
async fn waiting(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
         AND pid <> pg_backend_pid() AND wait_event_type = 'Lock'",
    )
    .fetch_one(pool)
    .await
    .expect("the sessions read")
}

/// The locks the sessions of the database `pool` reaches hold or wait for on `plain_jobs`, beside
/// the caller's own.
async fn table_locks(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE pid <> pg_backend_pid() \
         AND relation = 'plain_jobs'::regclass",
    )
    .fetch_one(pool)
    .await
    .expect("the locks read")
}

macro_rules! forms {
    ($($items:item)*) => {
        mod row_lock {
            use super::*;
            use crate::live::rows::row_lock::*;
            $($items)*
        }

        mod lease {
            use super::*;
            use crate::live::rows::lease::*;
            $($items)*
        }

        mod advisory {
            use super::*;
            use crate::live::rows::advisory::*;
            $($items)*
        }
    };
}

forms! {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_waits_for_the_claim_it_cut_short() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(Duration::from_millis(20))
            .connect()
            .await
            .expect("connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("opens");
        // A transaction of the test's own holds the table, so the claim waits on the server.
        let mut holder = db.pool.begin().await.expect("a transaction");
        sqlx::raw_sql("LOCK TABLE plain_jobs IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *holder)
            .await
            .expect("the table locks");
        {
            let mut deliveries = pin!(subscriber.stream());
            let blocked = async {
                while waiting(&db.pool).await == 0 {
                    tokio::task::yield_now().await;
                }
            };
            tokio::select! {
                _ = deliveries.next() => panic!("the claim went past the table's lock"),
                () = blocked => {}
            }
            // The stream drops with its claim waiting on the server.
        }
        drop(subscriber);
        let mut shutdown = pin!(connected.shutdown());
        assert!(
            poll!(shutdown.as_mut()).is_pending(),
            "shutdown returned while the claim it cut short waits on the server"
        );
        holder.rollback().await.expect("the table unlocks");
        tokio::time::timeout(GUARD, shutdown)
            .await
            .expect("shutdown ends once the claim ended")
            .expect("stops");
        assert_eq!(table_locks(&db.pool).await, 0, "the stopped broker holds no lock");
        db.finish().await;
    }
}
