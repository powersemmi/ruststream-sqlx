//! Two claimers on one table never take one row, in either form, on every stand.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::collections::BTreeSet;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::{InboxQueue, SqlxBroker};

const ROWS: usize = 200;

/// The id a delivery's payload names: each row of the suite carries its own id as text.
trait PayloadId {
    fn payload_id(&self) -> i64;
}

impl<Delivery: IncomingMessage> PayloadId for Delivery {
    fn payload_id(&self) -> i64 {
        str::from_utf8(self.payload())
            .expect("the payload is text")
            .parse()
            .expect("the payload is an id")
    }
}

live::matrix! {
    // Real concurrency: two brokers, each with its own claim loop, on a multi-threaded runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_claimers_never_share_a_row() {
        let Some(db) = database().await else { return };
        let ids: BTreeSet<i64> = db.plain_ids(ROWS).await;
        let claim = async move |pool: sqlx::Pool<Db>| -> Vec<i64> {
            let connected = SqlxBroker::new(pool)
                .poll_interval(Duration::from_millis(10))
                .connect()
                .await
                .expect("connects");
            let mut subscriber = InboxQueue::<Plain>::new("plain")
                .subscribe(&connected)
                .await
                .expect("opens");
            let mut taken = Vec::new();
            {
                let mut deliveries = pin!(subscriber.stream());
                while let Ok(Some(next)) =
                    tokio::time::timeout(Duration::from_millis(500), deliveries.next()).await
                {
                    let delivery = next.expect("a claim");
                    taken.push(delivery.payload_id());
                    delivery.ack().await.expect("the ack");
                }
            }
            drop(subscriber);
            connected.shutdown().await.expect("stops");
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
        db.finish().await;
    }
}

/// What only SQLite runs: a claim of the service's own reads before it writes, so its transaction
/// takes the write lock first.
#[cfg(feature = "sqlite")]
mod on_sqlite {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::{Claim, Fetch, Inbox};
    use sqlx::{FromRow, Sqlite, SqliteConnection};

    use super::*;
    use crate::live::sqlite::database;

    /// A lease job whose claim and fetch are the service's own; the crate stamps each row the
    /// claim picked inside the claim's transaction.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs", custom(claim, fetch))]
    struct OwnClaim {
        #[field(id, generated)]
        id: i64,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(locked_until)]
        locked_until: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
    }

    impl Claim<Sqlite> for OwnClaim {
        async fn claim(
            conn: &mut SqliteConnection,
            _queue: &str,
            limit: i64,
        ) -> Result<Vec<i64>, sqlx::Error> {
            sqlx::query_scalar(
                "SELECT id FROM plain_jobs WHERE locked_until IS NULL ORDER BY id LIMIT ?",
            )
            .bind(limit)
            .fetch_all(conn)
            .await
        }
    }

    impl Fetch<Sqlite> for OwnClaim {
        async fn fetch(conn: &mut SqliteConnection, ids: &[i64]) -> Result<Vec<Self>, sqlx::Error> {
            // SQLite binds no list as one parameter: a select per id.
            let mut rows = Vec::with_capacity(ids.len());
            for id in ids {
                let row = sqlx::query_as(
                    "SELECT id, attempt, locked_until, payload FROM plain_jobs WHERE id = ?",
                )
                .bind(id)
                .fetch_one(&mut *conn)
                .await?;
                rows.push(row);
            }
            Ok(rows)
        }
    }

    // Two claims that each read, then write, in transactions opened without the write lock wait
    // for each other's table locks, and SQLite fails one of them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_claimers_of_the_services_own_never_share_a_row() {
        let Some(db) = database().await else { return };
        let ids: BTreeSet<i64> = db.plain_ids(ROWS).await;
        let claim = async move |pool: sqlx::Pool<Sqlite>| -> Vec<i64> {
            let connected = SqlxBroker::new(pool)
                .poll_interval(Duration::from_millis(10))
                .connect()
                .await
                .expect("connects");
            let mut subscriber = InboxQueue::<OwnClaim>::new("plain")
                .subscribe(&connected)
                .await
                .expect("opens");
            let mut taken = Vec::new();
            {
                let mut deliveries = pin!(subscriber.stream());
                while let Ok(Some(next)) =
                    tokio::time::timeout(Duration::from_millis(500), deliveries.next()).await
                {
                    let delivery = next.expect("a claim");
                    taken.push(delivery.payload_id());
                    delivery.ack().await.expect("the ack");
                }
            }
            drop(subscriber);
            connected.shutdown().await.expect("stops");
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
        let _ = |row: OwnClaim| (row.id, row.attempt, row.locked_until, row.payload);
        db.finish().await;
    }
}
