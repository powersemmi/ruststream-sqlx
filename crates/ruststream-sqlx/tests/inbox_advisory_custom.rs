//! Who holds the keys of the advisory lock form. A table that lists `lock` and `unlock` in
//! `custom(..)` holds each delivery under the service's SQL: the dialect still selects the
//! candidates and takes each row, the lock the session holds while the handler works is the
//! service's, and its unlock releases it when the delivery settles. On SQLite the process then
//! keeps no registry of keys: the service's lock decides. Without one, SQLite's keys in work live in
//! the process, each one under its database: two databases hold one key apart, and two brokers on
//! one database pass over each other's keys.
//!
//! Neither a service's lock nor the process's registry shows in the candidates a claim selects, so
//! a claim reads as many candidates past its limit as there are keys in work: a delivery in work at
//! the head of the claim order does not hold back the row behind it.

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
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::{Inbox, InboxQueue, Lock, SqlxBroker, Unlock};
use sqlx::FromRow;

/// The longest a test waits for a job its broker should claim at once.
const AT_ONCE: Duration = Duration::from_secs(2);

/// A poll interval no test waits out: a claim that ends empty would wait it before the next one.
const AN_HOUR: Duration = Duration::from_secs(3600);

/// A plain job whose lock and unlock are the service's own.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "plain_jobs", advisory_lock = "own-{id}", custom(lock, unlock))]
struct Own {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(payload)]
    payload: Vec<u8>,
}

/// Postgres's session lock on the key's 32-bit `hashtext`, which the built-in dialect never takes:
/// a lock the database shows under that hash is the service's.
#[cfg(feature = "postgres")]
mod on_postgres {
    use sqlx::{PgConnection, PgPool, Postgres};

    use super::*;
    use crate::live::postgres::database;

    impl Lock<Postgres> for Own {
        async fn lock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
            sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext($1))")
                .bind(key)
                .fetch_one(conn)
                .await
        }
    }

    impl Unlock<Postgres> for Own {
        async fn unlock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
            sqlx::query_scalar("SELECT pg_advisory_unlock(hashtext($1))")
                .bind(key)
                .fetch_one(conn)
                .await
        }
    }

    /// The advisory locks sessions of the test's database hold: all of them, and those on `key`
    /// as `hashtext` hashes it (a 64-bit lock whose low half is the hash).
    async fn advisory_locks(pool: &PgPool, key: &str) -> (i64, i64) {
        sqlx::query_as(
            "SELECT count(*), count(*) FILTER (WHERE objid = hashtext($1)::oid AND objsubid = 1) \
             FROM pg_locks WHERE locktype = 'advisory' AND database = \
             (SELECT oid FROM pg_database WHERE datname = current_database())",
        )
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("the locks read")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_service_lock_runs_its_own_sql() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Own>::new("own")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let held = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(held.payload(), b"x");
            assert_eq!(
                advisory_locks(&db.pool, "own-1").await,
                (1, 1),
                "the delivery's session holds the service's lock on its key, and no other"
            );
            held.ack().await.expect("the row settles");
            assert_eq!(
                advisory_locks(&db.pool, "own-1").await,
                (0, 0),
                "the service's unlock released the key"
            );
        }
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        assert_eq!(
            db.count("plain_jobs").await,
            0,
            "the acknowledgement deleted the row"
        );
        db.finish().await;
    }

    // The broker polls once an hour: a claim that passed over the key in work and found nothing
    // would wait that long for the next one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_service_lock_reaches_past_its_keys_in_work() {
        let Some(db) = database().await else { return };
        db.plain(&[b"first".as_slice(), b"second".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(AN_HOUR)
            .connect()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Own>::new("own")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let first = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the first row");
            assert_eq!(first.payload(), b"first");
            let second = tokio::time::timeout(AT_ONCE, deliveries.next())
                .await
                .expect("the next claim reaches past the key in work to the row behind it")
                .expect("the stream goes on")
                .expect("the claim takes the second row");
            assert_eq!(second.payload(), b"second");
            assert_eq!(
                advisory_locks(&db.pool, "own-2").await,
                (2, 1),
                "each delivery's session holds the service's lock on its own key"
            );
            second.ack().await.expect("the row settles");
            first.ack().await.expect("the row settles");
        }
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        assert_eq!(
            db.count("plain_jobs").await,
            0,
            "both rows were acknowledged"
        );
        db.finish().await;
    }
}

/// SQLite takes no advisory locks, and the built-in dialect leaves the keys in work to the process;
/// a lock of the service's own replaces that registry. Here it is a table of the keys in work.
#[cfg(feature = "sqlite")]
mod on_sqlite {
    use sqlx::{Sqlite, SqliteConnection, SqlitePool};

    use super::*;
    use crate::live::sqlite::database;

    impl Lock<Sqlite> for Own {
        async fn lock(conn: &mut SqliteConnection, key: &str) -> Result<bool, sqlx::Error> {
            let taken = sqlx::query("INSERT OR IGNORE INTO held_keys (key) VALUES (?)")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(taken.rows_affected() == 1)
        }
    }

    impl Unlock<Sqlite> for Own {
        async fn unlock(conn: &mut SqliteConnection, key: &str) -> Result<bool, sqlx::Error> {
            let released = sqlx::query("DELETE FROM held_keys WHERE key = ?")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(released.rows_affected() == 1)
        }
    }

    /// The keys the service's lock holds, in key order.
    async fn held_keys(pool: &SqlitePool) -> Vec<String> {
        sqlx::query_scalar("SELECT key FROM held_keys ORDER BY key")
            .fetch_all(pool)
            .await
            .expect("the keys read")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_service_lock_replaces_the_process_registry() {
        let Some(db) = database().await else { return };
        sqlx::query("CREATE TABLE held_keys (key TEXT PRIMARY KEY)")
            .execute(&db.pool)
            .await
            .expect("the table of held keys creates");
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Own>::new("own")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let held = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(
                held_keys(&db.pool).await,
                ["own-1"],
                "the service's lock holds the delivery's key"
            );
            held.ack().await.expect("the row settles");
            assert_eq!(
                held_keys(&db.pool).await,
                Vec::<String>::new(),
                "the service's unlock released the key"
            );
        }
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        assert_eq!(
            db.count("plain_jobs").await,
            0,
            "the acknowledgement deleted the row"
        );
        db.finish().await;
    }
}

/// SQLite's keys in work, which the process keeps, each under the database it belongs to.
#[cfg(feature = "sqlite")]
mod sqlite_keys {
    use ruststream_sqlx::{ConnectedSqlxBroker, InboxDelivery, InboxSubscriber};
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Sqlite, SqlitePool};

    use super::*;
    use crate::live::sqlite::database;

    /// A plain job whose key is the same text in every database for its first row.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs", advisory_lock = "same-{id}")]
    struct Same {
        #[field(id, generated)]
        id: i64,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(payload)]
        payload: Vec<u8>,
    }

    /// How often the brokers of most tests poll the jobs.
    const POLL: Duration = Duration::from_millis(20);

    /// A broker on `pool` that polls every `interval`, and its subscription to the jobs.
    async fn subscribed(
        pool: &SqlitePool,
        interval: Duration,
    ) -> (ConnectedSqlxBroker<Sqlite>, InboxSubscriber<Sqlite, Same>) {
        let connected = SqlxBroker::new(pool.clone())
            .poll_interval(interval)
            .connect()
            .await
            .expect("the broker connects");
        let subscriber = InboxQueue::<Same>::new("same")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        (connected, subscriber)
    }

    /// The next delivery of `subscriber`, if it comes within `wait`.
    async fn next_within(
        subscriber: &mut InboxSubscriber<Sqlite, Same>,
        wait: Duration,
    ) -> Option<InboxDelivery<Sqlite, Same>> {
        let mut deliveries = pin!(subscriber.stream());
        tokio::time::timeout(wait, deliveries.next())
            .await
            .ok()
            .map(|next| {
                next.expect("the stream goes on")
                    .expect("the claim takes the job")
            })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_databases_hold_one_key_apart() {
        let (Some(first), Some(second)) = (database().await, database().await) else {
            return;
        };
        first.plain(&[b"first".as_slice()]).await;
        second.plain(&[b"second".as_slice()]).await;
        let (on_first, mut first_jobs) = subscribed(&first.pool, POLL).await;
        let (on_second, mut second_jobs) = subscribed(&second.pool, POLL).await;
        let held = next_within(&mut first_jobs, AT_ONCE)
            .await
            .expect("the first database's job is claimed");
        assert_eq!(held.payload(), b"first");
        let other = next_within(&mut second_jobs, AT_ONCE)
            .await
            .expect("the second database's job is claimed while the first holds the same key");
        assert_eq!(other.payload(), b"second");
        other.ack().await.expect("the job settles");
        held.ack().await.expect("the job settles");
        drop((first_jobs, second_jobs));
        on_first.shutdown().await.expect("the broker shuts down");
        on_second.shutdown().await.expect("the broker shuts down");
        first.finish().await;
        second.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_brokers_on_one_database_pass_over_each_others_key() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        // A second pool on the same in-memory database, as a second service would open it.
        let options = (*db.pool.connect_options()).clone();
        let other_pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .expect("a second pool opens the database");
        let (on_one, mut one_jobs) = subscribed(&db.pool, POLL).await;
        let (on_other, mut other_jobs) = subscribed(&other_pool, POLL).await;
        let held = next_within(&mut one_jobs, AT_ONCE)
            .await
            .expect("the job is claimed");
        assert!(
            next_within(&mut other_jobs, Duration::from_millis(300))
                .await
                .is_none(),
            "the second broker passes over the key the first holds"
        );
        held.nack(true).await.expect("the job returns");
        let again = next_within(&mut other_jobs, AT_ONCE)
            .await
            .expect("the second broker claims the job once its key is free");
        assert_eq!(again.payload(), b"x");
        again.ack().await.expect("the job settles");
        drop((one_jobs, other_jobs));
        on_one.shutdown().await.expect("the broker shuts down");
        on_other.shutdown().await.expect("the broker shuts down");
        other_pool.close().await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_claim_reaches_past_its_keys_in_work() {
        let Some(db) = database().await else { return };
        db.plain(&[b"first".as_slice(), b"second".as_slice()]).await;
        // An hour between polls: a claim that passed over the key in work and found nothing would
        // wait that long for the next one.
        let (connected, mut jobs) = subscribed(&db.pool, AN_HOUR).await;
        {
            let mut deliveries = pin!(jobs.stream());
            let first = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the first job");
            assert_eq!(first.payload(), b"first");
            let second = tokio::time::timeout(AT_ONCE, deliveries.next())
                .await
                .expect("the next claim reaches past the key in work to the job behind it")
                .expect("the stream goes on")
                .expect("the claim takes the second job");
            assert_eq!(second.payload(), b"second");
            second.ack().await.expect("the job settles");
            first.ack().await.expect("the job settles");
        }
        drop(jobs);
        connected.shutdown().await.expect("the broker shuts down");
        assert_eq!(
            db.count("plain_jobs").await,
            0,
            "both jobs were acknowledged"
        );
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_claim_reaches_past_the_keys_another_broker_holds() {
        let Some(db) = database().await else { return };
        db.plain(&[b"first".as_slice(), b"second".as_slice()]).await;
        // A second pool on the same in-memory database, as a second service would open it.
        let options = (*db.pool.connect_options()).clone();
        let other_pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .expect("a second pool opens the database");
        let (on_one, mut one_jobs) = subscribed(&db.pool, POLL).await;
        let (on_other, mut other_jobs) = subscribed(&other_pool, AN_HOUR).await;
        let held = next_within(&mut one_jobs, AT_ONCE)
            .await
            .expect("the first broker claims the first job");
        assert_eq!(held.payload(), b"first");
        let behind = next_within(&mut other_jobs, AT_ONCE)
            .await
            .expect("the second broker reaches past the key the first holds to the job behind it");
        assert_eq!(behind.payload(), b"second");
        behind.ack().await.expect("the job settles");
        held.ack().await.expect("the job settles");
        drop((one_jobs, other_jobs));
        on_one.shutdown().await.expect("the broker shuts down");
        on_other.shutdown().await.expect("the broker shuts down");
        other_pool.close().await;
        db.finish().await;
    }
}
