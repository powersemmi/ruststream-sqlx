//! The advisory lock form with a lock of the service's own: a table that lists `lock` and `unlock`
//! in `custom(..)` holds each delivery under the service's SQL. The dialect still selects the
//! candidates and takes each row; the lock the session holds while the handler works is the
//! service's, and its unlock releases it when the delivery settles. On SQLite the process then
//! keeps no registry of keys: the service's lock decides.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::pin::pin;

use futures::StreamExt;
use ruststream::testing::InProcess;
use ruststream::{ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::{Inbox, InboxQueue, Lock, SqlxBroker, Unlock};
use sqlx::FromRow;

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
