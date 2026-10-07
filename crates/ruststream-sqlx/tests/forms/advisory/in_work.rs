//! What the database shows of a delivery in work: its session holds its row's lock while no
//! transaction stays open, and on MySQL and MariaDB a lock name longer than the server takes is
//! locked by its hash.

#![cfg(any(feature = "postgres", feature = "mysql"))]

use std::pin::pin;

use futures::StreamExt;
use ruststream::testing::InProcess;
use ruststream::{ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::{InboxQueue, SqlxBroker};

use super::POLL;

/// What Postgres shows of a delivery in work: its session's lock and no open transaction.
#[cfg(feature = "postgres")]
mod on_postgres {
    use super::*;
    use crate::live::postgres::{advisory_locks, database, idle_in_transaction};
    use crate::live::rows::advisory::Plain;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_in_work_holds_no_transaction() {
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
            let held = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(
                idle_in_transaction(&db.pool).await,
                0,
                "no transaction stays open while the handler works"
            );
            assert_eq!(
                advisory_locks(&db.pool).await,
                1,
                "the delivery's session holds its row's lock"
            );
            held.ack().await.expect("the row settles");
            assert_eq!(
                advisory_locks(&db.pool).await,
                0,
                "the settlement freed the key"
            );
        }
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }
}

/// What MySQL and MariaDB show of a delivery in work: its session holds the lock its key names, and
/// a lock name longer than the server takes is locked by its hash.
#[cfg(feature = "mysql")]
mod on_mysql {
    use ruststream_sqlx::Inbox;
    use sqlx::{FromRow, MySqlPool};

    use super::*;
    use crate::live::mysql::{lock_held, lock_name};
    use crate::live::rows::advisory::Plain;

    /// The start of every key of `LongKeyed`, 40 characters: the database's name, a dot, this and
    /// an id of the right length make a lock name of 64 characters, or 65.
    const LONG_PREFIX: &str = "a-key-long-enough-to-pass-64-characters-";

    /// A job of `plain_jobs` whose lock name is as long as MySQL takes, or longer.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(
        table = "plain_jobs",
        advisory_lock = "a-key-long-enough-to-pass-64-characters-{id}"
    )]
    struct LongKeyed {
        #[field(id, generated)]
        id: i64,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(payload)]
        payload: Vec<u8>,
    }

    /// The SHA-256 of `key` in hex, as the server computes it.
    async fn sha2_hex(pool: &MySqlPool, key: &str) -> String {
        sqlx::query_scalar("SELECT SHA2(?, 256)")
            .bind(key)
            .fetch_one(pool)
            .await
            .expect("the hash reads")
    }

    /// Whether a session holds a lock named `name` as it is written: `None` where the server takes
    /// no such name, as MySQL takes none longer than 64 characters.
    async fn held_as_written(pool: &MySqlPool, name: &str) -> Option<bool> {
        // MySQL has no booleans: `IS NOT NULL` answers an integer, which sqlx reads as a `bool`.
        let held = sqlx::query_scalar("SELECT IS_USED_LOCK(?) IS NOT NULL")
            .bind(name)
            .fetch_one(pool)
            .await;
        match held {
            Ok(held) => Some(held),
            Err(sqlx::Error::Database(refused)) if refused.code().as_deref() == Some("42000") => {
                None
            }
            Err(failed) => panic!("the lock reads: {failed}"),
        }
    }

    crate::live::mysql_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_delivery_in_work_holds_its_key() {
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
                let held = deliveries
                    .next()
                    .await
                    .expect("the stream goes on")
                    .expect("the claim takes the row");
                let name = lock_name(&db.pool, "plain_jobs-1").await;
                assert!(
                    lock_held(&db.pool, &name).await,
                    "the delivery's session holds its row's lock"
                );
                held.ack().await.expect("the row settles");
                assert!(!lock_held(&db.pool, &name).await, "the settlement freed the key");
            }
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            db.finish().await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_key_longer_than_64_characters_locks_by_its_hash() {
            let Some(db) = database().await else { return };
            // The ids whose lock names come to 64 characters and to 65.
            let named = lock_name(&db.pool, LONG_PREFIX).await;
            let digits = 64_usize
                .checked_sub(named.chars().count())
                .and_then(|digits| u32::try_from(digits).ok())
                .filter(|&digits| digits > 0)
                .expect("the name leaves room for an id");
            let (short_id, long_id) = (10_i64.pow(digits - 1), 10_i64.pow(digits));
            sqlx::query("INSERT INTO plain_jobs (id, payload) VALUES (?, 'x'), (?, 'y')")
                .bind(short_id)
                .bind(long_id)
                .execute(&db.pool)
                .await
                .expect("the rows write");
            let (exact, longer) = (format!("{named}{short_id}"), format!("{named}{long_id}"));
            assert_eq!((exact.chars().count(), longer.chars().count()), (64, 65));
            let (exact_hash, longer_hash) =
                (sha2_hex(&db.pool, &exact).await, sha2_hex(&db.pool, &longer).await);
            let connected = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<LongKeyed>::new("plain")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            {
                let mut deliveries = pin!(subscriber.stream());
                let mut held = Vec::new();
                for _ in 0..2 {
                    held.push(
                        deliveries
                            .next()
                            .await
                            .expect("the stream goes on")
                            .expect("the claim takes the row"),
                    );
                }
                // A name of 64 characters is the lock's own; a longer one is locked by its hash.
                assert!(lock_held(&db.pool, &exact).await, "the name of 64 is the lock's own");
                assert!(!lock_held(&db.pool, &exact_hash).await);
                assert!(lock_held(&db.pool, &longer_hash).await, "the longer name is hashed");
                assert_ne!(held_as_written(&db.pool, &longer).await, Some(true));
                for delivery in held {
                    delivery.ack().await.expect("the row settles");
                }
                for name in [&exact, &exact_hash, &longer_hash] {
                    assert!(!lock_held(&db.pool, name).await, "the settlement freed {name}");
                }
                assert_ne!(held_as_written(&db.pool, &longer).await, Some(true));
            }
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            assert_eq!(db.count("plain_jobs").await, 0, "both rows are done");
            db.finish().await;
            let _ = |row: LongKeyed| (row.id, row.attempt, row.payload);
        }
    }
}
