//! The advisory lock form on a live database: how a delivery settles and frees its key, the
//! sessions the deliveries take, what a delivery in work holds, which rows a key locks, the form's
//! events on their own, a lock of the service's own, and what `shutdown` does to the locks in work.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod events;
mod in_work;
mod keys;
mod own_lock;
mod sessions;
mod settle;
mod shutdown;

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::{IncomingMessage, Outgoing};
use serde::{Deserialize, Serialize};
use sqlx::{Database, Pool};

use crate::live;

const POLL: Duration = Duration::from_millis(20);

/// The longest a test waits for a row its broker should claim at once.
const AT_ONCE: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Email {
    to: String,
}

fn email() -> Email {
    Email {
        to: "a@example.com".to_owned(),
    }
}

/// A stand's database, and where it keeps the advisory locks the broker's sessions hold.
trait DatabaseLocks: Database {
    /// How many locks the database `pool` reaches holds now, of those the broker takes on `keys`;
    /// `None` where it keeps none, as SQLite does, whose keys in work live in the process.
    ///
    /// Postgres lists every advisory lock of a database, so it counts them all. MySQL and MariaDB
    /// list none, and answer for the lock a key names.
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

/// Asserts the database `pool` reaches holds no lock the broker takes on `keys`, where it keeps
/// them.
async fn assert_no_lock<DB: DatabaseLocks>(pool: &Pool<DB>, keys: &[&str]) {
    if let Some(held) = DB::locks_held(pool, keys).await {
        assert_eq!(held, 0, "the database holds advisory locks of the broker");
    }
}

/// The id a delivery's payload names: each row of `plain_ids` carries its own id as text.
fn payload_id(delivery: &impl IncomingMessage) -> i64 {
    str::from_utf8(delivery.payload())
        .expect("the payload is text")
        .parse()
        .expect("the payload is an id")
}
