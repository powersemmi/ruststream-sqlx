//! What a stand's sessions still hold on a suite's table once the service stopped: row locks of
//! open transactions and advisory locks.

use std::future::{Future, ready};

#[cfg(feature = "mysql")]
use sqlx::AssertSqlSafe;
use sqlx::{Database, Pool};

#[cfg(feature = "mysql")]
use crate::live;

/// A stand's database, and how it lists the locks its sessions hold.
pub(crate) trait HeldLocks: Database {
    /// The locks the database `pool` reaches holds on `table` beside the caller's own session:
    /// the rows open transactions lock, and advisory locks, the latter named `{table}-{id}` for
    /// each of `ids` where the database does not list them.
    fn held_locks(pool: &Pool<Self>, table: &str, ids: &[i64]) -> impl Future<Output = i64> + Send;
}

#[cfg(feature = "postgres")]
impl HeldLocks for sqlx::Postgres {
    async fn held_locks(pool: &Pool<Self>, table: &str, _: &[i64]) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE pid <> pg_backend_pid() \
             AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
             AND (locktype = 'advisory' OR (locktype = 'relation' AND relation = $1::regclass))",
        )
        .bind(table)
        .fetch_one(pool)
        .await
        .expect("the locks read")
    }
}

#[cfg(feature = "mysql")]
impl HeldLocks for sqlx::MySql {
    async fn held_locks(pool: &Pool<Self>, table: &str, ids: &[i64]) -> i64 {
        // The server lists its transactions from a cache, so the rows another session locks are
        // counted instead: those a locking read skips.
        let mut tx = pool.begin().await.expect("a transaction opens");
        let all: i64 = sqlx::query_scalar(AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(&mut *tx)
            .await
            .expect("the rows count");
        let free: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM (SELECT id FROM {table} FOR UPDATE SKIP LOCKED) AS free"
        )))
        .fetch_one(&mut *tx)
        .await
        .expect("the free rows count");
        tx.rollback().await.expect("the transaction rolls back");
        let mut held = all - free;
        for id in ids {
            let name = live::mysql::lock_name(pool, &format!("{table}-{id}")).await;
            if live::mysql::lock_held(pool, &name).await {
                held += 1;
            }
        }
        held
    }
}

#[cfg(feature = "sqlite")]
impl HeldLocks for sqlx::Sqlite {
    // SQLite locks the whole file per transaction and keeps a key in work in the process: a
    // stopped service holds nothing a session could list.
    fn held_locks(_: &Pool<Self>, _: &str, _: &[i64]) -> impl Future<Output = i64> + Send {
        ready(0)
    }
}
