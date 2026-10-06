//! The transactions the inbox opens on connections of the service's pool.

use std::fmt;
use std::ops::{Deref, DerefMut};

use sqlx::pool::PoolConnection;
use sqlx::{Database, Error, Pool, SqlStr};
use sqlx_core::transaction::TransactionManager;

/// A transaction on a connection of the pool: a claim's, a batch's, or a settlement's of its own.
///
/// It ends with [`commit`](Self::commit) or [`rollback`](Self::rollback). Dropped while open, it
/// closes its connection, and the server rolls the transaction back.
///
/// Why not sqlx's `Transaction`: dropped while open, that one queues a rollback and returns its
/// connection to the pool. A transaction drops while open when the future that ran its statement
/// drops midway, as a subscription's stream does at shutdown; and when that statement was still
/// being prepared, sqlx-mysql reads the queued rollback's answer as part of the prepare's, waits
/// for a row that never comes, and the connection never returns: `Pool::close` then never
/// finishes. Closing the connection leaves nothing to read back.
// FIXME(sqlx-mysql 0.9.0): a `sqlx::Transaction` dropped open queues a rollback, and after a
// prepare whose future dropped midway that rollback reads the prepare's reply as its own and
// hangs `Pool::close()`. Once sqlx-mysql reads such a reply before it runs the rollback, this type
// can go: claims and settlements open `sqlx::Transaction`s with `Pool::begin_with`, and the direct
// `sqlx-core` dependency, kept for `TransactionManager`, goes with it.
pub(crate) struct PoolTx<DB: Database> {
    conn: PoolConnection<DB>,
    /// Whether the server may hold the transaction open: from the begin statement until a commit
    /// or a rollback finished.
    open: bool,
}

impl<DB: Database> PoolTx<DB> {
    /// Opens a transaction on a connection of `pool`, with `statement` in place of `BEGIN` where
    /// one is given.
    ///
    /// # Errors
    ///
    /// The pool's error, or the database's for the begin statement.
    pub(crate) async fn begin(
        pool: &Pool<DB>,
        statement: Option<&'static str>,
    ) -> Result<Self, Error> {
        let conn = pool.acquire().await?;
        // Open before the statement leaves: a begin dropped midway may have started the
        // transaction already.
        let mut tx = Self { conn, open: true };
        // Why sqlx's transaction manager: it counts the transaction on the connection, so the
        // service's own SQL inside it nests a savepoint where it begins one.
        DB::TransactionManager::begin(&mut tx.conn, statement.map(SqlStr::from_static)).await?;
        Ok(tx)
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// The database's error; the connection then closes when the transaction drops.
    pub(crate) async fn commit(mut self) -> Result<(), Error> {
        DB::TransactionManager::commit(&mut self.conn).await?;
        self.open = false;
        Ok(())
    }

    /// Rolls the transaction back.
    ///
    /// # Errors
    ///
    /// The database's error; the connection then closes when the transaction drops.
    pub(crate) async fn rollback(mut self) -> Result<(), Error> {
        DB::TransactionManager::rollback(&mut self.conn).await?;
        self.open = false;
        Ok(())
    }
}

impl<DB: Database> Deref for PoolTx<DB> {
    type Target = DB::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<DB: Database> DerefMut for PoolTx<DB> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl<DB: Database> Drop for PoolTx<DB> {
    fn drop(&mut self) {
        if self.open {
            self.conn.close_on_drop();
        }
    }
}

impl<DB: Database> fmt::Debug for PoolTx<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolTx")
            .field("open", &self.open)
            .finish_non_exhaustive()
    }
}
