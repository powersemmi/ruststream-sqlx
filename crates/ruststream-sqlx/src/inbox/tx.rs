//! The transactions the inbox opens on connections of the service's pool.

use std::fmt;
use std::ops::{Deref, DerefMut};

use sqlx::pool::PoolConnection;
use sqlx::{Database, Error, Pool, SqlStr};
use sqlx_core::transaction::TransactionManager;

use crate::inbox::form::advisory::session::Closing;

/// A transaction on a connection of the pool: a claim's, a batch's, or a settlement's of its own.
///
/// It ends with [`commit`](Self::commit) or [`rollback`](Self::rollback). Dropped while open, it
/// closes its connection, and the server rolls the transaction back; a claim's transaction drains
/// instead, so `shutdown` waits for the server to end it (see [`begin_claim`](Self::begin_claim)).
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
    /// The connection, until the transaction drops.
    conn: Option<PoolConnection<DB>>,
    /// Whether the server may hold the transaction open: from the begin statement until a commit
    /// or a rollback finished.
    open: bool,
    /// The closes of the broker whose claim runs in the transaction: dropped open, the claim was
    /// cut short, and its connection drains there instead of closing at once.
    claim: Option<&'static Closing>,
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
        Self::open(pool.acquire().await?, statement).await
    }

    /// Begins a transaction on `conn`, a connection of the pool the caller took, with
    /// `statement` in place of `BEGIN` where given.
    pub(crate) async fn open(
        conn: PoolConnection<DB>,
        statement: Option<&'static str>,
    ) -> Result<Self, Error> {
        // Open before the statement leaves: a begin dropped midway may have started the
        // transaction already.
        let mut tx = Self {
            conn: Some(conn),
            open: true,
            claim: None,
        };
        // Why sqlx's transaction manager: it counts the transaction on the connection, so the
        // service's own SQL inside it nests a savepoint where it begins one.
        DB::TransactionManager::begin(&mut *tx, statement.map(SqlStr::from_static)).await?;
        Ok(tx)
    }

    /// Opens a claim's transaction on a connection of `pool`, as [`begin`](Self::begin) does,
    /// until [`claimed`](Self::claimed). Dropped open before that, as a subscription's stream
    /// drops its claim in flight at shutdown, it drains through `closing`: the server ends the
    /// statement in flight and rolls the transaction back before `shutdown` returns.
    ///
    /// # Errors
    ///
    /// As [`begin`](Self::begin).
    pub(crate) async fn begin_claim(
        pool: &Pool<DB>,
        statement: Option<&'static str>,
        closing: &'static Closing,
    ) -> Result<Self, Error> {
        let conn = pool.acquire().await?;
        let mut tx = Self {
            conn: Some(conn),
            open: true,
            claim: Some(closing),
        };
        DB::TransactionManager::begin(&mut *tx, statement.map(SqlStr::from_static)).await?;
        Ok(tx)
    }

    /// The claim ended: the transaction goes on as one a delivery holds, and dropped open it
    /// closes its connection.
    pub(crate) fn claimed(&mut self) {
        self.claim = None;
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// The database's error; the connection then closes when the transaction drops.
    pub(crate) async fn commit(mut self) -> Result<(), Error> {
        DB::TransactionManager::commit(&mut *self).await?;
        self.open = false;
        Ok(())
    }

    /// Rolls the transaction back.
    ///
    /// # Errors
    ///
    /// The database's error; the connection then closes when the transaction drops.
    pub(crate) async fn rollback(mut self) -> Result<(), Error> {
        DB::TransactionManager::rollback(&mut *self).await?;
        self.open = false;
        Ok(())
    }
}

impl<DB: Database> Deref for PoolTx<DB> {
    type Target = DB::Connection;

    fn deref(&self) -> &Self::Target {
        self.conn
            .as_ref()
            .expect("a transaction holds its connection until it drops")
    }
}

impl<DB: Database> DerefMut for PoolTx<DB> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
            .as_mut()
            .expect("a transaction holds its connection until it drops")
    }
}

impl<DB: Database> Drop for PoolTx<DB> {
    fn drop(&mut self) {
        if !self.open {
            return;
        }
        let Some(mut conn) = self.conn.take() else {
            return;
        };
        match self.claim {
            Some(closing) => closing.drain(conn, true),
            None => conn.close_on_drop(),
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
