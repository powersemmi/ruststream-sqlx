//! The pool a suite hands its service: empty at first, and checked connection by connection once
//! the service stopped.

use std::time::Duration;

use sqlx::Pool;
use sqlx::pool::PoolOptions;

use crate::live;

/// How long a connection of the pool may take to lend or to answer: well below the guard.
const LEND: Duration = Duration::from_secs(5);

/// A pool on `db`'s database with no connection yet: the service opens every one it uses. It
/// hands a connection out without testing it first, so one that died is not replaced unseen.
pub(crate) fn fresh_pool<DB: sqlx::Database>(db: &live::Database<DB>) -> Pool<DB> {
    PoolOptions::<DB>::new()
        .max_connections(8)
        .acquire_timeout(LEND)
        .test_before_acquire(false)
        .connect_lazy_with(db.pool.connect_options().as_ref().clone())
}

/// Every connection the pool holds answers a query on the caller's runtime.
pub(crate) async fn every_connection_answers<DB: sqlx::Database>(pool: &Pool<DB>, after: &str)
where
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
{
    let size = pool.size();
    let mut lent = Vec::new();
    for _ in 0..size {
        let conn = tokio::time::timeout(LEND, pool.acquire())
            .await
            .unwrap_or_else(|_| panic!("{after}: the pool lends its connections"))
            .unwrap_or_else(|error| panic!("{after}: a connection: {error}"));
        lent.push(conn);
    }
    for (n, conn) in lent.iter_mut().enumerate() {
        tokio::time::timeout(LEND, sqlx::raw_sql("SELECT 1").execute(&mut **conn))
            .await
            .unwrap_or_else(|_| panic!("{after}: connection {n} of {size} hangs"))
            .unwrap_or_else(|error| panic!("{after}: connection {n} of {size} is dead: {error}"));
    }
}
