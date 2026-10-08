//! Queue tables described by hand beside the same tables derived: each scenario runs both forms
//! as an application through `TestApp::start_live` on an in-process SQLite and observes the same
//! description, the same events, the same delivery and the same settlement.

#![cfg(all(
    feature = "inbox",
    feature = "sqlite",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

#[path = "../live/mod.rs"]
mod live;

mod advisory;
mod flat_payload;
mod headers_layout;
mod row_mode;

use std::time::Duration;

use ruststream_sqlx::SqlxBroker;
use sqlx::{AssertSqlSafe, Sqlite, SqlitePool};

use live::Database;

/// How long a subscription waits after a claim that found its queue short.
const POLL: Duration = Duration::from_millis(20);

/// How long a test lets the claim loop run: every row it wrote settles in it.
const SETTLED: Duration = Duration::from_millis(400);

/// A fresh in-memory database with the scenario's own tables beside the shared schema.
///
/// # Panics
///
/// Panics when SQLite refuses the database or the scenario's tables.
async fn database(schema: &'static str) -> Database<Sqlite> {
    let db = live::sqlite::database()
        .await
        .expect("the SQLite stand runs in process");
    sqlx::raw_sql(AssertSqlSafe(schema))
        .execute(&db.pool)
        .await
        .expect("the scenario's tables apply");
    db
}

fn broker(pool: &SqlitePool) -> SqlxBroker<Sqlite> {
    SqlxBroker::new(pool.clone()).poll_interval(POLL)
}

/// The number of rows `sql`, a `SELECT count(*)`, counts.
async fn count(pool: &SqlitePool, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .expect("the table reads")
}
