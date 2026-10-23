//! Exports written into a SQLite table by another system: no publish wakes the subscription, so
//! it finds them by claiming at its poll interval.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_subscriptions_poll --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// --8<-- [start:poll_interval]
use std::time::Duration;

use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `export_jobs`, which the warehouse's nightly job inserts with its own SQL.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "export_jobs")]
struct Export {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The exports come from outside the service, so this queue claims five times a second.
#[subscriber(InboxQueue::<Export>::new("exports").poll_interval(Duration::from_millis(200)))]
async fn upload(file: &ExportFile) -> HandlerOutcome {
    tracing::info!(path = %file.path, "uploading an export");
    HandlerOutcome::ack()
}

fn service(pool: SqlitePool) -> RustStream {
    // Every other subscription of the broker waits five seconds after a claim that came back short.
    let broker = SqlxBroker::new(pool).poll_interval(Duration::from_secs(5));
    RustStream::new(AppInfo::new("exports", "0.1.0")).with_broker(broker, |b| {
        b.include(upload);
    })
}
// --8<-- [end:poll_interval]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:exports.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS export_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct ExportFile {
    path: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{ExportFile, SCHEMA, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool.
    async fn database(name: &str) -> Result<(SqlitePool, SqliteConnection), Box<dyn Error>> {
        let options: SqliteConnectOptions =
            format!("sqlite:file:{name}?mode=memory&cache=shared").parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((pool, keeper))
    }

    // A row written with plain SQL reaches the subscription only through a claim at its poll
    // interval, so the test runs on the real clock and lets that much time pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_written_by_another_system_is_claimed_at_the_poll_interval()
    -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_subscriptions_poll").await?;
        let tb = TestApp::start_live(service(pool.clone())).await?;
        // The subscription's first claim finds the table empty; then it waits its interval.
        tb.advance(Duration::from_millis(300)).await?;
        let file = ExportFile {
            path: "exports/2026-10-18.csv".to_owned(),
        };
        sqlx::query("INSERT INTO export_jobs (payload) VALUES (?)")
            .bind(serde_json::to_vec(&file)?)
            .execute(&pool)
            .await?;

        tb.advance(Duration::from_secs(1)).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("exports")
            .assert_called_once()
            .with(&file)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
