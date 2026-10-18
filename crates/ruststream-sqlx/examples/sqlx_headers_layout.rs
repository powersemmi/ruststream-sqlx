//! The headers layout: a headers struct describes the queue table and its headers, and the
//! message that flattens it is what the handler takes.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_headers_layout --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::SqlitePool;

use ruststream_sqlx::prelude::*;

// --8<-- [start:layout]
/// The queue table: the columns that run the queue, and the headers `tenant`, `trace` and
/// `order_id`.
#[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
#[inbox(table = "order_jobs")]
struct OrderHeaders {
    #[field(id, generated)]
    job_id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    tenant: String,
    trace: Option<String>,
    order_id: i64,
}

/// A job and its note, read from the queue table by the default fetch.
#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
struct OrderJob {
    #[field(headers)]
    #[sqlx(flatten)]
    headers: OrderHeaders,
    note: Option<String>,
}

// The header map is built from `OrderHeaders` on its first read, here by `Headers<Tenant>`.
#[subscriber(InboxQueue::<OrderJob>::new("packing"))]
async fn pack(job: &OrderJob, Headers(tenant): Headers<Tenant>) -> HandlerOutcome {
    tracing::info!(
        tenant = %tenant.tenant,
        order = job.headers.order_id,
        note = ?job.note,
        "packing"
    );
    HandlerOutcome::ack()
}
// --8<-- [end:layout]

/// The headers a handler reads, by name.
#[derive(Debug, Deserialize)]
struct Tenant {
    tenant: String,
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(pack);
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:shop.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS order_jobs (
    job_id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    tenant TEXT NOT NULL,
    trace TEXT,
    order_id INTEGER NOT NULL,
    note TEXT
)";

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::{TestApp, TestError};
    use ruststream_sqlx::SqlxBrokerError;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{OrderJob, SCHEMA, service};

    /// The queue table before the release that added `note`.
    const PREVIOUS: &str = "CREATE TABLE order_jobs (
        job_id INTEGER PRIMARY KEY,
        attempt INTEGER NOT NULL DEFAULT 1,
        locked_until TEXT,
        tenant TEXT NOT NULL,
        trace TEXT,
        order_id INTEGER NOT NULL
    )";

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool.
    async fn database(
        name: &str,
        schema: &'static str,
    ) -> Result<(SqlitePool, SqliteConnection), Box<dyn Error>> {
        let options: SqliteConnectOptions =
            format!("sqlite:file:{name}?mode=memory&cache=shared").parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(schema).execute(&pool).await?;
        Ok((pool, keeper))
    }

    // A row the service writes itself reaches the harness's books once a claim takes it, so the
    // test runs on the real clock and lets the claim happen.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_handler_takes_the_job_with_its_note() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_headers_layout_packed", SCHEMA).await?;
        sqlx::query("INSERT INTO order_jobs (tenant, order_id, note) VALUES (?, ?, ?)")
            .bind("acme")
            .bind(42_i64)
            .bind("fragile")
            .execute(&pool)
            .await?;
        let tb = TestApp::start_live(service(pool)).await?;
        tb.advance(Duration::from_millis(500)).await?;

        let lent = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("packing")
            .assert_called_once()
            .settled(HandlerOutcome::ack())
            .received_values::<OrderJob>();
        let read: Vec<(i64, Option<&str>)> = lent
            .iter()
            .map(|job| (job.headers.order_id, job.note.as_deref()))
            .collect();
        assert_eq!(read, [(42, Some("fragile"))]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_message_column_the_table_lacks_stops_startup() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_headers_layout_previous", PREVIOUS).await?;
        let failed: Option<TestError> = TestApp::start(service(pool)).await.err();

        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(sqlx) = err.downcast_ref::<SqlxBrokerError>() {
                assert!(
                    matches!(sqlx, SqlxBrokerError::Schema { table, .. } if table == "order_jobs"),
                    "{sqlx}"
                );
                return Ok(());
            }
            cause = err.source();
        }
        panic!("startup did not fail with a SqlxBrokerError: {failed:?}");
    }
}
