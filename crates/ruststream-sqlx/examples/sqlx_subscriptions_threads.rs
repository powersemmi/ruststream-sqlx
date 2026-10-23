//! A virus scan on dedicated threads on SQLite: each thread queries through a pool of its own,
//! and the broker holds every connection the service opens to a limit.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_subscriptions_threads --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:on_threads]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqlitePool};

/// One row of `scan_jobs`, claimed under a lease.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "scan_jobs")]
struct ScanJob {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// A scan computes for a while; the pool is the thread's own.
#[subscriber(InboxQueue::<ScanJob>::new("scans"))]
async fn scan(upload: &Upload, Ctx(pool): Ctx<keys::Pool<Sqlite>>) -> HandlerOutcome {
    let verdict = if looks_clean(upload) {
        "clean"
    } else {
        "infected"
    };
    let noted = sqlx::query("INSERT INTO scan_results (path, verdict) VALUES (?, ?)")
        .bind(&upload.path)
        .bind(verdict)
        .execute(&pool)
        .await;
    if noted.is_ok() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

fn service(pool: SqlitePool) -> RustStream {
    // The service's pool and the threads' 4 x 1 connections, within a limit of 12.
    let broker = SqlxBroker::new(pool)
        .route::<ScanJob>("scans")
        .connection_limit(nonzero!(12u32));
    RustStream::new(AppInfo::new("scanner", "0.1.0")).with_broker(broker, |b| {
        b.include(scan.on_threads(InboxThreads::new(nonzero!(4))));
    })
}
// --8<-- [end:on_threads]

/// A stand-in for the scanning engine, the work a dedicated thread is for: it flags the EICAR
/// test file.
fn looks_clean(upload: &Upload) -> bool {
    !upload.path.contains("eicar")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect("sqlite:scanner.db?mode=rwc")
        .await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS scan_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS scan_results (
    path TEXT NOT NULL,
    verdict TEXT NOT NULL
)";

impl Publish<Sqlite> for ScanJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO scan_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "scans")]
struct Upload {
    path: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Upload, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool, of `size` connections.
    async fn database(
        name: &str,
        size: u32,
    ) -> Result<(SqlitePool, SqliteConnection), Box<dyn Error>> {
        let options: SqliteConnectOptions =
            format!("sqlite:file:{name}?mode=memory&cache=shared").parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .max_connections(size)
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_scan_on_a_thread_writes_its_verdict() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_subscriptions_threads", 8).await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        let upload = Upload {
            path: "inbox/eicar.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&upload)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("scans")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let verdict: String = sqlx::query_scalar("SELECT verdict FROM scan_results WHERE path = ?")
            .bind(&upload.path)
            .fetch_one(&pool)
            .await?;
        assert_eq!(verdict, "infected");
        Ok(())
    }

    // Ten connections of the pool and four of the threads pass the limit of twelve.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pool_too_large_for_the_limit_stops_the_service() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_subscriptions_threads_limit", 10).await?;
        let Err(error) = TestApp::start(service(pool)).await else {
            panic!("the service started past its connection limit");
        };
        assert!(
            error.to_string().contains("connection limit of 12"),
            "the error names the limit: {error}"
        );
        Ok(())
    }
}
