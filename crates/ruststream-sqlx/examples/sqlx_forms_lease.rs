//! Reports that take minutes to render, claimed under a lease on SQLite: the claim commits at
//! once, the handler holds no connection, and the subscription extends the lease while it runs.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_forms_lease --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:lease]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `report_jobs`: `locked_until` makes the table a lease table.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "report_jobs")]
struct RenderReport {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// A report takes minutes to render. The subscription extends the minute's lease every thirty
/// seconds while the handler runs; after a crash the row waits out its minute, then returns.
#[subscriber(InboxQueue::<RenderReport>::new("reports").lease(Duration::from_mins(1)))]
async fn render(report: &Report) -> HandlerOutcome {
    draw(report).await;
    tracing::info!(report.id, "report rendered");
    HandlerOutcome::ack()
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<RenderReport>("reports"),
        |b| {
            b.include(render);
        },
    )
}
// --8<-- [end:lease]

/// A stand-in for the renderer: ten seconds a page.
async fn draw(report: &Report) {
    tokio::time::sleep(Duration::from_secs(10 * u64::from(report.pages))).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:reports.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS report_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for RenderReport {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO report_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "reports")]
struct Report {
    id: u64,
    pages: u32,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Report, SCHEMA, service};

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

    // The clock is paused, so the five minutes of rendering pass at once, and the lease's
    // extensions run as the clock reaches them.
    #[tokio::test]
    async fn a_report_rendered_past_its_lease_is_delivered_once() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_forms_lease").await?;
        tokio::time::pause();
        let tb = TestApp::start(service(pool.clone())).await?;
        let report = Report { id: 7, pages: 30 };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&report)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("reports")
            .assert_called_once()
            .with(&report)
            .settled(HandlerOutcome::ack());
        tokio::time::resume();
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM report_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            left, 0,
            "the acknowledgement held the lease and deleted the row"
        );
        Ok(())
    }
}
