//! Monthly statements on SQLite on the database's clock: every claim, delay and mark reads one
//! clock, whichever host runs the statement.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_time_database --features inbox,sqlite,chrono,testing
//! ```

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:database_clock]
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::DatabaseClock;
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `statement_jobs`, written by hosts whose clocks drift. The advisory lock form,
/// because the lease form reads the host's clock.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(
    table = "statement_jobs",
    clock = DatabaseClock,
    advisory_lock = "statement_jobs-{id}"
)]
struct SendStatement {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<SendStatement>::new("statements"))]
async fn send(account: &Account, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    if render(account, attempt).await {
        return HandlerOutcome::ack();
    }
    // A second by the database's clock, whichever host claimed the row.
    HandlerOutcome::retry_after(Duration::from_secs(1))
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("statements", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SendStatement>("statements"), |b| {
            b.include(send);
        })
}
// --8<-- [end:database_clock]

/// A stand-in for the ledger: the month's figures are not closed at the first attempt.
async fn render(account: &Account, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(account.id, ?attempt, "rendering a statement");
    attempt != Some(1)
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:statements.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS statement_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    processed_at TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for SendStatement {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO statement_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "statements")]
struct Account {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Account, SCHEMA, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool it reads the table through.
    async fn database(
        name: &str,
    ) -> Result<(String, SqlitePool, SqliteConnection), Box<dyn Error>> {
        let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
        let options: SqliteConnectOptions = url.parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((url, pool, keeper))
    }

    // The database's clock is real time, which a paused tokio clock does not move, so the test
    // runs on the real clock and lets the second pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delay_on_the_database_clock_brings_the_row_back() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_time_database").await?;
        let tb = TestApp::start_live(service(&url)).await?;
        let account = Account { id: 7 };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&account)
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("statements")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(Duration::from_secs(1)));

        tb.advance(Duration::from_secs(1)).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("statements")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        let marked: bool =
            sqlx::query_scalar("SELECT processed_at IS NOT NULL FROM statement_jobs")
                .fetch_one(&pool)
                .await?;
        assert!(marked, "the database's clock marked the row");
        Ok(())
    }
}
