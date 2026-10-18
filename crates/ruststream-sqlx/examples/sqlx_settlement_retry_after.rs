//! Webhook calls on SQLite that come back later: `retry_after(d)` hides the row until its
//! `retry_after` column comes due.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_settlement_retry_after --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:retry_after]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `webhook_jobs`: a claim takes it only once its `retry_after` has come.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "webhook_jobs")]
struct Webhook {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<Webhook>::new("webhooks"))]
async fn notify(call: &Call, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    match post(call, attempt).await {
        Answer::Done => HandlerOutcome::ack(),
        // The row stays out of the queue as long as the endpoint asked, then a claim takes it
        // again.
        Answer::TooManyRequests { retry_after } => HandlerOutcome::retry_after(retry_after),
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("webhooks", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<Webhook>("webhooks"),
        |b| {
            b.include(notify);
        },
    )
}
// --8<-- [end:retry_after]

/// What the endpoint answered: done, or "too many requests" with the `Retry-After` it sent.
enum Answer {
    Done,
    TooManyRequests { retry_after: Duration },
}

/// How long the endpoint asks a rate-limited caller to wait.
const RETRY_AFTER: Duration = Duration::from_secs(1);

/// A stand-in for the endpoint: it rate-limits the first attempt of every call.
async fn post(call: &Call, attempt: Option<u64>) -> Answer {
    tokio::task::yield_now().await;
    tracing::info!(url = %call.url, ?attempt, "calling");
    if attempt == Some(1) {
        Answer::TooManyRequests {
            retry_after: RETRY_AFTER,
        }
    } else {
        Answer::Done
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:webhooks.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS webhook_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for Webhook {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO webhook_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "webhooks")]
struct Call {
    url: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Call, RETRY_AFTER, SCHEMA, service};

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

    // The test runs on the real clock and lets the endpoint's second pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rate_limited_call_comes_back_when_its_wait_is_over() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_settlement_retry_after").await?;
        let tb = TestApp::start_live(service(pool)).await?;
        let call = Call {
            url: "https://hooks.example.com/orders".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&call)
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("webhooks")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RETRY_AFTER));

        tb.advance(RETRY_AFTER).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("webhooks")
            .assert_called(2)
            .with(&call)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
