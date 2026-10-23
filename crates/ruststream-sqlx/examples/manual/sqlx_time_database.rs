//! The service of `examples/sqlx_time_database.rs` without the macros: `.clock::<DatabaseClock>()`
//! makes the statements read the database's clock, and `Clock<DatabaseClock>` goes into the type.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_time_database --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Advisory, Attempt, Clock, Payload, ProcessedAt, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:database_clock]
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::DatabaseClock;
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `statement_jobs`, written by hosts whose clocks drift. The advisory lock form,
/// because the lease form reads the host's clock.
#[derive(sqlx::FromRow)]
struct SendStatement {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SendStatement {
    type Id = i64;
    type Table = InboxSpec<(
        Clock<DatabaseClock>,
        Advisory,
        Attempt,
        RetryAfter<DateTime<Utc>>,
        ProcessedAt<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("statement_jobs", Column::new("id").generated())
        .clock::<DatabaseClock>()
        .advisory(&[KeyPart::Literal("statement_jobs-"), KeyPart::Column("id")])
        .attempt(Column::new("attempt").generated())
        .retry_after(Column::new("retry_after"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for SendStatement {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for SendStatement {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct SendOne;

impl Handle<Account, (), (), InboxContext> for SendOne {
    async fn handle(
        &self,
        account: &Account,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        if render(account, attempt).await {
            return Ok(());
        }
        // A second by the database's clock, whichever host claimed the row.
        Err(HandlerOutcome::retry_after(Duration::from_secs(1)))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("statements", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendStatement>("statements"),
        |b| {
            let send = subscriber("statements", SendOne)
                .map_source(|_| InboxQueue::<SendStatement>::new("statements"));
            b.include(send.build());
        },
    )
}
// --8<-- [end:database_clock]

/// A stand-in for the ledger: the month's figures are not closed at the first attempt.
async fn render(account: &Account, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(account.id, ?attempt, "rendering a statement");
    attempt != Some(1)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:statements.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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

    // The database's clock is real time, which a paused tokio clock does not move, so the test
    // runs on the real clock and lets the second pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delay_on_the_database_clock_brings_the_row_back() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_time_database").await?;
        let tb = TestApp::start_live(service(pool.clone())).await?;
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
