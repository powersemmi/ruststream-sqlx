//! The service of `examples/sqlx_settlement_outcomes.rs` without the macros: the handler's
//! `Result` settles the row, `Ok` acknowledging it and `Err(outcome)` naming any other outcome.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_settlement_outcomes --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload, ProcessedAt};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:outcomes]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `mail_jobs`: `processed_at` keeps a finished row in the table, marked.
#[derive(sqlx::FromRow)]
struct SendMail {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SendMail {
    type Id = i64;
    type Table = InboxSpec<(
        Attempt,
        Lease<DateTime<Utc>>,
        ProcessedAt<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("mail_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for SendMail {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for SendMail {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct SendOne;

impl Handle<Mail, (), (), InboxContext> for SendOne {
    async fn handle(
        &self,
        mail: &Mail,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        if !mail.to.contains('@') {
            // Nothing to retry: the row is finished, as an acknowledgement finishes it.
            return Err(HandlerOutcome::drop());
        }
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        if deliver(mail, attempt).await {
            // `processed_at` is set, and the row stays.
            return Ok(());
        }
        // Back in the queue at once; the next delivery reads one more attempt.
        Err(HandlerOutcome::retry())
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendMail>("mail"),
        |b| {
            let send =
                subscriber("mail", SendOne).map_source(|_| InboxQueue::<SendMail>::new("mail"));
            b.include(send.build());
        },
    )
}
// --8<-- [end:outcomes]

/// A stand-in for the mail server: it greylists the first delivery of every message.
async fn deliver(mail: &Mail, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(to = %mail.to, ?attempt, "delivering");
    attempt != Some(1)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:mailer.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS mail_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    processed_at TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for SendMail {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO mail_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "mail")]
struct Mail {
    to: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Mail, SCHEMA, service};

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

    /// Whether the row is marked processed.
    async fn marked(pool: &SqlitePool) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT processed_at IS NOT NULL FROM mail_jobs")
            .fetch_one(pool)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_counts_the_attempt_and_the_acknowledgement_marks_the_row()
    -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_settlement_outcomes_retry").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        let mail = Mail {
            to: "ada@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&mail)
            .publish()
            .await?;

        assert_eq!(
            tb.broker::<SqlxBroker<Sqlite>>()
                .subscriber("mail")
                .outcomes(),
            [Outcome::Nack, Outcome::Ack]
        );
        // The second delivery read the attempt the retry counted, and the server took it.
        assert!(marked(&pool).await?, "the acknowledgement marked the row");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_row_is_marked_as_an_acknowledged_one() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_settlement_outcomes_drop").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        let mail = Mail {
            to: "nobody".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&mail)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("mail")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        assert!(marked(&pool).await?, "the drop marked the row");
        Ok(())
    }
}
