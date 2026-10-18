//! The service of `examples/sqlx_messages_decoding.rs` without the macros: the decode policy is
//! a step of the subscriber chain.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_messages_decoding --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::schemars::JsonSchema;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Lease, Payload, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

use ruststream_sqlx::prelude::*;

#[derive(sqlx::FromRow)]
struct SendEmail {
    job_id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SendEmail {
    type Id = i64;
    type Table = InboxSpec<(
        RetryAfter<DateTime<Utc>>,
        Attempt,
        Lease<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("job_id").generated())
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.job_id
    }
}

impl AttemptRow for SendEmail {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for SendEmail {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct Sender;

impl Handle<Email> for Sender {
    fn handle(
        &self,
        email: &Email,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %email.to, "sending");
        ready(Ok(()))
    }
}

// --8<-- [start:policy]
fn service(pool: SqlitePool) -> RustStream {
    // A row that does not decode stays in the table and comes back every ten minutes, until a
    // release that reads it ships.
    let keep = FailurePolicy::RetryAfter(Duration::from_secs(600));
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendEmail>("emails"),
        |b| {
            let send = subscriber("emails", Sender)
                .map_source(|_| InboxQueue::<SendEmail>::new("emails"))
                .on_failure(FailurePolicies::default().with_decode(keep));
            b.include(send.build());
        },
    )
}
// --8<-- [end:policy]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:mailer.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS email_jobs (
    job_id INTEGER PRIMARY KEY,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for SendEmail {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO email_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "emails")]
struct Email {
    to: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use serde::Serialize;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Email, SCHEMA, service};

    /// What an older release wrote: an email without a recipient.
    #[derive(Serialize, Outgoing)]
    struct Draft {
        subject: String,
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_that_does_not_decode_waits_ten_minutes() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_messages_decoding").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Draft {
                subject: "hello".to_owned(),
            })
            .to("emails")
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(Duration::from_secs(600)))
            .assert_last_failed_to_decode();
        let attempt: i64 = sqlx::query_scalar("SELECT attempt FROM email_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(attempt, 2, "the row stays in the table, its attempt spent");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_that_decodes_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_messages_decoding_readable").await?;
        let tb = TestApp::start(service(pool)).await?;
        let email = Email {
            to: "ops@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&email)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called_once()
            .with(&email)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
