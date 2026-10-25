//! Rows that do not decode: the subscription's decode policy settles them before the handler
//! runs, and here keeps them ten minutes at a time.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_messages_decoding --features inbox,sqlite,chrono,testing
//! ```

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

// --8<-- [start:policy]
/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    // A row that does not decode stays in the table and comes back every ten minutes, until a
    // release that reads it ships.
    let keep = FailurePolicy::RetryAfter(Duration::from_secs(600));
    RustStream::new(AppInfo::new("mailer", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SendEmail>("emails"), |b| {
            b.include(send.on_failure(FailurePolicies::default().with_decode(keep)));
        })
}
// --8<-- [end:policy]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:mailer.db?mode=rwc")
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_that_does_not_decode_waits_ten_minutes() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_messages_decoding").await?;
        let tb = TestApp::start(service(&url)).await?;
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
        let (url, _pool, _keeper) = database("sqlx_messages_decoding_readable").await?;
        let tb = TestApp::start(service(&url)).await?;
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
