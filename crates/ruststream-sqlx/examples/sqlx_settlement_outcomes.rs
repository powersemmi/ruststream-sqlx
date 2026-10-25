//! A mail queue on SQLite whose handler answers with each outcome: an acknowledgement marks the
//! row processed, a retry returns it at once with one more attempt, a drop finishes it.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_settlement_outcomes --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:outcomes]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `mail_jobs`: `processed_at` keeps a finished row in the table, marked.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "mail_jobs")]
struct SendMail {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<SendMail>::new("mail"))]
async fn send(mail: &Mail, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    if !mail.to.contains('@') {
        // Nothing to retry: the row is finished, as an acknowledgement finishes it.
        return HandlerOutcome::drop();
    }
    if deliver(mail, attempt).await {
        // `processed_at` is set, and the row stays.
        return HandlerOutcome::ack();
    }
    // Back in the queue at once; the next delivery reads one more attempt.
    HandlerOutcome::retry()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("mailer", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SendMail>("mail"), |b| {
            b.include(send);
        })
}
// --8<-- [end:outcomes]

/// A stand-in for the mail server: it greylists the first delivery of every message.
async fn deliver(mail: &Mail, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(to = %mail.to, ?attempt, "delivering");
    attempt != Some(1)
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:mailer.db?mode=rwc")
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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

    /// Whether the row is marked processed.
    async fn marked(pool: &SqlitePool) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT processed_at IS NOT NULL FROM mail_jobs")
            .fetch_one(pool)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_counts_the_attempt_and_the_acknowledgement_marks_the_row()
    -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_settlement_outcomes_retry").await?;
        let tb = TestApp::start(service(&url)).await?;
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
        let (url, pool, _keeper) = database("sqlx_settlement_outcomes_drop").await?;
        let tb = TestApp::start(service(&url)).await?;
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
