//! Row mode: a table without a payload column, whose handler takes the row itself.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_messages_rows --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

use ruststream_sqlx::prelude::*;

// --8<-- [start:rows]
/// One mail to send: the table has no payload column, so the handler takes the row.
#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
#[inbox(table = "mail_jobs")]
struct Mail {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    recipient: String,
    subject: Option<String>,
}

#[subscriber(InboxQueue::<Mail>::new("mail"))]
async fn send(mail: &Mail) -> HandlerOutcome {
    tracing::info!(to = %mail.recipient, subject = ?mail.subject, mail.attempt, "sending");
    HandlerOutcome::ack()
}
// --8<-- [end:rows]

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send);
    })
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
    recipient TEXT NOT NULL,
    subject TEXT
)";

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
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

    // A row the service writes itself reaches the harness's books once a claim takes it, so the
    // test runs on the real clock and lets the claim happen.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_handler_takes_the_row_itself() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_messages_rows").await?;
        sqlx::query("INSERT INTO mail_jobs (recipient, subject) VALUES (?, ?)")
            .bind("ops@example.com")
            .bind("disk full")
            .execute(&pool)
            .await?;
        let tb = TestApp::start_live(service(pool.clone())).await?;
        tb.advance(Duration::from_millis(500)).await?;

        let lent = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("mail")
            .assert_called_once()
            .settled(HandlerOutcome::ack())
            .received_values::<Mail>();
        let read: Vec<(&str, Option<&str>, i16)> = lent
            .iter()
            .map(|mail| {
                (
                    mail.recipient.as_str(),
                    mail.subject.as_deref(),
                    mail.attempt,
                )
            })
            .collect();
        assert_eq!(read, [("ops@example.com", Some("disk full"), 1)]);
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM mail_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "the acknowledgement deleted the row");
        Ok(())
    }
}
