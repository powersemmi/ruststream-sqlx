//! The service of the Retries guide: an email sender that tries again now or later, stops after
//! five attempts and parks what keeps failing in a dead-letter group.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example guide_retries --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

// --8<-- [start:schema]
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS mail_jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    locked_until TEXT,
    payload BLOB NOT NULL
)";
// --8<-- [end:schema]

// --8<-- [start:send]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqlitePool};

/// One row of `mail_jobs`; `name` is the queue the row waits in.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "mail_jobs")]
struct SendMail {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// How much longer each attempt waits after the provider said it was busy.
const BACKOFF: Duration = Duration::from_secs(1);

#[subscriber(InboxQueue::<SendMail>::new("emails"))]
async fn send(mail: &Mail, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    let attempt = attempt.unwrap_or(1);
    match deliver(mail, attempt).await {
        Ok(()) => HandlerOutcome::ack(),
        // The provider's rate limit is spent: a retry right now would be refused again.
        Err(Refusal::Busy) => {
            HandlerOutcome::retry_after(BACKOFF * u32::try_from(attempt).unwrap_or(u32::MAX))
        }
        // The provider did not answer: the next attempt may reach it.
        Err(Refusal::Unavailable) => HandlerOutcome::retry(),
        // The mailbox does not exist: every attempt gets the same answer.
        Err(Refusal::Rejected) => {
            tracing::warn!(to = %mail.to, "address rejected, dropped");
            HandlerOutcome::drop()
        }
    }
}
// --8<-- [end:send]

// --8<-- [start:dead_letters]
#[subscriber(InboxQueue::<SendMail>::new("emails.dead"))]
async fn review(mail: &Mail, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    tracing::error!(to = %mail.to, ?attempt, "email not sent, needs a person");
    HandlerOutcome::ack()
}
// --8<-- [end:dead_letters]

// --8<-- [start:app]
fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendMail>("emails"),
        |b| {
            // Five deliveries, the first included; the fifth failure moves the row into the
            // `emails.dead` group.
            b.include(send)
                .max_attempts(nonzero!(5u32))
                .dead_letter("emails.dead");
            b.include(review);
        },
    )
}
// --8<-- [end:app]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:mailer.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

/// A publish to `emails` writes a row into that group.
impl Publish<Sqlite> for SendMail {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO mail_jobs (name, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "emails")]
struct Mail {
    to: String,
}

/// Why the provider did not take an email.
enum Refusal {
    Busy,
    Unavailable,
    Rejected,
}

/// A stand-in for the mail provider, which answers by the recipient's domain: `busy.example`
/// is rate-limited on the first attempt, `down.example` never answers, `nobody.example` has no
/// mailboxes.
async fn deliver(mail: &Mail, attempt: u64) -> Result<(), Refusal> {
    tokio::task::yield_now().await;
    match mail.to.rsplit('@').next() {
        Some("busy.example") if attempt == 1 => Err(Refusal::Busy),
        Some("down.example") => Err(Refusal::Unavailable),
        Some("nobody.example") => Err(Refusal::Rejected),
        _ => {
            tracing::info!(to = %mail.to, attempt, "sent");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{BACKOFF, Mail, SCHEMA, service};

    // A database in memory that every connection naming it shares, alive while one of them is
    // open: the test holds that one beside the pool.
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

    /// Past the subscriptions' poll interval, one second by default.
    const POLL: Duration = Duration::from_secs(2);

    fn mail(to: &str) -> Mail {
        Mail { to: to.to_owned() }
    }

    // The retries come back at once, and the dead-letter group's subscription finds the moved row
    // on its next poll: the test runs on the real clock past the poll interval.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unanswered_email_is_tried_five_times_then_dead_lettered()
    -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("guide_retries_dead").await?;
        let tb = TestApp::start_live(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&mail("ada@down.example"))
            .publish()
            .await?;
        tb.advance(POLL).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called(5)
            .with(&mail("ada@down.example"));

        // The fifth failure moved the row into `emails.dead`, where `review` took it.
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails.dead")
            .assert_called_once()
            .with(&mail("ada@down.example"))
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM mail_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "the reviewed email left the table");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rejected_address_is_dropped_at_once() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("guide_retries_rejected").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&mail("ada@nobody.example"))
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM mail_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "a dropped email leaves the table");
        Ok(())
    }

    // The row comes back when its `retry_after` comes due on the real clock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_busy_provider_is_tried_again_after_the_backoff() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("guide_retries_busy").await?;
        let tb = TestApp::start_live(service(pool)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&mail("ada@busy.example"))
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
