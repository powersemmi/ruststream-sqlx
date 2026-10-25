//! The service of `examples/guide/retries.rs` without the macros: the table is described with
//! `InboxSpec`, and the handlers are named types mounted with `subscriber(..)`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_guide_retries --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
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
use sqlx::Connection;
use sqlx::Sqlite;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `mail_jobs`; `name` is the queue the row waits in.
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
        RetryAfter<DateTime<Utc>>,
        Lease<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("mail_jobs", Column::new("id").generated())
        .group(Column::new("name"))
        .attempt(Column::new("attempt").generated())
        .retry_after(Column::new("retry_after"))
        .lease(Column::new("locked_until"))
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

/// How much longer each attempt waits after the provider said it was busy.
const BACKOFF: Duration = Duration::from_secs(1);

struct Sender;

impl Handle<Mail, (), (), InboxContext> for Sender {
    async fn handle(
        &self,
        mail: &Mail,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        let attempt = attempt.unwrap_or(1);
        match deliver(mail, attempt).await {
            Ok(()) => Ok(()),
            // The provider's rate limit is spent: a retry right now would be refused again.
            Err(Refusal::Busy) => Err(HandlerOutcome::retry_after(
                BACKOFF * u32::try_from(attempt).unwrap_or(u32::MAX),
            )),
            // The provider did not answer: the next attempt may reach it.
            Err(Refusal::Unavailable) => Err(HandlerOutcome::retry()),
            // The mailbox does not exist: every attempt gets the same answer.
            Err(Refusal::Rejected) => {
                tracing::warn!(to = %mail.to, "address rejected, dropped");
                Err(HandlerOutcome::drop())
            }
        }
    }
}
// --8<-- [end:send]

// --8<-- [start:dead_letters]
struct Review;

impl Handle<Mail, (), (), InboxContext> for Review {
    async fn handle(
        &self,
        mail: &Mail,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        tracing::error!(to = %mail.to, ?attempt, "email not sent, needs a person");
        Ok(())
    }
}
// --8<-- [end:dead_letters]

// --8<-- [start:app]
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
        .with_broker(broker.route::<SendMail>("emails"), |b| {
            // Five deliveries, the first included; the fifth failure moves the row into the
            // `emails.dead` group.
            let send =
                subscriber("emails", Sender).map_source(|_| InboxQueue::<SendMail>::new("emails"));
            b.include(send.build())
                .max_attempts(nonzero!(5u32))
                .dead_letter("emails.dead");
            let review = subscriber("emails.dead", Review)
                .map_source(|_| InboxQueue::<SendMail>::new("emails.dead"));
            b.include(review.build());
        })
}
// --8<-- [end:app]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:mailer.db?mode=rwc")
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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Mail {
    to: String,
}

// `#[outgoing(name = "emails")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Mail {
    type Form = FixedName;
    const DESTINATION: &'static str = "emails";
}

impl MessageHeaders for Mail {
    type Contract = NoHeaders;
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
        let (url, pool, _keeper) = database("manual_guide_retries_dead").await?;
        let tb = TestApp::start_live(service(&url)).await?;
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
        let (url, pool, _keeper) = database("manual_guide_retries_rejected").await?;
        let tb = TestApp::start(service(&url)).await?;
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
        let (url, _pool, _keeper) = database("manual_guide_retries_busy").await?;
        let tb = TestApp::start_live(service(&url)).await?;
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
