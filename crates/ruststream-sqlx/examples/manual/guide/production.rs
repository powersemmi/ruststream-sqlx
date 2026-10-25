//! The service of `examples/guide/production.rs` without the macros: the table is described with
//! `InboxSpec`, and the handler is a named type mounted with `subscriber(..)` and its workers.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_guide_production --features inbox,postgres,testing,asyncapi
//! ```

use std::process::ExitCode;

use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

// --8<-- [start:migration]
/// The service's first migration; in a service crate, `migrations/0001_email_jobs.sql`, run by
/// `sqlx::migrate!`.
const MIGRATION: &str = "
CREATE TABLE IF NOT EXISTS email_jobs (
    job_id  BIGSERIAL PRIMARY KEY,
    name    TEXT NOT NULL,
    attempt SMALLINT NOT NULL DEFAULT 1,
    payload BYTEA NOT NULL
);
CREATE INDEX IF NOT EXISTS email_jobs_claim ON email_jobs (name, job_id);
";
// --8<-- [end:migration]

// --8<-- [start:table]
use ruststream_sqlx::prelude::*;
use sqlx::Postgres;

/// One row of `email_jobs`, claimed by row lock: the description names no other form. Each
/// subscription prepares its statements at startup; the compile-time check is the derive's.
#[derive(sqlx::FromRow)]
struct SendEmail {
    job_id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SendEmail {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("job_id").generated())
        .group(Column::new("name"))
        .attempt(Column::new("attempt").generated())
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
// --8<-- [end:table]

// --8<-- [start:service]
use std::num::NonZeroUsize;
use std::time::Duration;

use sqlx::Connection;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

/// The emails in work at once; each holds its row's lock and a connection until it settles.
const WORKERS: NonZeroUsize = nonzero!(8);

/// A connection for each worker, and one the subscription leaves to settlements and publishes.
fn pool() -> PgPoolOptions {
    let size = u32::try_from(WORKERS.get() + 1).unwrap_or(u32::MAX);
    PgPoolOptions::new()
        .max_connections(size)
        .acquire_timeout(Duration::from_secs(5))
}

struct Mailer;

impl Handle<Email, (), (), InboxContext> for Mailer {
    async fn handle(
        &self,
        email: &Email,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        tracing::info!(to = %email.to, ?attempt, "sending");
        Ok(())
    }
}

/// The service on the database `options` reach.
fn service(options: PgConnectOptions) -> RustStream {
    let migrate = options.clone();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_connect_options(options, pool()).route::<SendEmail>("emails");
    RustStream::new(AppInfo::new("mailer", "0.1.0"))
        // The schema the release reads exists before any subscription prepares its statements.
        .on_startup(async move |()| {
            let mut conn = PgConnection::connect_with(&migrate).await?;
            sqlx::raw_sql(MIGRATION).execute(&mut conn).await?;
            conn.close().await
        })
        // The label names the server in the generated AsyncAPI document.
        .with_broker_labeled("postgres", broker, |b| {
            let send = subscriber("emails", Mailer)
                .map_source(|_| InboxQueue::<SendEmail>::new("emails"))
                .workers(WORKERS);
            b.include(send.build())
                .max_attempts(nonzero!(5u32))
                .dead_letter("emails.dead");
        })
}
// --8<-- [end:service]

// --8<-- [start:main]
fn main() -> ExitCode {
    // The connection's settings come from the `PG*` environment variables.
    run_main(|| service(PgConnectOptions::new()))
}
// --8<-- [end:main]

impl Publish<Postgres> for SendEmail {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO email_jobs (name, payload) VALUES ($1, $2)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Email {
    to: String,
}

// `#[outgoing(name = "emails")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Email {
    type Form = FixedName;
    const DESTINATION: &'static str = "emails";
}

impl MessageHeaders for Email {
    type Contract = NoHeaders;
}

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::Postgres;
    use sqlx::postgres::PgConnectOptions;

    use super::{Email, live, service};

    const PASSWORD: &str = "hunter2";

    // The document is built with no I/O: nothing connects before the service starts.
    #[tokio::test]
    async fn the_document_names_the_server_and_never_its_password() -> Result<(), Box<dyn Error>> {
        let options: PgConnectOptions =
            format!("postgres://mailer:{PASSWORD}@db.internal:5432/mail").parse()?;
        let document = build_spec(&service(options)).to_json()?;

        assert!(document.contains("db.internal:5432"), "{document}");
        assert!(!document.contains(PASSWORD), "{document}");
        Ok(())
    }

    // Postgres only: skips without `POSTGRES_TEST_URL`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_table_of_an_older_release_stops_the_startup() -> Result<(), Box<dyn Error>> {
        let Some(db) = live::postgres::database().await else {
            return Ok(());
        };
        // The test database comes with the suites' `email_jobs`, which the migration leaves as it
        // is: without `attempt`, it is the table of a release before this one.
        sqlx::raw_sql("ALTER TABLE email_jobs DROP COLUMN attempt")
            .execute(&db.pool)
            .await?;
        let started = TestApp::start_live(service(db.options())).await;

        let error = started
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(error.contains("email_jobs"), "{error}");
        db.finish().await;
        Ok(())
    }

    // Postgres only: skips without `POSTGRES_TEST_URL`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_checked_statements_serve_the_queue() -> Result<(), Box<dyn Error>> {
        let Some(db) = live::postgres::database().await else {
            return Ok(());
        };
        let tb = TestApp::start_live(service(db.options())).await?;
        let email = Email {
            to: "ops@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Postgres>>()
            .message(&email)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Postgres>>()
            .subscriber("emails")
            .assert_called_once()
            .with(&email)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        db.finish().await;
        Ok(())
    }
}
