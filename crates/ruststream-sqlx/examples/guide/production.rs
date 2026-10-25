//! The service of the Production guide: a mailer on Postgres whose statements sqlx checks at
//! compile time, with its pool sized for its workers and its schema created before the app starts.
//!
//! The build reads the statements' descriptions from the offline data in `.sqlx`, which
//! `cargo sqlx prepare` captures, or from the database `DATABASE_URL` names.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example guide_production --features inbox,postgres,testing,asyncapi
//! ```

use ruststream::OutgoingMessage;
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

/// One row of `email_jobs`: every statement it determines goes through sqlx's compile-time check.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs", checked, db = postgres)]
struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(payload)]
    payload: Vec<u8>,
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

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    tracing::info!(to = %email.to, ?attempt, "sending");
    HandlerOutcome::ack()
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
            b.include(send.workers(WORKERS))
                .max_attempts(nonzero!(5u32))
                .dead_letter("emails.dead");
        })
}
// --8<-- [end:service]

// --8<-- [start:main]
#[ruststream::app]
fn app() -> impl App {
    // The connection's settings come from the `PG*` environment variables.
    service(PgConnectOptions::new())
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "emails")]
struct Email {
    to: String,
}

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
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
