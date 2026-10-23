//! A queue table whose statements sqlx checks at compile time against Postgres.
//!
//! The build reads the statements' descriptions from the offline data in `.sqlx`, which
//! `cargo sqlx prepare` captures, or from the database `DATABASE_URL` names. The service owns the
//! table:
//!
//! ```sql
//! CREATE TABLE email_jobs (
//!     job_id  BIGSERIAL PRIMARY KEY,
//!     name    TEXT NOT NULL,
//!     attempt SMALLINT NOT NULL DEFAULT 1,
//!     payload BYTEA NOT NULL
//! );
//! ```
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_tables_checked --features inbox,postgres,testing
//! ```

use std::error::Error;

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, Postgres};

use ruststream_sqlx::prelude::*;

// --8<-- [start:table]
/// Every statement this struct determines goes through sqlx's compile-time check.
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

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    tracing::info!(to = %email.to, ?attempt, "sending");
    HandlerOutcome::ack()
}

fn service(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendEmail>("emails"),
        |b| {
            b.include(send);
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // The pool reads its settings from the `PG*` environment variables.
    let pool = PgPool::connect_with(PgConnectOptions::new()).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

// The live suites' gate: the test runs on the Postgres stand `POSTGRES_TEST_URL` names, whose
// schema holds `email_jobs`, and skips without it.
#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::Postgres;

    use super::{Email, live, service};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_checked_statements_serve_the_queue() -> Result<(), Box<dyn Error>> {
        let Some(db) = live::postgres::database().await else {
            return Ok(());
        };
        let tb = TestApp::start(service(db.pool.clone())).await?;
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
