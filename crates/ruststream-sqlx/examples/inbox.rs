//! An email queue in a Postgres table, served by the inbox broker.
//!
//! The service owns the table:
//!
//! ```sql
//! CREATE TABLE email_jobs (
//!     job_id  BIGSERIAL PRIMARY KEY,
//!     name    TEXT NOT NULL,
//!     payload BYTEA NOT NULL
//! );
//! ```
//!
//! The pool reads its settings from the `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and
//! `PGDATABASE` environment variables.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example inbox --features inbox,postgres
//! ```

// --8<-- [start:service]
use std::error::Error;

use ruststream::OutgoingMessage;
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, Postgres};

/// One row of `email_jobs`: the struct names the table and the role of each column.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

/// A publish to `emails` writes a row through the service's own statement.
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

#[derive(Deserialize)]
struct Email {
    to: String,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    println!("sending to {}", email.to);
    HandlerOutcome::ack()
}

fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendEmail>("emails"),
        |b| {
            b.include(send);
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // sqlx builds a pool only inside a Tokio runtime, so the service builds it here, before the
    // app.
    let pool = PgPool::connect_with(PgConnectOptions::new()).await?;
    app(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}
// --8<-- [end:service]
