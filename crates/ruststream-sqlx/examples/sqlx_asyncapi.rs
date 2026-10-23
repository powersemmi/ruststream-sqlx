//! The `AsyncAPI` document of a service on the inbox: the database server, the table and group
//! behind each subscription's channel, and the schema of a row a handler takes. The tests build
//! the document from the app, on a pool that does no I/O.
//!
//! ```text
//! cargo test -p ruststream-sqlx --example sqlx_asyncapi --features inbox,postgres,asyncapi,testing
//! DATABASE_URL=postgres://office:secret@localhost/office \
//!     cargo run -p ruststream-sqlx --example sqlx_asyncapi --features inbox,postgres,asyncapi,testing
//! ```

use std::env;
use std::error::Error;

use ruststream::schemars::JsonSchema;
use serde::Deserialize;
use sqlx::postgres::{PgPool, PgPoolOptions};

use ruststream_sqlx::prelude::*;

// --8<-- [start:channels]
/// One row of `app.email_jobs`: the channel of each subscription names the table and its group.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs", schema = "app")]
struct SendEmail {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

/// One row of `reports`, a table without groups: its channel names the table alone.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "reports")]
struct RenderReport {
    #[field(id, generated)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send_email(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<RenderReport>::new("reports"))]
async fn render(report: &Report) -> HandlerOutcome {
    tracing::info!(month = report.month, "rendering");
    HandlerOutcome::ack()
}
// --8<-- [end:channels]

// --8<-- [start:rows]
/// A mail the handler takes as the row itself: its `JsonSchema` documents the message.
#[derive(Debug, Clone, Inbox, sqlx::FromRow, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[inbox(table = "mail_jobs")]
struct Mail {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    recipient: String,
    subject: Option<String>,
}

#[subscriber(InboxQueue::<Mail>::new("mail"))]
async fn send_mail(mail: &Mail) -> HandlerOutcome {
    tracing::info!(to = %mail.recipient, subject = ?mail.subject, "mailing");
    HandlerOutcome::ack()
}
// --8<-- [end:rows]

// --8<-- [start:server]
/// The pool on the database a deployment configures; it connects on first use.
fn pool(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new().max_connections(8).connect_lazy(url)
}
// --8<-- [end:server]

// --8<-- [start:app]
fn service(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("office", "0.1.0")).with_broker_labeled(
        "postgres",
        SqlxBroker::new(pool),
        |b| {
            b.include(send_email);
            b.include(render);
            b.include(send_mail);
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = pool(&env::var("DATABASE_URL")?)?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}
// --8<-- [end:app]

#[derive(Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Email {
    to: String,
}

#[derive(Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Report {
    month: u32,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{pool, service};

    const PASSWORD: &str = "hunter2";

    /// The document of the service on the pool a deployment configures, built with no I/O.
    fn document() -> Result<Value, Box<dyn Error>> {
        let pool = pool(&format!(
            "postgres://office:{PASSWORD}@db.internal:5432/office"
        ))?;
        Ok(serde_json::from_str(
            &build_spec(&service(pool)).to_json()?,
        )?)
    }

    #[tokio::test]
    async fn the_server_is_the_pools_host_and_port() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["postgres"],
            json!({ "host": "db.internal:5432", "protocol": "postgres" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_document_carries_no_password() -> Result<(), Box<dyn Error>> {
        let text = document()?.to_string();

        assert!(!text.contains(PASSWORD), "{text}");
        Ok(())
    }

    #[tokio::test]
    async fn each_channel_names_its_table_and_group() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channels = &spec["channels"];

        assert_eq!(
            channels["emails"]["bindings"]["x-sqlx"],
            json!({ "table": "app.email_jobs", "group": "emails" })
        );
        assert_eq!(
            channels["reports"]["bindings"]["x-sqlx"],
            json!({ "table": "reports" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_row_handler_documents_the_rows_columns() -> Result<(), Box<dyn Error>> {
        let text = document()?.to_string();

        for column in ["recipient", "subject"] {
            assert!(
                text.contains(column),
                "the row's {column} is documented: {text}"
            );
        }
        Ok(())
    }
}
