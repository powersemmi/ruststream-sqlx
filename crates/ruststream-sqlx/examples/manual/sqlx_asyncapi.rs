//! The service of `examples/sqlx_asyncapi.rs` without the macros: the tables are described with
//! `InboxSpec`, and the handlers are named types. The document is the same.
//!
//! ```text
//! cargo test -p ruststream-sqlx --example manual_sqlx_asyncapi --features inbox,postgres,asyncapi,testing
//! DATABASE_URL=postgres://office:secret@localhost/office \
//!     cargo run -p ruststream-sqlx --example manual_sqlx_asyncapi --features inbox,postgres,asyncapi,testing
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::PayloadRow;
use serde::Deserialize;
use sqlx::postgres::{PgPoolOptions, Postgres};

use ruststream_sqlx::prelude::*;

// --8<-- [start:channels]
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::Payload;
use ruststream_sqlx::{InboxSpec, InboxTable};

/// One row of `app.email_jobs`: the channel of each subscription names the table and its group.
#[derive(sqlx::FromRow)]
struct SendEmail {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for SendEmail {
    type Id = i64;
    type Table = InboxSpec<(Payload,)>;
    const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("id").generated())
        .within("app")
        .group(Column::new("name"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for SendEmail {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// One row of `reports`, a table without groups: its channel names the table alone.
#[derive(sqlx::FromRow)]
struct RenderReport {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for RenderReport {
    type Id = i64;
    type Table = InboxSpec<(Payload,)>;
    const TABLE: Self::Table =
        InboxSpec::new("reports", Column::new("id").generated()).payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for RenderReport {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct Mailer;

impl Handle<Email> for Mailer {
    fn handle(
        &self,
        email: &Email,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %email.to, "sending");
        ready(Ok(()))
    }
}

struct Renderer;

impl Handle<Report> for Renderer {
    fn handle(
        &self,
        report: &Report,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(month = report.month, "rendering");
        ready(Ok(()))
    }
}
// --8<-- [end:channels]

// --8<-- [start:rows]
use ruststream::runtime::SoloCarried;

/// A mail the handler takes as the row itself: its `JsonSchema` documents the message.
#[derive(Debug, Clone, sqlx::FromRow, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Mail {
    id: i64,
    recipient: String,
    subject: Option<String>,
}

impl InboxTable for Mail {
    type Id = i64;
    type Table = InboxSpec<()>;
    const TABLE: Self::Table = InboxSpec::new("mail_jobs", Column::new("id").generated())
        .group(Column::new("name"))
        .data(&[Column::new("recipient"), Column::new("subject")]);

    fn id(&self) -> &i64 {
        &self.id
    }
}

// Row mode: a handler takes the row itself.
impl Input for Mail {
    type Axis = SoloCarried<Self>;
}

struct Postman;

impl Handle<Mail> for Postman {
    fn handle(
        &self,
        mail: &Mail,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %mail.recipient, subject = ?mail.subject, "mailing");
        ready(Ok(()))
    }
}
// --8<-- [end:rows]

// --8<-- [start:server]
/// The broker on the database a deployment configures; it builds its pool when the service
/// starts.
fn broker(url: &str) -> SqlxBroker<Postgres> {
    SqlxBroker::from_url(url, PgPoolOptions::new().max_connections(8))
}
// --8<-- [end:server]

// --8<-- [start:app]
fn service(url: &str) -> RustStream {
    RustStream::new(AppInfo::new("office", "0.1.0")).with_broker_labeled(
        "postgres",
        broker(url),
        |b| {
            let email =
                subscriber("emails", Mailer).map_source(|_| InboxQueue::<SendEmail>::new("emails"));
            b.include(email.build());
            let report = subscriber("reports", Renderer)
                .map_source(|_| InboxQueue::<RenderReport>::new("reports"));
            b.include(report.build());
            let mail = subscriber("mail", Postman).map_source(|_| InboxQueue::<Mail>::new("mail"));
            b.include(mail.build());
        },
    )
}

fn main() -> ExitCode {
    let url = env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/office".to_owned());
    run_main(|| service(&url))
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

    use super::service;

    const PASSWORD: &str = "hunter2";

    /// The document of the service on the database a deployment configures, built with no I/O.
    fn document() -> Result<Value, Box<dyn Error>> {
        let url = format!("postgres://office:{PASSWORD}@db.internal:5432/office");
        Ok(serde_json::from_str(
            &build_spec(&service(&url)).to_json()?,
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
