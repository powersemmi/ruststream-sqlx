//! What a subscription adds to the `AsyncAPI` document: the table it reads, the schema of a row a
//! row-mode handler takes, and no password.

#![cfg(all(feature = "inbox", feature = "postgres", feature = "asyncapi"))]

use ruststream::asyncapi::build_spec;
use ruststream::conformance::harness;
use ruststream::schemars::JsonSchema;
use ruststream::{Connected, SubscriptionSource};
use ruststream_sqlx::prelude::*;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres};

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs", schema = "app")]
struct SendEmail {
    #[field(id)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "reports")]
struct Report {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

/// A mail the handler takes as the row itself; its schema documents the message.
#[derive(Debug, Clone, Inbox, sqlx::FromRow, JsonSchema)]
// The derive names `schemars` by path, and the crate reaches it through the core.
#[schemars(crate = "ruststream::schemars")]
#[inbox(table = "mail_jobs")]
struct Mail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    recipient: String,
    subject: Option<String>,
}

#[subscriber(InboxQueue::<Mail>::new("mail"))]
async fn send(mail: &Mail) -> HandlerOutcome {
    if mail.recipient.is_empty() {
        HandlerOutcome::drop()
    } else {
        HandlerOutcome::ack()
    }
}

/// The mailer as `main` builds it, on a pool that does no I/O until it is used.
fn mailer(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(
        SqlxBroker::<Postgres>::new(pool),
        |b| {
            b.include(send);
        },
    )
}

#[tokio::test]
async fn a_row_mode_handler_documents_the_rows_schema() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://app@db.internal:5432/app")
        .expect("the URL parses");
    let spec = build_spec(&mailer(pool));
    assert_eq!(spec.messages_without_schema(), Vec::<&str>::new());
    let document = spec.to_json().expect("the document serializes");
    for field in ["recipient", "subject"] {
        assert!(
            document.contains(field),
            "the row's {field} is documented: {document}"
        );
    }
}

#[test]
fn a_subscription_documents_its_table_and_group() {
    let queue = InboxQueue::<SendEmail>::new("emails");
    let bindings = SubscriptionSource::<Connected<SqlxBroker<Postgres>>>::channel_bindings(&queue);
    let rendered = format!("{bindings:?}");
    assert!(rendered.contains("x-sqlx"), "{rendered}");
    assert!(rendered.contains("app.email_jobs"), "{rendered}");
    assert!(rendered.contains("emails"), "{rendered}");
    let _ = |row: SendEmail| (row.job_id, row.name, row.payload);
}

#[test]
fn a_table_without_groups_documents_no_group() {
    let queue = InboxQueue::<Report>::new("reports");
    let bindings = SubscriptionSource::<Connected<SqlxBroker<Postgres>>>::channel_bindings(&queue);
    let rendered = format!("{bindings:?}");
    assert!(rendered.contains("reports"), "{rendered}");
    assert!(!rendered.contains("group"), "{rendered}");
    let _ = |row: Report| (row.id, row.payload);
}

#[tokio::test]
async fn the_document_carries_no_password() {
    // A pool built without I/O, from the URL a deployment configures.
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://app:hunter2@db.internal:5432/app")
        .expect("the URL parses");
    harness::describes_without_credentials(
        &SqlxBroker::new(pool),
        &InboxQueue::<SendEmail>::new("emails"),
        "hunter2",
    );
}
