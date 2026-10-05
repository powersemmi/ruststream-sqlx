//! What a subscription adds to the `AsyncAPI` document: the table it reads, and no password.

#![cfg(all(feature = "inbox", feature = "postgres", feature = "asyncapi"))]

use ruststream::conformance::harness;
use ruststream::{Connected, SubscriptionSource};
use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
use sqlx::postgres::PgPoolOptions;

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

#[test]
fn a_subscription_documents_its_table_and_group() {
    let queue = InboxQueue::<SendEmail>::new("emails");
    let bindings =
        SubscriptionSource::<Connected<SqlxBroker<sqlx::Postgres>>>::channel_bindings(&queue);
    let rendered = format!("{bindings:?}");
    assert!(rendered.contains("x-sqlx"), "{rendered}");
    assert!(rendered.contains("app.email_jobs"), "{rendered}");
    assert!(rendered.contains("emails"), "{rendered}");
    let _ = |row: SendEmail| (row.job_id, row.name, row.payload);
}

#[test]
fn a_table_without_groups_documents_no_group() {
    let queue = InboxQueue::<Report>::new("reports");
    let bindings =
        SubscriptionSource::<Connected<SqlxBroker<sqlx::Postgres>>>::channel_bindings(&queue);
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
