//! Subscriptions by name, run as an application through `TestApp::start_live` against the stand:
//! `#[subscriber("emails")]` reads the table the name's route leads to.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::prelude::*;
use ruststream::testing::{Outcome, TestApp};
use ruststream_sqlx::keys::Attempt;
use ruststream_sqlx::{Inbox, Publish, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Postgres};

use live::{SendEmail, database, email_rows};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Email {
    to: String,
}

fn email() -> Email {
    Email {
        to: "a@example.com".to_owned(),
    }
}

fn broker(pool: &PgPool) -> SqlxBroker<Postgres> {
    SqlxBroker::new(pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<SendEmail>("emails")
}

#[subscriber("emails")]
async fn send(_email: &Email) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_mounted_by_name_reads_the_table_its_route_leads_to() {
    let Some(db) = database().await else { return };
    let app = RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(send);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Postgres>>()
        .message(&email())
        .to("emails")
        .publish()
        .await
        .expect("the publish settles");

    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("emails")
        .assert_called_once()
        .with(&email())
        .settled(HandlerOutcome::ack());
    let rows = email_rows(&db.pool, "email_jobs").await;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].3, "an acknowledged row carries processed_at");
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[subscriber("orders")]
async fn bill(_email: &Email) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_name_without_a_route_stops_the_service() {
    let Some(db) = database().await else { return };
    let app = RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(bill);
    });
    let refused = TestApp::start_live(app).await.map(|_| ());
    let message = refused
        .expect_err("a name no route leads to a table cannot open")
        .to_string();
    assert!(message.contains("no route leads `orders`"), "{message}");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_cap_on_a_name_stops_the_service() {
    let Some(db) = database().await else { return };
    // The cap and the destination belong to the table's descriptor, which a bare name lacks.
    let app = RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(send).max_attempts(nonzero!(3u32));
    });
    let refused = TestApp::start_live(app).await.map(|_| ());
    let message = format!(
        "{:?}",
        refused.expect_err("a declared cap on a name is refused")
    );
    assert!(message.contains("RetryDeclareError"), "{message}");
    assert!(message.contains("emails"), "{message}");
    db.finish().await;
}

/// A queue of the other column types a by-name subscription reads: an `INTEGER` id and attempt, a
/// byte key and a text payload.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "text_jobs")]
struct TextJob {
    #[field(id, generated)]
    id: i32,
    #[field(partition_key)]
    tenant: Option<Vec<u8>>,
    #[field(attempt, generated)]
    attempt: i32,
    #[field(payload)]
    payload: String,
}

impl Publish<Postgres> for TextJob {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        let payload = str::from_utf8(message.payload())
            .map_err(|error| sqlx::Error::Encode(Box::new(error)))?;
        // Every job of this service belongs to one tenant.
        sqlx::query("INSERT INTO text_jobs (tenant, payload) VALUES ($1, $2)")
            .bind(b"acme".as_slice())
            .bind(payload)
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[subscriber("texts")]
async fn retry_once(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
    if attempt < Some(2) {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_by_name_row_of_other_column_types_settles_by_its_role_columns() {
    let Some(db) = database().await else { return };
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<TextJob>("texts");
    let app = RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker, |b| {
        b.include(retry_once);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Postgres>>()
        .message(&email())
        .to("texts")
        .publish()
        .await
        .expect("the publish settles");
    tb.advance(Duration::from_millis(500))
        .await
        .expect("the retry settles");
    // The retry counted the attempt by the row's `INTEGER` id, and the text reached the codec.
    let outcomes = tb
        .broker::<SqlxBroker<Postgres>>()
        .subscriber("texts")
        .assert_called(2)
        .with(&email())
        .outcomes();
    assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM text_jobs")
        .fetch_one(&db.pool)
        .await
        .expect("the table counts");
    assert_eq!(left, 0, "the acknowledgement deleted the row by its id");
    tb.shutdown().await.expect("the app stops");
    let _ = |row: TextJob| (row.id, row.tenant, row.attempt, row.payload);
    db.finish().await;
}
