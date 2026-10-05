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

use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::SqlxBroker;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres};

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
