//! Events a service implements itself, a claimed id whose row is gone, and where "now" comes from.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::{Ack, Clock, DatabaseClock, Fetch, Inbox, InboxQueue, Publish, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Postgres};

use live::database;

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Order {
    id: i64,
}

/// A job that carries no message of its own: the fetch reads the order's body; the
/// acknowledgement marks the job done instead of deleting it.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "order_jobs", custom(fetch, ack))]
struct OrderJob {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Fetch<Postgres> for OrderJob {
    async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT j.id, o.body AS payload FROM order_jobs j JOIN orders o ON o.id = j.order_id \
             WHERE j.id = ANY($1)",
        )
        .bind(ids)
        .fetch_all(conn)
        .await
    }
}

impl Ack<Postgres> for OrderJob {
    async fn ack(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE order_jobs SET done = true WHERE id = $1")
            .bind(id)
            .execute(conn)
            .await?;
        Ok(())
    }
}

impl Publish<Postgres> for OrderJob {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        let order: Order = serde_json::from_slice(message.payload())
            .map_err(|err| sqlx::Error::Decode(err.into()))?;
        sqlx::query("INSERT INTO order_jobs (order_id) VALUES ($1)")
            .bind(order.id)
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[subscriber(InboxQueue::<OrderJob>::new("orders"))]
async fn fulfil(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_custom_fetch_assembles_rows_and_a_custom_ack_marks_them() {
    let Some(db) = database().await else { return };
    sqlx::query("INSERT INTO orders (id, body) VALUES (7, $1)")
        .bind(serde_json::to_vec(&Order { id: 7 }).expect("json"))
        .execute(&db.pool)
        .await
        .expect("the order writes");
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<OrderJob>("orders");
    let app = RustStream::new(AppInfo::new("custom", "0.0.0")).with_broker(broker, |b| {
        b.include(fulfil);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Postgres>>()
        .message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("the publish settles");
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 7 })
        .settled(HandlerOutcome::ack());
    let done: bool = sqlx::query_scalar("SELECT done FROM order_jobs")
        .fetch_one(&db.pool)
        .await
        .expect("the job reads");
    assert!(done, "the service's own acknowledgement ran");
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claimed_id_without_a_row_is_delivered_undecodable() {
    let Some(db) = database().await else { return };
    // No order 9: the fetch's join finds no row for the job.
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<OrderJob>("orders");
    let app = RustStream::new(AppInfo::new("custom", "0.0.0")).with_broker(broker, |b| {
        b.include(fulfil.on_failure(FailurePolicies::default().with_decode(FailurePolicy::Drop)));
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Postgres>>()
        .message(&Order { id: 9 })
        .to("orders")
        .publish()
        .await
        .expect("the publish settles");
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

/// A queue on the database's clock.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "clock_jobs", clock = DatabaseClock)]
struct OnDatabaseTime {
    #[field(id, generated)]
    id: i64,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for OnDatabaseTime {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO clock_jobs (payload) VALUES ($1)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[subscriber(InboxQueue::<OnDatabaseTime>::new("clock"))]
async fn on_database_time(
    _order: &Order,
    Ctx(attempt): Ctx<ruststream_sqlx::keys::Attempt>,
) -> HandlerOutcome {
    if attempt < Some(2) {
        HandlerOutcome::retry_after(Duration::from_millis(200))
    } else {
        HandlerOutcome::ack()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_database_clock_claims_delays_and_marks_rows() {
    let Some(db) = database().await else { return };
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<OnDatabaseTime>("clock");
    let app = RustStream::new(AppInfo::new("clock", "0.0.0")).with_broker(broker, |b| {
        b.include(on_database_time);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Postgres>>()
        .message(&Order { id: 1 })
        .to("clock")
        .publish()
        .await
        .expect("the publish settles");
    tb.advance(Duration::from_millis(500))
        .await
        .expect("the delay passes");
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("clock")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    let finished: bool = sqlx::query_scalar("SELECT processed_at IS NOT NULL FROM clock_jobs")
        .fetch_one(&db.pool)
        .await
        .expect("the row reads");
    assert!(finished);
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

/// A host clock an hour behind: a row written for now is not due for it.
struct HourBehind;

impl Clock for HourBehind {
    fn now() -> SystemTime {
        SystemTime::now() - Duration::from_secs(3600)
    }
}

#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "clock_jobs", clock = HourBehind)]
struct Behind {
    #[field(id, generated)]
    id: i64,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<Behind>::new("behind"))]
async fn behind(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_clock_decides_when_a_row_is_due() {
    let Some(db) = database().await else { return };
    sqlx::query("INSERT INTO clock_jobs (payload) VALUES ($1)")
        .bind(serde_json::to_vec(&Order { id: 2 }).expect("json"))
        .execute(&db.pool)
        .await
        .expect("the row writes");
    let broker = SqlxBroker::new(db.pool.clone()).poll_interval(Duration::from_millis(20));
    let app = RustStream::new(AppInfo::new("clock", "0.0.0")).with_broker(broker, |b| {
        b.include(behind);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(Duration::from_millis(200))
        .await
        .expect("nothing runs");
    // The row is due for the database and an hour ahead for this clock.
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("behind")
        .assert_not_called();
    tb.shutdown().await.expect("the app stops");
    let _ = |row: Behind| (row.id, row.retry_after, row.payload);
    let _ = |row: OnDatabaseTime| {
        (
            row.id,
            row.retry_after,
            row.attempt,
            row.processed_at,
            row.payload,
        )
    };
    db.finish().await;
}
