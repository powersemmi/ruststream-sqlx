//! The core's conformance suites against the stand: the routing contract over by-name
//! subscriptions of a payload-mode table, and the lifecycle ladder through a typed descriptor and
//! repository.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::executor::block_on;
use ruststream::conformance::harness;
use ruststream::{OutgoingMessage, PublishPolicy};
use ruststream_sqlx::{HeaderColumn, Inbox, InboxQueue, Publish, Repository, SqlxBroker};
use sqlx::types::Json;
use sqlx::{PgConnection, PgPool, Postgres};

use live::database;

/// The by-name table: a group per name, a native delayed retry, headers and an attempt.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "conformance_jobs")]
struct Conformance {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(headers)]
    meta: Option<Json<BTreeMap<String, String>>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for Conformance {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO conformance_jobs (name, meta, payload) VALUES ($1, $2, $3)")
            .bind(message.name())
            .bind(Option::<Json<BTreeMap<String, String>>>::from_headers(
                message.headers(),
            ))
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// The lifecycle table: a group per name and a native delayed retry. It keeps no headers, so a
/// publish that carries some is refused.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "lifecycle_jobs")]
struct Lifecycle {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for Lifecycle {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO lifecycle_jobs (name, payload) VALUES ($1, $2)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

fn broker(pool: &PgPool) -> SqlxBroker<Postgres> {
    // The suites subscribe to names they generate; a prefix route opens them all.
    SqlxBroker::new(pool.clone())
        .poll_interval(Duration::from_millis(50))
        .route::<Conformance>("conformance.*")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn by_name_subscriptions_pass_the_routing_contract() {
    let Some(db) = database().await else { return };
    let pool = db.pool.clone();
    harness::run_suite(move || broker(&pool)).await;
    let _ = |row: Conformance| {
        (
            row.id,
            row.name,
            row.retry_after,
            row.attempt,
            row.meta,
            row.payload,
        )
    };
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_typed_descriptor_and_repository_climb_the_lifecycle() {
    let Some(db) = database().await else { return };
    let pool = db.pool.clone();
    harness::lifecycle(
        move || SqlxBroker::new(pool.clone()).poll_interval(Duration::from_millis(50)),
        |name| InboxQueue::<Lifecycle>::new(name.to_owned()),
        // Pairing a repository does no I/O, so its future is ready at once.
        |connected| {
            block_on(Repository::<Lifecycle>::default().pair(connected))
                .expect("a repository pairs with the connected broker")
        },
    )
    .await;
    let _ = |row: Lifecycle| (row.id, row.name, row.retry_after, row.attempt, row.payload);
    db.finish().await;
}
