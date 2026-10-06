//! A by-name subscription of a row that overrides an event settles through the service's code.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::{Ack, Inbox, Publish, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Postgres};

use crate::live::postgres::database;

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Task {
    n: u32,
}

#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "acked_jobs", custom(ack))]
struct AckedJob {
    #[field(id, generated)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Ack<Postgres> for AckedJob {
    async fn ack(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE acked_jobs SET acked = true WHERE id = $1")
            .bind(id)
            .execute(conn)
            .await?;
        Ok(())
    }
}

impl Publish<Postgres> for AckedJob {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO acked_jobs (payload) VALUES ($1)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[subscriber("acked")]
async fn acknowledge(_task: &Task) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_by_name_row_with_its_own_ack_settles_through_it() {
    let Some(db) = database().await else { return };
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<AckedJob>("acked");
    let app = RustStream::new(AppInfo::new("by-name", "0.0.0")).with_broker(broker, |b| {
        b.include(acknowledge);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Postgres>>()
        .message(&Task { n: 1 })
        .to("acked")
        .publish()
        .await
        .expect("the publish settles");
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("acked")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    let acked: Vec<bool> = sqlx::query_scalar("SELECT acked FROM acked_jobs")
        .fetch_all(&db.pool)
        .await
        .expect("the table reads");
    assert_eq!(
        acked,
        [true],
        "the service's ack marked the row; the default would delete it"
    );
    tb.shutdown().await.expect("the app stops");
    let _ = |row: AckedJob| (row.id, row.payload);
    db.finish().await;
}
