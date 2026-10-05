//! A batch handler: one claim is the batch, each delivery settles on its own.

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
use ruststream_sqlx::{InboxQueue, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::Postgres;

use live::{Plain, database, plain_rows};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Line {
    n: u32,
}

#[subscriber(InboxQueue::<Plain>::new("plain"))]
async fn settle(lines: &[Line]) -> Vec<HandlerOutcome> {
    lines
        .iter()
        .map(|line| {
            if line.n % 2 == 0 {
                HandlerOutcome::ack()
            } else {
                HandlerOutcome::drop()
            }
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_claim_is_the_batch_and_each_delivery_settles_on_its_own() {
    let Some(db) = database().await else { return };
    // Rows written before the app starts: the first claim takes four of them.
    for n in 0..6_u32 {
        sqlx::query("INSERT INTO plain_jobs (payload) VALUES ($1)")
            .bind(serde_json::to_vec(&Line { n }).expect("json"))
            .execute(&db.pool)
            .await
            .expect("the row writes");
    }
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<Plain>("plain");
    let app = RustStream::new(AppInfo::new("batches", "0.0.0")).with_broker(broker, |b| {
        b.include(settle.batch(nonzero!(4)));
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(Duration::from_millis(300))
        .await
        .expect("the batches settle");
    let sizes: Vec<usize> = tb
        .broker::<SqlxBroker<Postgres>>()
        .subscriber("plain")
        .batches::<Line>()
        .iter()
        .map(Vec::len)
        .collect();
    assert_eq!(
        sizes,
        [4, 2],
        "a full claim is followed at once by the next"
    );
    // Every row was settled: acknowledged or dropped, each deleted by its own statement.
    assert_eq!(
        plain_rows(&db.pool, "plain_jobs").await,
        Vec::<Vec<u8>>::new()
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}
