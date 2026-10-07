//! The advisory lock form on Postgres, where the database shows each session's locks and state:
//! the session that holds a delivery's key holds its transaction too.

#![cfg(feature = "postgres")]

use ruststream::testing::TestApp;
use ruststream_sqlx::prelude::*;
use sqlx::Postgres;

use super::{JOB, Job, POLL, audit};
use crate::live::postgres::{advisory_locks, database, idle_in_transaction};
use crate::live::rows::advisory::Plain;

#[subscriber(InboxQueue::<Plain>::new("plain"))]
async fn watched(
    job: &Job,
    Ctx(mut tx): Ctx<keys::Tx<Postgres>>,
    Ctx(pool): Ctx<keys::Pool<Postgres>>,
) -> HandlerOutcome {
    let seen = format!(
        "locks {}, idle in transaction {}",
        advisory_locks(&pool).await,
        idle_in_transaction(&pool).await
    );
    sqlx::raw_sql(audit(job, &seen))
        .execute(&mut *tx)
        .await
        .expect("the audit row writes");
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transactional_session_holds_its_lock_while_the_handler_writes() {
    let Some(db) = database().await else { return };
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(POLL)
        .route::<Plain>("plain");
    let app = RustStream::new(AppInfo::new("transactional", "0.0.0")).with_broker(broker, |b| {
        b.include(watched.transactional());
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Postgres>>()
        .message(&JOB)
        .to("plain")
        .publish()
        .await
        .expect("the publish settles");
    let notes: Vec<String> = sqlx::query_scalar("SELECT note FROM audit")
        .fetch_all(&db.pool)
        .await
        .expect("the audit reads");
    assert_eq!(
        notes,
        ["locks 1, idle in transaction 1"],
        "while the handler wrote, its session held the key's lock and the open transaction"
    );
    assert_eq!(
        (
            advisory_locks(&db.pool).await,
            idle_in_transaction(&db.pool).await
        ),
        (0, 0),
        "the acknowledgement committed, released the key and ended the transaction"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}
