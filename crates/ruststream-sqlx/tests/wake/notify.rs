//! `LISTEN/NOTIFY` on Postgres: a broker that listens claims a row another process announced with
//! `pg_notify` before its poll interval runs out, announces each row it publishes, keeps listening
//! across a lost connection, and leaves no session listening after `shutdown`.
//!
//! The subject is the transport, so the tests write and announce rows with the test's own SQL, and
//! read what the broker announces and which sessions listen through Postgres itself.

use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgListener;
use sqlx::{AssertSqlSafe, PgPool, Pool};

use super::{ASLEEP, IDLE, INTERVAL, WOKEN};
use crate::live::postgres::{Db, database};
use crate::live::rows::row_lock::SendEmail;

/// A table whose name, qualified with its schema, is longer than the 63 bytes a Postgres channel
/// holds, though each part fits a Postgres name.
const LONG: &str = "notifications_of_a_long_schema.jobs_of_a_queue_whose_name_runs_long";

/// How long a test waits for a notification it expects.
const ANNOUNCED: Duration = Duration::from_secs(5);

/// A job a test writes.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Job {
    n: u32,
}

/// A row of the table [`LONG`] names.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(
    table = "jobs_of_a_queue_whose_name_runs_long",
    schema = "notifications_of_a_long_schema"
)]
struct Long {
    #[field(id, generated)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<Long>::new("long"))]
async fn long(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
}

fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
    SqlxBroker::new(pool.clone())
        .poll_interval(INTERVAL)
        .route::<SendEmail>("a")
        .route::<SendEmail>("c")
        .listen_notify()
}

#[subscriber(InboxQueue::<SendEmail>::new("a"))]
async fn first(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<SendEmail>::new("b"))]
async fn second(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
}

fn app(pool: &PgPool) -> RustStream {
    RustStream::new(AppInfo::new("notified", "0.0.0")).with_broker(broker(pool), |b| {
        b.include(first);
        b.include(second);
    })
}

/// Writes a job of the group `name` from a connection of the test's own, as another process
/// would, without a notification.
async fn write(pool: &PgPool, name: &str, n: u32) {
    let payload = serde_json::to_vec(&Job { n }).expect("json");
    sqlx::query("INSERT INTO email_jobs (name, payload) VALUES ($1, $2)")
        .bind(name)
        .bind(payload)
        .execute(pool)
        .await
        .expect("the row writes");
}

/// Announces a row of `email_jobs` with `payload`, as another process's publish would.
async fn announce(pool: &PgPool, payload: &str) {
    sqlx::query("SELECT pg_notify('email_jobs', $1)")
        .bind(payload)
        .execute(pool)
        .await
        .expect("the notification sends");
}

/// The sessions of the test's database whose last statement was a `LISTEN`.
async fn listening(pool: &PgPool) -> Vec<i32> {
    sqlx::query_scalar(
        "SELECT pid FROM pg_stat_activity WHERE datname = current_database() \
         AND query LIKE 'LISTEN%'",
    )
    .fetch_all(pool)
    .await
    .expect("the sessions read")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_notification_wakes_the_subscription_of_its_group() {
    let Some(db) = database().await else { return };
    let tb = TestApp::start_live_within(app(&db.pool), WOKEN)
        .await
        .expect("the app starts");
    tb.advance(IDLE)
        .await
        .expect("both subscriptions wait their interval");
    write(&db.pool, "b", 2).await;
    write(&db.pool, "a", 1).await;
    announce(&db.pool, "a").await;
    tb.advance(ASLEEP).await.expect("`a` claims its row");
    tb.broker::<SqlxBroker<Db>>()
        .subscriber("a")
        .assert_called_once()
        .with(&Job { n: 1 })
        .settled(HandlerOutcome::ack());
    tb.broker::<SqlxBroker<Db>>()
        .subscriber("b")
        .assert_not_called();
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_nobody_announced_waits_for_the_interval() {
    let Some(db) = database().await else { return };
    let tb = TestApp::start_live_within(app(&db.pool), WOKEN)
        .await
        .expect("the app starts");
    tb.advance(IDLE)
        .await
        .expect("both subscriptions wait their interval");
    write(&db.pool, "a", 1).await;
    tb.advance(ASLEEP).await.expect("`a` sleeps on");
    tb.broker::<SqlxBroker<Db>>()
        .subscriber("a")
        .assert_not_called();
    assert_eq!(db.count("email_jobs").await, 1, "the row waits");
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_notification_without_a_group_wakes_every_subscription_of_the_table() {
    let Some(db) = database().await else { return };
    let tb = TestApp::start_live_within(app(&db.pool), WOKEN)
        .await
        .expect("the app starts");
    tb.advance(IDLE)
        .await
        .expect("both subscriptions wait their interval");
    write(&db.pool, "a", 1).await;
    write(&db.pool, "b", 2).await;
    announce(&db.pool, "").await;
    tb.advance(ASLEEP).await.expect("both claim their rows");
    tb.broker::<SqlxBroker<Db>>()
        .subscriber("a")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.broker::<SqlxBroker<Db>>()
        .subscriber("b")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

/// A publish through the harness, on a live and on an in-process connection, reaches a listener
/// of the test's own as a notification on the table's channel naming the group. The group is one
/// no subscription of the app reads, so the publish returns once its row and its notification are
/// written, whatever the subscriptions are doing.
async fn announces_its_publish(tb: TestApp<()>, pool: &PgPool) {
    let mut listener = PgListener::connect_with(pool)
        .await
        .expect("the test listens");
    listener
        .listen("email_jobs")
        .await
        .expect("the channel listens");
    tb.broker::<SqlxBroker<Db>>()
        .message(&Job { n: 1 })
        .to("c")
        .publish()
        .await
        .expect("the publish is written");
    let notification = tokio::time::timeout(ANNOUNCED, listener.recv())
        .await
        .expect("the publish announces its row")
        .expect("the notification reads");
    assert_eq!(
        (notification.channel(), notification.payload()),
        ("email_jobs", "c")
    );
    listener
        .unlisten_all()
        .await
        .expect("the test stops listening");
    tb.shutdown().await.expect("the app stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_announces_its_row_to_other_processes() {
    let Some(db) = database().await else { return };
    let tb = TestApp::start_live_within(app(&db.pool), WOKEN)
        .await
        .expect("the app starts");
    announces_its_publish(tb, &db.pool).await;
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_in_process_publish_announces_its_row_too() {
    let Some(db) = database().await else { return };
    let tb = TestApp::start(app(&db.pool)).await.expect("the app starts");
    announces_its_publish(tb, &db.pool).await;
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_listening_connection_listens_again_and_wakes_every_subscription() {
    let Some(db) = database().await else { return };
    let tb = TestApp::start_live_within(app(&db.pool), WOKEN)
        .await
        .expect("the app starts");
    tb.advance(IDLE)
        .await
        .expect("both subscriptions wait their interval");
    // Written before the connection is lost: only the wake-up after the reconnect reaches it, for
    // a notification sent while no session listened is gone.
    write(&db.pool, "b", 1).await;
    let listeners = listening(&db.pool).await;
    assert_eq!(listeners.len(), 1, "the broker listens on one session");
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(listeners[0])
        .fetch_one(&db.pool)
        .await
        .expect("the session terminates");
    assert!(terminated);
    tb.advance(ASLEEP).await.expect("the reconnect wakes `b`");
    tb.broker::<SqlxBroker<Db>>()
        .subscriber("b")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    // The wake-up came after the channel was listened again, so a later notification reaches it.
    write(&db.pool, "a", 2).await;
    announce(&db.pool, "a").await;
    tb.advance(ASLEEP).await.expect("`a` claims its row");
    tb.broker::<SqlxBroker<Db>>()
        .subscriber("a")
        .assert_called_once()
        .with(&Job { n: 2 })
        .settled(HandlerOutcome::ack());
    assert_eq!(
        listening(&db.pool).await.len(),
        1,
        "one session listens again"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_leaves_no_session_listening() {
    let Some(db) = database().await else { return };
    let tb = TestApp::start_live_within(app(&db.pool), WOKEN)
        .await
        .expect("the app starts");
    assert_eq!(listening(&db.pool).await.len(), 1, "the broker listens");
    tb.shutdown().await.expect("the app stops");
    assert!(listening(&db.pool).await.is_empty(), "no session listens");
    // Every connection of the pool, the listener's own among them once it is back, listens on no
    // channel.
    let mut connections = Vec::new();
    for _ in 0..db.pool.options().get_max_connections() {
        let mut conn = db
            .pool
            .acquire()
            .await
            .expect("the pool lends a connection");
        let channels: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_listening_channels()")
            .fetch_one(&mut *conn)
            .await
            .expect("the channels read");
        assert_eq!(channels, 0, "a connection of the pool listens");
        connections.push(conn);
    }
    drop(connections);
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_table_named_past_a_channel_stops_its_subscription_at_startup() {
    let Some(db) = database().await else { return };
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE SCHEMA notifications_of_a_long_schema; \
         CREATE TABLE {LONG} (id BIGSERIAL PRIMARY KEY, payload BYTEA NOT NULL)"
    )))
    .execute(&db.pool)
    .await
    .expect("the table creates");
    let app = RustStream::new(AppInfo::new("long", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(long);
    });
    let Err(refused) = TestApp::start_live_within(app, WOKEN).await else {
        panic!("the subscription refuses to start");
    };
    let refused = refused.to_string();
    assert!(
        refused.contains(&format!(
            "subscription `long` on table `{LONG}` (wake::notify::Long): the table's qualified name is \
             {} bytes, and a Postgres notification channel holds at most 63",
            LONG.len()
        )),
        "{refused}"
    );
    db.finish().await;
}
