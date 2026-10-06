//! Rows whose columns do not decode into their struct: the decode-failure policy settles them and
//! the claim loop goes on, and each reports its attempt as its struct reads it; an id that does
//! not decode fails the claim.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::prelude::*;
use ruststream::testing::{InProcess, TestApp};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscriber, SubscriptionSource,
};
use ruststream_sqlx::{
    Claim, ConnectedSqlxBroker, Inbox, InboxQueue, Publish, SqlxBroker, SqlxBrokerError,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Postgres};

use live::postgres::database;
use live::rows::unreadable;

/// The broker's connected form on the stand.
type Connected = ConnectedSqlxBroker<Postgres>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Task {
    n: u32,
}

/// Reads `payload` as bytes from a column that holds text.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "mistyped_jobs")]
struct Mistyped {
    #[field(id, generated)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for Mistyped {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        let text = String::from_utf8_lossy(message.payload()).into_owned();
        sqlx::query("INSERT INTO mistyped_jobs (payload) VALUES ($1)")
            .bind(text)
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// `note` takes no NULL, so a row without a note does not decode.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "partial_jobs")]
struct Partial {
    #[field(id, generated)]
    id: i64,
    note: String,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The id is an integer in the struct and text in the table.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "text_key_jobs")]
struct TextKey {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

fn decode_drops() -> FailurePolicies {
    FailurePolicies::default().with_decode(FailurePolicy::Drop)
}

#[subscriber(InboxQueue::<Mistyped>::new("mistyped"))]
async fn never_decoded(_task: &Task) -> HandlerOutcome {
    // The decode policy drops what does not decode; reaching this would acknowledge instead.
    HandlerOutcome::ack()
}

async fn rows(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
        .fetch_one(pool)
        .await
        .expect("the table counts")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_that_does_not_decode_is_settled_by_the_decode_policy() {
    let Some(db) = database().await else { return };
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<Mistyped>("mistyped");
    let app = RustStream::new(AppInfo::new("decoding", "0.0.0")).with_broker(broker, |b| {
        b.include(never_decoded.on_failure(decode_drops()));
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    for n in 0..2 {
        tb.broker::<SqlxBroker<Postgres>>()
            .message(&Task { n })
            .to("mistyped")
            .publish()
            .await
            .expect("the publish settles");
    }
    // Both rows reach the policy: the claim loop did not stop at the first.
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("mistyped")
        .assert_called(2)
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    assert_eq!(
        rows(&db.pool, "mistyped_jobs").await,
        0,
        "the policy dropped both rows"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

/// `unreadable_jobs` read with an unsigned attempt, which sqlx converts from the column's
/// `SMALLINT`; the payload column holds an integer, so no row decodes.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "unreadable_jobs")]
struct Converted {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    #[sqlx(try_from = "i16")]
    attempt: u16,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for Converted {
    async fn publish(conn: &mut PgConnection, _: &OutgoingMessage<'_>) -> Result<(), sqlx::Error> {
        unreadable::<Postgres>(conn).await
    }
}

#[subscriber(InboxQueue::<Converted>::new("converted"))]
async fn never_converted(_task: &Task) -> HandlerOutcome {
    // No row of the table decodes; reaching this would acknowledge instead.
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_that_does_not_decode_reports_the_attempt_its_field_converts() {
    let Some(db) = database().await else { return };
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<Converted>("converted");
    let retries = FailurePolicies::default().with_decode(FailurePolicy::Retry);
    let app = RustStream::new(AppInfo::new("decoding", "0.0.0")).with_broker(broker, |b| {
        b.include(never_converted.on_failure(retries))
            .max_attempts(nonzero!(2u32));
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Postgres>>()
        .message(&Task { n: 0 })
        .to("converted")
        .publish()
        .await
        .expect("the publish settles");
    tb.advance(Duration::from_millis(800))
        .await
        .expect("the retries settle");
    // The attempt is read as the field reads it, through `try_from`, so the cap spends the row.
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("converted")
        .assert_called(2)
        .assert_last_failed_to_decode();
    assert_eq!(
        rows(&db.pool, "unreadable_jobs").await,
        0,
        "the second delivery spent the row's attempts, and the cap finished it"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

/// `unreadable_jobs` read with a wider attempt than its `SMALLINT` column holds, which Postgres
/// does not decode.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "unreadable_jobs")]
struct Widened {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The attempt of the first row `subscription` claims.
async fn first_attempt<Row>(subscription: InboxQueue<Row>, connected: &Connected) -> Option<u64>
where
    InboxQueue<Row>: SubscriptionSource<Connected>,
{
    let mut subscriber = subscription
        .subscribe(connected)
        .await
        .expect("the subscription opens");
    let mut deliveries = pin!(subscriber.stream());
    let delivery = deliveries
        .next()
        .await
        .expect("the stream goes on")
        .expect("the claim");
    let attempt = delivery.redelivery_count();
    delivery.ack().await.expect("the row settles");
    attempt
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_whose_attempt_does_not_decode_either_reports_none() {
    let Some(db) = database().await else { return };
    let connected = SqlxBroker::new(db.pool.clone())
        .connect()
        .await
        .expect("the broker connects");
    // The column holds a `SMALLINT`, which the struct reads as an `i64`.
    sqlx::query("INSERT INTO unreadable_jobs (attempt, payload) VALUES (1, 7)")
        .execute(&db.pool)
        .await
        .expect("the row writes");
    let widened = first_attempt(InboxQueue::<Widened>::new("widened"), &connected).await;
    assert_eq!(widened, None, "the struct reads no attempt from the column");
    // A negative attempt, which does not convert into the struct's `u16`.
    sqlx::query("INSERT INTO unreadable_jobs (attempt, payload) VALUES (-1, 7)")
        .execute(&db.pool)
        .await
        .expect("the row writes");
    let converted = first_attempt(InboxQueue::<Converted>::new("converted"), &connected).await;
    assert_eq!(converted, None, "the attempt does not convert");
    assert_eq!(rows(&db.pool, "unreadable_jobs").await, 0);
    connected.shutdown().await.expect("the broker shuts down");
    db.finish().await;
}

#[subscriber("mistyped")]
async fn never_decoded_by_name(_task: &Task) -> HandlerOutcome {
    // The decode policy drops what does not decode; reaching this would acknowledge instead.
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_by_name_row_is_held_to_the_types_of_its_struct() {
    let Some(db) = database().await else { return };
    let broker = SqlxBroker::new(db.pool.clone())
        .poll_interval(Duration::from_millis(20))
        .route::<Mistyped>("mistyped");
    let app = RustStream::new(AppInfo::new("decoding", "0.0.0")).with_broker(broker, |b| {
        b.include(never_decoded_by_name.on_failure(decode_drops()));
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    for n in 0..2 {
        tb.broker::<SqlxBroker<Postgres>>()
            .message(&Task { n })
            .to("mistyped")
            .publish()
            .await
            .expect("the publish settles");
    }
    // The subscription reads the row by its role columns, and a text column still does not hold
    // the bytes the struct reads: both rows reach the policy, as through the struct itself.
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("mistyped")
        .assert_called(2)
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    assert_eq!(
        rows(&db.pool, "mistyped_jobs").await,
        0,
        "the policy dropped both rows"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[subscriber(InboxQueue::<Partial>::new("partial"))]
async fn settle_batch(tasks: &[Task]) -> Vec<HandlerOutcome> {
    tasks.iter().map(|_| HandlerOutcome::ack()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_row_that_does_not_decode_leaves_its_batch_alone() {
    let Some(db) = database().await else { return };
    // Rows written before the app starts, so the first claim takes all three as one batch.
    for (n, note) in [(0_u32, Some("a")), (1, None), (2, Some("c"))] {
        sqlx::query("INSERT INTO partial_jobs (note, payload) VALUES ($1, $2)")
            .bind(note)
            .bind(serde_json::to_vec(&Task { n }).expect("json"))
            .execute(&db.pool)
            .await
            .expect("the row writes");
    }
    let broker = SqlxBroker::new(db.pool.clone()).poll_interval(Duration::from_millis(20));
    let app = RustStream::new(AppInfo::new("decoding", "0.0.0")).with_broker(broker, |b| {
        b.include(settle_batch.batch(nonzero!(3)).on_failure(decode_drops()));
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(Duration::from_millis(300))
        .await
        .expect("the batch settles");
    let batches = tb
        .broker::<SqlxBroker<Postgres>>()
        .subscriber("partial")
        .batches::<Task>();
    assert_eq!(
        batches,
        [vec![Task { n: 0 }, Task { n: 2 }]],
        "the rows that decode reach the handler together"
    );
    assert_eq!(
        rows(&db.pool, "partial_jobs").await,
        0,
        "the batch committed: two rows acknowledged, the third dropped by the policy"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

/// The service claims the ids; the crate fetches their rows.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "partial_jobs", custom(claim))]
struct SelfClaimed {
    #[field(id, generated)]
    id: i64,
    note: String,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Claim<Postgres> for SelfClaimed {
    async fn claim(
        conn: &mut PgConnection,
        _queue: &str,
        limit: i64,
    ) -> Result<Vec<i64>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT id FROM partial_jobs ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED",
        )
        .bind(limit)
        .fetch_all(conn)
        .await
    }
}

#[subscriber(InboxQueue::<SelfClaimed>::new("claimed"))]
async fn fetched_by_id(_task: &Task) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_fetched_by_its_id_that_does_not_decode_is_settled_by_the_decode_policy() {
    let Some(db) = database().await else { return };
    sqlx::query("INSERT INTO partial_jobs (note, payload) VALUES (NULL, $1)")
        .bind(serde_json::to_vec(&Task { n: 0 }).expect("json"))
        .execute(&db.pool)
        .await
        .expect("the row writes");
    let broker = SqlxBroker::new(db.pool.clone()).poll_interval(Duration::from_millis(20));
    let app = RustStream::new(AppInfo::new("decoding", "0.0.0")).with_broker(broker, |b| {
        b.include(fetched_by_id.on_failure(decode_drops()));
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(Duration::from_millis(300))
        .await
        .expect("the row settles");
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("claimed")
        .assert_called_once()
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    assert_eq!(
        rows(&db.pool, "partial_jobs").await,
        0,
        "the policy dropped the row"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

/// What the flattened struct reads: a note that takes no NULL.
#[derive(Debug, sqlx::FromRow)]
struct Note {
    note: String,
}

/// Flattens `Note`, so the statements select `*`, and the table holds the id last.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "flat_jobs")]
struct Flat {
    #[sqlx(flatten)]
    note: Note,
    #[field(payload)]
    payload: Vec<u8>,
    #[field(id, generated)]
    id: i64,
}

#[subscriber(InboxQueue::<Flat>::new("flat"))]
async fn flattened(_task: &Task) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flattened_row_that_does_not_decode_is_found_by_its_id_column() {
    let Some(db) = database().await else { return };
    sqlx::query("INSERT INTO flat_jobs (note, payload) VALUES (NULL, $1)")
        .bind(serde_json::to_vec(&Task { n: 0 }).expect("json"))
        .execute(&db.pool)
        .await
        .expect("the row writes");
    let broker = SqlxBroker::new(db.pool.clone()).poll_interval(Duration::from_millis(20));
    let app = RustStream::new(AppInfo::new("decoding", "0.0.0")).with_broker(broker, |b| {
        b.include(flattened.on_failure(decode_drops()));
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(Duration::from_millis(300))
        .await
        .expect("the row settles");
    tb.broker::<SqlxBroker<Postgres>>()
        .subscriber("flat")
        .assert_called_once()
        .settled(HandlerOutcome::drop())
        .assert_last_failed_to_decode();
    assert_eq!(
        rows(&db.pool, "flat_jobs").await,
        0,
        "the policy dropped the row"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

// The in-process mode keeps a paused clock still while the database answers.
#[tokio::test]
async fn an_id_that_does_not_decode_fails_the_claim() {
    let Some(db) = database().await else { return };
    sqlx::query("INSERT INTO text_key_jobs (id, payload) VALUES ('k', '\\x00')")
        .execute(&db.pool)
        .await
        .expect("the row writes");
    let connected = SqlxBroker::new(db.pool.clone())
        .connect_in_process()
        .await
        .expect("the broker connects");
    let mut subscriber = InboxQueue::<TextKey>::new("text")
        .subscribe(&connected)
        .await
        .expect("the subscription opens: the startup check reads names");
    {
        let mut deliveries = pin!(subscriber.stream());
        let failed = deliveries
            .next()
            .await
            .expect("the stream goes on")
            .expect_err("a row nobody can settle fails the claim");
        assert!(
            matches!(&failed, SqlxBrokerError::Sqlx { subscription, table, .. }
                if subscription == "text" && table == "text_key_jobs"),
            "{failed:?}"
        );
    }
    drop(subscriber);
    connected.shutdown().await.expect("the broker shuts down");
    let _ = |row: TextKey| (row.id, row.payload);
    let _ = |row: Partial| (row.id, row.note, row.payload);
    let _ = |row: SelfClaimed| (row.id, row.note, row.payload);
    let _ = |row: Flat| (row.id, row.note.note, row.payload);
    db.finish().await;
}
