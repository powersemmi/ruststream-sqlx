//! What a message costs in allocations on the paths that look a name up at run time: a by-name
//! subscription allocates what a typed one does, and a `Routed` publish what a `Repository` one does.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json"
))]

mod live;

use std::pin::pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, Lend, OutgoingMessage, PublishPolicy, Publisher,
    Subscribe, Subscriber, SubscriptionSource,
};
use ruststream_sqlx::{InboxQueue, Repository, Routed, SqlxBroker, SqlxBrokerError};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Postgres};

use live::postgres::{URL, database};
use live::rows::row_lock::Plain;
use live::{Database, url};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

/// Messages a path runs before it is measured: statement caches, buffers and the pool's
/// connection fill on first use.
const WARMUP: u64 = 10;

/// Messages a path is measured over.
const MESSAGES: u64 = 50;

/// Every allocation of the process so far.
fn blocks() -> u64 {
    dhat::HeapStats::get().total_blocks
}

/// A pool on the test's database that does nothing in the background: one connection, none opened
/// ahead of a call or closed behind one, so every message runs on the same warm connection.
async fn quiet_pool(db: &Database<Postgres>) -> PgPool {
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&db.pool)
        .await
        .expect("the test database names itself");
    let options: PgConnectOptions = url(URL)
        .expect("the stand's URL")
        .parse()
        .expect("the stand's URL parses");
    PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .idle_timeout(None)
        .max_lifetime(None)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options.database(&name))
        .await
        .expect("the test database accepts connections")
}

/// Writes `count` rows into `plain_jobs`.
async fn fill(pool: &PgPool, count: u64) {
    for _ in 0..count {
        sqlx::query("INSERT INTO plain_jobs (payload) VALUES ('\\x01')")
            .execute(pool)
            .await
            .expect("the row writes");
    }
}

/// Allocations per message to claim and acknowledge `MESSAGES` rows from `deliveries`, after
/// `WARMUP` of them.
async fn claim_and_ack<Message, Deliveries>(deliveries: Deliveries) -> u64
where
    Message: IncomingMessage,
    Deliveries: Stream<Item = Result<Message, SqlxBrokerError>>,
{
    let mut deliveries = pin!(deliveries);
    let mut start = 0;
    for message in 0..WARMUP + MESSAGES {
        if message == WARMUP {
            start = blocks();
        }
        let delivery = deliveries.next().await.expect("a row").expect("a claim");
        delivery.ack().await.expect("the ack");
    }
    (blocks() - start) / MESSAGES
}

/// Allocations per message to publish `MESSAGES` messages to `plain` through `publisher`, after
/// `WARMUP` of them.
async fn publish<Live>(publisher: &Live) -> u64
where
    Live: Publisher<Payload = Lend, Options = ()>,
{
    let mut start = 0;
    for message in 0..WARMUP + MESSAGES {
        if message == WARMUP {
            start = blocks();
        }
        publisher
            .publish(OutgoingMessage::new("plain", b"\x01"), None)
            .await
            .expect("the publish");
    }
    (blocks() - start) / MESSAGES
}

// One test in this binary: dhat's testing profiler is one per process.
#[tokio::test(flavor = "current_thread")]
async fn named_paths_allocate_what_typed_paths_do() {
    let Some(db) = database().await else { return };
    let pool = quiet_pool(&db).await;
    fill(&db.pool, 2 * (WARMUP + MESSAGES)).await;
    // A failed assertion saves the profile beside the build, not in the source tree.
    let _profiler = dhat::Profiler::builder()
        .testing()
        .file_name(concat!(
            env!("CARGO_TARGET_TMPDIR"),
            "/allocations-heap.json"
        ))
        .build();
    let connected = SqlxBroker::new(pool)
        .route::<Plain>("plain")
        .connect()
        .await
        .expect("the broker connects");

    let mut typed = InboxQueue::<Plain>::new("plain")
        .subscribe(&connected)
        .await
        .expect("the typed subscription opens");
    let typed_cost = claim_and_ack(typed.stream()).await;
    drop(typed);
    let mut named = connected
        .subscribe("plain")
        .await
        .expect("the by-name subscription opens");
    let named_cost = claim_and_ack(named.stream()).await;
    drop(named);
    dhat::assert_eq!(named_cost, typed_cost);

    let repository = Repository::<Plain>::default()
        .pair(&connected)
        .await
        .expect("the repository pairs");
    let routed = Routed
        .pair(&connected)
        .await
        .expect("the route table pairs");
    let repository_cost = publish(&repository).await;
    let routed_cost = publish(&routed).await;
    dhat::assert_eq!(routed_cost, repository_cost);

    connected.shutdown().await.expect("the broker shuts down");
    db.finish().await;
}
