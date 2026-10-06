//! What a message costs in allocations on the paths that look a name up at run time: a by-name
//! subscription allocates what a typed one does, in both forms and whatever its ids hold, and a
//! `Routed` publish what a `Repository` one does.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json"
))]

mod live;

use std::pin::pin;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, Lend, OutgoingMessage, PublishPolicy, Publisher,
    Subscribe, Subscriber, SubscriptionSource,
};
use ruststream_sqlx::{
    ConnectedSqlxBroker, Inbox, InboxQueue, Publish, Repository, Routed, SqlxBroker,
    SqlxBrokerError,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgConnection, PgPool, Postgres};

use live::postgres::{URL, database};
use live::rows::lease;
use live::rows::row_lock::Plain;
use live::{Database, url};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

/// Messages a path runs before it is measured: statement caches, buffers and the pool's
/// connection fill on first use.
const WARMUP: u64 = 10;

/// Messages a path is measured over.
const MESSAGES: u64 = 50;

/// A lease queue shaped like `plain_jobs` whose ids are text: a delivery's id has storage of its
/// own, which the lease book copies into storage it keeps.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "keyed_jobs")]
struct Keyed {
    #[field(id)]
    id: String,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for Keyed {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO keyed_jobs (id, payload) VALUES (gen_random_uuid()::text, $1)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

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

/// Writes `count` rows into `keyed_jobs`, their ids of one length.
async fn fill_keyed(pool: &PgPool, count: u64) {
    sqlx::query(
        "INSERT INTO keyed_jobs (id, payload) \
         SELECT 'job-' || lpad(n::text, 6, '0'), '\\x01' FROM generate_series(1, $1) AS n",
    )
    .bind(i64::try_from(count).expect("a count of rows"))
    .execute(pool)
    .await
    .expect("the rows write");
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

/// Allocations per message of a subscription through `typed`, then of a by-name subscription to
/// its name, each over `WARMUP + MESSAGES` rows.
async fn subscription_costs<Source>(
    connected: &ConnectedSqlxBroker<Postgres>,
    typed: Source,
) -> (u64, u64)
where
    Source: SubscriptionSource<ConnectedSqlxBroker<Postgres>>,
    Source::Subscriber: Subscriber<Error = SqlxBrokerError>,
{
    let name = typed.name().to_owned();
    let mut subscriber = typed
        .subscribe(connected)
        .await
        .expect("the typed subscription opens");
    let typed_cost = claim_and_ack(subscriber.stream()).await;
    drop(subscriber);
    let mut named = connected
        .subscribe(&name)
        .await
        .expect("the by-name subscription opens");
    let named_cost = claim_and_ack(named.stream()).await;
    drop(named);
    (typed_cost, named_cost)
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
    let connected = SqlxBroker::new(pool.clone())
        .route::<Plain>("plain")
        .connect()
        .await
        .expect("the broker connects");

    let (typed_cost, named_cost) =
        subscription_costs(&connected, InboxQueue::<Plain>::new("plain")).await;
    dhat::assert_eq!(named_cost, typed_cost);

    // The lease form: each delivery enters its subscription's lease book and settles by its lease.
    // The lease is long enough that no round of extensions falls inside the measure.
    let leased = SqlxBroker::new(pool)
        .lease(Duration::from_secs(600))
        .route::<lease::Plain>("plain")
        .route::<Keyed>("keyed")
        .connect()
        .await
        .expect("the broker connects");
    fill(&db.pool, 2 * (WARMUP + MESSAGES)).await;
    let (leased_typed, leased_named) =
        subscription_costs(&leased, InboxQueue::<lease::Plain>::new("plain")).await;
    dhat::assert_eq!(leased_named, leased_typed);
    // Ids with storage of their own: a text id costs its decode alone, for the book copies each
    // into the storage the id before it left.
    fill_keyed(&db.pool, 2 * (WARMUP + MESSAGES)).await;
    let (keyed_typed, keyed_named) =
        subscription_costs(&leased, InboxQueue::<Keyed>::new("keyed")).await;
    dhat::assert_eq!(keyed_named, keyed_typed);
    dhat::assert_eq!(keyed_typed, leased_typed + 1);
    leased.shutdown().await.expect("the broker shuts down");

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
