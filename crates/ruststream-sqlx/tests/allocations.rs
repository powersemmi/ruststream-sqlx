//! What a message costs in allocations on the paths that look a name up at run time, in row mode,
//! in the headers layout and on a wake-up: a by-name subscription allocates what a typed one does,
//! in both forms and whatever its ids hold, a `Routed` publish what a `Repository` one does, a
//! row-mode subscription, single or batched, what a payload-mode one over the same table does, a
//! headers-layout delivery what a flat row-mode one does until its headers are read and then what
//! its header map needs, and a publish that wakes a waiting subscription what one that wakes none
//! does.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json"
))]

mod live;

use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use ruststream::{
    BatchSubscriber, Broker, Bytes, Carries, CarriesBatch, ConnectedBroker, HeaderMap,
    IncomingMessage, Lend, OutgoingMessage, PublishPolicy, Publisher, Str, Subscribe, Subscriber,
    SubscriptionSource, nonzero,
};
use ruststream_sqlx::{
    ConnectedSqlxBroker, Inbox, InboxQueue, Publish, Repository, Routed, SqlxBroker,
    SqlxBrokerError,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgConnection, PgPool, Postgres};
use tokio::time::timeout;

use live::postgres::{URL, database};
use live::rows::lease;
use live::rows::row_lock::{OrderJob, Plain};
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

/// `plain_jobs` in row mode: the payload column is a data field of the same type, so a row costs
/// the driver's decode of the same columns as `Plain`'s.
#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
#[inbox(table = "plain_jobs")]
struct PlainAsRow {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    payload: Vec<u8>,
}

/// `plain_jobs` in row mode, in the lease form.
#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
#[inbox(table = "plain_jobs")]
struct LeasedAsRow {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    payload: Vec<u8>,
}

/// `headed_jobs` read flat, in row mode: the headers layout's columns as the struct's own fields,
/// so a row costs the driver's decode of the same columns as `OrderJob`'s.
#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
#[inbox(table = "headed_jobs")]
struct HeadedAsRow {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(attempt, generated)]
    attempt: i16,
    tenant: String,
    trace: Option<String>,
    order_id: i64,
    note: Option<String>,
}

/// The group of the headed jobs, and the name of their subscription.
const ORDERS: &str = "orders";

/// How long a subscription with nothing to claim is given to reach its wait.
const PARK: Duration = Duration::from_millis(50);

/// How long a woken subscription is given to claim the row: far below its poll interval.
const WOKEN: Duration = Duration::from_secs(5);

/// How many rows a measured batch claims.
const BATCH: NonZeroUsize = nonzero!(10usize);

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

/// Writes `count` jobs of the group `orders` into `headed_jobs`, each with a tenant, a trace and
/// an order.
async fn fill_headed(pool: &PgPool, count: u64) {
    sqlx::query(
        "INSERT INTO headed_jobs (name, tenant, trace, order_id, note) \
         SELECT $1, 'acme', 'trace-' || lpad(n::text, 6, '0'), n, 'a note' \
         FROM generate_series(1, $2) AS n",
    )
    .bind(ORDERS)
    .bind(i64::try_from(count).expect("a count of rows"))
    .execute(pool)
    .await
    .expect("the jobs write");
}

/// Allocations per message to claim, `read` and acknowledge `MESSAGES` rows from `deliveries`,
/// after `WARMUP` of them.
async fn claim_and_ack<Message, Deliveries>(deliveries: Deliveries, read: fn(&Message)) -> u64
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
        read(&delivery);
        delivery.ack().await.expect("the ack");
    }
    (blocks() - start) / MESSAGES
}

/// What a codec reads of a payload-mode delivery.
fn read_payload<Message: IncomingMessage>(delivery: &Message) {
    assert!(
        !delivery.payload().is_empty(),
        "a payload-mode delivery lends its payload"
    );
}

/// What a `&Row` handler reads of a row-mode delivery.
fn read_row<Row, Message: Carries<Row>>(delivery: &Message) {
    assert!(
        delivery.carried().is_some(),
        "a row-mode delivery lends its row"
    );
}

/// What a handler that takes `Context` headers reads of a headers-layout delivery: the map, which
/// this first call builds.
fn read_headers<Message: IncomingMessage>(delivery: &Message) {
    assert_eq!(
        delivery.headers().len(),
        3,
        "`tenant`, `trace` and `order_id`"
    );
}

/// Allocations of the header map an `OrderJob` delivery's headers come to, built by hand at its
/// least: the map's storage and one buffer per value. A name is its column's, a constant the map
/// holds without a copy.
fn header_map_cost() -> u64 {
    let start = blocks();
    let mut headers = HeaderMap::with_capacity(3);
    headers.insert(Str::from_static("tenant"), Bytes::from(b"acme".to_vec()));
    headers.insert(
        Str::from_static("trace"),
        Bytes::from(b"trace-000001".to_vec()),
    );
    headers.insert(Str::from_static("order_id"), Bytes::from(b"1".to_vec()));
    let cost = blocks() - start;
    drop(headers);
    cost
}

/// Allocations per message to publish `MESSAGES` messages to `plain` through `publisher`, after
/// `WARMUP` of them, each while `subscriber` waits on its interval with nothing to claim: the
/// publish wakes it, and it claims and acknowledges the row outside the measure.
async fn publish_waking<Live, Subscription>(publisher: &Live, subscriber: &mut Subscription) -> u64
where
    Live: Publisher<Payload = Lend, Options = ()>,
    Subscription: Subscriber<Error = SqlxBrokerError>,
{
    let mut deliveries = pin!(subscriber.stream());
    let mut cost = 0;
    for message in 0..WARMUP + MESSAGES {
        assert!(
            timeout(PARK, deliveries.next()).await.is_err(),
            "the subscription finds nothing and waits"
        );
        let start = blocks();
        publisher
            .publish(OutgoingMessage::new("plain", b"\x01"), None)
            .await
            .expect("the publish");
        if message >= WARMUP {
            cost += blocks() - start;
        }
        let delivery = timeout(WOKEN, deliveries.next())
            .await
            .expect("the publish wakes the subscription")
            .expect("a row")
            .expect("a claim");
        delivery.ack().await.expect("the ack");
    }
    cost / MESSAGES
}

/// Allocations per message of a typed subscription through `source`, reading each delivery as
/// `read` does, over `WARMUP + MESSAGES` rows.
async fn typed_subscription_cost<Source>(
    connected: &ConnectedSqlxBroker<Postgres>,
    source: Source,
    read: fn(&<Source::Subscriber as Subscriber>::Message),
) -> u64
where
    Source: SubscriptionSource<ConnectedSqlxBroker<Postgres>>,
    Source::Subscriber: Subscriber<Error = SqlxBrokerError>,
{
    let mut subscriber = source
        .subscribe(connected)
        .await
        .expect("the typed subscription opens");
    claim_and_ack(subscriber.stream(), read).await
}

/// Allocations per batch to claim, `read` and acknowledge `MESSAGES` rows in batches of `BATCH`
/// through `source`, after `WARMUP` rows of them.
async fn batch_cost<Source>(
    connected: &ConnectedSqlxBroker<Postgres>,
    source: Source,
    read: fn(&<Source::Subscriber as BatchSubscriber>::Batch),
) -> u64
where
    Source: SubscriptionSource<ConnectedSqlxBroker<Postgres>>,
    Source::Subscriber: BatchSubscriber<Error = SqlxBrokerError>,
    <Source::Subscriber as BatchSubscriber>::Batch:
        IntoIterator<Item = <Source::Subscriber as Subscriber>::Message>,
{
    let mut subscriber = source
        .subscribe(connected)
        .await
        .expect("the batch subscription opens");
    let mut batches = pin!(subscriber.batches(BATCH));
    let size = BATCH.get() as u64;
    let mut start = 0;
    for batch in 0..(WARMUP + MESSAGES) / size {
        if batch == WARMUP / size {
            start = blocks();
        }
        let batch = batches.next().await.expect("a batch").expect("a claim");
        read(&batch);
        let mut count = 0;
        for delivery in batch {
            delivery.ack().await.expect("the ack");
            count += 1;
        }
        assert_eq!(count, size, "a full batch");
    }
    (blocks() - start) / (MESSAGES / size)
}

/// What a codec reads of a payload-mode batch.
fn read_payloads<Message: IncomingMessage>(batch: &[Message]) {
    assert!(batch.iter().all(|delivery| !delivery.payload().is_empty()));
}

/// What a `&[Row]` handler reads of a row-mode batch.
fn read_rows<Row, Batch: CarriesBatch<Row>>(batch: &Batch) {
    assert_eq!(batch.carried().len(), BATCH.get(), "a row per delivery");
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
    let typed_cost = claim_and_ack(subscriber.stream(), read_payload).await;
    drop(subscriber);
    let mut named = connected
        .subscribe(&name)
        .await
        .expect("the by-name subscription opens");
    let named_cost = claim_and_ack(named.stream(), read_payload).await;
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

/// The headers layout: until a handler reads the headers, a delivery costs what a flat row-mode
/// one over the same columns does; its first `headers()` builds the map, and nothing beyond it.
/// `fills` writes the rows.
async fn assert_headers_layout(connected: &ConnectedSqlxBroker<Postgres>, fills: &PgPool) {
    fill_headed(fills, WARMUP + MESSAGES).await;
    let flat_cost = typed_subscription_cost(
        connected,
        InboxQueue::<HeadedAsRow>::new(ORDERS),
        read_row::<HeadedAsRow, _>,
    )
    .await;
    fill_headed(fills, 2 * (WARMUP + MESSAGES)).await;
    let unread_cost = typed_subscription_cost(
        connected,
        InboxQueue::<OrderJob>::new(ORDERS),
        read_row::<OrderJob, _>,
    )
    .await;
    dhat::assert_eq!(unread_cost, flat_cost);
    let read_cost =
        typed_subscription_cost(connected, InboxQueue::<OrderJob>::new(ORDERS), |delivery| {
            read_row::<OrderJob, _>(delivery);
            read_headers(delivery);
        })
        .await;
    let map_cost = header_map_cost();
    dhat::assert_eq!(read_cost, unread_cost + map_cost);
}

/// The wake-up: a publish into a table no subscription of the broker reads wakes nobody; one into
/// a table whose subscription waits on its interval wakes it, and costs the same. `fills` empties
/// the table between the measures.
async fn assert_wake_ups(pool: &PgPool, fills: &PgPool) {
    sqlx::query("DELETE FROM plain_jobs")
        .execute(fills)
        .await
        .expect("the table empties");
    let waking = SqlxBroker::new(pool.clone())
        .poll_interval(Duration::from_secs(3600))
        .route::<Plain>("plain")
        .connect()
        .await
        .expect("the broker connects");
    let repository = Repository::<Plain>::default()
        .pair(&waking)
        .await
        .expect("the repository pairs");
    let routed = Routed.pair(&waking).await.expect("the route table pairs");
    let repository_alone = publish(&repository).await;
    let routed_alone = publish(&routed).await;
    sqlx::query("DELETE FROM plain_jobs")
        .execute(fills)
        .await
        .expect("the table empties");
    let mut subscriber = InboxQueue::<Plain>::new("plain")
        .subscribe(&waking)
        .await
        .expect("the subscription opens");
    let repository_waking = publish_waking(&repository, &mut subscriber).await;
    let routed_waking = publish_waking(&routed, &mut subscriber).await;
    dhat::assert_eq!(repository_waking, repository_alone);
    dhat::assert_eq!(routed_waking, routed_alone);
    drop(subscriber);
    waking.shutdown().await.expect("the broker shuts down");
}

// One test in this binary: dhat's testing profiler is one per process.
#[tokio::test(flavor = "current_thread")]
async fn each_path_allocates_what_its_reference_does() {
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

    // Row mode over the same table: the driver decodes the same columns, and the delivery lends
    // the row it decoded where payload mode lends one of its columns.
    fill(&db.pool, WARMUP + MESSAGES).await;
    let row_cost = typed_subscription_cost(
        &connected,
        InboxQueue::<PlainAsRow>::new("plain"),
        read_row::<PlainAsRow, _>,
    )
    .await;
    dhat::assert_eq!(row_cost, typed_cost);

    // A batch: payload mode hands over its deliveries, row mode its rows as one slice beside the
    // claim, and each costs the same per batch in this form.
    fill(&db.pool, 2 * (WARMUP + MESSAGES)).await;
    let payload_batch = batch_cost(&connected, InboxQueue::<Plain>::new("plain"), |batch| {
        read_payloads(batch);
    })
    .await;
    let row_batch = batch_cost(
        &connected,
        InboxQueue::<PlainAsRow>::new("plain"),
        read_rows::<PlainAsRow, _>,
    )
    .await;
    dhat::assert_eq!(row_batch, payload_batch);

    // The lease form: each delivery enters its subscription's lease book and settles by its lease.
    // The lease is long enough that no round of extensions falls inside the measure.
    let leased = SqlxBroker::new(pool.clone())
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
    fill(&db.pool, WARMUP + MESSAGES).await;
    let leased_row = typed_subscription_cost(
        &leased,
        InboxQueue::<LeasedAsRow>::new("plain"),
        read_row::<LeasedAsRow, _>,
    )
    .await;
    dhat::assert_eq!(leased_row, leased_typed);
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

    assert_headers_layout(&connected, &db.pool).await;
    connected.shutdown().await.expect("the broker shuts down");
    assert_wake_ups(&pool, &db.pool).await;
    db.finish().await;
}
