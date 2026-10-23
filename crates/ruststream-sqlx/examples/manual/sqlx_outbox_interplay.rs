//! The service of `examples/sqlx_outbox_interplay.rs` without the macros: the queue tables and the
//! outbox record are described by hand, and the handlers are named types mounted with
//! `subscriber(..)`.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example manual_sqlx_outbox_interplay --features outbox,inbox,sqlite,chrono,json,testing,asyncapi
//! ```

use std::collections::BTreeMap;
use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::ProcessedAt;
use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable, TrackedName};
use ruststream_sqlx::spec::{Headers as HeadersColumn, Lease, Payload};
use ruststream_sqlx::{HeaderColumn, HeaderRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:interplay]
use ruststream_sqlx::keys::TxContext;
use ruststream_sqlx::outbox::Outbox;
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// The `headers` column: a JSON object of strings, `NULL` for none.
type Headers = Option<Json<BTreeMap<String, String>>>;

/// A queue of receipts whose headers column carries the outbox's record id.
#[derive(sqlx::FromRow)]
struct ReceiptJob {
    id: i64,
    headers: Headers,
    payload: Vec<u8>,
}

impl InboxTable for ReceiptJob {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, HeadersColumn, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("receipt_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .headers(Column::new("headers"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl HeaderRow for ReceiptJob {
    type Column = Headers;

    fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers
    }
}

impl PayloadRow for ReceiptJob {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The row keeps the message's headers, the record id among them.
impl Publish<Sqlite> for ReceiptJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO receipt_jobs (headers, payload) VALUES (?, ?)")
            .bind(Headers::from_headers(message.headers()))
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// The receipt's record commits on its own; the order's row commits with the delivery.
struct Take;

// Transactional mode lends the handler the delivery's transaction through its context.
impl<Receipts> Handle<PlaceOrder, (), Outs<(Receipts,)>, TxContext<Sqlite>> for Take
where
    Receipts: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &PlaceOrder,
        outs: &Outs<(Receipts,)>,
        ctx: &mut Context<'_, TxContext<Sqlite>>,
    ) -> Result<(), HandlerOutcome> {
        let receipt = Receipt { order: order.id };
        if outs
            .get(DefaultSlot)
            .message(&receipt)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        let Ok(Ctx(mut tx)) = Ctx::<keys::Tx<Sqlite>>::from_context(ctx).await;
        let written = sqlx::query("INSERT INTO orders (id) VALUES (?)")
            .bind(order.id)
            .execute(&mut *tx)
            .await;
        match (written, order.valid) {
            (Ok(_), true) => Ok(()),
            // Dropped: the transaction discards the order's row, and the receipt stays sent.
            (Ok(_), false) => Err(HandlerOutcome::drop()),
            (Err(_), _) => Err(HandlerOutcome::retry()),
        }
    }
}

/// The acknowledgement marks the receipt's record processed.
struct File;

impl Handle<Receipt> for File {
    fn handle(
        &self,
        receipt: &Receipt,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order = receipt.order, "filing the receipt");
        ready(Ok(()))
    }
}

/// The name `Receipt` is published under.
struct Receipts;

impl TrackedName for Receipts {
    const NAME: &'static str = "receipts";
}

fn service(pool: SqlitePool) -> impl App {
    let tracking = Outbox::new(pool.clone()).track::<ReceiptOutbox, Receipts>();
    let broker = SqlxBroker::new(pool).route::<OrderJob>("orders");
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(broker, |b| {
            let take = subscriber("orders", Take)
                .map_source(|_| InboxQueue::<OrderJob>::new("orders"))
                .transactional();
            b.include(take.build())
                .out(DefaultSlot, Repository::<ReceiptJob>::default())
                .build();
            let file = subscriber("receipts", File)
                .map_source(|_| InboxQueue::<ReceiptJob>::new("receipts"));
            b.include(file.build());
            b.after_startup(Repository::<ReceiptJob>::default(), tracking.republish());
        })
}
// --8<-- [end:interplay]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:orders.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS order_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS receipt_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    headers TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    payload BLOB NOT NULL,
    processed_at TEXT
);
";

/// A queue of orders to take.
#[derive(sqlx::FromRow)]
struct OrderJob {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for OrderJob {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("order_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for OrderJob {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl Publish<Sqlite> for OrderJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO order_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// One record of the `outbox` table.
#[derive(sqlx::FromRow)]
struct ReceiptOutbox {
    id: i64,
    name: String,
    payload: Vec<u8>,
}

impl OutboxTable for ReceiptOutbox {
    type Id = i64;
    type Table = OutboxSpec<(ProcessedAt,)>;
    const TABLE: Self::Table = OutboxSpec::new(
        "outbox",
        Column::new("id"),
        Column::new("name"),
        Column::new("payload"),
    )
    .processed_at(Column::new("processed_at"));

    fn id(&self) -> &i64 {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl outbox::Publish<Sqlite> for ReceiptOutbox {
    async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES (?, ?) RETURNING id")
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PlaceOrder {
    id: i64,
    valid: bool,
}

impl OutgoingDestination for PlaceOrder {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for PlaceOrder {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    order: i64,
}

impl OutgoingDestination for Receipt {
    type Form = FixedName;
    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{PlaceOrder, Receipt, SCHEMA, service};

    // A test build tracks only with `RUSTSTREAM_SQLX_OUTBOX=on`, read once per process: without it
    // there is nothing to check, and a job that asked for every suite fails instead of passing.
    fn tracking_on() -> bool {
        if env::var("RUSTSTREAM_SQLX_OUTBOX").as_deref() == Ok("on") {
            return true;
        }
        assert!(
            env::var("RUSTSTREAM_REQUIRE_LIVE").map_or(true, |value| value.is_empty()),
            "RUSTSTREAM_REQUIRE_LIVE is set, but RUSTSTREAM_SQLX_OUTBOX is not `on`"
        );
        eprintln!("RUSTSTREAM_SQLX_OUTBOX is not `on`; skipping the outbox test");
        false
    }

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool.
    async fn database(name: &str) -> Result<(SqlitePool, SqliteConnection), Box<dyn Error>> {
        let options: SqliteConnectOptions =
            format!("sqlite:file:{name}?mode=memory&cache=shared").parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((pool, keeper))
    }

    /// The orders written, and whether each receipt's record is processed.
    async fn state(pool: &SqlitePool) -> Result<(Vec<i64>, Vec<bool>), sqlx::Error> {
        let orders = sqlx::query_scalar("SELECT id FROM orders ORDER BY id")
            .fetch_all(pool)
            .await?;
        let records = sqlx::query_scalar("SELECT processed_at IS NOT NULL FROM outbox ORDER BY id")
            .fetch_all(pool)
            .await?;
        Ok((orders, records))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_taken_order_commits_and_its_receipt_is_recorded_and_marked()
    -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("manual_sqlx_outbox_interplay").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&PlaceOrder { id: 1, valid: true })
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("receipts")
            .assert_called_once()
            .with(&Receipt { order: 1 });
        assert_eq!(state(&pool).await?, (vec![1], vec![true]));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_order_discards_its_row_and_its_receipt_stays() -> Result<(), Box<dyn Error>>
    {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("manual_sqlx_outbox_interplay_dropped").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&PlaceOrder {
                id: 2,
                valid: false,
            })
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("receipts")
            .assert_called_once()
            .with(&Receipt { order: 2 });
        assert_eq!(state(&pool).await?, (vec![], vec![true]));
        Ok(())
    }
}
