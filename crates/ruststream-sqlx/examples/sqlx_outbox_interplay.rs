//! The outbox beside the inbox: a transactional inbox handler sends a tracked receipt into another
//! queue table and writes the order through the delivery's transaction. The receipt's record is
//! written on a connection of its own, so it stays whatever the delivery's outcome; its id rides
//! the receipt table's headers column, so the consumer's acknowledgement marks the record.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example sqlx_outbox_interplay --features outbox,inbox,sqlite,chrono,json,testing
//! ```

use std::collections::BTreeMap;
use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_sqlx::HeaderColumn;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:interplay]
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{Outbox, outbox};
use sqlx::SqlitePool;

/// The `headers` column: a JSON object of strings, `NULL` for none.
type Headers = Option<Json<BTreeMap<String, String>>>;

/// A queue of receipts whose headers column carries the outbox's record id.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "receipt_jobs")]
struct ReceiptJob {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(headers)]
    headers: Headers,
    #[field(payload)]
    payload: Vec<u8>,
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
#[subscriber(InboxQueue::<OrderJob>::new("orders"))]
async fn take(
    order: &PlaceOrder,
    Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
    Out(out): Out<impl Publisher>,
) -> HandlerOutcome {
    if out
        .message(&Receipt { order: order.id })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    let written = sqlx::query("INSERT INTO orders (id) VALUES (?)")
        .bind(order.id)
        .execute(&mut *tx)
        .await;
    match (written, order.valid) {
        (Ok(_), true) => HandlerOutcome::ack(),
        // Dropped: the transaction discards the order's row, and the receipt stays sent.
        (Ok(_), false) => HandlerOutcome::drop(),
        (Err(_), _) => HandlerOutcome::retry(),
    }
}

/// The acknowledgement marks the receipt's record processed.
#[subscriber(InboxQueue::<ReceiptJob>::new("receipts"))]
async fn file(receipt: &Receipt) -> HandlerOutcome {
    tracing::info!(order = receipt.order, "filing the receipt");
    HandlerOutcome::ack()
}

fn service(pool: SqlitePool) -> impl App {
    let tracking = outbox! { pool: pool.clone(), "receipts" => ReceiptOutbox };
    let broker = SqlxBroker::new(pool).route::<OrderJob>("orders");
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(broker, |b| {
            b.include(take.transactional())
                .out(DefaultSlot, Repository::<ReceiptJob>::default())
                .build();
            b.include(file);
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
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "order_jobs")]
struct OrderJob {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
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
#[derive(Outbox, sqlx::FromRow)]
#[outbox(table = "outbox")]
struct ReceiptOutbox {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct PlaceOrder {
    id: i64,
    valid: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: i64,
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
        let (pool, _keeper) = database("sqlx_outbox_interplay").await?;
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
        let (pool, _keeper) = database("sqlx_outbox_interplay_dropped").await?;
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
