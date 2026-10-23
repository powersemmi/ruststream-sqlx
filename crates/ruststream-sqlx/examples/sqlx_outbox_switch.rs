//! A test of a service on the outbox, in a test build: the outbox is off unless the process runs
//! with `RUSTSTREAM_SQLX_OUTBOX=on`, so the test's database needs no outbox table.
//!
//! ```text
//! cargo test -p ruststream-sqlx --example sqlx_outbox_switch --features outbox,sqlite,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream::memory::prelude::*;
use ruststream_sqlx::{Outbox, outbox};
use sqlx::SqlitePool;

/// One record of the `outbox` table.
#[derive(Outbox, sqlx::FromRow)]
#[outbox(table = "outbox")]
struct OrderOutbox {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
}

/// The service writes the record; the outbox sends its id with the message.
impl outbox::Publish<Sqlite> for OrderOutbox {
    async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES (?, ?) RETURNING id")
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}

/// The reply to `orders` is recorded before it is sent.
#[subscriber("checkout", reply)]
async fn place(cmd: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: cmd.id }
}

/// After the acknowledgement, the record is marked processed.
#[subscriber("orders")]
async fn fulfil(order: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(order = order.id, "fulfilling the order");
    HandlerOutcome::ack()
}

fn service(pool: SqlitePool) -> impl App {
    let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(place).out_reply(Publish);
            b.include(fulfil);
            // Once the subscriptions are open, the unprocessed records are sent again.
            b.after_startup(Publish, tracking.republish());
        })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:orders.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    payload BLOB NOT NULL,
    processed_at TEXT
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "checkout")]
struct PlaceOrder {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct OrderPlaced {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::outbox::OUTBOX_ID_HEADER;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, SqliteConnection, SqlitePool};

    use super::{OrderPlaced, PlaceOrder, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool. It has no tables at all.
    async fn database(name: &str) -> Result<(SqlitePool, SqliteConnection), Box<dyn Error>> {
        let options: SqliteConnectOptions =
            format!("sqlite:file:{name}?mode=memory&cache=shared").parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        Ok((pool, keeper))
    }

    // --8<-- [start:switch]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_test_of_the_handlers_needs_no_outbox_table() -> Result<(), Box<dyn Error>> {
        // The switch is read once per process: with it on, this process tracks every message.
        if env::var("RUSTSTREAM_SQLX_OUTBOX").as_deref() == Ok("on") {
            return Ok(());
        }
        let (pool, _keeper) = database("sqlx_outbox_switch").await?;
        let tb = TestApp::start(service(pool)).await?;
        tb.broker::<MemoryBroker>()
            .message(&PlaceOrder { id: 7 })
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&OrderPlaced { id: 7 })
            .settled(HandlerOutcome::ack());
        // Nothing was recorded, so the order carries no record id.
        let published = tb.broker::<MemoryBroker>().published::<()>("orders");
        let ids: Vec<_> = published
            .messages()
            .iter()
            .map(|message| message.headers().get_str(OUTBOX_ID_HEADER))
            .collect();
        assert_eq!(ids, [None]);
        Ok(())
    }
    // --8<-- [end:switch]
}
