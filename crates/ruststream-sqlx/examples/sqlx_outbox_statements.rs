//! A record whose take and retry are the service's own statements: the fetch stamps `taken_at`
//! when a consumer takes the record into work, and the retry releases it and counts the attempt.
//! The acknowledgement keeps the default statement, which marks the record processed.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example sqlx_outbox_statements --features outbox,sqlite,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

// --8<-- [start:statements]
use ruststream_sqlx::{Outbox, outbox};
use sqlx::{Sqlite, SqliteConnection};

/// One record of `order_outbox`, taken and released by statements of the service's own.
#[derive(Outbox, sqlx::FromRow)]
#[outbox(table = "order_outbox", custom(fetch, retry))]
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

impl outbox::Publish<Sqlite> for OrderOutbox {
    async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar("INSERT INTO order_outbox (name, payload) VALUES (?, ?) RETURNING id")
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}

/// Takes the record into work, or finds it taken or processed already (`None`).
impl outbox::Fetch<Sqlite> for OrderOutbox {
    async fn fetch(conn: &mut SqliteConnection, id: &i64) -> sqlx::Result<Option<Self>> {
        sqlx::query_as(
            "UPDATE order_outbox SET taken_at = CURRENT_TIMESTAMP
             WHERE id = ? AND taken_at IS NULL AND processed_at IS NULL
             RETURNING id, name, payload, processed_at",
        )
        .bind(id)
        .fetch_optional(conn)
        .await
    }
}

/// Releases a record its consumer asked for again, and counts the attempt.
impl outbox::Retry<Sqlite> for OrderOutbox {
    async fn retry(conn: &mut SqliteConnection, id: &i64) -> sqlx::Result<()> {
        sqlx::query("UPDATE order_outbox SET taken_at = NULL, attempts = attempts + 1 WHERE id = ?")
            .bind(id)
            .execute(conn)
            .await
            .map(drop)
    }
}
// --8<-- [end:statements]

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS order_outbox (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    payload BLOB NOT NULL,
    taken_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    processed_at TEXT
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "checkout")]
struct PlaceOrder {
    id: u64,
    in_stock: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct OrderPlaced {
    id: u64,
    in_stock: bool,
}

#[subscriber("checkout", reply)]
async fn place(cmd: &PlaceOrder) -> OrderPlaced {
    OrderPlaced {
        id: cmd.id,
        in_stock: cmd.in_stock,
    }
}

#[subscriber("orders")]
async fn fulfil(order: &OrderPlaced) -> HandlerOutcome {
    if order.in_stock {
        tracing::info!(order = order.id, "fulfilling the order");
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

fn service(pool: SqlitePool) -> impl App {
    let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(place).out_reply(Publish);
            // One attempt: an order out of stock is not delivered again, and its record waits.
            b.include(fulfil).max_attempts(nonzero!(1u32));
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, SqliteConnection, SqlitePool};

    use super::{PlaceOrder, SCHEMA, service};

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_services_own_take_and_release_settle_the_records() -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("sqlx_outbox_statements").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&PlaceOrder {
                id: 1,
                in_stock: true,
            })
            .publish()
            .await?;
        broker
            .message(&PlaceOrder {
                id: 2,
                in_stock: false,
            })
            .publish()
            .await?;

        // `(taken, attempts, processed)` of each record, in id order.
        let records: Vec<(bool, i64, bool)> = sqlx::query_as(
            "SELECT taken_at IS NOT NULL, attempts, processed_at IS NOT NULL
             FROM order_outbox ORDER BY id",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(records, [(true, 0, true), (false, 1, false)]);
        Ok(())
    }
}
