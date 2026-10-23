//! The two middlewares of the outbox: the publish records an order before it is sent, and the
//! consumer's outcome settles the record. An order in stock is acknowledged and its record marked;
//! an order out of stock is asked again, and its record waits for the next startup.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example sqlx_outbox_tracking --features outbox,sqlite,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::Outbox;

// --8<-- [start:tracking]
use ruststream::memory::prelude::*;
use ruststream_sqlx::outbox;
use sqlx::SqlitePool;

#[subscriber("checkout", reply)]
async fn place(cmd: &PlaceOrder) -> OrderPlaced {
    OrderPlaced {
        id: cmd.id,
        in_stock: cmd.in_stock,
    }
}

/// The outcome settles the record: an acknowledgement marks it, a retry leaves it.
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
        // Takes a delivery that carries a record id into work and settles the record.
        .layer(tracking.layer())
        // Records a message under a registered name, then sends it with the record's id.
        .publish_layer(tracking.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(place).out_reply(Publish);
            // One attempt: an order out of stock is not delivered again, and its record waits.
            b.include(fulfil).max_attempts(nonzero!(1u32));
        })
}
// --8<-- [end:tracking]

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

impl outbox::Publish<Sqlite> for OrderOutbox {
    async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES (?, ?) RETURNING id")
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}

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

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::Serialize;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, SqliteConnection, SqlitePool};

    use super::{OrderPlaced, PlaceOrder, SCHEMA, service};

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

    /// Whether each record of `outbox` is processed, in id order.
    async fn processed(pool: &SqlitePool) -> Result<Vec<bool>, sqlx::Error> {
        sqlx::query_scalar("SELECT processed_at IS NOT NULL FROM outbox ORDER BY id")
            .fetch_all(pool)
            .await
    }

    /// The header a producer's tracked message carries.
    #[derive(Serialize)]
    struct OutboxId {
        #[serde(rename = "x-ruststream-outbox-id")]
        id: String,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acknowledgement_marks_the_record_and_a_retry_leaves_it()
    -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("sqlx_outbox_tracking").await?;
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

        assert_eq!(processed(&pool).await?, [true, false]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_processed_record_is_acknowledged_without_the_handler() -> Result<(), Box<dyn Error>>
    {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("sqlx_outbox_tracking_processed").await?;
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO outbox (name, payload, processed_at)
             VALUES ('orders', ?, '2026-01-01T00:00:00+00:00') RETURNING id",
        )
        .bind(br#"{"id":3,"in_stock":false}"#.as_slice())
        .fetch_one(&pool)
        .await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        // A copy of a message a consumer processed already: the handler would ask for it again.
        let order = OrderPlaced {
            id: 3,
            in_stock: false,
        };
        let header = OutboxId { id: id.to_string() };
        tb.broker::<MemoryBroker>()
            .publish_with_headers("orders", &order, &header)
            .await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
