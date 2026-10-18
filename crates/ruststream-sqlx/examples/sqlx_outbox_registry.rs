//! One record type tracks two names: orders and refunds share the `outbox` table, and each name
//! republishes only its own records.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example sqlx_outbox_registry --features outbox,sqlite,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

use ruststream_sqlx::Outbox;

// --8<-- [start:registry]
use ruststream_sqlx::outbox;

fn service(pool: SqlitePool) -> impl App {
    // Each name has one record type; a name written twice does not compile.
    let tracking = outbox! {
        pool: pool,
        "orders" => OrderOutbox,
        "refunds" => OrderOutbox,
    };
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(place).out_reply(Publish);
            b.include(cancel).out_reply(Publish);
            b.include(fulfil);
            b.include(pay_back);
            b.after_startup(Publish, tracking.republish());
        })
}
// --8<-- [end:registry]

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

/// One record of the `outbox` table, under either name.
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
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "cancellations")]
struct CancelOrder {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct OrderPlaced {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "refunds")]
struct RefundDue {
    id: u64,
}

#[subscriber("checkout", reply)]
async fn place(cmd: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: cmd.id }
}

#[subscriber("cancellations", reply)]
async fn cancel(cmd: &CancelOrder) -> RefundDue {
    RefundDue { id: cmd.id }
}

#[subscriber("orders")]
async fn fulfil(order: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(order = order.id, "fulfilling the order");
    HandlerOutcome::ack()
}

#[subscriber("refunds")]
async fn pay_back(refund: &RefundDue) -> HandlerOutcome {
    tracing::info!(order = refund.id, "paying the refund");
    HandlerOutcome::ack()
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, SqliteConnection, SqlitePool};

    use super::{CancelOrder, OrderPlaced, PlaceOrder, RefundDue, SCHEMA, service};

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

    /// The records of `outbox` in id order, as `(name, processed)`.
    async fn records(pool: &SqlitePool) -> Result<Vec<(String, bool)>, sqlx::Error> {
        sqlx::query_as("SELECT name, processed_at IS NOT NULL FROM outbox ORDER BY id")
            .fetch_all(pool)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_name_is_recorded_under_its_own_name() -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("sqlx_outbox_registry").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<MemoryBroker>()
            .message(&PlaceOrder { id: 1 })
            .publish()
            .await?;
        tb.broker::<MemoryBroker>()
            .message(&CancelOrder { id: 2 })
            .publish()
            .await?;

        assert_eq!(
            records(&pool).await?,
            [("orders".to_owned(), true), ("refunds".to_owned(), true)]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_name_republishes_its_own_records() -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("sqlx_outbox_registry_republish").await?;
        sqlx::raw_sql(
            r#"INSERT INTO outbox (name, payload)
            VALUES ('orders', CAST('{"id":3}' AS BLOB)), ('refunds', CAST('{"id":4}' AS BLOB))"#,
        )
        .execute(&pool)
        .await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.settle().await?;

        let broker = tb.broker::<MemoryBroker>();
        broker
            .subscriber("orders")
            .assert_called_once()
            .with(&OrderPlaced { id: 3 });
        broker
            .subscriber("refunds")
            .assert_called_once()
            .with(&RefundDue { id: 4 });
        assert_eq!(
            records(&pool).await?,
            [("orders".to_owned(), true), ("refunds".to_owned(), true)]
        );
        Ok(())
    }
}
