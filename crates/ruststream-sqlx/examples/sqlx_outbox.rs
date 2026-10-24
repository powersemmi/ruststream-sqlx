//! Orders tracked by the outbox over the in-memory broker, with the outbox table in SQLite.
//!
//! The service answers a request on `checkout` with an order on `orders`. The outbox records the
//! order before it is sent, the consumer's acknowledgement marks the record processed, and at
//! startup every unprocessed record is sent again.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example sqlx_outbox --features outbox,sqlite,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:service]
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
// --8<-- [end:service]

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_is_recorded_sent_with_its_id_and_marked_once_fulfilled()
    -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("sqlx_outbox").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<MemoryBroker>()
            .message(&PlaceOrder { id: 7 })
            .publish()
            .await?;

        let records: Vec<(i64, String, bool)> =
            sqlx::query_as("SELECT id, name, processed_at IS NOT NULL FROM outbox")
                .fetch_all(&pool)
                .await?;
        assert_eq!(records, [(1, "orders".to_owned(), true)]);
        let published = tb.broker::<MemoryBroker>().published::<()>("orders");
        let ids: Vec<_> = published
            .messages()
            .iter()
            .map(|message| message.headers().get_str(OUTBOX_ID_HEADER))
            .collect();
        assert_eq!(ids, [Some("1")], "the order carries its record's id");
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&OrderPlaced { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_startup_sends_an_unprocessed_record_again() -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("sqlx_outbox_startup").await?;
        sqlx::query("INSERT INTO outbox (name, payload) VALUES ('orders', ?)")
            .bind(br#"{"id":3}"#.as_slice())
            .execute(&pool)
            .await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.settle().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&OrderPlaced { id: 3 });
        let left: i64 =
            sqlx::query_scalar("SELECT count(*) FROM outbox WHERE processed_at IS NULL")
                .fetch_one(&pool)
                .await?;
        assert_eq!(left, 0, "the consumer processed the republished record");
        Ok(())
    }
}
