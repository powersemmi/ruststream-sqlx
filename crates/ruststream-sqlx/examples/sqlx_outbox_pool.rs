//! The outbox inside `#[ruststream::app]`: the registry starts without a pool, `on_startup` builds
//! the pool inside the Tokio runtime and hands it over, and `after_shutdown` closes it.
//!
//! The service owns the table:
//!
//! ```sql
//! CREATE TABLE outbox (
//!     id           INTEGER PRIMARY KEY,
//!     name         TEXT NOT NULL,
//!     payload      BLOB NOT NULL,
//!     processed_at TEXT
//! );
//! ```
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example sqlx_outbox_pool --features outbox,sqlite,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::Outbox;

// --8<-- [start:pool]
use std::convert::Infallible;
use std::io;

use ruststream::memory::prelude::*;
use ruststream_sqlx::outbox;
use sqlx::SqlitePool;

fn service(url: String) -> impl App {
    // No pool yet: the Tokio runtime starts after this builder, and sqlx builds a pool only there.
    let tracking = outbox! { "orders" => OrderOutbox };
    let registry = tracking.clone();

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| {
            let pool = SqlitePool::connect(&url).await.map_err(io::Error::other)?;
            // Every handle the registry gave out reads this one pool.
            registry.set_pool(pool.clone()).map_err(io::Error::other)?;
            Ok::<_, io::Error>(pool)
        })
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        // Runs once the handlers have finished and the brokers have stopped.
        .after_shutdown(async move |pool| {
            pool.close().await;
            Ok::<_, Infallible>(())
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(place).out_reply(Publish);
            b.include(fulfil);
            b.after_startup(Publish, tracking.republish());
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:orders.db".to_owned())
}
// --8<-- [end:pool]

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
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct OrderPlaced {
    id: u64,
}

#[subscriber("checkout", reply)]
async fn place(cmd: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: cmd.id }
}

#[subscriber("orders")]
async fn fulfil(order: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(order = order.id, "fulfilling the order");
    HandlerOutcome::ack()
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use sqlx::{Connection, SqliteConnection};

    use super::{PlaceOrder, service};

    const SCHEMA: &str = "CREATE TABLE outbox (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        payload BLOB NOT NULL,
        processed_at TEXT
    )";

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_pool_built_at_startup_tracks_the_orders() -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        // An in-memory database every connection naming it shares, alive while the test holds
        // this connection: the service's own pool opens on the same URL.
        let url = "sqlite:file:sqlx_outbox_pool?mode=memory&cache=shared";
        let mut keeper = SqliteConnection::connect(url).await?;
        sqlx::raw_sql(SCHEMA).execute(&mut keeper).await?;
        let tb = TestApp::start(service(url.to_owned())).await?;
        tb.broker::<MemoryBroker>()
            .message(&PlaceOrder { id: 7 })
            .publish()
            .await?;

        let records: Vec<(String, bool)> =
            sqlx::query_as("SELECT name, processed_at IS NOT NULL FROM outbox")
                .fetch_all(&mut keeper)
                .await?;
        assert_eq!(records, [("orders".to_owned(), true)]);
        Ok(())
    }
}
