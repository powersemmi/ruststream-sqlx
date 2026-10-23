//! The service of `examples/sqlx_outbox_pool.rs` without the derive, `outbox!` and the handler
//! attribute: the registry starts with `Outbox::deferred`, the record is described with
//! `OutboxSpec`, and the handlers are named types mounted with `subscriber(..)`.
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
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example manual_sqlx_outbox_pool --features outbox,sqlite,testing,asyncapi
//! ```

use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::ProcessedAt;
use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable, TrackedName};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:pool]
use std::convert::Infallible;
use std::io;

use ruststream::memory::prelude::*;
use ruststream_sqlx::outbox::Outbox;
use sqlx::SqlitePool;

fn service(url: String) -> impl App {
    // No pool yet: the Tokio runtime starts after this builder, and sqlx builds a pool only there.
    let tracking = Outbox::deferred().track::<OrderOutbox, Orders>();
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
            b.include(subscriber("checkout", Place).reply().build())
                .out_reply(Publish);
            b.include(subscriber("orders", Fulfil).build());
            b.after_startup(Publish, tracking.republish());
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:orders.db".to_owned())
}
// --8<-- [end:pool]

/// One record of the `outbox` table.
#[derive(sqlx::FromRow)]
struct OrderOutbox {
    id: i64,
    name: String,
    payload: Vec<u8>,
}

impl OutboxTable for OrderOutbox {
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

impl outbox::Publish<Sqlite> for OrderOutbox {
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
    id: u64,
}

impl OutgoingDestination for PlaceOrder {
    type Form = FixedName;
    const DESTINATION: &'static str = "checkout";
}

impl MessageHeaders for PlaceOrder {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderPlaced {
    id: u64,
}

impl OutgoingDestination for OrderPlaced {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for OrderPlaced {
    type Contract = NoHeaders;
}

/// The name `OrderPlaced` is published under.
struct Orders;

impl TrackedName for Orders {
    const NAME: &'static str = "orders";
}

struct Place;

// The app's state is the pool `on_startup` returns.
impl Handle<PlaceOrder, OrderPlaced, (), (), SqlitePool> for Place {
    fn handle(
        &self,
        cmd: &PlaceOrder,
        _outs: &(),
        _ctx: &mut Context<'_, (), SqlitePool>,
    ) -> impl Future<Output = Result<OrderPlaced, HandlerOutcome>> {
        ready(Ok(OrderPlaced { id: cmd.id }))
    }
}

struct Fulfil;

impl Handle<OrderPlaced, (), (), (), SqlitePool> for Fulfil {
    fn handle(
        &self,
        order: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_, (), SqlitePool>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order = order.id, "fulfilling the order");
        ready(Ok(()))
    }
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
        let url = "sqlite:file:manual_sqlx_outbox_pool?mode=memory&cache=shared";
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
