//! The registry of `examples/sqlx_outbox_registry.rs` without the macros: the record is described
//! with `OutboxSpec`, each name is a type the registry tracks, and the handlers are named types
//! mounted with `subscriber(..)`.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example manual_sqlx_outbox_registry --features outbox,sqlite,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::ProcessedAt;
use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

// --8<-- [start:registry]
use ruststream_sqlx::outbox::{Outbox, TrackedName};

/// The name `OrderPlaced` is published under.
struct Orders;

impl TrackedName for Orders {
    const NAME: &'static str = "orders";
}

/// The name `RefundDue` is published under.
struct Refunds;

impl TrackedName for Refunds {
    const NAME: &'static str = "refunds";
}

fn service(pool: SqlitePool) -> impl App {
    // Each name has one record type; a name tracked twice stops the build.
    let tracking = Outbox::new(pool)
        .track::<OrderOutbox, Orders>()
        .track::<OrderOutbox, Refunds>();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("checkout", Place).reply().build())
                .out_reply(Publish);
            b.include(subscriber("cancellations", Cancel).reply().build())
                .out_reply(Publish);
            b.include(subscriber("orders", Fulfil).build());
            b.include(subscriber("refunds", PayBack).build());
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
struct CancelOrder {
    id: u64,
}

impl OutgoingDestination for CancelOrder {
    type Form = FixedName;
    const DESTINATION: &'static str = "cancellations";
}

impl MessageHeaders for CancelOrder {
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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct RefundDue {
    id: u64,
}

impl OutgoingDestination for RefundDue {
    type Form = FixedName;
    const DESTINATION: &'static str = "refunds";
}

impl MessageHeaders for RefundDue {
    type Contract = NoHeaders;
}

struct Place;

impl Handle<PlaceOrder, OrderPlaced> for Place {
    fn handle(
        &self,
        cmd: &PlaceOrder,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<OrderPlaced, HandlerOutcome>> {
        ready(Ok(OrderPlaced { id: cmd.id }))
    }
}

struct Cancel;

impl Handle<CancelOrder, RefundDue> for Cancel {
    fn handle(
        &self,
        cmd: &CancelOrder,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<RefundDue, HandlerOutcome>> {
        ready(Ok(RefundDue { id: cmd.id }))
    }
}

struct Fulfil;

impl Handle<OrderPlaced> for Fulfil {
    fn handle(
        &self,
        order: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order = order.id, "fulfilling the order");
        ready(Ok(()))
    }
}

struct PayBack;

impl Handle<RefundDue> for PayBack {
    fn handle(
        &self,
        refund: &RefundDue,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order = refund.id, "paying the refund");
        ready(Ok(()))
    }
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
        let (pool, _keeper) = database("manual_sqlx_outbox_registry").await?;
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
        let (pool, _keeper) = database("manual_sqlx_outbox_registry_republish").await?;
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
