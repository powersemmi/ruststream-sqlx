//! The test of `examples/sqlx_outbox_switch.rs` over the service written without the macros: the
//! record is described with `OutboxSpec`, and the handlers are named types.
//!
//! ```text
//! cargo test -p ruststream-sqlx --example manual_sqlx_outbox_switch --features outbox,sqlite,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::ProcessedAt;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream::memory::prelude::*;
use ruststream_sqlx::outbox::{self, Outbox, OutboxSpec, OutboxTable, TrackedName};
use sqlx::SqlitePool;

/// One record of the `outbox` table: the struct holds what the outbox reads.
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

/// The name `OrderPlaced` is published under.
struct Orders;

impl TrackedName for Orders {
    const NAME: &'static str = "orders";
}

/// The reply to `orders` is recorded before it is sent.
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

/// After the acknowledgement, the record is marked processed.
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

fn service(pool: SqlitePool) -> impl App {
    let tracking = Outbox::new(pool).track::<OrderOutbox, Orders>();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("checkout", Place).reply().build())
                .out_reply(Publish);
            b.include(subscriber("orders", Fulfil).build());
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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PlaceOrder {
    id: u64,
}

// `#[outgoing(name = "checkout")]` by hand: the type names the subscription it is published to.
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
        let (pool, _keeper) = database("manual_sqlx_outbox_switch").await?;
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
