//! The service of `examples/sqlx_outbox.rs` without the macros: the record is described with
//! `OutboxSpec` and tracked under a name that is a type, and the handlers are named types mounted
//! with `subscriber(..)`.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example manual_sqlx_outbox --features outbox,sqlite,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::ProcessedAt;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:service]
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
// --8<-- [end:service]

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
        let (pool, _keeper) = database("manual_sqlx_outbox").await?;
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
        let (pool, _keeper) = database("manual_sqlx_outbox_startup").await?;
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
