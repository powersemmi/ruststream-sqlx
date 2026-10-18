//! The republish of `examples/sqlx_outbox_republish.rs` without the macros: the record is
//! described with `OutboxSpec` and tracked under a name that is a type, and the handlers are named
//! types mounted with `subscriber(..)`.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example manual_sqlx_outbox_republish --features outbox,sqlite,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::ProcessedAt;
use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable, TrackedName};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:republish]
use ruststream::memory::prelude::*;
use ruststream_sqlx::outbox::Outbox;
use sqlx::SqlitePool;

fn service(pool: SqlitePool) -> impl App {
    let tracking = Outbox::new(pool).track::<OrderOutbox, Orders>();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("checkout", Place).reply().build())
                .out_reply(Publish);
            b.include(subscriber("orders", Fulfil).build());
            // Runs once the subscriptions are open, through this broker's publisher.
            b.after_startup(Publish, tracking.republish());
        })
}
// --8<-- [end:republish]

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

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::outbox::OUTBOX_ID_HEADER;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, SqliteConnection, SqlitePool};

    use super::{OrderPlaced, SCHEMA, service};

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
    async fn the_records_no_consumer_processed_are_sent_again_with_their_ids()
    -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("manual_sqlx_outbox_republish").await?;
        // Two records a consumer never processed, and one it did.
        sqlx::raw_sql(
            r#"INSERT INTO outbox (id, name, payload, processed_at) VALUES
                (1, 'orders', CAST('{"id":1}' AS BLOB), NULL),
                (2, 'orders', CAST('{"id":2}' AS BLOB), '2026-01-01T00:00:00+00:00'),
                (3, 'orders', CAST('{"id":3}' AS BLOB), NULL)"#,
        )
        .execute(&pool)
        .await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.settle().await?;

        assert_eq!(
            tb.broker::<MemoryBroker>()
                .subscriber("orders")
                .received::<OrderPlaced>(),
            [OrderPlaced { id: 1 }, OrderPlaced { id: 3 }]
        );
        let published = tb.broker::<MemoryBroker>().published::<()>("orders");
        let ids: Vec<_> = published
            .messages()
            .iter()
            .map(|message| message.headers().get_str(OUTBOX_ID_HEADER))
            .collect();
        assert_eq!(ids, [Some("1"), Some("3")]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn records_that_cannot_be_read_fail_the_startup() -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("manual_sqlx_outbox_republish_unread").await?;
        sqlx::raw_sql("DROP TABLE outbox").execute(&pool).await?;

        let Err(error) = TestApp::start(service(pool)).await else {
            panic!("a republish that cannot read its records must fail the startup");
        };
        let mut text = error.to_string();
        let mut source = error.source();
        while let Some(next) = source {
            text.push('\n');
            text.push_str(&next.to_string());
            source = next.source();
        }
        assert!(
            text.contains("did not read the unprocessed records of `orders`"),
            "the error names what failed: {text}"
        );
        Ok(())
    }
}
