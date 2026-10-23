//! The endpoint of `examples/sqlx_outbox_errors.rs` without the macros: the record is described
//! with `OutboxSpec` and tracked under a name that is a type.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example manual_sqlx_outbox_errors --features outbox,sqlite,testing,asyncapi
//! ```

use std::error::Error;
use std::fmt::Display;

use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::ProcessedAt;
use ruststream_sqlx::outbox::{self, Outbox, OutboxSpec, OutboxTable, TrackedName};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

// --8<-- [start:errors]
use ruststream::runtime::PublishError;
use ruststream_sqlx::outbox::TrackedPublishError;

/// The status an HTTP endpoint answers an order with.
fn status<E: Display>(published: Result<(), PublishError<TrackedPublishError<E>>>) -> u16 {
    match published {
        Ok(()) => 202,
        // Recorded but not sent: the next startup sends it.
        Err(PublishError::Publish(TrackedPublishError::Publish(error))) => {
            tracing::warn!(%error, "the order waits for the next startup");
            202
        }
        // Neither recorded nor sent: the client sends the order again.
        Err(PublishError::Publish(TrackedPublishError::Outbox(error))) => {
            tracing::warn!(%error, "the order was not recorded");
            503
        }
        Err(error) => {
            tracing::error!(%error, "the order was not published");
            500
        }
    }
}
async fn serve(pool: SqlitePool, orders: &[u64]) -> Result<Vec<u16>, Box<dyn Error + Send + Sync>> {
    let tracking = Outbox::new(pool).track::<OrderOutbox, Orders>();
    let broker = MemoryBroker::new().bindable();
    let egress = broker.bind(Publish);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .publish_layer(tracking.publish_layer())
        .with_broker(broker, |_b| {});
    let running = app.start().await?;
    let publisher = tracking.wrap(running.publisher(egress).await?);
    let mut answers = Vec::with_capacity(orders.len());
    for &id in orders {
        answers.push(status(
            publisher.message(&OrderPlaced { id }).publish().await,
        ));
    }
    running.shutdown().await?;
    Ok(answers)
}

// --8<-- [end:errors]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let pool = SqlitePool::connect("sqlite:orders.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    let answers = serve(pool.clone(), &[7]).await?;
    tracing::info!(?answers, "orders answered");
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, SqliteConnection, SqlitePool};

    use super::{SCHEMA, serve};

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
    async fn database(
        name: &str,
    ) -> Result<(SqlitePool, SqliteConnection), Box<dyn Error + Send + Sync>> {
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

    // The subject is a publisher outside the harness, so the tests run the service as `main` does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_recorded_order_is_accepted() -> Result<(), Box<dyn Error + Send + Sync>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("manual_sqlx_outbox_errors").await?;
        assert_eq!(serve(pool, &[7]).await?, [202]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_the_outbox_cannot_record_is_refused()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("manual_sqlx_outbox_errors_unrecorded").await?;
        sqlx::raw_sql("DROP TABLE outbox").execute(&pool).await?;
        assert_eq!(serve(pool, &[7]).await?, [503]);
        Ok(())
    }
}
