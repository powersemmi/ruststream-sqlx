//! An outbox record with every role: the id, the name and the payload, the headers the message
//! carried, and the time a consumer processed it.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example sqlx_outbox_record --features outbox,sqlite,json,testing
//! ```

use std::collections::BTreeMap;
use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream_sqlx::HeaderColumn;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

// --8<-- [start:record]
use ruststream_sqlx::{Outbox, outbox};

/// The `headers` column: a JSON object of strings, `NULL` for none.
type Headers = Option<Json<BTreeMap<String, String>>>;

/// One record of `order_outbox`.
#[derive(Outbox, sqlx::FromRow)]
#[outbox(table = "order_outbox")]
struct OrderOutbox {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
    #[field(headers)]
    headers: Headers,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
}

/// The record keeps what the message carried, so a republish sends the same headers again.
impl outbox::Publish<Sqlite> for OrderOutbox {
    async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar(
            "INSERT INTO order_outbox (name, payload, headers) VALUES (?, ?, ?) RETURNING id",
        )
        .bind(msg.name())
        .bind(msg.payload())
        .bind(Headers::from_headers(msg.headers()))
        .fetch_one(conn)
        .await
    }
}
// --8<-- [end:record]

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS order_outbox (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    payload BLOB NOT NULL,
    headers TEXT,
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

/// Stamps every order with the tenant it belongs to.
struct Tenant;

impl<C, Options> PublishTransform<ForReply<C>, Options> for Tenant {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        _cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut().insert("tenant", "acme");
    }
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

fn service(pool: SqlitePool) -> impl App {
    let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(place).out_reply(Publish).transform(Tenant);
            b.include(fulfil);
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::env;
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::types::Json;
    use sqlx::{Connection, SqliteConnection, SqlitePool};

    use super::{Headers, PlaceOrder, SCHEMA, service};

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
    async fn the_record_keeps_the_name_the_payload_and_the_headers_and_is_marked()
    -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (pool, _keeper) = database("sqlx_outbox_record").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<MemoryBroker>()
            .message(&PlaceOrder { id: 7 })
            .publish()
            .await?;

        let records: Vec<(String, Vec<u8>, Headers, bool)> = sqlx::query_as(
            "SELECT name, payload, headers, processed_at IS NOT NULL FROM order_outbox",
        )
        .fetch_all(&pool)
        .await?;
        let tenant = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
        assert_eq!(
            records,
            [(
                "orders".to_owned(),
                br#"{"id":7}"#.to_vec(),
                Some(Json(tenant)),
                true
            )]
        );
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
