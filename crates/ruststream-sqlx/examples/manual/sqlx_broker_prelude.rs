//! The service of `examples/sqlx_broker_prelude.rs` without the macros: the table is described
//! with `InboxSpec`, the slot is a marker type, and the handler reads its slot and its context.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_broker_prelude --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

#[derive(sqlx::FromRow)]
struct OrderJob {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for OrderJob {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("order_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for OrderJob {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for OrderJob {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

// --8<-- [start:imports]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::Sqlite;
use sqlx::sqlite::SqlitePoolOptions;

/// The slot receipts leave through.
struct Receipts;

impl OutSlot for Receipts {
    const NAME: &'static str = "Receipts";
    // `Receipt` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Receipts> for Receipt {}

struct Confirm;

impl<Entry> Handle<Order, (), Outs<(Entry,)>, InboxContext> for Confirm
where
    Entry: OutEntry<Receipts, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ok(Ctx(attempt)) = Ctx::<keys::Attempt>::from_context(ctx).await;
        tracing::info!(order.id, ?attempt, "confirming");
        let receipt = Receipt { order: order.id };
        outs.get(Receipts)
            .message(&receipt)
            .publish()
            .await
            .map_err(|error| {
                tracing::warn!(subscription = "orders", order.id, %error, "no receipt");
                HandlerOutcome::retry()
            })
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    let broker = broker
        .route::<OrderJob>("orders")
        .route::<ReceiptJob>("receipts");
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            let confirm = subscriber("orders", Confirm).map_source(|_| {
                InboxQueue::<OrderJob>::new("orders").poll_interval(Duration::from_millis(200))
            });
            b.include(confirm.build()).out(Receipts, Routed).build();
        })
}
// --8<-- [end:imports]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:orders.db?mode=rwc")
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS order_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS receipt_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
";

/// The queue of receipts another service sends.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "receipt_jobs")]
struct ReceiptJob {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Sqlite> for OrderJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO order_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

impl Publish<Sqlite> for ReceiptJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO receipt_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Order, Receipt, SCHEMA, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool it reads the table through.
    async fn database(
        name: &str,
    ) -> Result<(String, SqlitePool, SqliteConnection), Box<dyn Error>> {
        let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
        let options: SqliteConnectOptions = url.parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((url, pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_confirmed_order_writes_its_receipt() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_broker_prelude").await?;
        let tb = TestApp::start(service(&url)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 });
        let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM receipt_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(receipts, 1, "the route wrote the receipt into its table");
        Ok(())
    }
}
