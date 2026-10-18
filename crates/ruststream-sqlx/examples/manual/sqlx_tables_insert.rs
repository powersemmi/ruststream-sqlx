//! The service of `examples/sqlx_tables_insert.rs` without the macros: the table is described by
//! hand, and its insert runs the text `dialect::insert` renders from the description.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_tables_insert --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::keys::PoolContext;
use serde::{Deserialize, Serialize};
use sqlx::Sqlite;

use ruststream_sqlx::prelude::*;

// --8<-- [start:insert]
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::dialect::insert::{self, Sql};
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use sqlx::{SqliteConnection, SqlitePool};

#[derive(sqlx::FromRow)]
struct SendReceipt {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for SendReceipt {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("receipt_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for SendReceipt {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

// INSERT INTO `receipt_jobs` (`locked_until`, `payload`) VALUES (?, ?)
const INSERT: Sql<128> = insert::sqlite(&SendReceipt::TABLE.spec());

impl Insert<SqliteConnection> for SendReceipt {
    async fn insert(&self, conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
        sqlx::query(INSERT.as_str())
            .bind(None::<DateTime<Utc>>)
            .bind(&self.payload)
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// Places an order and queues its receipt in one transaction: both commit, or neither does.
async fn place_order(pool: &SqlitePool, order: &Order) -> Result<(), Box<dyn Error>> {
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO orders (id, total) VALUES (?, ?)")
        .bind(order.id)
        .bind(order.total)
        .execute(&mut *tx)
        .await?;
    let receipt = SendReceipt {
        id: 0,
        payload: serde_json::to_vec(&Receipt { order: order.id })?,
    };
    receipt.insert(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
// --8<-- [end:insert]

struct Checkout;

impl Handle<Order, (), (), PoolContext<Sqlite>> for Checkout {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, PoolContext<Sqlite>>,
    ) -> Result<(), HandlerOutcome> {
        let Ok(Ctx(pool)) = Ctx::<keys::Pool<Sqlite>>::from_context(ctx).await;
        place_order(&pool, order).await.map_err(|error| {
            tracing::warn!(subscription = "checkouts", order.id, %error, "order not placed");
            HandlerOutcome::drop()
        })
    }
}

struct Sender;

impl Handle<Receipt> for Sender {
    fn handle(
        &self,
        receipt: &Receipt,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(receipt.order, "sending a receipt");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<CheckoutJob>("checkouts"),
        |b| {
            let checkout = subscriber("checkouts", Checkout)
                .map_source(|_| InboxQueue::<CheckoutJob>::new("checkouts"));
            b.include(checkout.build());
            let send = subscriber("receipts", Sender)
                .map_source(|_| InboxQueue::<SendReceipt>::new("receipts"));
            b.include(send.build());
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:shop.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS checkout_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS receipt_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
";

/// The checkouts the shop's front end queues.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "checkout_jobs")]
struct CheckoutJob {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Sqlite> for CheckoutJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO checkout_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "checkouts")]
struct Order {
    id: i64,
    total: i64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    order: i64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::INSERT;
    use super::{Order, Receipt, SCHEMA, service};

    #[test]
    fn the_insert_names_every_column_the_database_does_not_fill() {
        assert_eq!(
            INSERT.as_str(),
            "INSERT INTO `receipt_jobs` (`locked_until`, `payload`) VALUES (?, ?)"
        );
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

    async fn count(pool: &SqlitePool, table: &str) -> Result<i64, Box<dyn Error>> {
        let sql = format!("SELECT count(*) FROM {table}");
        Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .fetch_one(pool)
            .await?)
    }

    // The receipt row the checkout writes wakes no subscription and reaches the harness's books
    // once a claim takes it, so the test runs on the real clock past a poll interval.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_receipt_commits_with_its_order_and_is_sent() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_tables_insert").await?;
        let tb = TestApp::start_live(service(pool.clone())).await?;
        let order = Order { id: 7, total: 1200 };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&order)
            .publish()
            .await?;
        tb.advance(Duration::from_secs(2)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 })
            .settled(HandlerOutcome::ack());

        // The same order again: its insert fails, and the receipt rolls back with it.
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&order)
            .publish()
            .await?;
        tb.advance(Duration::from_secs(2)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("checkouts")
            .assert_called(2)
            .settled(HandlerOutcome::drop());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("receipts")
            .assert_called_once();
        assert_eq!(count(&pool, "orders").await?, 1, "the order is there once");
        assert_eq!(
            count(&pool, "receipt_jobs").await?,
            0,
            "and no second receipt waits"
        );
        Ok(())
    }
}
