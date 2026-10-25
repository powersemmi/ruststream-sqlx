//! A task written with the derive's insert, inside the service's own transaction: the receipt
//! commits with the order it belongs to.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_tables_insert --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:insert]
use sqlx::SqlitePool;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "receipt_jobs")]
struct SendReceipt {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
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
        locked_until: None,
        payload: serde_json::to_vec(&Receipt { order: order.id })?,
    };
    // `id` is generated: the insert leaves it to the database.
    receipt.insert(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
// --8<-- [end:insert]

#[subscriber(InboxQueue::<CheckoutJob>::new("checkouts"))]
async fn checkout(order: &Order, Ctx(pool): Ctx<keys::Pool<Sqlite>>) -> HandlerOutcome {
    match place_order(&pool, order).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(error) => {
            tracing::warn!(subscription = "checkouts", order.id, %error, "order not placed");
            HandlerOutcome::drop()
        }
    }
}

#[subscriber(InboxQueue::<SendReceipt>::new("receipts"))]
async fn send_receipt(receipt: &Receipt) -> HandlerOutcome {
    tracing::info!(receipt.order, "sending a receipt");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("shop", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<CheckoutJob>("checkouts"), |b| {
            b.include(checkout);
            b.include(send_receipt);
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:shop.db?mode=rwc")
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "checkouts")]
struct Order {
    id: i64,
    total: i64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
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
        let (url, pool, _keeper) = database("sqlx_tables_insert").await?;
        let tb = TestApp::start_live(service(&url)).await?;
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
