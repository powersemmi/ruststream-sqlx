//! The prelude: one glob brings the core's prelude, the derives, the broker, the subscription
//! descriptor, the publish policies and the context keys.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_broker_prelude --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

// --8<-- [start:imports]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqlitePool};

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "order_jobs")]
struct OrderJob {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Receipts;

#[subscriber(InboxQueue::<OrderJob>::new("orders").poll_interval(Duration::from_millis(200)))]
async fn confirm(
    order: &Order,
    Out(receipts): Out<impl Publisher, Receipts>,
    Ctx(attempt): Ctx<keys::Attempt>,
) -> HandlerOutcome {
    tracing::info!(order.id, ?attempt, "confirming");
    match receipts
        .message(&Receipt { order: order.id })
        .publish()
        .await
    {
        Ok(()) => HandlerOutcome::ack(),
        Err(error) => {
            tracing::warn!(subscription = "orders", order.id, %error, "no receipt");
            HandlerOutcome::retry()
        }
    }
}

fn service(pool: SqlitePool) -> RustStream {
    let broker = SqlxBroker::new(pool)
        .route::<OrderJob>("orders")
        .route::<ReceiptJob>("receipts");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm).out(Receipts, Routed).build();
    })
}
// --8<-- [end:imports]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:orders.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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
    async fn a_confirmed_order_writes_its_receipt() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_broker_prelude").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
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
