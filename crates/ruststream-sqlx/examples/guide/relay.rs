//! The service of the Relay guide: an order and its event commit in one transaction, and a relay
//! subscription publishes each event to another broker, here the in-memory one.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example guide_relay --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};

// --8<-- [start:schema]
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, total_cents INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS checkout_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS order_events (
    id INTEGER PRIMARY KEY,
    aggregate TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
);
";
// --8<-- [end:schema]

// --8<-- [start:write]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Sqlite, SqliteConnection};

/// One row of `order_events`: an event waiting for the relay, keyed by its order.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "order_events")]
struct OrderEvent {
    #[field(id, generated)]
    id: i64,
    #[field(partition_key)]
    aggregate: String,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The order and its event, in the checkout's transaction: both commit, or neither does.
async fn place(
    conn: &mut SqliteConnection,
    order: &Order,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    sqlx::query("INSERT INTO orders (id, total_cents) VALUES (?, ?)")
        .bind(order.id)
        .bind(order.total_cents)
        .execute(&mut *conn)
        .await?;
    let placed = OrderPlaced {
        order: order.id,
        total_cents: order.total_cents,
    };
    let event = OrderEvent {
        id: 0,
        aggregate: order.id.to_string(),
        locked_until: None,
        payload: serde_json::to_vec(&placed)?,
    };
    event.insert(conn).await?;
    Ok(())
}

#[subscriber(InboxQueue::<Checkout>::new("checkouts"))]
async fn checkout(order: &Order, Ctx(mut tx): Ctx<keys::Tx<Sqlite>>) -> HandlerOutcome {
    match place(&mut tx, order).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(error) => {
            tracing::warn!(subscription = "checkouts", order.id, %error, "order not placed");
            HandlerOutcome::drop()
        }
    }
}
// --8<-- [end:write]

// --8<-- [start:relay]
use ruststream::memory::{MemoryBroker, MemoryPublish};

/// Each event goes out on the bus as it was written; the acknowledgement deletes its row.
#[subscriber(InboxQueue::<OrderEvent>::new("order-events"), reply)]
async fn relay(event: &OrderPlaced) -> OrderPlaced {
    event.clone()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    // The target broker is wrapped first, so its publish token exists before the scopes.
    let bus = MemoryBroker::new().bindable();
    let to_bus = bus.bind(MemoryPublish);
    RustStream::new(AppInfo::new("shop", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<Checkout>("checkouts"), |b| {
            b.include(checkout.transactional());
            b.include(relay.workers_by_key(nonzero!(4)))
                .out_reply(to_bus);
        })
        .with_broker(bus, |_b| {})
}
// --8<-- [end:relay]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:shop.db?mode=rwc")
}

/// The checkouts the shop's front end queues.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "checkout_jobs")]
struct Checkout {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Sqlite> for Checkout {
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

/// An order as the shop's front end sends it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "checkouts")]
struct Order {
    id: i64,
    total_cents: i64,
}

/// The event other services read on the bus.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct OrderPlaced {
    order: i64,
    total_cents: i64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::memory::MemoryBroker;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Order, OrderPlaced, SCHEMA, service};

    // A database in memory that every connection naming it shares, alive while one of them is
    // open: the test holds that one beside the pool.
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

    fn order(id: i64) -> Order {
        Order {
            id,
            total_cents: 4200,
        }
    }

    // The event row the checkout writes through its transaction wakes no subscription: the relay
    // finds it on its next poll, so the tests run on the real clock past the poll interval.
    const POLL: Duration = Duration::from_secs(2);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_placed_order_reaches_the_bus_once() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("guide_relay_placed").await?;
        let tb = TestApp::start_live(service(&url)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&order(7))
            .publish()
            .await?;
        tb.advance(POLL).await?;

        tb.broker::<MemoryBroker>()
            .published::<OrderPlaced>("orders")
            .assert_called_once()
            .with(&OrderPlaced {
                order: 7,
                total_cents: 4200,
            });
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM order_events")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "the relay's acknowledgement deleted the event");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_that_fails_publishes_nothing() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("guide_relay_failed").await?;
        let tb = TestApp::start_live(service(&url)).await?;
        // The same order twice: the second insert fails, and its event rolls back with it.
        for _ in 0..2 {
            tb.broker::<SqlxBroker<Sqlite>>()
                .message(&order(7))
                .publish()
                .await?;
        }
        tb.advance(POLL).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("checkouts")
            .assert_called(2)
            .settled(HandlerOutcome::drop());
        tb.broker::<MemoryBroker>()
            .published::<OrderPlaced>("orders")
            .assert_called_once();
        Ok(())
    }
}
