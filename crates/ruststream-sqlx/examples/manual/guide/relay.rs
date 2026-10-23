//! The service of `examples/guide/relay.rs` without the macros: the tables are described with
//! `InboxSpec`, and the handlers are named types, the relay's with a reply.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_guide_relay --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::dialect::insert::{self, Sql};
use ruststream_sqlx::keys::TxContext;
use ruststream_sqlx::spec::{Key, Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, KeyRow, PayloadRow};
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
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

/// One row of `order_events`: an event waiting for the relay, keyed by its order.
#[derive(sqlx::FromRow)]
struct OrderEvent {
    id: i64,
    aggregate: String,
    payload: Vec<u8>,
}

impl InboxTable for OrderEvent {
    type Id = i64;
    type Table = InboxSpec<(Key, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("order_events", Column::new("id").generated())
        .partition_key(Column::new("aggregate"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl KeyRow for OrderEvent {
    type Key = String;

    fn partition_key(&self) -> &String {
        &self.aggregate
    }
}

impl PayloadRow for OrderEvent {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

// INSERT INTO `order_events` (`aggregate`, `locked_until`, `payload`) VALUES (?, ?, ?)
const INSERT: Sql<160> = insert::sqlite(&OrderEvent::TABLE.spec());

impl Insert<SqliteConnection> for OrderEvent {
    async fn insert(&self, conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
        sqlx::query(INSERT.as_str())
            .bind(&self.aggregate)
            .bind(None::<DateTime<Utc>>)
            .bind(&self.payload)
            .execute(conn)
            .await?;
        Ok(())
    }
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
        payload: serde_json::to_vec(&placed)?,
    };
    event.insert(conn).await?;
    Ok(())
}

struct PlaceOrder;

impl Handle<Order, (), (), TxContext<Sqlite>> for PlaceOrder {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, TxContext<Sqlite>>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(mut tx) = Ctx::<keys::Tx<Sqlite>>::from_context(ctx).await?;
        place(&mut tx, order).await.map_err(|error| {
            tracing::warn!(subscription = "checkouts", order.id, %error, "order not placed");
            HandlerOutcome::drop()
        })
    }
}
// --8<-- [end:write]

// --8<-- [start:relay]
use ruststream::memory::{MemoryBroker, MemoryPublish};

/// Each event goes out on the bus as it was written; the acknowledgement deletes its row.
struct Relay;

impl Handle<OrderPlaced, OrderPlaced> for Relay {
    fn handle(
        &self,
        event: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<OrderPlaced, HandlerOutcome>> {
        ready(Ok(event.clone()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    // The target broker is wrapped first, so its publish token exists before the scopes.
    let bus = MemoryBroker::new().bindable();
    let to_bus = bus.bind(MemoryPublish);
    RustStream::new(AppInfo::new("shop", "0.1.0"))
        .with_broker(SqlxBroker::new(pool).route::<Checkout>("checkouts"), |b| {
            let checkout = subscriber("checkouts", PlaceOrder)
                .map_source(|_| InboxQueue::<Checkout>::new("checkouts"))
                .transactional();
            b.include(checkout.build());
            let relay = subscriber("order-events", Relay)
                .map_source(|_| InboxQueue::<OrderEvent>::new("order-events"))
                .workers_by_key(nonzero!(4))
                .reply();
            b.include(relay.build()).out_reply(to_bus);
        })
        .with_broker(bus, |_b| {})
}
// --8<-- [end:relay]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:shop.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

/// The checkouts the shop's front end queues.
#[derive(sqlx::FromRow)]
struct Checkout {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Checkout {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("checkout_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Checkout {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
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
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: i64,
    total_cents: i64,
}

// `#[outgoing(name = "checkouts")]` by hand: the type names where it is published.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "checkouts";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// The event other services read on the bus.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderPlaced {
    order: i64,
    total_cents: i64,
}

// `#[outgoing(name = "orders")]` by hand: the type names where it is published.
impl OutgoingDestination for OrderPlaced {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for OrderPlaced {
    type Contract = NoHeaders;
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
        let (pool, _keeper) = database("manual_guide_relay_placed").await?;
        let tb = TestApp::start_live(service(pool.clone())).await?;
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
        let (pool, _keeper) = database("manual_guide_relay_failed").await?;
        let tb = TestApp::start_live(service(pool)).await?;
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
