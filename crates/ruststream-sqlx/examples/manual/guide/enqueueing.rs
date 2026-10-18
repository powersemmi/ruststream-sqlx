//! The service of `examples/guide/enqueueing.rs` without the macros: the table is described
//! with `InboxSpec`, its insert is written by hand, and the handler is a named type.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_guide_enqueueing --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, TimeDelta, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::PoolContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload, ProcessedAt, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};

// --8<-- [start:schema]
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, email TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS checkout_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS email_jobs (
    id INTEGER PRIMARY KEY,
    priority INTEGER NOT NULL,
    retry_after TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    processed_at TEXT,
    locked_until TEXT,
    payload BLOB NOT NULL
);
";
// --8<-- [end:schema]

// --8<-- [start:table]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

/// One row of `email_jobs`.
#[derive(sqlx::FromRow)]
struct SendEmail {
    id: i64,
    priority: i64,
    retry_after: DateTime<Utc>,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SendEmail {
    type Id = i64;
    type Table = InboxSpec<(
        RetryAfter<DateTime<Utc>>,
        Attempt,
        ProcessedAt<DateTime<Utc>>,
        Lease<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("id").generated())
        .priority(Column::new("priority"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt").generated())
        .processed_at(Column::new("processed_at"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for SendEmail {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for SendEmail {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl Insert<SqliteConnection> for SendEmail {
    async fn insert(&self, conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO email_jobs (priority, retry_after, payload) VALUES (?, ?, ?)")
            .bind(self.priority)
            .bind(self.retry_after)
            .bind(&self.payload)
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// A smaller priority is claimed first.
const URGENT: i64 = 0;
const ROUTINE: i64 = 10;

impl SendEmail {
    /// A job for `email`, due at `due`.
    fn new(email: &Email, priority: i64, due: DateTime<Utc>) -> serde_json::Result<Self> {
        Ok(Self {
            id: 0,
            priority,
            retry_after: due,
            attempt: 1,
            payload: serde_json::to_vec(email)?,
        })
    }
}
// --8<-- [end:table]

// --8<-- [start:place_order]
/// The order, its confirmation and a reminder three days later commit together, or not at all.
async fn place_order(pool: &SqlitePool, order: &Order) -> Result<(), Box<dyn Error>> {
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO orders (id, email) VALUES (?, ?)")
        .bind(order.id)
        .bind(&order.email)
        .execute(&mut *tx)
        .await?;
    let now = Utc::now();
    let confirmation = Email::confirmation(order);
    SendEmail::new(&confirmation, URGENT, now)?
        .insert(&mut *tx)
        .await?;
    let reminder = Email::review_request(order);
    SendEmail::new(&reminder, ROUTINE, now + TimeDelta::days(3))?
        .insert(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
// --8<-- [end:place_order]

// --8<-- [start:service]
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

impl Handle<Email> for Sender {
    fn handle(
        &self,
        email: &Email,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order = email.order, template = %email.template, "sending");
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
            let send =
                subscriber("emails", Sender).map_source(|_| InboxQueue::<SendEmail>::new("emails"));
            b.include(send.build());
        },
    )
}
// --8<-- [end:service]

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
struct CheckoutJob {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for CheckoutJob {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("checkout_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for CheckoutJob {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
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

/// An order as the shop's front end sends it.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: i64,
    email: String,
}

// `#[outgoing(name = "checkouts")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "checkouts";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// An email to send, as the payload column holds it.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Email {
    order: i64,
    to: String,
    template: String,
}

impl Email {
    fn confirmation(order: &Order) -> Self {
        Self {
            order: order.id,
            to: order.email.clone(),
            template: "confirmation".to_owned(),
        }
    }

    fn review_request(order: &Order) -> Self {
        Self {
            order: order.id,
            to: order.email.clone(),
            template: "review-request".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use chrono::Utc;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Email, Order, ROUTINE, SCHEMA, SendEmail, URGENT, service};

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
            email: "ada@example.com".to_owned(),
        }
    }

    // The rows `place_order` writes wake no subscription: the service sees them within its poll
    // interval, so these tests run on the real clock past it.
    const POLL: Duration = Duration::from_secs(2);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_confirmation_is_sent_and_the_reminder_waits() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_guide_enqueueing_order").await?;
        let tb = TestApp::start_live(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&order(7))
            .publish()
            .await?;
        tb.advance(POLL).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called_once()
            .with(&Email::confirmation(&order(7)))
            .settled(HandlerOutcome::ack());
        // The sent email stays, marked; the reminder waits for its day.
        let rows: Vec<(i64, bool)> =
            sqlx::query_as("SELECT priority, processed_at IS NOT NULL FROM email_jobs ORDER BY id")
                .fetch_all(&pool)
                .await?;
        assert_eq!(rows, [(URGENT, true), (ROUTINE, false)]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_that_fails_queues_no_email() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_guide_enqueueing_rollback").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        // The same order twice: the second insert fails, and its emails roll back with it.
        for _ in 0..2 {
            tb.broker::<SqlxBroker<Sqlite>>()
                .message(&order(7))
                .publish()
                .await?;
        }

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("checkouts")
            .assert_called(2)
            .settled(HandlerOutcome::drop());
        let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM email_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(jobs, 2, "only the first order's two emails");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_urgent_email_goes_first() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_guide_enqueueing_priority").await?;
        let now = Utc::now();
        let newsletter = Email {
            order: 0,
            to: "ada@example.com".to_owned(),
            template: "newsletter".to_owned(),
        };
        let reset = Email {
            order: 0,
            to: "ada@example.com".to_owned(),
            template: "password-reset".to_owned(),
        };
        // Both wait in the table before the service starts; the routine one was written first.
        SendEmail::new(&newsletter, ROUTINE, now)?
            .insert(&mut *pool.acquire().await?)
            .await?;
        SendEmail::new(&reset, URGENT, now)?
            .insert(&mut *pool.acquire().await?)
            .await?;
        let tb = TestApp::start_live(service(pool)).await?;
        tb.advance(POLL).await?;

        let sent = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called(2)
            .received::<Email>();
        assert_eq!(sent, [reset, newsletter]);
        Ok(())
    }
}
