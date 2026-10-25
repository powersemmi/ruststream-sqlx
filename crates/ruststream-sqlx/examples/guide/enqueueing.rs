//! The service of the Enqueueing jobs guide: an order and its emails written in one transaction,
//! a reminder scheduled for later, urgent emails first, and sent emails kept for an audit.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example guide_enqueueing --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, TimeDelta, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

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
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Sqlite, SqlitePool};

/// One row of `email_jobs`.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
struct SendEmail {
    #[field(id, generated)]
    id: i64,
    #[field(priority)]
    priority: i64,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
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
            processed_at: None,
            locked_until: None,
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
#[subscriber(InboxQueue::<Checkout>::new("checkouts"))]
async fn checkout(order: &Order, Ctx(pool): Ctx<keys::Pool<Sqlite>>) -> HandlerOutcome {
    match place_order(&pool, order).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(error) => {
            tracing::warn!(subscription = "checkouts", order.id, %error, "order not placed");
            HandlerOutcome::drop()
        }
    }
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    tracing::info!(order = email.order, template = %email.template, "sending");
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
        .with_broker(broker.route::<Checkout>("checkouts"), |b| {
            b.include(checkout);
            b.include(send);
        })
}
// --8<-- [end:service]

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
    email: String,
}

/// An email to send, as the payload column holds it.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
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
            email: "ada@example.com".to_owned(),
        }
    }

    // The rows `place_order` writes wake no subscription: the service sees them within its poll
    // interval, so these tests run on the real clock past it.
    const POLL: Duration = Duration::from_secs(2);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_confirmation_is_sent_and_the_reminder_waits() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("guide_enqueueing_order").await?;
        let tb = TestApp::start_live(service(&url)).await?;
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
        let (url, pool, _keeper) = database("guide_enqueueing_rollback").await?;
        let tb = TestApp::start(service(&url)).await?;
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
        let (url, pool, _keeper) = database("guide_enqueueing_priority").await?;
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
        let tb = TestApp::start_live(service(&url)).await?;
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
