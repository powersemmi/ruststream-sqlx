//! `SqlxBrokerError` where a handler meets it: a publish the database refuses.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_broker_errors --features inbox,sqlite,chrono,testing
//! ```

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "payment_jobs")]
struct PaymentJob {
    #[field(id, generated)]
    id: i64,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(OutSlot)]
#[publishes(Invoice)]
struct Invoices;

// --8<-- [start:handler]
#[subscriber(InboxQueue::<PaymentJob>::new("payments"))]
async fn bill(payment: &Payment, Out(invoices): Out<impl Publisher, Invoices>) -> HandlerOutcome {
    let invoice = Invoice {
        payment: payment.id,
    };
    if let Err(error) = invoices.message(&invoice).publish().await {
        // The error names the destination, the table and the database's reason.
        tracing::warn!(subscription = "payments", payment.id, %error, "invoice not written");
        return HandlerOutcome::retry_after(Duration::from_secs(60));
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    let broker = broker
        .route::<PaymentJob>("payments")
        .route::<InvoiceJob>("invoices");
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            b.include(bill).out(Invoices, Routed).build();
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:billing.db?mode=rwc")
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS payment_jobs (
    id INTEGER PRIMARY KEY,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS invoice_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
";

/// The queue of invoices another service sends.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "invoice_jobs")]
struct InvoiceJob {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Sqlite> for PaymentJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO payment_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

impl Publish<Sqlite> for InvoiceJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO invoice_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    payment: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Invoice, Payment, SCHEMA, service};

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
    async fn a_written_invoice_acknowledges_the_payment() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_broker_errors_written").await?;
        let tb = TestApp::start(service(&url)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Payment { id: 7 })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .published::<Invoice>("invoices")
            .assert_called_once()
            .with(&Invoice { payment: 7 });
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_insert_the_database_refuses_delays_the_payment() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_broker_errors_refused").await?;
        let tb = TestApp::start(service(&url)).await?;
        // The table goes after startup, which creates it.
        sqlx::raw_sql("DROP TABLE invoice_jobs")
            .execute(&pool)
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Payment { id: 8 })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(Duration::from_secs(60)));
        let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM payment_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(waiting, 1, "the payment waits in its table");
        Ok(())
    }
}
