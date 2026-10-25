//! Invoices on SQLite on the default clock: the queue reads "now" off the host's system clock
//! to claim, delay and mark rows, in the type of each time column.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_time --features inbox,sqlite,chrono,testing
//! ```

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:time]
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `invoice_jobs`. No `clock = ..`: the table reads `SystemClock`.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "invoice_jobs")]
struct SendInvoice {
    #[field(id, generated)]
    id: i64,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<SendInvoice>::new("invoices"))]
async fn send(invoice: &Invoice) -> HandlerOutcome {
    if deliver(invoice).await {
        // `processed_at` takes the host's time.
        return HandlerOutcome::ack();
    }
    // An hour by the host's clock: `retry_after` takes now plus an hour.
    HandlerOutcome::retry_after(Duration::from_hours(1))
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SendInvoice>("invoices"), |b| {
            b.include(send);
        })
}
// --8<-- [end:time]

/// A stand-in for the mail server: it takes every invoice that has an address.
async fn deliver(invoice: &Invoice) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(number = %invoice.number, "sending an invoice");
    !invoice.to.is_empty()
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:billing.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS invoice_jobs (
    id INTEGER PRIMARY KEY,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    locked_until TEXT,
    processed_at TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for SendInvoice {
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
#[outgoing(name = "invoices")]
struct Invoice {
    number: String,
    to: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use chrono::{DateTime, Utc};
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Invoice, SCHEMA, service};

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

    // In process the queue's "now" is the tokio clock, anchored to the system clock when the
    // broker connected: two hours on a paused clock are two hours for the queue.
    #[tokio::test]
    async fn in_process_the_queue_marks_rows_on_the_tokio_clock() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_time").await?;
        tokio::time::pause();
        let tb = TestApp::start(service(&url)).await?;
        tb.advance(Duration::from_hours(2)).await?;
        let invoice = Invoice {
            number: "2026-1042".to_owned(),
            to: "ada@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&invoice)
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::ack());

        tokio::time::resume();
        let marked: DateTime<Utc> = sqlx::query_scalar("SELECT processed_at FROM invoice_jobs")
            .fetch_one(&pool)
            .await?;
        let ahead = marked - Utc::now();
        assert!(
            (119..=120).contains(&ahead.num_minutes()),
            "marked two hours ahead of the wall clock: {ahead}"
        );
        Ok(())
    }
}
