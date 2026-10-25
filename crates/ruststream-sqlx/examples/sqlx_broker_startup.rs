//! A release that counts attempts: its table gains an `attempt` column, and the service applies
//! the migration before it starts, so the subscription's startup check finds the column.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_broker_startup --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:service]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "invoice_jobs")]
struct SendInvoice {
    #[field(id, generated)]
    id: i64,
    // The column this release adds; a table without it stops the subscription at startup.
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<SendInvoice>::new("invoices"))]
async fn send(invoice: &Invoice, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    tracing::info!(invoice.number, ?attempt, "sending an invoice");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        // The schema this release reads, applied before the subscriptions check it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SendInvoice>("invoices"), |b| {
            b.include(send);
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:billing.db?mode=rwc")
}
// --8<-- [end:service]

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS invoice_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
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
    number: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::SqlxBrokerError;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Invoice, SCHEMA, service};

    /// The table as the previous release left it, before the migration added `attempt`.
    const PREVIOUS: &str = "CREATE TABLE invoice_jobs (
        id INTEGER PRIMARY KEY,
        locked_until TEXT,
        payload BLOB NOT NULL
    )";

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool it reads the table through.
    async fn database(
        name: &str,
        schema: &'static str,
    ) -> Result<(String, SqlitePool, SqliteConnection), Box<dyn Error>> {
        let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
        let options: SqliteConnectOptions = url.parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(schema).execute(&pool).await?;
        Ok((url, pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_migrated_table_starts_and_counts_the_attempt() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_broker_startup_migrated", SCHEMA).await?;
        let tb = TestApp::start(service(&url)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Invoice { number: 7 })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("invoices")
            .assert_called_once()
            .with(&Invoice { number: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_table_without_the_column_stops_startup() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_broker_startup_previous", PREVIOUS).await?;
        let failed = TestApp::start(service(&url)).await.err();

        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(sqlx) = err.downcast_ref::<SqlxBrokerError>() {
                assert!(
                    matches!(sqlx, SqlxBrokerError::Schema { table, .. } if table == "invoice_jobs"),
                    "{sqlx}"
                );
                return Ok(());
            }
            cause = err.source();
        }
        panic!("startup did not fail with a SqlxBrokerError: {failed:?}");
    }
}
