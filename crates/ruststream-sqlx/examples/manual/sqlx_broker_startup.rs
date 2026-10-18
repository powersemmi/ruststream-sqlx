//! The service of `examples/sqlx_broker_startup.rs` without the macros: the table is described
//! with `InboxSpec`, and the handler reads the attempt off its context by its key.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_broker_startup --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:service]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

#[derive(sqlx::FromRow)]
struct SendInvoice {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SendInvoice {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    // The column this release adds; a table without it stops the subscription at startup.
    const TABLE: Self::Table = InboxSpec::new("invoice_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for SendInvoice {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for SendInvoice {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct Mail;

impl Handle<Invoice, (), (), InboxContext> for Mail {
    async fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ok(Ctx(attempt)) = Ctx::<keys::Attempt>::from_context(ctx).await;
        tracing::info!(invoice.number, ?attempt, "sending an invoice");
        Ok(())
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendInvoice>("invoices"),
        |b| {
            let send = subscriber("invoices", Mail)
                .map_source(|_| InboxQueue::<SendInvoice>::new("invoices"));
            b.include(send.build());
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:billing.db?mode=rwc").await?;
    // The schema this release reads, applied before the subscriptions check it.
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
    // the test holds one beside the pool.
    async fn database(
        name: &str,
        schema: &'static str,
    ) -> Result<(SqlitePool, SqliteConnection), Box<dyn Error>> {
        let options: SqliteConnectOptions =
            format!("sqlite:file:{name}?mode=memory&cache=shared").parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(schema).execute(&pool).await?;
        Ok((pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_migrated_table_starts_and_counts_the_attempt() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_broker_startup_migrated", SCHEMA).await?;
        let tb = TestApp::start(service(pool)).await?;
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
        let (pool, _keeper) = database("manual_sqlx_broker_startup_previous", PREVIOUS).await?;
        let failed = TestApp::start(service(pool)).await.err();

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
