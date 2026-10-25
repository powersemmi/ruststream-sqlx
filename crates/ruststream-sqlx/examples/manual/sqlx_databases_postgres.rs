//! The service of `examples/sqlx_databases_postgres.rs` without the macros: the table is
//! described with `InboxSpec`, and the handler is a named type mounted with `subscriber(..)`.
//!
//! The pool reads its settings from the `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and
//! `PGDATABASE` environment variables.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_databases_postgres --features inbox,postgres,testing,asyncapi
//! ```

use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, Postgres};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::postgres::PgPoolOptions;

// --8<-- [start:service]
/// One row of `invoice_jobs`. No `.lease(..)` and no `.advisory(..)`: the table takes its rows by
/// row lock.
#[derive(sqlx::FromRow)]
struct SendInvoice {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SendInvoice {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("invoice_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
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

struct SendInvoices;

impl Handle<Invoice> for SendInvoices {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(number = %invoice.number, "sending the invoice");
        ready(Ok(()))
    }
}

/// The service on the database `options` reach.
fn service(options: PgConnectOptions) -> RustStream {
    let schema = options.clone();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_connect_options(options, PgPoolOptions::new());
    RustStream::new(AppInfo::new("billing", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = PgConnection::connect_with(&schema).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SendInvoice>("invoices"), |b| {
            // Four claims in flight: each holds a connection of the pool while its handler runs.
            let send = subscriber("invoices", SendInvoices)
                .map_source(|_| InboxQueue::<SendInvoice>::new("invoices"));
            b.include(send.workers(nonzero!(4)).build());
        })
}
// --8<-- [end:service]

#[ruststream::app]
fn app() -> impl App {
    // The options read the `PG*` environment variables.
    service(PgConnectOptions::new())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS invoice_jobs (
    id BIGSERIAL PRIMARY KEY,
    attempt SMALLINT NOT NULL DEFAULT 1,
    payload BYTEA NOT NULL
)";

/// A publish to `invoices` writes a row.
impl Publish<Postgres> for SendInvoice {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO invoice_jobs (payload) VALUES ($1)")
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
    number: String,
}

// The live suites' gate: the test runs on a database of its own on the Postgres stand that
// `POSTGRES_TEST_URL` names, and skips without it.
#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::Postgres;

    use super::{Invoice, SCHEMA, live, service};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_invoice_is_sent_and_its_row_deleted() -> Result<(), Box<dyn Error>> {
        let Some(db) = live::postgres::database().await else {
            return Ok(());
        };
        sqlx::raw_sql(SCHEMA).execute(&db.pool).await?;
        let tb = TestApp::start(service(db.options())).await?;
        let invoice = Invoice {
            number: "2026-0042".to_owned(),
        };
        tb.broker::<SqlxBroker<Postgres>>()
            .message(&invoice)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Postgres>>()
            .subscriber("invoices")
            .assert_called_once()
            .with(&invoice)
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM invoice_jobs")
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(left, 0, "the acknowledgement deleted the row");
        tb.shutdown().await?;
        db.finish().await;
        Ok(())
    }
}
