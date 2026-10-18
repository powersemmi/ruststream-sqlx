//! The service of `examples/sqlx_forms_row_lock.rs` without the macros: a description with no
//! `.lease(..)` and no `.advisory(..)` takes rows by row lock.
//!
//! The pool reads its settings from the `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and
//! `PGDATABASE` environment variables.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_forms_row_lock --features inbox,postgres,testing
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::Payload;
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgConnection, Postgres};

// --8<-- [start:row_lock]
use ruststream_sqlx::prelude::*;
use sqlx::PgPool;

/// One row of `invoice_jobs`. No `.lease(..)` and no `.advisory(..)`: the row lock form.
#[derive(sqlx::FromRow)]
struct IssueInvoice {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for IssueInvoice {
    type Id = i64;
    type Table = InboxSpec<(Payload,)>;
    const TABLE: Self::Table = InboxSpec::new("invoice_jobs", Column::new("id").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for IssueInvoice {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The claim's transaction holds the row while the handler runs; the acknowledgement deletes
/// the row and commits. A crash rolls the transaction back, and the row returns at once.
struct Issue;

impl Handle<Invoice> for Issue {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(invoice.number, "issuing an invoice");
        ready(Ok(()))
    }
}

fn service(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<IssueInvoice>("invoices"),
        |b| {
            // Each of the four workers holds a connection while its invoice is in work.
            let issue = subscriber("invoices", Issue)
                .map_source(|_| InboxQueue::<IssueInvoice>::new("invoices"))
                .workers(nonzero!(4));
            b.include(issue.build());
        },
    )
}
// --8<-- [end:row_lock]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect_with(PgConnectOptions::new()).await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS invoice_jobs (
    id BIGSERIAL PRIMARY KEY,
    payload BYTEA NOT NULL
)";

impl Publish<Postgres> for IssueInvoice {
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
    number: u64,
}

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

    // Postgres is a server: the test takes a database of its own on the stand that
    // `POSTGRES_TEST_URL` names, and skips without it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acknowledged_invoice_deletes_its_row_and_commits() -> Result<(), Box<dyn Error>> {
        let Some(db) = live::postgres::database().await else {
            return Ok(());
        };
        sqlx::raw_sql(SCHEMA).execute(&db.pool).await?;
        let tb = TestApp::start(service(db.pool.clone())).await?;
        let invoice = Invoice { number: 1042 };
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
