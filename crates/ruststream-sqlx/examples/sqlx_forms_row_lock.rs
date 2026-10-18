//! An invoice queue on Postgres in the row lock form, the form a table takes when it names no
//! other: a claim locks its rows with `FOR UPDATE SKIP LOCKED` in a transaction the settlement
//! commits.
//!
//! The pool reads its settings from the `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and
//! `PGDATABASE` environment variables.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_forms_row_lock --features inbox,postgres,testing
//! ```

use std::error::Error;

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgConnection, Postgres};

// --8<-- [start:row_lock]
use ruststream_sqlx::prelude::*;
use sqlx::PgPool;

/// One row of `invoice_jobs`. No `locked_until` and no `advisory_lock`: the row lock form.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "invoice_jobs")]
struct IssueInvoice {
    #[field(id, generated)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The claim's transaction holds the row while the handler runs; the acknowledgement deletes
/// the row and commits. A crash rolls the transaction back, and the row returns at once.
#[subscriber(InboxQueue::<IssueInvoice>::new("invoices"))]
async fn issue(invoice: &Invoice) -> HandlerOutcome {
    tracing::info!(invoice.number, "issuing an invoice");
    HandlerOutcome::ack()
}

fn service(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<IssueInvoice>("invoices"),
        |b| {
            // Each of the four workers holds a connection while its invoice is in work.
            b.include(issue.workers(nonzero!(4)));
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    number: u64,
}

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
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
