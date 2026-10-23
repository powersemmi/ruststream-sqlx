//! The service of `examples/sqlx_subscriptions_batches.rs` without the macros: the table is
//! described with `InboxSpec`, and the batch handler implements `Handle<[Email]>`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_subscriptions_batches --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};

// --8<-- [start:batches]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `newsletter_jobs`, claimed under a lease.
#[derive(sqlx::FromRow)]
struct SendNewsletter {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for SendNewsletter {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("newsletter_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for SendNewsletter {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Up to fifty emails per claim; an address that cannot receive mail is dropped, and only its
/// own row is.
struct SendAll;

impl Handle<[Email]> for SendAll {
    fn handle(
        &self,
        emails: &[Email],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = emails.len(), "sending a batch");
        let outcomes = emails
            .iter()
            .map(|email| {
                if email.to.contains('@') {
                    HandlerOutcome::ack()
                } else {
                    HandlerOutcome::drop()
                }
            })
            .collect();
        // One outcome per email, in the batch's order.
        ready(Err(outcomes))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("newsletter", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        let send_all = subscriber("newsletter", SendAll)
            .map_source(|_| InboxQueue::<SendNewsletter>::new("newsletter"))
            .batch(nonzero!(50));
        b.include(send_all.build());
    })
}
// --8<-- [end:batches]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:newsletter.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS newsletter_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Email {
    to: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Email, SCHEMA, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool.
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

    // The rows wait in the table before the service starts, so its first claim takes them
    // together; the harness counts a row once a claim takes it, so the test runs on the real
    // clock and lets the claim happen.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_claim_is_the_batch_and_each_row_settles_on_its_own() -> Result<(), Box<dyn Error>>
    {
        let (pool, _keeper) = database("manual_sqlx_subscriptions_batches").await?;
        for to in ["ada@example.com", "no-address", "grace@example.com"] {
            sqlx::query("INSERT INTO newsletter_jobs (payload) VALUES (?)")
                .bind(serde_json::to_vec(&Email { to: to.to_owned() })?)
                .execute(&pool)
                .await?;
        }
        let tb = TestApp::start_live(service(pool.clone())).await?;
        tb.advance(Duration::from_millis(500)).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("newsletter")
            .assert_called_once()
            .assert_batch_sizes(&[3]);
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM newsletter_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            left, 0,
            "the acknowledged rows and the dropped one are deleted"
        );
        Ok(())
    }
}
