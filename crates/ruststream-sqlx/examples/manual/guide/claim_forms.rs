//! The service of `examples/guide/claim_forms.rs` without the macros: the table is described with
//! `InboxSpec`, and the handler is a named type mounted with `subscriber(..)`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_guide_claim_forms --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

// --8<-- [start:schema]
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS report_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";
// --8<-- [end:schema]

// --8<-- [start:lease]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqlitePool};

/// One row of `report_jobs`: `.lease(..)` claims it under a lease.
#[derive(sqlx::FromRow)]
struct RenderReport {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for RenderReport {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("report_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for RenderReport {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for RenderReport {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// How long the row of a crashed process stays out of the queue.
const LEASE: Duration = Duration::from_mins(1);

struct Render;

impl Handle<Report, (), (), InboxContext> for Render {
    async fn handle(
        &self,
        report: &Report,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        if attempt > Some(1) {
            tracing::warn!(report.id, ?attempt, "rendering again after a lost attempt");
        }
        draw(report).await;
        tracing::info!(report.id, "report rendered");
        Ok(())
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<RenderReport>("reports"),
        |b| {
            let render = subscriber("reports", Render)
                .map_source(|_| InboxQueue::<RenderReport>::new("reports").lease(LEASE));
            b.include(render.build());
        },
    )
}
// --8<-- [end:lease]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:reports.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

/// A publish to `reports` writes a row with the service's own statement.
impl Publish<Sqlite> for RenderReport {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO report_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Report {
    id: u64,
    pages: u32,
}

// `#[outgoing(name = "reports")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Report {
    type Form = FixedName;
    const DESTINATION: &'static str = "reports";
}

impl MessageHeaders for Report {
    type Contract = NoHeaders;
}

/// A stand-in for the rendering: ten seconds a page.
async fn draw(report: &Report) {
    tokio::time::sleep(Duration::from_secs(10 * u64::from(report.pages))).await;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use chrono::{TimeDelta, Utc};
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Report, SCHEMA, service};

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

    // The clock is paused, so five minutes of rendering pass at once, and the lease's extensions
    // run as the clock reaches them.
    #[tokio::test]
    async fn a_report_rendered_past_its_lease_is_delivered_once() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_guide_claim_forms_long").await?;
        tokio::time::pause();
        let tb = TestApp::start(service(pool.clone())).await?;
        let report = Report { id: 7, pages: 30 };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&report)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("reports")
            .assert_called_once()
            .with(&report)
            .settled(HandlerOutcome::ack());
        tokio::time::resume();
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM report_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            left, 0,
            "the acknowledgement held the lease and deleted the row"
        );
        Ok(())
    }

    // Two rows another process claimed: one whose lease ran out when that process crashed, one
    // whose lease is still in force. The rows wake no subscription, so the test runs on the real
    // clock past the poll interval.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_crashed_claim_comes_back_once_its_lease_runs_out() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_guide_claim_forms_crash").await?;
        let now = Utc::now();
        for (id, locked_until) in [
            (1, now - TimeDelta::seconds(1)),
            (2, now + TimeDelta::hours(1)),
        ] {
            sqlx::query(
                "INSERT INTO report_jobs (attempt, locked_until, payload) VALUES (2, ?, ?)",
            )
            .bind(locked_until)
            .bind(serde_json::to_vec(&Report { id, pages: 0 })?)
            .execute(&pool)
            .await?;
        }
        let tb = TestApp::start_live(service(pool)).await?;
        tb.advance(Duration::from_secs(2)).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("reports")
            .assert_called_once()
            .with(&Report { id: 1, pages: 0 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
