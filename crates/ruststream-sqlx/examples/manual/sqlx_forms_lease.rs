//! The service of `examples/sqlx_forms_lease.rs` without the macros: `.lease(..)` selects the
//! form, and `Lease<Time>` in the type gives the lease's time type.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_forms_lease --features inbox,sqlite,chrono,testing
//! ```

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:lease]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `report_jobs`: `.lease(..)` makes the table a lease table.
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

/// A report takes minutes to render. The subscription extends the minute's lease every thirty
/// seconds while the handler runs; after a crash the row waits out its minute, then returns.
struct Render;

impl Handle<Report> for Render {
    async fn handle(
        &self,
        report: &Report,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        draw(report).await;
        tracing::info!(report.id, "report rendered");
        Ok(())
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("reports", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<RenderReport>("reports"), |b| {
            let render = subscriber("reports", Render).map_source(|_| {
                InboxQueue::<RenderReport>::new("reports").lease(Duration::from_mins(1))
            });
            b.include(render.build());
        })
}
// --8<-- [end:lease]

/// A stand-in for the renderer: ten seconds a page.
async fn draw(report: &Report) {
    tokio::time::sleep(Duration::from_secs(10 * u64::from(report.pages))).await;
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:reports.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS report_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "reports")]
struct Report {
    id: u64,
    pages: u32,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Report, SCHEMA, service};

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

    // The clock is paused, so the five minutes of rendering pass at once, and the lease's
    // extensions run as the clock reaches them.
    #[tokio::test]
    async fn a_report_rendered_past_its_lease_is_delivered_once() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_forms_lease").await?;
        tokio::time::pause();
        let tb = TestApp::start(service(&url)).await?;
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
}
