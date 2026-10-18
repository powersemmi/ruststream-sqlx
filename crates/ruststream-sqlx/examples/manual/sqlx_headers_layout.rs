//! The service of `examples/sqlx_headers_layout.rs` without the macros: one description on the
//! message holds the whole table, and `HeaderFields` builds the header map from its fields.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_headers_layout --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::schemars::JsonSchema;
use sqlx::SqlitePool;

use ruststream_sqlx::prelude::*;

// --8<-- [start:layout]
use ruststream::HeaderMap;
use ruststream::runtime::SoloCarried;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{self, Attempt, Lease};
use ruststream_sqlx::{AttemptRow, HeaderFields, InboxSpec, InboxTable, put_header};

#[derive(Debug, Clone, sqlx::FromRow, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderHeaders {
    job_id: i64,
    attempt: i16,
    tenant: String,
    trace: Option<String>,
    order_id: i64,
}

#[derive(Debug, Clone, sqlx::FromRow, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderJob {
    #[sqlx(flatten)]
    headers: OrderHeaders,
    note: Option<String>,
}

impl InboxTable for OrderJob {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Attempt, spec::HeaderFields)>;
    const TABLE: Self::Table = InboxSpec::new("order_jobs", Column::new("job_id").generated())
        .lease(Column::new("locked_until"))
        .attempt(Column::new("attempt").generated())
        .data(&[
            Column::new("tenant"),
            Column::new("trace"),
            Column::new("order_id"),
        ])
        .fetching(&[Column::new("note")])
        .header_fields();

    fn id(&self) -> &i64 {
        &self.headers.job_id
    }
}

impl Input for OrderJob {
    type Axis = SoloCarried<Self>;
}

impl AttemptRow for OrderJob {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.headers.attempt
    }
}

impl HeaderFields for OrderJob {
    const NAMES: &'static [&'static str] = &["tenant", "trace", "order_id"];

    fn header_map(&self) -> HeaderMap {
        let mut headers = HeaderMap::with_capacity(Self::NAMES.len());
        put_header(&mut headers, "tenant", &self.headers.tenant);
        put_header(&mut headers, "trace", &self.headers.trace);
        put_header(&mut headers, "order_id", &self.headers.order_id);
        headers
    }
}

struct Packer;

impl Handle<OrderJob> for Packer {
    fn handle(
        &self,
        job: &OrderJob,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        // The header map is built from the fields on its first read.
        let tenant = ctx.headers().get_str("tenant").unwrap_or_default();
        tracing::info!(tenant, order = job.headers.order_id, note = ?job.note, "packing");
        ready(Ok(()))
    }
}
// --8<-- [end:layout]

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        let pack =
            subscriber("packing", Packer).map_source(|_| InboxQueue::<OrderJob>::new("packing"));
        b.include(pack.build());
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:shop.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS order_jobs (
    job_id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    tenant TEXT NOT NULL,
    trace TEXT,
    order_id INTEGER NOT NULL,
    note TEXT
)";

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::{TestApp, TestError};
    use ruststream_sqlx::SqlxBrokerError;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{OrderJob, SCHEMA, service};

    /// The queue table before the release that added `note`.
    const PREVIOUS: &str = "CREATE TABLE order_jobs (
        job_id INTEGER PRIMARY KEY,
        attempt INTEGER NOT NULL DEFAULT 1,
        locked_until TEXT,
        tenant TEXT NOT NULL,
        trace TEXT,
        order_id INTEGER NOT NULL
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

    // A row the service writes itself reaches the harness's books once a claim takes it, so the
    // test runs on the real clock and lets the claim happen.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_handler_takes_the_job_with_its_note() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_headers_layout_packed", SCHEMA).await?;
        sqlx::query("INSERT INTO order_jobs (tenant, order_id, note) VALUES (?, ?, ?)")
            .bind("acme")
            .bind(42_i64)
            .bind("fragile")
            .execute(&pool)
            .await?;
        let tb = TestApp::start_live(service(pool)).await?;
        tb.advance(Duration::from_millis(500)).await?;

        let lent = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("packing")
            .assert_called_once()
            .settled(HandlerOutcome::ack())
            .received_values::<OrderJob>();
        let read: Vec<(i64, Option<&str>)> = lent
            .iter()
            .map(|job| (job.headers.order_id, job.note.as_deref()))
            .collect();
        assert_eq!(read, [(42, Some("fragile"))]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_message_column_the_table_lacks_stops_startup() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_headers_layout_previous", PREVIOUS).await?;
        let failed: Option<TestError> = TestApp::start(service(pool)).await.err();

        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(sqlx) = err.downcast_ref::<SqlxBrokerError>() {
                assert!(
                    matches!(sqlx, SqlxBrokerError::Schema { table, .. } if table == "order_jobs"),
                    "{sqlx}"
                );
                return Ok(());
            }
            cause = err.source();
        }
        panic!("startup did not fail with a SqlxBrokerError: {failed:?}");
    }
}
