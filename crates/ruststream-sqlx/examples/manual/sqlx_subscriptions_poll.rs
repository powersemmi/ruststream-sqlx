//! The service of `examples/sqlx_subscriptions_poll.rs` without the macros: the table is
//! described with `InboxSpec`, and the queue's poll interval rides the descriptor `map_source`
//! mounts.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_subscriptions_poll --features inbox,sqlite,chrono,testing
//! ```

use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};

// --8<-- [start:poll_interval]
use std::time::Duration;

use ruststream_sqlx::prelude::*;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Connection, SqliteConnection};

/// One row of `export_jobs`, which the warehouse's nightly job inserts with its own SQL.
#[derive(sqlx::FromRow)]
struct Export {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Export {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("export_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Export {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct Upload;

impl Handle<ExportFile> for Upload {
    fn handle(
        &self,
        file: &ExportFile,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(path = %file.path, "uploading an export");
        ready(Ok(()))
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    // Every other subscription of the broker waits five seconds after a claim that came back short.
    let broker = broker.poll_interval(Duration::from_secs(5));
    RustStream::new(AppInfo::new("exports", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            // The exports come from outside the service, so this queue claims five times a second.
            let upload = subscriber("exports", Upload).map_source(|_| {
                InboxQueue::<Export>::new("exports").poll_interval(Duration::from_millis(200))
            });
            b.include(upload.build());
        })
}
// --8<-- [end:poll_interval]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:exports.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS export_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct ExportFile {
    path: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{ExportFile, SCHEMA, service};

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

    // A row written with plain SQL reaches the subscription only through a claim at its poll
    // interval, so the test runs on the real clock and lets that much time pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_written_by_another_system_is_claimed_at_the_poll_interval()
    -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_subscriptions_poll").await?;
        let tb = TestApp::start_live(service(&url)).await?;
        // The subscription's first claim finds the table empty; then it waits its interval.
        tb.advance(Duration::from_millis(300)).await?;
        let file = ExportFile {
            path: "exports/2026-10-18.csv".to_owned(),
        };
        sqlx::query("INSERT INTO export_jobs (payload) VALUES (?)")
            .bind(serde_json::to_vec(&file)?)
            .execute(&pool)
            .await?;

        tb.advance(Duration::from_secs(1)).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("exports")
            .assert_called_once()
            .with(&file)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
