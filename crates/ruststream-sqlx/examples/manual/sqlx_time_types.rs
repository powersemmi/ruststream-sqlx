//! The service of `examples/sqlx_time_types.rs` without the macros: `Lease<OffsetDateTime>` and
//! `ProcessedAt<OffsetDateTime>` in the type give the columns' time type.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_time_types --features inbox,sqlite,time,testing
//! ```

use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload, ProcessedAt};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:queue_time]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;
use time::OffsetDateTime;

/// One row of `backup_jobs`, with its times in the `time` crate's type.
#[derive(sqlx::FromRow)]
struct Backup {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Backup {
    type Id = i64;
    type Table = InboxSpec<(Lease<OffsetDateTime>, ProcessedAt<OffsetDateTime>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("backup_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Backup {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct BackUp;

impl Handle<Volume> for BackUp {
    fn handle(
        &self,
        volume: &Volume,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(name = %volume.name, "backing up");
        ready(Ok(()))
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("backups", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<Backup>("backups"), |b| {
            let back_up =
                subscriber("backups", BackUp).map_source(|_| InboxQueue::<Backup>::new("backups"));
            b.include(back_up.build());
        })
}
// --8<-- [end:queue_time]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:backups.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS backup_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    processed_at TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for Backup {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO backup_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "backups")]
struct Volume {
    name: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Volume, service};

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_mark_is_written_as_the_time_crate_writes_it() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_time_types").await?;
        let tb = TestApp::start(service(&url)).await?;
        let volume = Volume {
            name: "db-primary".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&volume)
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("backups")
            .assert_called_once()
            .settled(HandlerOutcome::ack());

        // sqlx writes a `time` value as RFC 3339 text in UTC with a `Z`, where a `chrono` time
        // ends in `+00:00`.
        let marked: String = sqlx::query_scalar("SELECT processed_at FROM backup_jobs")
            .fetch_one(&pool)
            .await?;
        assert!(marked.ends_with('Z'), "{marked}");
        Ok(())
    }
}
