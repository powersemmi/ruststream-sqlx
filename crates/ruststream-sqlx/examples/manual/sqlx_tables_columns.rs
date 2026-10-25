//! The service of `examples/sqlx_tables_columns.rs` without the macros: the description names
//! each column as the table writes it, and sqlx's attributes name the struct's own fields.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_tables_columns --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:table]
#[derive(sqlx::FromRow)]
struct LegacyJob {
    #[sqlx(rename = "JobID")]
    job_id: i64,
    body: Vec<u8>,
}

impl InboxTable for LegacyJob {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("LegacyJobs", Column::new("JobID").generated())
        .group(Column::new("type"))
        .lease(Column::new("lockedUntil"))
        .payload(Column::new("body"));

    fn id(&self) -> &i64 {
        &self.job_id
    }
}

impl PayloadRow for LegacyJob {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.body
    }
}
// --8<-- [end:table]

struct Importer;

impl Handle<Import> for Importer {
    fn handle(
        &self,
        import: &Import,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(file = %import.file, "importing");
        ready(Ok(()))
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("importer", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<LegacyJob>("imports"), |b| {
            let import = subscriber("imports", Importer)
                .map_source(|_| InboxQueue::<LegacyJob>::new("imports"));
            b.include(import.build());
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:imports.db?mode=rwc")
}

/// A table another system created, with its own column names.
const SCHEMA: &str = r#"CREATE TABLE IF NOT EXISTS "LegacyJobs" (
    "JobID" INTEGER PRIMARY KEY,
    "type" TEXT NOT NULL,
    "lockedUntil" TEXT,
    "body" BLOB NOT NULL
)"#;

/// A publish writes a row of the type it names.
impl Publish<Sqlite> for LegacyJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(r#"INSERT INTO "LegacyJobs" ("type", "body") VALUES (?, ?)"#)
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "imports")]
struct Import {
    file: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Import, SCHEMA, service};

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
    async fn a_table_with_its_own_column_names_serves_the_queue() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_tables_columns").await?;
        let tb = TestApp::start(service(&url)).await?;
        let import = Import {
            file: "prices.csv".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&import)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("imports")
            .assert_called_once()
            .with(&import)
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar(r#"SELECT count(*) FROM "LegacyJobs""#)
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "the acknowledgement deleted the row");
        Ok(())
    }
}
