//! The service of `examples/sqlx_headers.rs` without the macros: `.headers(..)` names the column,
//! and `HeaderRow` lends the field that holds it.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_headers --features inbox,sqlite,chrono,json,testing,asyncapi
//! ```

use std::collections::BTreeMap;
use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

use ruststream_sqlx::prelude::*;

// --8<-- [start:headers]
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Headers as HeadersColumn, Lease, Payload, ProcessedAt};
use ruststream_sqlx::{HeaderColumn, HeaderRow, InboxSpec, InboxTable, PayloadRow};
use sqlx::types::Json;

#[derive(sqlx::FromRow)]
struct ExportJob {
    id: i64,
    meta: Option<Json<BTreeMap<String, String>>>,
    payload: Vec<u8>,
}

impl InboxTable for ExportJob {
    type Id = i64;
    type Table = InboxSpec<(
        ProcessedAt<DateTime<Utc>>,
        Lease<DateTime<Utc>>,
        HeadersColumn,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("export_jobs", Column::new("id").generated())
        .processed_at(Column::new("processed_at"))
        .lease(Column::new("locked_until"))
        .headers(Column::new("meta"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl HeaderRow for ExportJob {
    type Column = Option<Json<BTreeMap<String, String>>>;

    fn headers_mut(&mut self) -> &mut Self::Column {
        &mut self.meta
    }
}

impl PayloadRow for ExportJob {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl Publish<Sqlite> for ExportJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        // The column keeps the message's headers as the delivery reads them back.
        let headers = Json::<BTreeMap<String, String>>::from_headers(message.headers());
        sqlx::query("INSERT INTO export_jobs (meta, payload) VALUES (?, ?)")
            .bind(headers)
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

struct Exporter;

impl Handle<Export> for Exporter {
    fn handle(
        &self,
        export: &Export,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let tenant = ctx.headers().get_str("x-tenant").unwrap_or_default();
        if tenant != "acme" {
            tracing::warn!(tenant, "a tenant this service does not serve");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(export.report, tenant, "exporting");
        ready(Ok(()))
    }
}
// --8<-- [end:headers]

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("exports", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<ExportJob>("exports"),
        |b| {
            let export = subscriber("exports", Exporter)
                .map_source(|_| InboxQueue::<ExportJob>::new("exports"));
            b.include(export.build());
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:exports.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS export_jobs (
    id INTEGER PRIMARY KEY,
    processed_at TEXT,
    locked_until TEXT,
    meta TEXT,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "exports")]
struct Export {
    report: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::HeaderMap;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Export, SCHEMA, service};

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

    fn tenant(name: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", name.to_owned());
        headers
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_header_rides_the_column_to_the_handler() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_headers_served").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Export { report: 7 })
            .with_headers(tenant("acme"))
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("exports")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let kept: String = sqlx::query_scalar("SELECT meta FROM export_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            kept, r#"{"x-tenant":"acme"}"#,
            "the column keeps the headers"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tenant_the_service_does_not_serve_is_dropped() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_headers_foreign").await?;
        let tb = TestApp::start(service(pool)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Export { report: 8 })
            .with_headers(tenant("globex"))
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("exports")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }
}
