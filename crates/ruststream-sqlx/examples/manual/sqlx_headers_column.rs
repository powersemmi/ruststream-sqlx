//! The service of `examples/sqlx_headers_column.rs` without the macros: `HeaderRow` lends the
//! field of the service's own column type.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_headers_column --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Headers as HeadersColumn, Lease, Payload, ProcessedAt};
use ruststream_sqlx::{HeaderRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:column]
use ruststream::HeaderMap;
use ruststream_sqlx::HeaderColumn;

/// Headers kept one per line, `name: value`, in a `TEXT` column.
#[derive(sqlx::Type)]
#[sqlx(transparent)]
struct Lines(String);

impl HeaderColumn for Lines {
    fn take_headers(&mut self) -> HeaderMap {
        let headers = self
            .0
            .lines()
            .filter_map(|line| line.split_once(": "))
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        self.0.clear();
        headers
    }

    fn from_headers(headers: &HeaderMap) -> Self {
        let lines: Vec<String> = headers
            .iter()
            .map(|(name, value)| format!("{name}: {}", String::from_utf8_lossy(value)))
            .collect();
        Self(lines.join("\n"))
    }

    fn unfit(headers: &HeaderMap) -> Option<&str> {
        // A value comes back as it went in when it is text on one line.
        let fits = |value: &[u8]| str::from_utf8(value).is_ok_and(|text| !text.contains('\n'));
        headers
            .iter()
            .find(|(_, value)| !fits(value))
            .map(|(name, _)| name)
    }
}

#[derive(sqlx::FromRow)]
struct ExportJob {
    id: i64,
    meta: Lines,
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
    type Column = Lines;

    fn headers_mut(&mut self) -> &mut Lines {
        &mut self.meta
    }
}

impl Publish<Sqlite> for ExportJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        // The column keeps the message's headers as the delivery reads them back.
        let headers = Lines::from_headers(message.headers());
        sqlx::query("INSERT INTO export_jobs (meta, payload) VALUES (?, ?)")
            .bind(headers)
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}
// --8<-- [end:column]

impl PayloadRow for ExportJob {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
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

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("exports", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<ExportJob>("exports"), |b| {
            let export = subscriber("exports", Exporter)
                .map_source(|_| InboxQueue::<ExportJob>::new("exports"));
            b.include(export.build());
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:exports.db?mode=rwc")
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

    fn tenant(name: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", name.to_owned());
        headers
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_header_rides_the_column_to_the_handler() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_headers_column_served").await?;
        let tb = TestApp::start(service(&url)).await?;
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
        assert_eq!(kept, "x-tenant: acme", "the column keeps the headers");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tenant_the_service_does_not_serve_is_dropped() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("manual_sqlx_headers_column_foreign").await?;
        let tb = TestApp::start(service(&url)).await?;
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
