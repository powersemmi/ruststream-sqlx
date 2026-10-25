//! A headers column: the delivery's headers kept as a JSON object of strings.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_headers --features inbox,sqlite,chrono,json,testing
//! ```

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:headers]
use ruststream_sqlx::HeaderColumn;
use sqlx::types::Json;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "export_jobs")]
struct ExportJob {
    #[field(id, generated)]
    id: i64,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(headers)]
    meta: Option<Json<BTreeMap<String, String>>>,
    #[field(payload)]
    payload: Vec<u8>,
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

#[subscriber(InboxQueue::<ExportJob>::new("exports"))]
async fn send_export(export: &Export, Headers(tenant): Headers<Tenant>) -> HandlerOutcome {
    if tenant.tenant != "acme" {
        tracing::warn!(tenant = %tenant.tenant, "a tenant this service does not serve");
        return HandlerOutcome::drop();
    }
    tracing::info!(export.report, tenant = %tenant.tenant, "exporting");
    HandlerOutcome::ack()
}
// --8<-- [end:headers]

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
            b.include(send_export);
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "exports")]
struct Export {
    report: u64,
}

/// The header the service routes on.
#[derive(Debug, Deserialize)]
struct Tenant {
    #[serde(rename = "x-tenant")]
    tenant: String,
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
        let (url, pool, _keeper) = database("sqlx_headers_served").await?;
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
        assert_eq!(
            kept, r#"{"x-tenant":"acme"}"#,
            "the column keeps the headers"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tenant_the_service_does_not_serve_is_dropped() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_headers_foreign").await?;
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
