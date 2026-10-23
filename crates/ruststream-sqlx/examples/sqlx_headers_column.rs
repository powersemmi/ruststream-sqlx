//! A headers column of the service's own type: `Lines` keeps the headers one per line in text.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_headers_column --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

use ruststream_sqlx::prelude::*;

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
    meta: Lines,
    #[field(payload)]
    payload: Vec<u8>,
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

#[subscriber(InboxQueue::<ExportJob>::new("exports"))]
async fn send_export(export: &Export, Headers(tenant): Headers<Tenant>) -> HandlerOutcome {
    if tenant.tenant != "acme" {
        tracing::warn!(tenant = %tenant.tenant, "a tenant this service does not serve");
        return HandlerOutcome::drop();
    }
    tracing::info!(export.report, tenant = %tenant.tenant, "exporting");
    HandlerOutcome::ack()
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("exports", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<ExportJob>("exports"),
        |b| {
            b.include(send_export);
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
        let (pool, _keeper) = database("sqlx_headers_column_served").await?;
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
        assert_eq!(kept, "x-tenant: acme", "the column keeps the headers");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tenant_the_service_does_not_serve_is_dropped() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_headers_column_foreign").await?;
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
