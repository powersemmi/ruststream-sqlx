//! The header values of a headers struct: each field without a role becomes a header, its text
//! given by `HeaderField`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_headers_field --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::{Sqlite, SqlitePool};

use ruststream_sqlx::prelude::*;

// --8<-- [start:fields]
use ruststream_sqlx::HeaderField;

#[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
#[inbox(table = "shipment_jobs")]
struct ShipmentHeaders {
    #[field(id, generated)]
    job_id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    // Each field without a role is a header named after its column.
    carrier: Carrier,
    order_id: i64,
    express: bool,
    placed_at: DateTime<Utc>,
    trace: Option<String>,
}

/// A carrier, kept by its code in the table and named in capitals in the header.
#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(transparent)]
struct Carrier(String);

impl HeaderField for Carrier {
    fn header(&self) -> Option<Vec<u8>> {
        Some(self.0.to_uppercase().into_bytes())
    }
}
// --8<-- [end:fields]

#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
struct Shipment {
    #[field(headers)]
    #[sqlx(flatten)]
    headers: ShipmentHeaders,
}

#[subscriber(InboxQueue::<Shipment>::new("shipments"))]
async fn audit(
    shipment: &Shipment,
    Headers(seen): Headers<Seen>,
    Ctx(pool): Ctx<keys::Pool<Sqlite>>,
) -> HandlerOutcome {
    tracing::info!(job = shipment.headers.job_id, carrier = %seen.carrier, "auditing");
    let written = sqlx::query(
        "INSERT INTO shipment_audit (carrier, order_id, express, placed_at, trace) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&seen.carrier)
    .bind(&seen.order_id)
    .bind(&seen.express)
    .bind(&seen.placed_at)
    .bind(&seen.trace)
    .execute(&pool)
    .await;
    match written {
        Ok(_) => HandlerOutcome::ack(),
        Err(error) => {
            tracing::warn!(subscription = "shipments", %error, "audit not written");
            HandlerOutcome::retry()
        }
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(audit);
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:shipping.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS shipment_jobs (
    job_id INTEGER PRIMARY KEY,
    locked_until TEXT,
    carrier TEXT NOT NULL,
    order_id INTEGER NOT NULL,
    express INTEGER NOT NULL,
    placed_at TEXT NOT NULL,
    trace TEXT
);
CREATE TABLE IF NOT EXISTS shipment_audit (
    carrier TEXT NOT NULL,
    order_id TEXT NOT NULL,
    express TEXT NOT NULL,
    placed_at TEXT NOT NULL,
    trace TEXT
);
";

/// The delivery's headers as the audit keeps them: text, as the header map holds them.
#[derive(Debug, Deserialize)]
struct Seen {
    carrier: String,
    order_id: String,
    express: String,
    placed_at: String,
    trace: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, service};

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

    // A row the service writes itself reaches the harness's books once a claim takes it, so the
    // test runs on the real clock and lets the claim happen.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_field_becomes_its_header_text() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_headers_field").await?;
        sqlx::query(
            "INSERT INTO shipment_jobs (carrier, order_id, express, placed_at) \
             VALUES ('dhl', 42, 1, '2026-10-18T09:30:00+00:00')",
        )
        .execute(&pool)
        .await?;
        let tb = TestApp::start_live(service(pool.clone())).await?;
        tb.advance(Duration::from_millis(500)).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("shipments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let audit: (String, String, String, String, Option<String>) = sqlx::query_as(
            "SELECT carrier, order_id, express, placed_at, trace FROM shipment_audit",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            audit,
            (
                "DHL".to_owned(),
                "42".to_owned(),
                "true".to_owned(),
                "2026-10-18T09:30:00+00:00".to_owned(),
                None
            ),
            "the carrier by its own impl, numbers in decimal, times in RFC 3339, None left out"
        );
        Ok(())
    }
}
