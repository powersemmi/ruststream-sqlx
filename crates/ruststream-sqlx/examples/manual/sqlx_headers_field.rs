//! The service of `examples/sqlx_headers_field.rs` without the macros: `HeaderFields` builds the
//! header map with `put_header`, which writes each value through its `HeaderField`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_headers_field --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::runtime::SoloCarried;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::PoolContext;
use ruststream_sqlx::spec::{self, Lease};
use ruststream_sqlx::{InboxSpec, InboxTable};
use serde::Deserialize;
use sqlx::{Sqlite, SqlitePool};

use ruststream_sqlx::prelude::*;

// --8<-- [start:fields]
use ruststream::HeaderMap;
use ruststream_sqlx::{HeaderField, HeaderFields, put_header};

#[derive(Debug, Clone, sqlx::FromRow, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    job_id: i64,
    carrier: Carrier,
    order_id: i64,
    express: bool,
    #[schemars(with = "String")]
    placed_at: DateTime<Utc>,
    trace: Option<String>,
}

impl HeaderFields for Shipment {
    const NAMES: &'static [&'static str] =
        &["carrier", "order_id", "express", "placed_at", "trace"];

    fn header_map(&self) -> HeaderMap {
        let mut headers = HeaderMap::with_capacity(Self::NAMES.len());
        put_header(&mut headers, "carrier", &self.carrier);
        put_header(&mut headers, "order_id", &self.order_id);
        put_header(&mut headers, "express", &self.express);
        put_header(&mut headers, "placed_at", &self.placed_at);
        put_header(&mut headers, "trace", &self.trace);
        headers
    }
}

/// A carrier, kept by its code in the table and named in capitals in the header.
#[derive(Debug, Clone, sqlx::Type, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[sqlx(transparent)]
struct Carrier(String);

impl HeaderField for Carrier {
    fn header(&self) -> Option<Vec<u8>> {
        Some(self.0.to_uppercase().into_bytes())
    }
}
// --8<-- [end:fields]

impl InboxTable for Shipment {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, spec::HeaderFields)>;
    const TABLE: Self::Table = InboxSpec::new("shipment_jobs", Column::new("job_id").generated())
        .lease(Column::new("locked_until"))
        .data(&[
            Column::new("carrier"),
            Column::new("order_id"),
            Column::new("express"),
            Column::new("placed_at"),
            Column::new("trace"),
        ])
        .header_fields();

    fn id(&self) -> &i64 {
        &self.job_id
    }
}

impl Input for Shipment {
    type Axis = SoloCarried<Self>;
}

struct Audit;

impl Handle<Shipment, (), (), PoolContext<Sqlite>> for Audit {
    async fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        ctx: &mut Context<'_, PoolContext<Sqlite>>,
    ) -> Result<(), HandlerOutcome> {
        let Ok(Headers(seen)) = Headers::<Seen>::from_context(ctx).await else {
            return Err(HandlerOutcome::drop());
        };
        let Ok(Ctx(pool)) = Ctx::<keys::Pool<Sqlite>>::from_context(ctx).await;
        tracing::info!(job = shipment.job_id, carrier = %seen.carrier, "auditing");
        sqlx::query(
            "INSERT INTO shipment_audit (carrier, order_id, express, placed_at, trace) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&seen.carrier)
        .bind(&seen.order_id)
        .bind(&seen.express)
        .bind(&seen.placed_at)
        .bind(&seen.trace)
        .execute(&pool)
        .await
        .map(drop)
        .map_err(|error| {
            tracing::warn!(subscription = "shipments", %error, "audit not written");
            HandlerOutcome::retry()
        })
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        let audit =
            subscriber("shipments", Audit).map_source(|_| InboxQueue::<Shipment>::new("shipments"));
        b.include(audit.build());
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
        let (pool, _keeper) = database("manual_sqlx_headers_field").await?;
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
