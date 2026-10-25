//! The service of `examples/guide/scaling.rs` without the macros: the table is described with
//! `InboxSpec`, and the handler is a named type mounted with `subscriber(..)` and its workers.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_guide_scaling --features inbox,sqlite,postgres,chrono,testing,asyncapi
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS invoice_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

const POSTGRES_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS invoice_jobs (
    id BIGSERIAL PRIMARY KEY,
    locked_until TIMESTAMPTZ,
    payload BYTEA NOT NULL
)";

// --8<-- [start:service]
use ruststream_sqlx::prelude::*;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Connection, Sqlite, SqliteConnection};

/// One row of `invoice_jobs`.
#[derive(sqlx::FromRow)]
struct RenderInvoice {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for RenderInvoice {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("invoice_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for RenderInvoice {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct Render;

impl Handle<Invoice> for Render {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(invoice.number, "rendering");
        ready(Ok(()))
    }
}

fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // A row another program writes is seen within a quarter of a second.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new())
        .poll_interval(Duration::from_millis(250))
        .route::<RenderInvoice>("invoices");
    RustStream::new(AppInfo::new("invoices", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            // Up to four invoices in work at once in this instance.
            let render = subscriber("invoices", Render)
                .map_source(|_| InboxQueue::<RenderInvoice>::new("invoices"))
                .workers(nonzero!(4));
            b.include(render.build());
        })
}
// --8<-- [end:service]

// --8<-- [start:notify]
use sqlx::PgConnection;
use sqlx::postgres::PgPoolOptions;

/// The same service on Postgres: notifications wake it, and the poll each minute reaches a row
/// whose notification was lost.
fn service_on_postgres(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    let broker = SqlxBroker::from_url(url, PgPoolOptions::new())
        .poll_interval(Duration::from_mins(1))
        .listen_notify();
    RustStream::new(AppInfo::new("invoices", "0.1.0"))
        // The table and its trigger, created before the subscription checks them.
        .on_startup(async move |()| {
            let mut conn = PgConnection::connect(&schema_url).await?;
            sqlx::raw_sql(POSTGRES_SCHEMA).execute(&mut conn).await?;
            sqlx::raw_sql(ANNOUNCE).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            let render = subscriber("invoices", Render)
                .map_source(|_| InboxQueue::<RenderInvoice>::new("invoices"))
                .workers(nonzero!(4));
            b.include(render.build());
        })
}

/// The trigger announces every row another program inserts: the channel is the table, and an
/// empty payload wakes every subscription of it.
const ANNOUNCE: &str = "
CREATE OR REPLACE FUNCTION announce_invoice() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME, '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER invoice_announced AFTER INSERT ON invoice_jobs
    FOR EACH ROW EXECUTE FUNCTION announce_invoice();
";
// --8<-- [end:notify]

/// Runs on Postgres when `DATABASE_URL` names it, on a SQLite file otherwise.
/// The service on Postgres where `DATABASE_URL` names it, and on a SQLite file otherwise.
fn app() -> RustStream {
    if let Ok(url) = env::var("DATABASE_URL")
        && url.starts_with("postgres")
    {
        return service_on_postgres(&url);
    }
    service("sqlite:invoices.db?mode=rwc")
}

fn main() -> ExitCode {
    run_main(app)
}

/// A publish to `invoices` writes a row with the service's own statement.
impl Publish<Sqlite> for RenderInvoice {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO invoice_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    number: u32,
}

// `#[outgoing(name = "invoices")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Invoice {
    type Form = FixedName;
    const DESTINATION: &'static str = "invoices";
}

impl MessageHeaders for Invoice {
    type Contract = NoHeaders;
}

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Postgres, Sqlite, SqliteConnection, SqlitePool};

    use super::{ANNOUNCE, Invoice, POSTGRES_SCHEMA, SCHEMA, live, service, service_on_postgres};

    // A database in memory that every connection naming it shares, alive while one of them is
    // open: the test holds that one beside the pool it reads and writes the table through.
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

    // Two instances of the service on one table, each with the pool its broker builds. The rows another
    // program wrote wake neither, so the test runs on the real clock past the poll interval.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_instances_never_render_one_invoice_twice() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_guide_scaling_instances").await?;
        for number in 1..=20 {
            sqlx::query("INSERT INTO invoice_jobs (payload) VALUES (?)")
                .bind(serde_json::to_vec(&Invoice { number })?)
                .execute(&pool)
                .await?;
        }
        let one = TestApp::start_live(service(&url)).await?;
        let two = TestApp::start_live(service(&url)).await?;
        one.advance(Duration::from_secs(1)).await?;
        two.settle().await?;

        let mut rendered = Vec::new();
        for tb in [&one, &two] {
            rendered.extend(
                tb.broker::<SqlxBroker<Sqlite>>()
                    .subscriber("invoices")
                    .received::<Invoice>()
                    .into_iter()
                    .map(|invoice| invoice.number),
            );
        }
        let distinct: BTreeSet<u32> = rendered.iter().copied().collect();
        assert_eq!(rendered.len(), 20, "every invoice rendered once");
        assert_eq!(distinct, (1..=20).collect(), "no invoice rendered twice");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_publish_wakes_its_own_instance_at_once() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("manual_guide_scaling_wake").await?;
        let tb = TestApp::start(service(&url)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Invoice { number: 7 })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("invoices")
            .assert_called_once()
            .with(&Invoice { number: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    // A row another program writes wakes no subscription: the test runs on the real clock, for
    // less than the default poll interval of a second.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_outside_row_is_seen_within_the_poll_interval() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_guide_scaling_poll").await?;
        let tb = TestApp::start_live(service(&url)).await?;
        // The first claim finds the table empty: the subscription then waits its interval.
        tb.advance(Duration::from_millis(50)).await?;
        sqlx::query("INSERT INTO invoice_jobs (payload) VALUES (?)")
            .bind(serde_json::to_vec(&Invoice { number: 8 })?)
            .execute(&pool)
            .await?;
        tb.advance(Duration::from_millis(500)).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("invoices")
            .assert_called_once()
            .with(&Invoice { number: 8 });
        Ok(())
    }

    // Postgres only: skips without `POSTGRES_TEST_URL`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_trigger_wakes_the_postgres_instance_before_its_poll() -> Result<(), Box<dyn Error>> {
        let Some(db) = live::postgres::database().await else {
            return Ok(());
        };
        sqlx::raw_sql(POSTGRES_SCHEMA).execute(&db.pool).await?;
        sqlx::raw_sql(ANNOUNCE).execute(&db.pool).await?;
        let tb = TestApp::start_live(service_on_postgres(&db.url())).await?;
        // The first claim finds the table empty: the subscription then waits its minute.
        tb.advance(Duration::from_millis(200)).await?;
        sqlx::query("INSERT INTO invoice_jobs (payload) VALUES ($1)")
            .bind(serde_json::to_vec(&Invoice { number: 7 })?)
            .execute(&db.pool)
            .await?;
        // Far less than the minute the poll waits.
        tb.advance(Duration::from_secs(3)).await?;

        tb.broker::<SqlxBroker<Postgres>>()
            .subscriber("invoices")
            .assert_called_once()
            .with(&Invoice { number: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        db.finish().await;
        Ok(())
    }
}
