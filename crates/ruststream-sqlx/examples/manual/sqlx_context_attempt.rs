//! The service of `examples/sqlx_context_attempt.rs` without the macros: the handler resolves
//! the attempt through `FromContext` on `InboxContext`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_context_attempt --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:attempt]
use std::time::Duration;

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `stock_jobs`: `attempt` counts the deliveries, 1 for the first.
#[derive(sqlx::FromRow)]
struct SyncStock {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SyncStock {
    type Id = i64;
    type Table = InboxSpec<(
        Attempt,
        RetryAfter<DateTime<Utc>>,
        Lease<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("stock_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .retry_after(Column::new("retry_after"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for SyncStock {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for SyncStock {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct FetchLevel;

impl Handle<Item, (), (), InboxContext> for FetchLevel {
    async fn handle(
        &self,
        item: &Item,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        let attempt = attempt.unwrap_or(1);
        if fetch_level(item, attempt).await {
            return Ok(());
        }
        // Doubling backoff: 1 s, 2 s, 4 s, ...
        let exponent = u32::try_from(attempt.min(16)).unwrap_or(16);
        Err(HandlerOutcome::retry_after(Duration::from_secs(
            1 << (exponent - 1),
        )))
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("stock", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SyncStock>("stock"), |b| {
            let sync = subscriber("stock", FetchLevel)
                .map_source(|_| InboxQueue::<SyncStock>::new("stock"));
            b.include(sync.build());
        })
}
// --8<-- [end:attempt]

/// A stand-in for the supplier's API: it answers from the third attempt on.
async fn fetch_level(item: &Item, attempt: u64) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(sku = %item.sku, attempt, "fetching the stock level");
    attempt >= 3
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:stock.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS stock_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for SyncStock {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO stock_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "stock")]
struct Item {
    sku: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Item, SCHEMA, service};

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

    // The delays are real seconds, so the test runs on the real clock and lets them pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_attempt_waits_twice_as_long_as_the_one_before() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("manual_sqlx_context_attempt").await?;
        let tb = TestApp::start_live(service(&url)).await?;
        let item = Item {
            sku: "BOLT-M6".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&item)
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("stock")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(Duration::from_secs(1)));

        tb.advance(Duration::from_secs(1)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("stock")
            .assert_called(2)
            .settled(HandlerOutcome::retry_after(Duration::from_secs(2)));

        tb.advance(Duration::from_secs(2)).await?;
        assert_eq!(
            tb.broker::<SqlxBroker<Sqlite>>()
                .subscriber("stock")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Ack]
        );
        Ok(())
    }
}
