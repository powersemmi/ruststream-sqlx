//! The service of `examples/sqlx_subscriptions_fifo.rs` without the macros: `.fifo_group(..)`
//! names the group's column, `Fifo` goes into the type, and the handler reads its attempt
//! through `FromContext`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_subscriptions_fifo --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Fifo, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:fifo]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `sync_jobs`; each group holds the changes bound for one system, in order.
#[derive(sqlx::FromRow)]
struct SyncChange {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SyncChange {
    type Id = i64;
    type Table = InboxSpec<(Fifo, Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("sync_jobs", Column::new("id").generated())
        .fifo_group(Column::new("target"))
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for SyncChange {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for SyncChange {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The CRM receives its changes one at a time, in the order they were queued.
struct PushChange;

impl Handle<Change, (), (), InboxContext> for PushChange {
    async fn handle(
        &self,
        change: &Change,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        if push(change, attempt).await {
            return Ok(());
        }
        // At once, so the change keeps its place at the head of the group.
        Err(HandlerOutcome::retry())
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("crm-sync", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SyncChange>("crm"), |b| {
            let sync =
                subscriber("crm", PushChange).map_source(|_| InboxQueue::<SyncChange>::new("crm"));
            b.include(sync.build());
        })
}
// --8<-- [end:fifo]

/// A stand-in for the CRM's API: it times out on the first call for each change.
async fn push(change: &Change, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(record = %change.record, ?attempt, "pushing a change");
    attempt != Some(1)
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:crm-sync.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS sync_jobs (
    id INTEGER PRIMARY KEY,
    target TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for SyncChange {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO sync_jobs (target, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "crm")]
struct Change {
    record: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::try_join_all;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Change, SCHEMA, service};

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

    // The three changes reach the table together, so a group that let its second row into work
    // beside a failing head would deliver them out of order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_head_keeps_its_place_in_the_group() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("manual_sqlx_subscriptions_fifo").await?;
        let tb = TestApp::start(service(&url)).await?;
        let changes: Vec<Change> = ["lead-1", "lead-2", "lead-3"]
            .into_iter()
            .map(|record| Change {
                record: record.to_owned(),
            })
            .collect();
        let broker = tb.broker::<SqlxBroker<Sqlite>>();
        try_join_all(
            changes
                .iter()
                .map(|change| broker.message(change).publish()),
        )
        .await?;

        let order: Vec<String> = broker
            .subscriber("crm")
            .received::<Change>()
            .into_iter()
            .map(|change| change.record)
            .collect();
        assert_eq!(
            order,
            ["lead-1", "lead-1", "lead-2", "lead-2", "lead-3", "lead-3"]
        );
        Ok(())
    }
}
