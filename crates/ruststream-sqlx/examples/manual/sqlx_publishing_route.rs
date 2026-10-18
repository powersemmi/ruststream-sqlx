//! The service of `examples/sqlx_publishing_route.rs` without the macros: the table is
//! described with `InboxSpec`, and the two handlers are named types.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_publishing_route --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use ruststream_sqlx::prelude::*;

// --8<-- [start:service]
use ruststream::OutgoingMessage;
use sqlx::{Sqlite, SqliteConnection};

/// One row of `report_jobs`: the group column holds the name a message was published to.
#[derive(sqlx::FromRow)]
struct Report {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Report {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("report_jobs", Column::new("id").generated())
        .group(Column::new("name"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Report {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The service's own insert: the name goes to the group column, the bytes to the payload.
impl Publish<Sqlite> for Report {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO report_jobs (name, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

struct RollUp;

impl Handle<Daily, Weekly> for RollUp {
    fn handle(
        &self,
        daily: &Daily,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Weekly, HandlerOutcome>> {
        ready(Ok(Weekly {
            week: daily.day / 7 + 1,
        }))
    }
}

struct File;

impl Handle<Weekly> for File {
    fn handle(
        &self,
        weekly: &Weekly,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(week = weekly.week, "filing the weekly report");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    // `reports.daily` and `reports.weekly` both lead into `report_jobs`.
    let broker = SqlxBroker::new(pool).route::<Report>("reports.*");
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(broker, |b| {
        let roll_up = subscriber("reports.daily", RollUp)
            .map_source(|_| InboxQueue::<Report>::new("reports.daily"));
        // A reply without a policy of its own publishes through the routes.
        b.include(roll_up.reply().build());
        let file = subscriber("reports.weekly", File)
            .map_source(|_| InboxQueue::<Report>::new("reports.weekly"));
        b.include(file.build());
    })
}
// --8<-- [end:service]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:reports.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS report_jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "reports.daily")]
struct Daily {
    day: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "reports.weekly")]
struct Weekly {
    week: u32,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Daily, SCHEMA, Weekly, service};

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn both_reports_go_through_the_prefix_route() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_publishing_route").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Daily { day: 9 })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("reports.daily")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("reports.weekly")
            .assert_called_once()
            .with(&Weekly { week: 2 })
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM report_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "both rows were acknowledged");
        Ok(())
    }
}
