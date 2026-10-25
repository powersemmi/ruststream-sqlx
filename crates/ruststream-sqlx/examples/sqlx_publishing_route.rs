//! One prefix route leads every report into one table: the daily report a client publishes and
//! the weekly report the service replies with both become rows of `report_jobs`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_publishing_route --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:service]
use ruststream::OutgoingMessage;
use sqlx::{Sqlite, SqliteConnection};

/// One row of `report_jobs`: the group column holds the name a message was published to.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "report_jobs")]
struct Report {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
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

#[subscriber(InboxQueue::<Report>::new("reports.daily"), reply)]
async fn roll_up(daily: &Daily) -> Weekly {
    Weekly {
        week: daily.day / 7 + 1,
    }
}

#[subscriber(InboxQueue::<Report>::new("reports.weekly"))]
async fn file(weekly: &Weekly) -> HandlerOutcome {
    tracing::info!(week = weekly.week, "filing the weekly report");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    // `reports.daily` and `reports.weekly` both lead into `report_jobs`.
    let broker = broker.route::<Report>("reports.*");
    RustStream::new(AppInfo::new("reports", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            // A reply without a policy of its own publishes through the routes.
            b.include(roll_up);
            b.include(file);
        })
}
// --8<-- [end:service]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:reports.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS report_jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "reports.daily")]
struct Daily {
    day: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn both_reports_go_through_the_prefix_route() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_publishing_route").await?;
        let tb = TestApp::start(service(&url)).await?;
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
