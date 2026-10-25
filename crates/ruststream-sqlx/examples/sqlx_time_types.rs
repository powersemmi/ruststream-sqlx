//! A queue table on SQLite whose time columns hold `time::OffsetDateTime`: the crate reads "now"
//! and writes it in the column's own type, as the service's own statements do.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_time_types --features inbox,sqlite,time,testing
//! ```

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:queue_time]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;
use time::OffsetDateTime;

/// One row of `backup_jobs`, with its times in the `time` crate's type.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "backup_jobs")]
struct Backup {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<OffsetDateTime>,
    #[field(processed_at)]
    processed_at: Option<OffsetDateTime>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<Backup>::new("backups"))]
async fn back_up(volume: &Volume) -> HandlerOutcome {
    tracing::info!(name = %volume.name, "backing up");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("backups", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<Backup>("backups"), |b| {
            b.include(back_up);
        })
}
// --8<-- [end:queue_time]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:backups.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS backup_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    processed_at TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for Backup {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO backup_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "backups")]
struct Volume {
    name: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Volume, service};

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
    async fn the_mark_is_written_as_the_time_crate_writes_it() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_time_types").await?;
        let tb = TestApp::start(service(&url)).await?;
        let volume = Volume {
            name: "db-primary".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&volume)
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("backups")
            .assert_called_once()
            .settled(HandlerOutcome::ack());

        // sqlx writes a `time` value as RFC 3339 text in UTC with a `Z`, where a `chrono` time
        // ends in `+00:00`.
        let marked: String = sqlx::query_scalar("SELECT processed_at FROM backup_jobs")
            .fetch_one(&pool)
            .await?;
        assert!(marked.ends_with('Z'), "{marked}");
        Ok(())
    }
}
