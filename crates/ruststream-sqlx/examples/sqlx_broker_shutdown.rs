//! Shutdown: the runtime stops the subscriptions and shuts the broker down, and the service
//! closes its own pool after `run` returns.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_broker_shutdown --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "export_jobs")]
struct Export {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<Export>::new("exports"))]
async fn export(request: &ExportRequest) -> HandlerOutcome {
    tracing::info!(request.account, "exporting");
    HandlerOutcome::ack()
}

// --8<-- [start:service]
fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("exports", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<Export>("exports"),
        |b| {
            b.include(export);
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:exports.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    // `run` returns once the handlers have drained and the broker has shut down; the pool is
    // still open, and closing it is the service's call.
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}
// --8<-- [end:service]

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS export_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for Export {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO export_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "exports")]
struct ExportRequest {
    account: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{ExportRequest, SCHEMA, service};

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
    async fn shutdown_leaves_the_pool_to_the_service() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_broker_shutdown").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&ExportRequest { account: 7 })
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("exports")
            .assert_called_once()
            .settled(HandlerOutcome::ack());

        tb.shutdown().await?;
        assert!(!pool.is_closed(), "the broker left the pool open");
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM export_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            left, 0,
            "the acknowledged export left the table before shutdown"
        );
        pool.close().await;
        Ok(())
    }
}
