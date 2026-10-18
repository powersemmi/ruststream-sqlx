//! A queue in a table with column names of its own: sqlx's attributes name every column.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_tables_columns --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

use ruststream_sqlx::prelude::*;

// --8<-- [start:table]
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "LegacyJobs")]
#[sqlx(rename_all = "camelCase")]
struct LegacyJob {
    // `rename` names a column as written.
    #[field(id, generated)]
    #[sqlx(rename = "JobID")]
    job_id: i64,
    // A raw identifier loses its `r#`: the column is `type`.
    #[field(group)]
    r#type: String,
    // `rename_all` recases the rest: the column is `lockedUntil`.
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    body: Vec<u8>,
}
// --8<-- [end:table]

#[subscriber(InboxQueue::<LegacyJob>::new("imports"))]
async fn run_import(import: &Import) -> HandlerOutcome {
    tracing::info!(file = %import.file, "importing");
    HandlerOutcome::ack()
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("importer", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<LegacyJob>("imports"),
        |b| {
            b.include(run_import);
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:imports.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

/// A table another system created, with its own column names.
const SCHEMA: &str = r#"CREATE TABLE IF NOT EXISTS "LegacyJobs" (
    "JobID" INTEGER PRIMARY KEY,
    "type" TEXT NOT NULL,
    "lockedUntil" TEXT,
    "body" BLOB NOT NULL
)"#;

/// A publish writes a row of the type it names.
impl Publish<Sqlite> for LegacyJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(r#"INSERT INTO "LegacyJobs" ("type", "body") VALUES (?, ?)"#)
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "imports")]
struct Import {
    file: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Import, SCHEMA, service};

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
    async fn a_table_with_its_own_column_names_serves_the_queue() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_tables_columns").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        let import = Import {
            file: "prices.csv".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&import)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("imports")
            .assert_called_once()
            .with(&import)
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar(r#"SELECT count(*) FROM "LegacyJobs""#)
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "the acknowledgement deleted the row");
        Ok(())
    }
}
