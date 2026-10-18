//! A thumbnail queue served by the built-in SQLite dialect: the broker's type names the dialect,
//! which builds the subscription's statements from the table's description.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_databases --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

// --8<-- [start:service]
use ruststream_sqlx::BuiltIn;

/// One row of `thumbnail_jobs`, claimed under a lease. The same struct serves a `PgPool` and a
/// `MySqlPool`: only its `Publish` is written per database.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "thumbnail_jobs")]
struct MakeThumbnail {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<MakeThumbnail>::new("thumbnails"))]
async fn thumbnail(image: &Image) -> HandlerOutcome {
    tracing::info!(path = %image.path, "making a thumbnail");
    HandlerOutcome::ack()
}

// `SqlxBroker<Sqlite>` names the same type: the built-in dialect of the pool's database is the
// default.
fn broker(pool: SqlitePool) -> SqlxBroker<Sqlite, BuiltIn<Sqlite>> {
    SqlxBroker::new(pool).route::<MakeThumbnail>("thumbnails")
}

fn service(broker: SqlxBroker<Sqlite>) -> RustStream {
    RustStream::new(AppInfo::new("thumbnails", "0.1.0")).with_broker(broker, |b| {
        b.include(thumbnail);
    })
}
// --8<-- [end:service]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:thumbnails.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(broker(pool.clone())).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS thumbnail_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

/// A publish to `thumbnails` writes a row through the service's own statement.
impl Publish<Sqlite> for MakeThumbnail {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO thumbnail_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "thumbnails")]
struct Image {
    path: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Image, SCHEMA, broker, service};

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
    async fn a_published_image_is_claimed_and_its_row_deleted() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_databases").await?;
        let tb = TestApp::start(service(broker(pool.clone()))).await?;
        let image = Image {
            path: "cat.png".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&image)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("thumbnails")
            .assert_called_once()
            .with(&image)
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM thumbnail_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "the acknowledgement deleted the row");
        Ok(())
    }
}
