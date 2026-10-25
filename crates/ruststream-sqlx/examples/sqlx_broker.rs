//! A thumbnail queue in a SQLite table, served by the inbox broker.
//!
//! The broker builds its pool when the service starts; the service creates the table before.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_broker --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};

// --8<-- [start:service]
use ruststream_sqlx::prelude::*;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Connection, Sqlite, SqliteConnection};

/// One row of `thumbnail_jobs`, claimed under a lease.
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

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("thumbnails", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<MakeThumbnail>("thumbnails"), |b| {
            b.include(thumbnail);
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:thumbnails.db?mode=rwc")
}
// --8<-- [end:service]

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

    use super::{Image, SCHEMA, service};

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
    async fn a_published_image_is_claimed_and_its_row_deleted() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_broker").await?;
        let tb = TestApp::start(service(&url)).await?;
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
