//! Tests of a thumbnail service on `TestApp::start`, over a SQLite database in memory: one
//! publishes an image and reads how the handler settled it, the other moves a paused clock past a
//! delayed retry.
//!
//! ```text
//! cargo test -p ruststream-sqlx --example sqlx_testing --features inbox,sqlite,chrono,testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `thumbnail_jobs`, claimed under a lease, with a time it is due.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "thumbnail_jobs")]
struct MakeThumbnail {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The first attempt finds the storage still writing the upload, and tries again a minute later.
#[subscriber(InboxQueue::<MakeThumbnail>::new("thumbnails"))]
async fn thumbnail(image: &Image, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    if attempt == Some(1) {
        return HandlerOutcome::retry_after(Duration::from_secs(60));
    }
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

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS thumbnail_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    locked_until TEXT,
    payload BLOB NOT NULL
)";

/// A publish to `thumbnails` writes a row due at once.
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
    use std::time::Duration;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Image, SCHEMA, service};

    // --8<-- [start:testapp]
    /// A database in memory that every connection naming it shares, alive while one of them is
    /// open: the test holds that one beside the pool.
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
    async fn an_image_is_put_off_once() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_testing").await?;
        let tb = TestApp::start(service(&url)).await?;
        let image = Image {
            path: "cat.png".to_owned(),
        };
        // Through the route and the service's own `Publish`, as in production.
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&image)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("thumbnails")
            .assert_called_once()
            .with(&image)
            .settled(HandlerOutcome::retry_after(Duration::from_secs(60)));
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:clock]
    #[tokio::test]
    async fn a_minute_later_the_thumbnail_is_made() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_testing_clock").await?;
        // The queue reads "now" from this clock, and the database calls run off it. A paused
        // clock takes the single-threaded runtime `#[tokio::test]` starts.
        tokio::time::pause();
        let tb = TestApp::start(service(&url)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Image {
                path: "cat.png".to_owned(),
            })
            .publish()
            .await?;
        tb.advance(Duration::from_secs(61)).await?;

        // The delayed retry, then the acknowledgement once its minute had passed.
        let outcomes = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("thumbnails")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        Ok(())
    }
    // --8<-- [end:clock]
}
