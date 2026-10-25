//! One test body, two modes: in process under `TestApp::start` on a paused clock, and on the real
//! clock under `TestApp::start_live`. Both run over a SQLite database in memory.
//!
//! ```text
//! cargo test -p ruststream-sqlx --example sqlx_testing_live --features inbox,sqlite,chrono,testing
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

/// The first attempt finds the storage still writing the upload, and tries again a second later.
#[subscriber(InboxQueue::<MakeThumbnail>::new("thumbnails"))]
async fn thumbnail(image: &Image, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    if attempt == Some(1) {
        return HandlerOutcome::retry_after(Duration::from_secs(1));
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

    // --8<-- [start:start-live]
    /// The body both modes run; only the start call differs.
    async fn the_thumbnail_is_made_a_second_later(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Image {
                path: "cat.png".to_owned(),
            })
            .publish()
            .await?;
        // In process the paused clock jumps; live, two seconds pass.
        tb.advance(Duration::from_secs(2)).await?;

        let outcomes = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("thumbnails")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        tb.shutdown().await?;
        Ok(())
    }

    // A paused clock takes the single-threaded runtime `#[tokio::test]` starts.
    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_testing_live_in_process").await?;
        tokio::time::pause();
        the_thumbnail_is_made_a_second_later(TestApp::start(service(&url)).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_the_real_clock() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_testing_live_real_clock").await?;
        the_thumbnail_is_made_a_second_later(TestApp::start_live(service(&url)).await?).await
    }
    // --8<-- [end:start-live]
}
