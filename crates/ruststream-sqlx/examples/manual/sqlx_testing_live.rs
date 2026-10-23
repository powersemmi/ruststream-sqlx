//! The tests of `examples/sqlx_testing_live.rs` on the service without the macros: the table is
//! described with `InboxSpec`, and the handler is a named type. The test bodies are the same.
//!
//! ```text
//! cargo test -p ruststream-sqlx --example manual_sqlx_testing_live --features inbox,sqlite,chrono,testing,asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

use ruststream_sqlx::prelude::*;

/// One row of `thumbnail_jobs`, claimed under a lease, with a time it is due.
#[derive(sqlx::FromRow)]
struct MakeThumbnail {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for MakeThumbnail {
    type Id = i64;
    type Table = InboxSpec<(
        Attempt,
        RetryAfter<DateTime<Utc>>,
        Lease<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("thumbnail_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .retry_after(Column::new("retry_after").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for MakeThumbnail {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for MakeThumbnail {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The first attempt finds the storage still writing the upload, and tries again a second later.
struct Thumbnail;

impl Handle<Image, (), (), InboxContext> for Thumbnail {
    async fn handle(
        &self,
        image: &Image,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ok(Ctx(attempt)) = Ctx::<keys::Attempt>::from_context(ctx).await;
        if attempt == Some(1) {
            return Err(HandlerOutcome::retry_after(Duration::from_secs(1)));
        }
        tracing::info!(path = %image.path, "making a thumbnail");
        Ok(())
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("thumbnails", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<MakeThumbnail>("thumbnails"),
        |b| {
            let thumbnail = subscriber("thumbnails", Thumbnail)
                .map_source(|_| InboxQueue::<MakeThumbnail>::new("thumbnails"));
            b.include(thumbnail.build());
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:thumbnails.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
        let (pool, _keeper) = database("manual_sqlx_testing_live_in_process").await?;
        tokio::time::pause();
        the_thumbnail_is_made_a_second_later(TestApp::start(service(pool)).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_the_real_clock() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_testing_live_real_clock").await?;
        the_thumbnail_is_made_a_second_later(TestApp::start_live(service(pool)).await?).await
    }
    // --8<-- [end:start-live]
}
