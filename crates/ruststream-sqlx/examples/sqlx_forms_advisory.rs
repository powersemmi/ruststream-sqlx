//! Video transcoding in the advisory lock form on SQLite: a lock on each row's key holds the row,
//! and the videos of one channel go into work one at a time.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_forms_advisory --features inbox,sqlite,testing
//! ```

use std::error::Error;

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:advisory]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `video_jobs`, held by a lock on its channel's key while it is in work.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "video_jobs", advisory_lock = "video_jobs-{channel}")]
struct Transcode {
    #[field(id, generated)]
    id: i64,
    channel: String,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(payload)]
    payload: Vec<u8>,
}

/// A video takes minutes to encode. The delivery's connection keeps the channel's key while the
/// handler runs, with no transaction open; a retry runs no statement and releases the key.
#[subscriber(InboxQueue::<Transcode>::new("videos"))]
async fn transcode(video: &Video, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    if encode(video, attempt).await {
        return HandlerOutcome::ack();
    }
    HandlerOutcome::retry()
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<Transcode>("videos"),
        |b| {
            b.include(transcode);
        },
    )
}
// --8<-- [end:advisory]

/// A stand-in for the encoder: its first attempt at a video fails.
async fn encode(video: &Video, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(path = %video.path, ?attempt, "encoding");
    attempt != Some(1)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:transcoder.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS video_jobs (
    id INTEGER PRIMARY KEY,
    channel TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    payload BLOB NOT NULL
)";

/// A publish takes the channel out of the message.
impl Publish<Sqlite> for Transcode {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO video_jobs (channel, payload) \
             VALUES (json_extract(CAST(?1 AS TEXT), '$.channel'), ?1)",
        )
        .bind(message.payload())
        .execute(conn)
        .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "videos")]
struct Video {
    channel: String,
    path: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::try_join_all;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Video, service};

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
    async fn each_video_returns_after_its_retry_and_is_encoded() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_forms_advisory").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        let videos: Vec<Video> = ["intro.mp4", "talk.mp4"]
            .into_iter()
            .map(|path| Video {
                channel: "conference".to_owned(),
                path: path.to_owned(),
            })
            .collect();
        let broker = tb.broker::<SqlxBroker<Sqlite>>();
        try_join_all(videos.iter().map(|video| broker.message(video).publish())).await?;

        broker.subscriber("videos").assert_called(4);
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM video_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "both videos were encoded and their rows deleted");
        Ok(())
    }
}
