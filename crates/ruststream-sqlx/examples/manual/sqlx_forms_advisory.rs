//! The service of `examples/sqlx_forms_advisory.rs` without the macros: `.advisory(..)` takes
//! the key as its parts, text and columns, and the handler reads its attempt through
//! `FromContext`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_forms_advisory --features inbox,sqlite,testing
//! ```

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Advisory, Attempt, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:advisory]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `video_jobs`, held by a lock on its channel's key while it is in work.
#[derive(sqlx::FromRow)]
struct Transcode {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for Transcode {
    type Id = i64;
    type Table = InboxSpec<(Advisory, Attempt, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("video_jobs", Column::new("id").generated())
        .advisory(&[KeyPart::Literal("video_jobs-"), KeyPart::Column("channel")])
        .attempt(Column::new("attempt").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for Transcode {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for Transcode {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// A video takes minutes to encode. The delivery's connection keeps the channel's key while the
/// handler runs, with no transaction open; a retry runs no statement and releases the key.
struct TranscodeVideo;

impl Handle<Video, (), (), InboxContext> for TranscodeVideo {
    async fn handle(
        &self,
        video: &Video,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        if encode(video, attempt).await {
            return Ok(());
        }
        Err(HandlerOutcome::retry())
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("transcoder", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<Transcode>("videos"), |b| {
            let transcode = subscriber("videos", TranscodeVideo)
                .map_source(|_| InboxQueue::<Transcode>::new("videos"));
            b.include(transcode.build());
        })
}
// --8<-- [end:advisory]

/// A stand-in for the encoder: its first attempt at a video fails.
async fn encode(video: &Video, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(path = %video.path, ?attempt, "encoding");
    attempt != Some(1)
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:transcoder.db?mode=rwc")
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
    async fn each_video_returns_after_its_retry_and_is_encoded() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_forms_advisory").await?;
        let tb = TestApp::start(service(&url)).await?;
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
