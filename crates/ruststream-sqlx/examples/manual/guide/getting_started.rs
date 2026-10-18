//! The service of `examples/guide/getting_started.rs` without the macros: the table is described
//! with `InboxSpec`, and the handler is a named type mounted with `subscriber(..)`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_guide_getting_started --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

// --8<-- [start:schema]
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS thumbnail_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";
// --8<-- [end:schema]

// --8<-- [start:service]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqlitePool};

/// One row of `thumbnail_jobs`.
#[derive(sqlx::FromRow)]
struct MakeThumbnail {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for MakeThumbnail {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("thumbnail_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
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

/// A publish to `thumbnails` writes a row with the service's own statement.
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

/// The job, as the payload column holds it.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Image {
    path: String,
}

// `#[outgoing(name = "thumbnails")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Image {
    type Form = FixedName;
    const DESTINATION: &'static str = "thumbnails";
}

impl MessageHeaders for Image {
    type Contract = NoHeaders;
}

struct Thumbnail;

impl Handle<Image, (), (), InboxContext> for Thumbnail {
    async fn handle(
        &self,
        image: &Image,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        if image.path.is_empty() {
            tracing::warn!("an image without a path, dropped");
            return Err(HandlerOutcome::drop());
        }
        tracing::info!(path = %image.path, ?attempt, "making a thumbnail");
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
// --8<-- [end:service]

// --8<-- [start:main]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:thumbnails.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}
// --8<-- [end:main]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Image, SCHEMA, service};

    // A database in memory that every connection naming it shares, alive while one of them is
    // open: the test holds that one beside the pool.
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
    async fn an_image_is_made_into_a_thumbnail_and_its_row_deleted() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_guide_getting_started_made").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_image_without_a_path_is_dropped() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_guide_getting_started_dropped").await?;
        let tb = TestApp::start(service(pool)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Image {
                path: String::new(),
            })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("thumbnails")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }
}
