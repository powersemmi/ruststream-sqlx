//! The service of `examples/sqlx_databases_sqlite.rs` without the macros: the table is described with
//! `InboxSpec`, and the handler is a named type mounted with `subscriber(..)`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_databases_sqlite --features inbox,sqlite,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::spec::{Advisory, Attempt, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:service]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `thumbnail_jobs`, held by the lock on its key while a handler runs.
#[derive(sqlx::FromRow)]
struct MakeThumbnail {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for MakeThumbnail {
    type Id = i64;
    type Table = InboxSpec<(Advisory, Attempt, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("thumbnail_jobs", Column::new("id").generated())
        .advisory(&[KeyPart::Literal("thumbnails-"), KeyPart::Column("id")])
        .attempt(Column::new("attempt").generated())
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

struct Thumbnail;

impl Handle<Image> for Thumbnail {
    fn handle(
        &self,
        image: &Image,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(path = %image.path, "making a thumbnail");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("thumbnails", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<MakeThumbnail>("thumbnails"),
        |b| {
            // The keys live in this process: one process serves the database file.
            let thumbnail = subscriber("thumbnails", Thumbnail)
                .map_source(|_| InboxQueue::<MakeThumbnail>::new("thumbnails"));
            b.include(thumbnail.workers(nonzero!(2)).build());
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
// --8<-- [end:service]

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS thumbnail_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Image, SCHEMA, service};

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
        let (pool, _keeper) = database("manual_sqlx_databases_sqlite").await?;
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
}
