//! The service of `examples/sqlx_broker.rs` without the macros: the table is described with
//! `InboxSpec`, and the handler is a named type mounted with `subscriber(..)`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_broker --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};

// --8<-- [start:service]
use ruststream_sqlx::prelude::*;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Connection, Sqlite, SqliteConnection};

/// One row of `thumbnail_jobs`, claimed under a lease.
#[derive(sqlx::FromRow)]
struct MakeThumbnail {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for MakeThumbnail {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("thumbnail_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
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
            let thumbnail = subscriber("thumbnails", Thumbnail)
                .map_source(|_| InboxQueue::<MakeThumbnail>::new("thumbnails"));
            b.include(thumbnail.build());
        })
}

fn main() -> ExitCode {
    run_main(|| service("sqlite:thumbnails.db?mode=rwc"))
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
        let (url, pool, _keeper) = database("manual_sqlx_broker").await?;
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
