//! The service of `examples/guide/testing.rs` without the macros, with the same tests: the table
//! is described with `InboxSpec`, and the handler is a named type mounted with `subscriber(..)`.
//!
//! ```text
//! cargo test -p ruststream-sqlx --example manual_guide_testing --features inbox,sqlite,postgres,chrono,testing,asyncapi
//! ```

use std::env;
use std::process::ExitCode;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::spec::{Attempt, Lease, Payload, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Connection, PgConnection, Postgres, Sqlite, SqliteConnection};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS thumbnail_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    locked_until TEXT,
    payload BLOB NOT NULL
)";

/// The same table on Postgres.
const POSTGRES_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS thumbnail_jobs (
    id BIGSERIAL PRIMARY KEY,
    attempt SMALLINT NOT NULL DEFAULT 1,
    retry_after TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    payload BYTEA NOT NULL
)";

/// One row of `thumbnail_jobs`.
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
        .retry_after(Column::new("retry_after"))
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

/// How long a large image waits while the resizer is busy.
const BUSY: Duration = Duration::from_mins(1);

struct Thumbnail;

impl Handle<Image, (), (), InboxContext> for Thumbnail {
    async fn handle(
        &self,
        image: &Image,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        // The resizer takes a large image only on a second try.
        if image.large && attempt == Some(1) {
            return Err(HandlerOutcome::retry_after(BUSY));
        }
        tracing::info!(path = %image.path, "making a thumbnail");
        Ok(())
    }
}

fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
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

/// The same service on Postgres.
fn service_on_postgres(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    let broker = SqlxBroker::from_url(url, PgPoolOptions::new());
    RustStream::new(AppInfo::new("thumbnails", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = PgConnection::connect(&schema_url).await?;
            sqlx::raw_sql(POSTGRES_SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<MakeThumbnail>("thumbnails"), |b| {
            let thumbnail = subscriber("thumbnails", Thumbnail)
                .map_source(|_| InboxQueue::<MakeThumbnail>::new("thumbnails"));
            b.include(thumbnail.build());
        })
}

/// Runs on Postgres when `DATABASE_URL` names it, on a SQLite file otherwise.
/// The service on Postgres where `DATABASE_URL` names it, and on a SQLite file otherwise.
fn app() -> RustStream {
    if let Ok(url) = env::var("DATABASE_URL")
        && url.starts_with("postgres")
    {
        return service_on_postgres(&url);
    }
    service("sqlite:thumbnails.db?mode=rwc")
}

fn main() -> ExitCode {
    run_main(app)
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

impl Publish<Postgres> for MakeThumbnail {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO thumbnail_jobs (payload) VALUES ($1)")
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
    large: bool,
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
#[path = "../../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::{BrokerHandle, Outcome, TestApp};
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Postgres, Sqlite, SqliteConnection, SqlitePool};

    use super::{Image, live, service, service_on_postgres};

    // --8<-- [start:in_process]
    /// A database in memory that every connection naming it shares, alive while one of them is
    /// open: the test holds that one beside the pool it reads the table through. The service
    /// creates the table when it starts.
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
        Ok((url, pool, keeper))
    }

    /// The scenario, on whichever database the app runs.
    async fn a_small_image_is_made_at_once(broker: BrokerHandle<'_>) -> Result<(), Box<dyn Error>> {
        let image = Image {
            path: "cat.png".to_owned(),
            large: false,
        };
        broker.message(&image).publish().await?;

        broker
            .subscriber("thumbnails")
            .assert_called_once()
            .with(&image)
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_guide_testing").await?;
        let tb = TestApp::start(service(&url)).await?;
        a_small_image_is_made_at_once(tb.broker::<SqlxBroker<Sqlite>>()).await?;

        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM thumbnail_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "the acknowledgement deleted the row");
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:clock]
    #[tokio::test]
    async fn a_large_image_is_made_a_minute_later() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("manual_guide_testing_clock").await?;
        // The queue reads "now" from this clock, and the database calls run off it. A paused
        // clock takes the single-threaded runtime `#[tokio::test]` starts.
        tokio::time::pause();
        let tb = TestApp::start(service(&url)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Image {
                path: "panorama.png".to_owned(),
                large: true,
            })
            .publish()
            .await?;
        tb.advance(Duration::from_secs(61)).await?;

        // Put off for a minute, then made.
        let outcomes = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("thumbnails")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        Ok(())
    }
    // --8<-- [end:clock]

    // --8<-- [start:live]
    // Skips without `POSTGRES_TEST_URL`; fails the skip under `RUSTSTREAM_REQUIRE_LIVE`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_postgres() -> Result<(), Box<dyn Error>> {
        // A database of the test's own on the server, dropped at the end.
        let Some(db) = live::postgres::database().await else {
            return Ok(());
        };
        let tb = TestApp::start_live(service_on_postgres(&db.url())).await?;
        a_small_image_is_made_at_once(tb.broker::<SqlxBroker<Postgres>>()).await?;

        tb.shutdown().await?;
        db.finish().await;
        Ok(())
    }
    // --8<-- [end:live]
}
