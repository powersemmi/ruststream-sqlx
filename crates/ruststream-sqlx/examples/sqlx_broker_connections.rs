//! A pool sized for a subscription with four workers whose handlers write through the pool.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_broker_connections --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:service]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::Sqlite;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "resize_jobs")]
struct ResizeImage {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

// The handler takes a connection of the pool for its own write.
#[subscriber(InboxQueue::<ResizeImage>::new("resizes"))]
async fn resize(image: &Image, Ctx(pool): Ctx<keys::Pool<Sqlite>>) -> HandlerOutcome {
    let recorded = sqlx::query("INSERT INTO resized (path) VALUES (?)")
        .bind(&image.path)
        .execute(&pool)
        .await;
    match recorded {
        Ok(_) => HandlerOutcome::ack(),
        Err(error) => {
            tracing::warn!(subscription = "resizes", path = %image.path, %error, "not recorded");
            HandlerOutcome::retry()
        }
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Four workers, one connection the subscription always leaves free, and one for the
    // handlers' own writes and the publishes.
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new().max_connections(6));
    RustStream::new(AppInfo::new("resizer", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<ResizeImage>("resizes"), |b| {
            b.include(resize.workers(nonzero!(4)));
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:resizes.db?mode=rwc")
}
// --8<-- [end:service]

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS resize_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS resized (path TEXT NOT NULL);
";

impl Publish<Sqlite> for ResizeImage {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO resize_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "resizes")]
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
            .max_connections(6)
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((url, pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_handler_writes_through_the_pool() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_broker_connections").await?;
        let tb = TestApp::start(service(&url)).await?;
        for path in ["a.png", "b.png", "c.png", "d.png"] {
            tb.broker::<SqlxBroker<Sqlite>>()
                .message(&Image {
                    path: path.to_owned(),
                })
                .publish()
                .await?;
        }

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("resizes")
            .assert_called(4)
            .settled(HandlerOutcome::ack());
        let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM resized")
            .fetch_one(&pool)
            .await?;
        assert_eq!(recorded, 4, "every handler wrote its row through the pool");
        Ok(())
    }
}
