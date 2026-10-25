//! Payload mode: the claim lends the payload column's bytes to the codec, and the handler takes
//! the decoded message.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_messages_payload --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:payload]
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "event_jobs")]
struct EventJob {
    #[field(id, generated)]
    id: i64,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    // The message bytes, decoded by the codec of the mount: JSON unless it names another.
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<EventJob>::new("signups"))]
async fn welcome(signup: &Signup) -> HandlerOutcome {
    tracing::info!(email = %signup.email, "welcoming");
    HandlerOutcome::ack()
}
// --8<-- [end:payload]

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("signups", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<EventJob>("signups"), |b| {
            b.include(welcome);
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:events.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS event_jobs (
    id INTEGER PRIMARY KEY,
    processed_at TEXT,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

/// A publish writes the encoded message into the payload column as it is.
impl Publish<Sqlite> for EventJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO event_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "signups")]
struct Signup {
    email: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Signup, service};

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
    async fn the_handler_decodes_the_bytes_the_column_holds() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_messages_payload").await?;
        let tb = TestApp::start(service(&url)).await?;
        let signup = Signup {
            email: "new@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&signup)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("signups")
            .assert_called_once()
            .with(&signup)
            .with_raw(br#"{"email":"new@example.com"}"#)
            .settled(HandlerOutcome::ack());
        // The acknowledged row keeps the bytes the handler decoded.
        let kept: Vec<u8> = sqlx::query_scalar("SELECT payload FROM event_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(kept, br#"{"email":"new@example.com"}"#);
        Ok(())
    }
}
