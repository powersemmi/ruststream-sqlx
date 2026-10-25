//! A publish wakes the subscription that reads it: the welcome a signup's handler replies with
//! is handled at once, though the broker polls an empty queue once an hour.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_wake --features inbox,sqlite,chrono,testing
//! ```

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:service]
use ruststream::OutgoingMessage;
use sqlx::{Sqlite, SqliteConnection};

/// One row of `jobs`: the group column holds the name a message was published to.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
struct Job {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The service's own insert: the name goes to the group column, the bytes to the payload.
impl Publish<Sqlite> for Job {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO jobs (name, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[subscriber(InboxQueue::<Job>::new("signups"), reply)]
async fn greet(signup: &Signup) -> Welcome {
    Welcome {
        email: signup.email.clone(),
    }
}

// The welcome's row wakes this subscription: no poll interval in between.
#[subscriber(InboxQueue::<Job>::new("welcome"))]
async fn send(welcome: &Welcome) -> HandlerOutcome {
    tracing::info!(to = %welcome.email, "sending the welcome");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    // The hourly poll reaches a row the service wrote with its own SQL.
    let broker = broker
        .poll_interval(Duration::from_secs(3600))
        .route::<Job>("signups");
    RustStream::new(AppInfo::new("signup", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            b.include(greet).out_reply(Repository::<Job>::default());
            b.include(send);
        })
}
// --8<-- [end:service]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:signups.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "signups")]
struct Signup {
    email: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "welcome")]
struct Welcome {
    email: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Signup, Welcome, service};

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
    async fn the_welcome_is_sent_long_before_the_next_poll() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_wake").await?;
        let tb = TestApp::start(service(&url)).await?;
        let signup = Signup {
            email: "ada@example.com".to_owned(),
        };
        // Both subscriptions found the table empty as they opened, and wait their hour.
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&signup)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("signups")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("welcome")
            .assert_called_once()
            .with(&Welcome {
                email: "ada@example.com".to_owned(),
            })
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "both rows were acknowledged within the hour");
        Ok(())
    }
}
