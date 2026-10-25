//! A queue table described with `#[derive(Inbox)]`: one table, a group per channel.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_tables_inbox --features inbox,sqlite,chrono,testing
//! ```

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:table]
use chrono::{DateTime, Utc};
use sqlx::Sqlite;

/// One row of `notifications`: the struct names the table, sqlx names the columns, and
/// `#[field(..)]` gives the columns that run the queue their roles.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "notifications")]
struct Notification {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    channel: String,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<Notification>::new("email"))]
async fn email(note: &Note, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    tracing::info!(to = %note.to, ?attempt, "emailing");
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<Notification>::new("sms"))]
async fn sms(note: &Note) -> HandlerOutcome {
    tracing::info!(to = %note.to, "texting");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    let broker = broker
        .route::<Notification>("email")
        .route::<Notification>("sms");
    RustStream::new(AppInfo::new("notifier", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            b.include(email);
            b.include(sms);
        })
}
// --8<-- [end:table]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:notifications.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY,
    channel TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

/// A publish writes a row into the group it names.
impl Publish<Sqlite> for Notification {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO notifications (channel, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Note {
    to: String,
    text: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Note, SCHEMA, service};

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
    async fn a_note_reaches_the_subscription_of_its_group() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_tables_inbox").await?;
        let tb = TestApp::start(service(&url)).await?;
        let note = Note {
            to: "ops@example.com".to_owned(),
            text: "disk full".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&note)
            .to("email")
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("email")
            .assert_called_once()
            .with(&note)
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("sms")
            .assert_not_called();
        Ok(())
    }
}
