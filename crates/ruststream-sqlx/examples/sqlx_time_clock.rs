//! A clock of the service's own on SQLite: the host's time in whole seconds, as the service's
//! other tables keep it, read by every statement of the table that binds "now".
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_time_clock --features inbox,sqlite,chrono,testing
//! ```

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:clock]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{Clock, SystemClock};
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// The host's time in whole seconds.
struct WholeSeconds;

impl Clock for WholeSeconds {
    fn now() -> SystemTime {
        let since = SystemClock::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        UNIX_EPOCH + Duration::from_secs(since.as_secs())
    }
}

/// One row of `reminder_jobs`, on the service's clock.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "reminder_jobs", clock = WholeSeconds)]
struct Remind {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<Remind>::new("reminders"))]
async fn remind(note: &Note) -> HandlerOutcome {
    tracing::info!(text = %note.text, "reminding");
    // `processed_at` takes `WholeSeconds::now()`.
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("reminders", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<Remind>("reminders"), |b| {
            b.include(remind);
        })
}
// --8<-- [end:clock]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:reminders.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS reminder_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    processed_at TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for Remind {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO reminder_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "reminders")]
struct Note {
    text: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use chrono::{DateTime, Timelike, Utc};
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

    // The service's clock is the subject, so the test runs on the real clock, where the queue
    // reads the table's own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_mark_takes_the_time_of_the_services_clock() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_time_clock").await?;
        let tb = TestApp::start_live(service(&url)).await?;
        let note = Note {
            text: "Renew the certificate".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&note)
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("reminders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());

        let marked: DateTime<Utc> = sqlx::query_scalar("SELECT processed_at FROM reminder_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(marked.nanosecond(), 0, "whole seconds: {marked}");
        assert!((Utc::now() - marked).num_seconds() < 5, "{marked} is now");
        Ok(())
    }
}
