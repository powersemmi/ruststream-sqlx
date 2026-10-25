//! One table described twice: `Welcome` with the derive, `Reminder` by hand, each serving its
//! own group of `email_jobs`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_tables_spec --features inbox,sqlite,chrono,testing
//! ```

use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:table]
use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};

/// The derive writes the description from the struct.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
struct Welcome {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The same table by hand: the struct holds only what the service reads.
#[derive(sqlx::FromRow)]
struct Reminder {
    job_id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for Reminder {
    type Id = i64;
    // The settings the crate reads as types, in the order the chain sets them.
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Attempt, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("job_id").generated())
        .lease(Column::new("locked_until"))
        .group(Column::new("name"))
        .attempt(Column::new("attempt").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.job_id
    }
}

impl AttemptRow for Reminder {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for Reminder {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}
// --8<-- [end:table]

#[subscriber(InboxQueue::<Welcome>::new("welcomes"))]
async fn welcome(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "welcoming");
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<Reminder>::new("reminders"))]
async fn remind(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "reminding");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    let broker = broker
        .route::<Welcome>("welcomes")
        .route::<Reminder>("reminders");
    RustStream::new(AppInfo::new("mailer", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            b.include(welcome);
            b.include(remind);
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:mailer.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS email_jobs (
    job_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

async fn write_email(
    conn: &mut SqliteConnection,
    message: &OutgoingMessage<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO email_jobs (name, payload) VALUES (?, ?)")
        .bind(message.name())
        .bind(message.payload())
        .execute(conn)
        .await?;
    Ok(())
}

impl Publish<Sqlite> for Reminder {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        write_email(conn, message).await
    }
}

impl Publish<Sqlite> for Welcome {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        write_email(conn, message).await
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Email {
    to: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Email, SCHEMA, service};

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
    async fn each_group_of_the_table_reaches_its_subscription() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_tables_spec").await?;
        let tb = TestApp::start(service(&url)).await?;
        let welcome = Email {
            to: "new@example.com".to_owned(),
        };
        let reminder = Email {
            to: "late@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&welcome)
            .to("welcomes")
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&reminder)
            .to("reminders")
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("welcomes")
            .assert_called_once()
            .with(&welcome)
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("reminders")
            .assert_called_once()
            .with(&reminder)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
