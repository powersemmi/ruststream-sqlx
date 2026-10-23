//! The table of `examples/sqlx_tables_spec.rs` described by hand, with its handlers written
//! without the macros: one handler type serves both groups.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_tables_spec --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

use ruststream_sqlx::prelude::*;

// --8<-- [start:table]
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

struct Mailer;

impl Handle<Email> for Mailer {
    fn handle(
        &self,
        email: &Email,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %email.to, group = ctx.name(), "sending");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    let broker = SqlxBroker::new(pool)
        .route::<Reminder>("welcomes")
        .route::<Reminder>("reminders");
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(broker, |b| {
        for group in ["welcomes", "reminders"] {
            let send =
                subscriber(group, Mailer).map_source(move |_| InboxQueue::<Reminder>::new(group));
            b.include(send.build());
        }
    })
}
// --8<-- [end:table]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:mailer.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
    async fn each_group_of_the_table_reaches_its_subscription() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_tables_spec").await?;
        let tb = TestApp::start(service(pool)).await?;
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
