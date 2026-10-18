//! The service of `examples/sqlx_tables_events.rs` without the macros: the table names its own
//! acknowledgement in its chain and in its type.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_tables_events --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_sqlx::prelude::*;

// --8<-- [start:events]
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload, own};
use ruststream_sqlx::{Ack, InboxSpec, InboxTable, PayloadRow};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

#[derive(sqlx::FromRow)]
struct SendEmail {
    job_id: i64,
    payload: Vec<u8>,
}

impl InboxTable for SendEmail {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload, own::Ack)>;
    const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("job_id").generated())
        .group(Column::new("name"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"))
        .own::<own::Ack>();

    fn id(&self) -> &i64 {
        &self.job_id
    }
}

impl PayloadRow for SendEmail {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

// A sent email stays in its table, in the `sent` group, for an audit.
impl Ack<Sqlite> for SendEmail {
    async fn ack(conn: &mut SqliteConnection, id: &i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE email_jobs SET name = 'sent' WHERE job_id = ?")
            .bind(id)
            .execute(conn)
            .await?;
        Ok(())
    }
}
// --8<-- [end:events]

struct Sender;

impl Handle<Email> for Sender {
    fn handle(
        &self,
        email: &Email,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %email.to, "sending");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendEmail>("emails"),
        |b| {
            let send =
                subscriber("emails", Sender).map_source(|_| InboxQueue::<SendEmail>::new("emails"));
            b.include(send.build());
        },
    )
}

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
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for SendEmail {
    async fn publish(
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
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "emails")]
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
    async fn a_sent_email_stays_in_the_sent_group() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_tables_events").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Email {
                to: "ops@example.com".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let groups: Vec<String> = sqlx::query_scalar("SELECT name FROM email_jobs")
            .fetch_all(&pool)
            .await?;
        assert_eq!(
            groups,
            ["sent"],
            "the service's own ack kept the row in the sent group"
        );
        Ok(())
    }
}
