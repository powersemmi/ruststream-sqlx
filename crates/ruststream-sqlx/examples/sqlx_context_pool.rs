//! A delivery log written through the service's pool on SQLite: each write commits on its own,
//! so the log keeps every attempt whatever the handler answers.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_context_pool --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

// --8<-- [start:pool]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqlitePool};

/// One row of `sms_jobs`.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "sms_jobs")]
struct SendSms {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<SendSms>::new("sms"))]
async fn send(
    sms: &Sms,
    Ctx(pool): Ctx<keys::Pool<Sqlite>>,
    Ctx(attempt): Ctx<keys::Attempt>,
) -> HandlerOutcome {
    let sent = deliver(sms, attempt).await;
    // The line commits on its own: a retry leaves it in place.
    let _ = sqlx::query("INSERT INTO delivery_log (phone, sent) VALUES (?, ?)")
        .bind(&sms.phone)
        .bind(sent)
        .execute(&pool)
        .await;
    if sent {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("sms", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendSms>("sms"),
        |b| {
            b.include(send);
        },
    )
}
// --8<-- [end:pool]

/// A stand-in for the SMS gateway: it is busy on the first attempt of every message.
async fn deliver(sms: &Sms, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(phone = %sms.phone, ?attempt, "sending a text message");
    attempt != Some(1)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:sms.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS sms_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_log (phone TEXT NOT NULL, sent BOOLEAN NOT NULL)";

impl Publish<Sqlite> for SendSms {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO sms_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "sms")]
struct Sms {
    phone: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Sms, service};

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
    async fn the_log_keeps_the_attempt_the_retry_answered() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_context_pool").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        let sms = Sms {
            phone: "+15550100".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&sms)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("sms")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        let log: Vec<bool> = sqlx::query_scalar("SELECT sent FROM delivery_log ORDER BY rowid")
            .fetch_all(&pool)
            .await?;
        assert_eq!(log, [false, true], "both attempts are in the log");
        Ok(())
    }
}
