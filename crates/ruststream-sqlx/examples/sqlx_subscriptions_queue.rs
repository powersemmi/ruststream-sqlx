//! Notifications in one SQLite table, read by two subscriptions: the name of each `InboxQueue`
//! selects the group of rows it reads.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_subscriptions_queue --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:queue]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `notification_jobs`: `channel` is the group, and a subscription's name selects one.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "notification_jobs")]
struct Notify {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    channel: String,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// Reads the rows of the `email` group.
#[subscriber(InboxQueue::<Notify>::new("email"))]
async fn send_email(notice: &Notice) -> HandlerOutcome {
    tracing::info!(to = %notice.to, "sending an email");
    HandlerOutcome::ack()
}

/// Reads the rows of the `sms` group of the same table.
#[subscriber(InboxQueue::<Notify>::new("sms"))]
async fn send_sms(notice: &Notice) -> HandlerOutcome {
    tracing::info!(to = %notice.to, "sending a text message");
    HandlerOutcome::ack()
}

fn service(pool: SqlitePool) -> RustStream {
    let broker = SqlxBroker::new(pool)
        .route::<Notify>("email")
        .route::<Notify>("sms");
    RustStream::new(AppInfo::new("notifier", "0.1.0")).with_broker(broker, |b| {
        b.include(send_email);
        b.include(send_sms);
    })
}
// --8<-- [end:queue]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:notifications.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS notification_jobs (
    id INTEGER PRIMARY KEY,
    channel TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

/// A publish writes the message's name into `channel`: the name is the group.
impl Publish<Sqlite> for Notify {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO notification_jobs (channel, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Notice {
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

    use super::{Notice, SCHEMA, service};

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
    async fn a_notice_reaches_the_subscription_of_its_group() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_subscriptions_queue").await?;
        let tb = TestApp::start(service(pool)).await?;
        let notice = Notice {
            to: "+15550100".to_owned(),
            text: "Your order shipped".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&notice)
            .to("sms")
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("sms")
            .assert_called_once()
            .with(&notice)
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("email")
            .assert_not_called();
        Ok(())
    }
}
