//! The service of `examples/sqlx_subscriptions_queue.rs` without the macros: the table is
//! described with `InboxSpec`, and each handler is a named type mounted with `subscriber(..)`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_subscriptions_queue --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:queue]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `notification_jobs`: `channel` is the group, and a subscription's name selects one.
#[derive(sqlx::FromRow)]
struct Notify {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Notify {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("notification_jobs", Column::new("id").generated())
        .group(Column::new("channel"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Notify {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Reads the rows of the `email` group.
struct SendEmail;

impl Handle<Notice> for SendEmail {
    fn handle(
        &self,
        notice: &Notice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %notice.to, "sending an email");
        ready(Ok(()))
    }
}

/// Reads the rows of the `sms` group of the same table.
struct SendSms;

impl Handle<Notice> for SendSms {
    fn handle(
        &self,
        notice: &Notice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %notice.to, "sending a text message");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    let broker = SqlxBroker::new(pool)
        .route::<Notify>("email")
        .route::<Notify>("sms");
    RustStream::new(AppInfo::new("notifier", "0.1.0")).with_broker(broker, |b| {
        let email =
            subscriber("email", SendEmail).map_source(|_| InboxQueue::<Notify>::new("email"));
        b.include(email.build());
        let sms = subscriber("sms", SendSms).map_source(|_| InboxQueue::<Notify>::new("sms"));
        b.include(sms.build());
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
        let (pool, _keeper) = database("manual_sqlx_subscriptions_queue").await?;
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
