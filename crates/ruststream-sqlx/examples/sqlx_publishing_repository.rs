//! A reply written into a queue table through a `Repository`: the confirmation of an order
//! becomes a row of the `emails` group, which another subscription of the same service reads.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_publishing_repository --features inbox,sqlite,chrono,testing
//! ```

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

#[subscriber(InboxQueue::<Job>::new("orders"), reply)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { order: order.id }
}

#[subscriber(InboxQueue::<Job>::new("emails"))]
async fn email(confirmation: &Confirmation) -> HandlerOutcome {
    tracing::info!(order = confirmation.order, "emailing the confirmation");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<Job>("orders"), |b| {
            // The confirmation becomes a row of `jobs` in the group `emails`.
            b.include(confirm).out_reply(Repository::<Job>::default());
            b.include(email);
        })
}
// --8<-- [end:service]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:orders.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "emails")]
struct Confirmation {
    order: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Confirmation, Order, SCHEMA, service};

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
    async fn the_reply_becomes_a_row_its_group_reads() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_publishing_repository").await?;
        let tb = TestApp::start(service(&url)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("emails")
            .assert_called_once()
            .with(&Confirmation { order: 7 })
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "both rows were acknowledged");
        Ok(())
    }
}
