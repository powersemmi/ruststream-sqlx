//! Card payments on SQLite with a retry cap: after three attempts a payment the card network
//! keeps declining moves to the `payments.failed` group, where an operator reviews it.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_settlement_dead_letter --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:dead_letter]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `payment_jobs`: `attempt` counts the deliveries the cap reads.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "payment_jobs")]
struct Charge {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<Charge>::new("payments"))]
async fn charge(payment: &Payment) -> HandlerOutcome {
    if authorize(payment).await {
        return HandlerOutcome::ack();
    }
    HandlerOutcome::retry()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<Charge>("payments"), |b| {
            // Three attempts, the first included; the third failure moves the row into the
            // `payments.failed` group instead.
            b.include(charge)
                .max_attempts(nonzero!(3u32))
                .dead_letter("payments.failed");
        })
}
// --8<-- [end:dead_letter]

/// A stand-in for the card network: it declines the test card that always declines.
async fn authorize(payment: &Payment) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(payment.order, "authorizing");
    payment.card != "4000000000000002"
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:payments.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS payment_jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for Charge {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO payment_jobs (name, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    order: u64,
    card: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Payment, SCHEMA, service};

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
    async fn a_payment_declined_three_times_moves_to_the_failed_group() -> Result<(), Box<dyn Error>>
    {
        let (url, pool, _keeper) = database("sqlx_settlement_dead_letter").await?;
        let tb = TestApp::start(service(&url)).await?;
        let payment = Payment {
            order: 1042,
            card: "4000000000000002".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&payment)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("payments")
            .assert_called(3)
            .with(&payment);
        let groups: Vec<String> = sqlx::query_scalar("SELECT name FROM payment_jobs")
            .fetch_all(&pool)
            .await?;
        assert_eq!(groups, ["payments.failed"], "the row waits for an operator");
        Ok(())
    }
}
