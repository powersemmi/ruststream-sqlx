//! The service of the Transactional handlers guide: a refund, its record and the receipt job it
//! chains commit with the refund's own row, or not at all.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example guide_transactional --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};

// --8<-- [start:schema]
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS refunds (order_id INTEGER PRIMARY KEY, amount_cents INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS refund_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS receipt_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
";
// --8<-- [end:schema]

// --8<-- [start:tables]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

/// One row of `refund_jobs`, the refunds the shop's support queues.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "refund_jobs")]
struct IssueRefund {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// One row of `receipt_jobs`, the mailer's queue.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "receipt_jobs")]
struct SendReceipt {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}
// --8<-- [end:tables]

// --8<-- [start:handler]
/// The refund's record and its receipt, written on the connection of the delivery's transaction.
async fn record(
    conn: &mut SqliteConnection,
    refund: &Refund,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    sqlx::query("INSERT INTO refunds (order_id, amount_cents) VALUES (?, ?)")
        .bind(refund.order)
        .bind(refund.amount_cents)
        .execute(&mut *conn)
        .await?;
    let receipt = SendReceipt {
        id: 0,
        locked_until: None,
        payload: serde_json::to_vec(&Receipt {
            order: refund.order,
        })?,
    };
    receipt.insert(conn).await?;
    Ok(())
}

#[subscriber(InboxQueue::<IssueRefund>::new("refunds"))]
async fn issue(
    refund: &Refund,
    Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
    Ctx(attempt): Ctx<keys::Attempt>,
) -> HandlerOutcome {
    if let Err(error) = record(&mut tx, refund).await {
        tracing::warn!(subscription = "refunds", refund.order, %error, "refund not recorded");
        return HandlerOutcome::drop();
    }
    // The processor takes the order as its idempotency key: a call repeated after a retry
    // refunds the card once.
    if !card_refund(refund, attempt).await {
        // The retry rolls the record and the receipt back first.
        return HandlerOutcome::retry();
    }
    // The commit keeps the record and the receipt, and finishes the refund's row.
    HandlerOutcome::ack()
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<IssueRefund>("refunds"),
        |b| {
            b.include(issue.transactional());
        },
    )
}
// --8<-- [end:handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:refunds.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

/// A publish to `refunds` writes a row with the service's own statement.
impl Publish<Sqlite> for IssueRefund {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO refund_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "refunds")]
struct Refund {
    order: i64,
    amount_cents: i64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Receipt {
    order: i64,
}

/// A stand-in for the card processor: order 13 times out on its first attempt.
async fn card_refund(refund: &Refund, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    !(refund.order == 13 && attempt == Some(1))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Refund, SCHEMA, service};

    // A database in memory that every connection naming it shares, alive while one of them is
    // open: the test holds that one beside the pool.
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

    /// The rows of `refunds`, `receipt_jobs` and `refund_jobs`.
    async fn counts(pool: &SqlitePool) -> Result<(i64, i64, i64), Box<dyn Error>> {
        Ok(sqlx::query_as(
            "SELECT (SELECT count(*) FROM refunds), (SELECT count(*) FROM receipt_jobs), \
             (SELECT count(*) FROM refund_jobs)",
        )
        .fetch_one(pool)
        .await?)
    }

    fn refund(order: i64) -> Refund {
        Refund {
            order,
            amount_cents: 1250,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refund_commits_its_record_and_its_receipt() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("guide_transactional_ack").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&refund(7))
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(counts(&pool).await?, (1, 1, 0));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_discards_the_first_attempts_writes() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("guide_transactional_retry").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&refund(13))
            .publish()
            .await?;

        let outcomes = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("refunds")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        assert_eq!(counts(&pool).await?, (1, 1, 0), "one record, one receipt");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_second_refund_of_an_order_is_dropped() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("guide_transactional_twice").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        for _ in 0..2 {
            tb.broker::<SqlxBroker<Sqlite>>()
                .message(&refund(7))
                .publish()
                .await?;
        }

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("refunds")
            .assert_called(2)
            .settled(HandlerOutcome::drop());
        assert_eq!(counts(&pool).await?, (1, 1, 0));
        Ok(())
    }
}
