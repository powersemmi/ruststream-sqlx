//! The service of `examples/guide/transactional.rs` without the macros: the handler's `Handle`
//! impl names `TxContext`, and `.transactional()` is a step of the mount chain.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_guide_transactional --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::TxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
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
#[derive(sqlx::FromRow)]
struct IssueRefund {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for IssueRefund {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("refund_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for IssueRefund {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for IssueRefund {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
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
    // The mailer's queue, `receipt_jobs`.
    sqlx::query("INSERT INTO receipt_jobs (payload) VALUES (?)")
        .bind(serde_json::to_vec(&Receipt {
            order: refund.order,
        })?)
        .execute(conn)
        .await?;
    Ok(())
}

struct Issue;

impl Handle<Refund, (), (), TxContext<Sqlite>> for Issue {
    async fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        ctx: &mut Context<'_, TxContext<Sqlite>>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(mut tx) = Ctx::<keys::Tx<Sqlite>>::from_context(ctx).await?;
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        if let Err(error) = record(&mut tx, refund).await {
            tracing::warn!(subscription = "refunds", refund.order, %error, "refund not recorded");
            return Err(HandlerOutcome::drop());
        }
        // The processor takes the order as its idempotency key: a call repeated after a retry
        // refunds the card once.
        if !card_refund(refund, attempt).await {
            // The retry rolls the record and the receipt back first.
            return Err(HandlerOutcome::retry());
        }
        // The commit keeps the record and the receipt, and finishes the refund's row.
        Ok(())
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<IssueRefund>("refunds"),
        |b| {
            let issue = subscriber("refunds", Issue)
                .map_source(|_| InboxQueue::<IssueRefund>::new("refunds"))
                .transactional();
            b.include(issue.build());
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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Refund {
    order: i64,
    amount_cents: i64,
}

// `#[outgoing(name = "refunds")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Refund {
    type Form = FixedName;
    const DESTINATION: &'static str = "refunds";
}

impl MessageHeaders for Refund {
    type Contract = NoHeaders;
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
        let (pool, _keeper) = database("manual_guide_transactional_ack").await?;
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
        let (pool, _keeper) = database("manual_guide_transactional_retry").await?;
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
        let (pool, _keeper) = database("manual_guide_transactional_twice").await?;
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
