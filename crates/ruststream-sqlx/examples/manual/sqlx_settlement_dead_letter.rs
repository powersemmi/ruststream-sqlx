//! The service of `examples/sqlx_settlement_dead_letter.rs` without the macros: the cap and the
//! dead-letter group are the same steps of the mount chain.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_settlement_dead_letter --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:dead_letter]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `payment_jobs`: `attempt` counts the deliveries the cap reads.
#[derive(sqlx::FromRow)]
struct Charge {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for Charge {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("payment_jobs", Column::new("id").generated())
        .group(Column::new("name"))
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for Charge {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for Charge {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct ChargeCard;

impl Handle<Payment> for ChargeCard {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if authorize(payment).await {
            return Ok(());
        }
        Err(HandlerOutcome::retry())
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<Charge>("payments"),
        |b| {
            let charge = subscriber("payments", ChargeCard)
                .map_source(|_| InboxQueue::<Charge>::new("payments"));
            // Three attempts, the first included; the third failure moves the row into the
            // `payments.failed` group instead.
            b.include(charge.build())
                .max_attempts(nonzero!(3u32))
                .dead_letter("payments.failed");
        },
    )
}
// --8<-- [end:dead_letter]

/// A stand-in for the card network: it declines the test card that always declines.
async fn authorize(payment: &Payment) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(payment.order, "authorizing");
    payment.card != "4000000000000002"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:payments.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
    async fn a_payment_declined_three_times_moves_to_the_failed_group() -> Result<(), Box<dyn Error>>
    {
        let (pool, _keeper) = database("manual_sqlx_settlement_dead_letter").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
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
