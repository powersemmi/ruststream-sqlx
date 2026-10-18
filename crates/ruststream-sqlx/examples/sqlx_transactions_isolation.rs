//! Withdrawals on SQLite at `mode = immediate`: each delivery's transaction takes the database's
//! write lock when it begins, so the balance it reads is the one it writes over.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_transactions_isolation --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

// --8<-- [start:isolation]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqlitePool};

/// One row of `withdrawal_jobs`; its transactions open with `BEGIN IMMEDIATE`.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "withdrawal_jobs", mode = immediate)]
struct Withdraw {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<Withdraw>::new("withdrawals"))]
async fn withdraw(order: &Withdrawal, Ctx(mut tx): Ctx<keys::Tx<Sqlite>>) -> HandlerOutcome {
    let balance: Result<i64, _> =
        sqlx::query_scalar("SELECT cents FROM balances WHERE account = ?")
            .bind(&order.account)
            .fetch_one(&mut *tx)
            .await;
    let Ok(balance) = balance else {
        return HandlerOutcome::retry();
    };
    if balance < order.cents {
        tracing::warn!(account = %order.account, "insufficient funds");
        return HandlerOutcome::drop();
    }
    let paid = sqlx::query("UPDATE balances SET cents = ? WHERE account = ?")
        .bind(balance - order.cents)
        .bind(&order.account)
        .execute(&mut *tx)
        .await;
    if paid.is_ok() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("wallet", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<Withdraw>("withdrawals"),
        |b| {
            b.include(withdraw.transactional());
        },
    )
}
// --8<-- [end:isolation]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:wallet.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS withdrawal_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS balances (account TEXT PRIMARY KEY, cents INTEGER NOT NULL)";

impl Publish<Sqlite> for Withdraw {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO withdrawal_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "withdrawals")]
struct Withdrawal {
    account: String,
    cents: i64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Withdrawal, service};

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
        sqlx::query("INSERT INTO balances (account, cents) VALUES ('ada', 10000)")
            .execute(&pool)
            .await?;
        Ok((pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_withdrawal_pays_out_and_an_overdraft_is_dropped() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_transactions_isolation").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        for cents in [3_000, 9_000] {
            let order = Withdrawal {
                account: "ada".to_owned(),
                cents,
            };
            tb.broker::<SqlxBroker<Sqlite>>()
                .message(&order)
                .publish()
                .await?;
        }

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("withdrawals")
            .assert_called(2)
            .settled(HandlerOutcome::drop());
        let balance: i64 = sqlx::query_scalar("SELECT cents FROM balances WHERE account = 'ada'")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            balance, 7_000,
            "the first withdrawal paid out, the second one was refused"
        );
        Ok(())
    }
}
