//! Payouts that share a lock with the billing system on SQLite: `custom(lock, unlock)` hands
//! each key's lock and release to the service's SQL, a table of the keys in work that billing
//! writes too.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_forms_lock --features inbox,sqlite,testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:lock]
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{Lock, Unlock};
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Sqlite, SqliteConnection};

/// One row of `payouts`, held by the key of its account.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(
    table = "payouts",
    advisory_lock = "accounts-{account_id}",
    custom(lock, unlock)
)]
struct Payout {
    #[field(id, generated)]
    id: i64,
    account_id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

/// Billing holds an account's key in `held_keys` while it changes the account, so a payout
/// never runs beside a billing run of its account.
impl Lock<Sqlite> for Payout {
    async fn lock(conn: &mut SqliteConnection, key: &str) -> Result<bool, sqlx::Error> {
        let taken = sqlx::query("INSERT OR IGNORE INTO held_keys (key) VALUES (?)")
            .bind(key)
            .execute(conn)
            .await?;
        Ok(taken.rows_affected() == 1)
    }
}

impl Unlock<Sqlite> for Payout {
    async fn unlock(conn: &mut SqliteConnection, key: &str) -> Result<bool, sqlx::Error> {
        let released = sqlx::query("DELETE FROM held_keys WHERE key = ?")
            .bind(key)
            .execute(conn)
            .await?;
        Ok(released.rows_affected() == 1)
    }
}

#[subscriber(InboxQueue::<Payout>::new("payouts"))]
async fn pay(transfer: &Transfer) -> HandlerOutcome {
    tracing::info!(transfer.cents, "paying out");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("payouts", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            b.include(pay);
        })
}
// --8<-- [end:lock]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:payouts.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS held_keys (key TEXT PRIMARY KEY)";

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Transfer {
    cents: i64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Transfer, service};

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

    // The payout and the billing run are written with plain SQL, as the billing system writes
    // them, so the harness does not count the payout until a claim takes it: the test runs on the
    // real clock and lets the claims happen.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payout_waits_for_the_billing_run_of_its_account() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_forms_lock").await?;
        sqlx::query("INSERT INTO held_keys (key) VALUES ('accounts-7')")
            .execute(&pool)
            .await?;
        let transfer = Transfer { cents: 12_500 };
        sqlx::query("INSERT INTO payouts (account_id, payload) VALUES (7, ?)")
            .bind(serde_json::to_vec(&transfer)?)
            .execute(&pool)
            .await?;
        let tb = TestApp::start_live(service(&url)).await?;
        tb.advance(Duration::from_millis(500)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("payouts")
            .assert_not_called();

        // Billing ends its run and releases the account's key.
        sqlx::query("DELETE FROM held_keys WHERE key = 'accounts-7'")
            .execute(&pool)
            .await?;
        tb.advance(Duration::from_secs(2)).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("payouts")
            .assert_called_once()
            .with(&transfer)
            .settled(HandlerOutcome::ack());
        let held: i64 = sqlx::query_scalar("SELECT count(*) FROM held_keys")
            .fetch_one(&pool)
            .await?;
        assert_eq!(held, 0, "the payout released the key it took");
        Ok(())
    }
}
