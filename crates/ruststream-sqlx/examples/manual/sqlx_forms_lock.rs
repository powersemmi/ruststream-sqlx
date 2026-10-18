//! The service of `examples/sqlx_forms_lock.rs` without the macros: the chain names the
//! service's lock and release with `.own::<..>()`, and the type lists them beside `Advisory`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_forms_lock --features inbox,sqlite,testing
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::spec::{Advisory, Payload, own};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};

// --8<-- [start:lock]
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{Lock, Unlock};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

/// One row of `payouts`, held by the key of its account.
#[derive(sqlx::FromRow)]
struct Payout {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Payout {
    type Id = i64;
    type Table = InboxSpec<(Advisory, Payload, own::Lock, own::Unlock)>;
    const TABLE: Self::Table = InboxSpec::new("payouts", Column::new("id").generated())
        .advisory(&[KeyPart::Literal("accounts-"), KeyPart::Column("account_id")])
        .payload(Column::new("payload"))
        .own::<own::Lock>()
        .own::<own::Unlock>();

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Payout {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
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

struct Pay;

impl Handle<Transfer> for Pay {
    fn handle(
        &self,
        transfer: &Transfer,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(transfer.cents, "paying out");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("payouts", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        let pay = subscriber("payouts", Pay).map_source(|_| InboxQueue::<Payout>::new("payouts"));
        b.include(pay.build());
    })
}
// --8<-- [end:lock]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:payouts.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS held_keys (key TEXT PRIMARY KEY)";

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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

    // The payout and the billing run are written with plain SQL, as the billing system writes
    // them, so the harness does not count the payout until a claim takes it: the test runs on the
    // real clock and lets the claims happen.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payout_waits_for_the_billing_run_of_its_account() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_forms_lock").await?;
        sqlx::query("INSERT INTO held_keys (key) VALUES ('accounts-7')")
            .execute(&pool)
            .await?;
        let transfer = Transfer { cents: 12_500 };
        sqlx::query("INSERT INTO payouts (account_id, payload) VALUES (7, ?)")
            .bind(serde_json::to_vec(&transfer)?)
            .execute(&pool)
            .await?;
        let tb = TestApp::start_live(service(pool.clone())).await?;
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
