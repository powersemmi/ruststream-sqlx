//! The service of the Ordering guide: changes for the CRM in a FIFO group, one at a time however
//! many instances run, and ledger postings kept in order per account across four workers.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example guide_ordering --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

// --8<-- [start:schema]
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS crm_jobs (
    id INTEGER PRIMARY KEY,
    target TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS crm_jobs_claim ON crm_jobs (target, id);
CREATE TABLE IF NOT EXISTS ledger_jobs (
    id INTEGER PRIMARY KEY,
    account TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
);
";
// --8<-- [end:schema]

// --8<-- [start:fifo]
use ruststream_sqlx::prelude::*;
use sqlx::{Sqlite, SqlitePool};

/// One row of `crm_jobs`; each group holds the changes bound for one system, in order.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "crm_jobs")]
struct CrmChange {
    #[field(id, generated)]
    id: i64,
    #[field(group, fifo = true)]
    target: String,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The CRM receives its changes one at a time, in the order they were queued.
#[subscriber(InboxQueue::<CrmChange>::new("crm"))]
async fn sync(change: &Contact) -> HandlerOutcome {
    if push(change).await {
        return HandlerOutcome::ack();
    }
    // At once: the change keeps its place at the head of the group.
    HandlerOutcome::retry()
}
// --8<-- [end:fifo]

// --8<-- [start:keyed]
/// One row of `ledger_jobs`: the postings of one account apply in the order they were written.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "ledger_jobs")]
struct LedgerPosting {
    #[field(id, generated)]
    id: i64,
    #[field(partition_key)]
    account: String,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber(InboxQueue::<LedgerPosting>::new("postings"))]
async fn post(posting: &Posting) -> HandlerOutcome {
    tracing::info!(account = %posting.account, posting.n, "posting");
    HandlerOutcome::ack()
}
// --8<-- [end:keyed]

// --8<-- [start:app]
fn service(pool: SqlitePool) -> RustStream {
    let broker = SqlxBroker::new(pool)
        .route::<CrmChange>("crm")
        .route::<LedgerPosting>("postings");
    RustStream::new(AppInfo::new("accounts", "0.1.0")).with_broker(broker, |b| {
        // A change the CRM refuses three times moves out of the group, which then moves on.
        b.include(sync)
            .max_attempts(nonzero!(3u32))
            .dead_letter("crm.parked");
        // Four workers, and the postings of one account on one of them, one after another.
        b.include(post.workers_by_key(nonzero!(4)));
    })
}
// --8<-- [end:app]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:accounts.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

/// A publish to `crm` writes a row into that group.
impl Publish<Sqlite> for CrmChange {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO crm_jobs (target, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// A publish to `postings` writes a row keyed by the posting's account.
impl Publish<Sqlite> for LedgerPosting {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        let posting: Posting = serde_json::from_slice(message.payload())
            .map_err(|error| sqlx::Error::Decode(error.into()))?;
        sqlx::query("INSERT INTO ledger_jobs (account, payload) VALUES (?, ?)")
            .bind(posting.account)
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// A contact the CRM keeps.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "crm")]
struct Contact {
    id: u64,
    email: String,
}

/// A posting to one account.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "postings")]
struct Posting {
    account: String,
    n: u32,
}

/// A stand-in for the CRM's API: it refuses a contact without an email.
async fn push(change: &Contact) -> bool {
    tokio::task::yield_now().await;
    !change.email.is_empty()
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use chrono::{TimeDelta, Utc};
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Contact, Posting, SCHEMA, service};

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

    fn contact(id: u64, email: &str) -> Contact {
        Contact {
            id,
            email: email.to_owned(),
        }
    }

    // Past the subscriptions' poll interval, one second by default.
    const POLL: Duration = Duration::from_secs(2);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_change_is_parked_and_the_group_moves_on() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("guide_ordering_parked").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        for change in [contact(1, ""), contact(2, "ada@example.com")] {
            tb.broker::<SqlxBroker<Sqlite>>()
                .message(&change)
                .publish()
                .await?;
        }

        let pushed = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("crm")
            .assert_called(4)
            .received::<Contact>();
        assert_eq!(
            pushed,
            [
                contact(1, ""),
                contact(1, ""),
                contact(1, ""),
                contact(2, "ada@example.com")
            ]
        );
        let parked: Vec<String> = sqlx::query_scalar("SELECT target FROM crm_jobs")
            .fetch_all(&pool)
            .await?;
        assert_eq!(parked, ["crm.parked"]);
        Ok(())
    }

    // The group's head is in work on another instance, under a lease that runs for an hour. The
    // rows wake no subscription, so the test runs on the real clock past the poll interval.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_change_in_work_elsewhere_holds_its_group() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("guide_ordering_held").await?;
        let leased = Utc::now() + TimeDelta::hours(1);
        for (change, locked_until) in [
            (contact(1, "ada@example.com"), Some(leased)),
            (contact(2, "grace@example.com"), None),
        ] {
            sqlx::query(
                "INSERT INTO crm_jobs (target, locked_until, payload) VALUES ('crm', ?, ?)",
            )
            .bind(locked_until)
            .bind(serde_json::to_vec(&change)?)
            .execute(&pool)
            .await?;
        }
        let tb = TestApp::start_live(service(pool)).await?;
        tb.advance(POLL).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("crm")
            .assert_not_called();
        Ok(())
    }

    // Postings written before the service starts, by another program: they wake no subscription,
    // so the test runs on the real clock past the poll interval.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_postings_of_an_account_apply_in_order() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("guide_ordering_postings").await?;
        let written = [("a", 1), ("b", 1), ("a", 2), ("b", 2), ("a", 3)];
        for (account, n) in written {
            let posting = Posting {
                account: account.to_owned(),
                n,
            };
            sqlx::query("INSERT INTO ledger_jobs (account, payload) VALUES (?, ?)")
                .bind(account)
                .bind(serde_json::to_vec(&posting)?)
                .execute(&pool)
                .await?;
        }
        let tb = TestApp::start_live(service(pool)).await?;
        tb.advance(POLL).await?;

        let posted = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("postings")
            .assert_called(5)
            .received::<Posting>();
        let of = |account: &str| -> Vec<u32> {
            posted
                .iter()
                .filter(|posting| posting.account == account)
                .map(|posting| posting.n)
                .collect()
        };
        assert_eq!(of("a"), [1, 2, 3]);
        assert_eq!(of("b"), [1, 2]);
        Ok(())
    }
}
