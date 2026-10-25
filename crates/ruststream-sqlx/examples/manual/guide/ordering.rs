//! The service of `examples/guide/ordering.rs` without the macros: `.fifo_group(..)` names the
//! group's column, `.partition_key(..)` the key's, and the handlers are named types.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_guide_ordering --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Fifo, Key, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, KeyRow, PayloadRow};
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
use sqlx::Connection;
use sqlx::Sqlite;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `crm_jobs`; each group holds the changes bound for one system, in order.
#[derive(sqlx::FromRow)]
struct CrmChange {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for CrmChange {
    type Id = i64;
    type Table = InboxSpec<(Fifo, Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("crm_jobs", Column::new("id").generated())
        .fifo_group(Column::new("target"))
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for CrmChange {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for CrmChange {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The CRM receives its changes one at a time, in the order they were queued.
struct PushToCrm;

impl Handle<Contact> for PushToCrm {
    async fn handle(
        &self,
        change: &Contact,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if push(change).await {
            return Ok(());
        }
        // At once: the change keeps its place at the head of the group.
        Err(HandlerOutcome::retry())
    }
}
// --8<-- [end:fifo]

// --8<-- [start:keyed]
/// One row of `ledger_jobs`: the postings of one account apply in the order they were written.
#[derive(sqlx::FromRow)]
struct LedgerPosting {
    id: i64,
    account: String,
    payload: Vec<u8>,
}

impl InboxTable for LedgerPosting {
    type Id = i64;
    type Table = InboxSpec<(Key, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("ledger_jobs", Column::new("id").generated())
        .partition_key(Column::new("account"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl KeyRow for LedgerPosting {
    type Key = String;

    fn partition_key(&self) -> &String {
        &self.account
    }
}

impl PayloadRow for LedgerPosting {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct Post;

impl Handle<Posting> for Post {
    fn handle(
        &self,
        posting: &Posting,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(account = %posting.account, posting.n, "posting");
        ready(Ok(()))
    }
}
// --8<-- [end:keyed]

// --8<-- [start:app]
/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    let broker = broker
        .route::<CrmChange>("crm")
        .route::<LedgerPosting>("postings");
    RustStream::new(AppInfo::new("accounts", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            // A change the CRM refuses three times moves out of the group, which then moves on.
            let sync =
                subscriber("crm", PushToCrm).map_source(|_| InboxQueue::<CrmChange>::new("crm"));
            b.include(sync.build())
                .max_attempts(nonzero!(3u32))
                .dead_letter("crm.parked");
            // Four workers, and the postings of one account on one of them, one after another.
            let post = subscriber("postings", Post)
                .map_source(|_| InboxQueue::<LedgerPosting>::new("postings"))
                .workers_by_key(nonzero!(4));
            b.include(post.build());
        })
}
// --8<-- [end:app]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:accounts.db?mode=rwc")
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
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Contact {
    id: u64,
    email: String,
}

// `#[outgoing(name = "crm")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Contact {
    type Form = FixedName;
    const DESTINATION: &'static str = "crm";
}

impl MessageHeaders for Contact {
    type Contract = NoHeaders;
}

/// A posting to one account.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Posting {
    account: String,
    n: u32,
}

// `#[outgoing(name = "postings")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Posting {
    type Form = FixedName;
    const DESTINATION: &'static str = "postings";
}

impl MessageHeaders for Posting {
    type Contract = NoHeaders;
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
        let (url, pool, _keeper) = database("manual_guide_ordering_parked").await?;
        let tb = TestApp::start(service(&url)).await?;
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
        let (url, pool, _keeper) = database("manual_guide_ordering_held").await?;
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
        let tb = TestApp::start_live(service(&url)).await?;
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
        let (url, pool, _keeper) = database("manual_guide_ordering_postings").await?;
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
        let tb = TestApp::start_live(service(&url)).await?;
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
