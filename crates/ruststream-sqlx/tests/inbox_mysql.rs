//! What MySQL and MariaDB add, against both stands: the server's version read when a subscription
//! opens, claims at READ COMMITTED, and a dead letter into a table moved in two statements.

#![cfg(all(
    feature = "inbox",
    feature = "mysql",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::future::{Future, pending, ready};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream::{
    AckError, Broker, ConnectedBroker, DescribeServer, IncomingMessage, RetryDeclaration,
    Subscriber, SubscriptionSource,
};
use ruststream_sqlx::dialect::{
    self, ClaimShape, Dialect, Lease, RowLock, Statement, StatementError, TableName, TableSpec,
};
use ruststream_sqlx::{
    Claim, ConnectedSqlxBroker, Fetch, Inbox, InboxQueue, InboxRow, SqlxBroker, SqlxBrokerError,
};
use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, MySqlConnection, MySqlPool};

use live::rows::lease;
use live::rows::row_lock::{Plain, SendEmail};

const POLL: Duration = Duration::from_millis(20);

/// The release a doctored dialect asks for: newer than any server.
const FUTURE_RELEASE: &str = "MySQL 99";

/// The query a doctored dialect reads the version with, which no server answers.
const UNANSWERED: &str = "SELECT no_such_function()";

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Email {
    to: String,
}

fn email() -> Email {
    Email {
        to: "a@example.com".to_owned(),
    }
}

/// The answer to an email, which the `followups` group of the same table receives.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "followups")]
struct Followup {
    to: String,
}

/// The MySQL dialect with one answer changed, as a dialect of a service's own may change it.
#[derive(Debug, Clone, Copy)]
enum Doctored {
    /// Needs a server newer than any there is.
    Floor,
    /// Reads the version with a query the server cannot answer.
    Unanswered,
    /// Refuses every server, for a reason other than its age.
    Refusing,
    /// Moves a dead letter with a delete that finds no row, as one does after another claim took
    /// the row between the copy and the delete.
    Missing,
}

impl Dialect for Doctored {
    fn name(&self) -> &'static str {
        "doctored"
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        dialect::MySql.quote_into(ident, out);
    }

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        dialect::MySql.placeholder_into(index, out);
    }

    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::MySql.fetch(spec)
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::MySql.ack(spec)
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        dialect::MySql.retry(spec)
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::MySql.retry_after(spec)
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::MySql.discard(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::MySql.dead_letter_group(spec)
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        let mut moves = dialect::MySql.dead_letter_table(spec, target)?;
        if let (Self::Missing, Some(delete)) = (self, moves.last_mut()) {
            *delete = Statement::new(
                format!("{} AND FALSE", delete.sql()),
                delete.params().iter().copied(),
            );
        }
        Ok(moves)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::MySql.insert(spec)
    }

    fn server_version(&self) -> Option<&'static str> {
        match self {
            Self::Unanswered => Some(UNANSWERED),
            Self::Floor | Self::Refusing | Self::Missing => dialect::MySql.server_version(),
        }
    }

    fn check_server(&self, spec: &TableSpec<'_>, version: &str) -> Result<(), StatementError> {
        match self {
            Self::Floor => Err(StatementError::ServerTooOld {
                dialect: self.name(),
                server: version.to_owned(),
                required: FUTURE_RELEASE,
            }),
            Self::Refusing => Err(StatementError::UnsupportedForm {
                dialect: self.name(),
                form: spec.form().name(),
            }),
            Self::Unanswered | Self::Missing => dialect::MySql.check_server(spec, version),
        }
    }
}

impl RowLock for Doctored {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        dialect::MySql.lock_claim(spec, shape)
    }

    fn begin_lock_claim(&self) -> Option<&'static str> {
        dialect::MySql.begin_lock_claim()
    }
}

impl Lease for Doctored {
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        dialect::MySql.lease_claim(spec, shape)
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::MySql.extend(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::MySql.stamp(spec)
    }

    fn claim_writes_lease(&self) -> bool {
        dialect::MySql.claim_writes_lease()
    }

    fn begin_lease_claim(&self) -> Option<&'static str> {
        dialect::MySql.begin_lease_claim()
    }
}

/// A job whose claim is the service's own and never finishes: it sends a statement of its own to
/// be prepared and waits, as a claim does that a subscription's shutdown interrupts while the
/// server prepares its statement.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "plain_jobs", custom(claim, fetch))]
struct Interrupted {
    #[field(id, generated)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Claim<MySql> for Interrupted {
    async fn claim(
        conn: &mut MySqlConnection,
        _queue: &str,
        limit: i64,
    ) -> Result<Vec<i64>, sqlx::Error> {
        {
            // Prepared for this run alone, so the server prepares it now: one poll sends it, and
            // its answer stays unread.
            let mut ids = pin!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT id FROM plain_jobs ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED"
                )
                .bind(limit)
                .persistent(false)
                .fetch(&mut *conn)
            );
            let _ = futures::poll!(ids.try_next());
        }
        pending().await
    }
}

impl Fetch<MySql> for Interrupted {
    // The claim never returns ids, so nothing is ever fetched.
    fn fetch(
        _conn: &mut MySqlConnection,
        _ids: &[i64],
    ) -> impl Future<Output = Result<Vec<Self>, sqlx::Error>> + Send {
        ready(Ok(Vec::new()))
    }
}

/// Whether `error` reports a lease that ran out.
fn lost(error: &AckError) -> bool {
    let AckError::Broker(source) = error else {
        return false;
    };
    matches!(
        source.downcast_ref::<SqlxBrokerError>(),
        Some(SqlxBrokerError::LeaseLost { .. })
    )
}

/// What opening a subscription to `plain_jobs` under `dialect` reports.
async fn refusal(pool: MySqlPool, dialect: Doctored) -> SqlxBrokerError {
    let connected = SqlxBroker::with_dialect(pool, dialect)
        .connect()
        .await
        .expect("connects");
    let refused = InboxQueue::<Plain>::new("plain")
        .subscribe(&connected)
        .await
        .expect_err("the startup check refuses the subscription");
    connected.shutdown().await.expect("stops");
    refused
}

live::mysql_stands! {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_server_version_is_read_at_subscribe() {
        let Some(db) = database().await else { return };
        let version: String = sqlx::query_scalar("SELECT VERSION()")
            .fetch_one(&db.pool)
            .await
            .expect("the server reports its version");
        dialect::MySql
            .check_server(&Plain::SPEC, &version)
            .expect("the stand runs a release that skips locked rows");
        let connected = SqlxBroker::new(db.pool.clone())
            .connect()
            .await
            .expect("connects");
        let subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("the subscription opens on the version it read");
        drop(subscriber);
        connected.shutdown().await.expect("stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_server_older_than_the_dialect_needs_stops_the_subscription() {
        let Some(db) = database().await else { return };
        let version: String = sqlx::query_scalar("SELECT VERSION()")
            .fetch_one(&db.pool)
            .await
            .expect("the server reports its version");
        let refused = refusal(db.pool.clone(), Doctored::Floor).await;
        assert!(
            matches!(&refused, SqlxBrokerError::ServerTooOld {
                subscription, table, server, required, ..
            } if subscription == "plain"
                && table == "plain_jobs"
                && *server == version
                && *required == FUTURE_RELEASE),
            "{refused:?}"
        );
        let message = refused.to_string();
        assert!(
            message.ends_with(&format!(
                "the server reports `{version}`, and this form needs MySQL 99 or later"
            )),
            "{message}"
        );
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_version_check_that_fails_stops_the_subscription() {
        let Some(db) = database().await else { return };
        let unanswered = refusal(db.pool.clone(), Doctored::Unanswered).await;
        assert!(
            matches!(&unanswered, SqlxBrokerError::Sqlx { subscription, statement, .. }
                if subscription == "plain" && *statement == UNANSWERED),
            "{unanswered:?}"
        );
        let refused = refusal(db.pool.clone(), Doctored::Refusing).await;
        assert!(
            matches!(&refused, SqlxBrokerError::Dialect {
                subscription,
                source: StatementError::UnsupportedForm { dialect: "doctored", .. },
                ..
            } if subscription == "plain"),
            "{refused:?}"
        );
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"), reply)]
    async fn answer(emails: &[Email]) -> Vec<Followup> {
        emails
            .iter()
            .map(|email| Followup {
                to: email.to.clone(),
            })
            .collect()
    }

    #[subscriber(InboxQueue::<SendEmail>::new("followups"))]
    async fn follow(_followup: &Followup) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    // A claim that takes fewer rows than it asks for reads to the end of the table. At the default
    // REPEATABLE READ it would also lock the gap after the last row, where every insert lands,
    // until its batch settles: the answer's insert would wait for the batch, and the batch for the
    // answer, until the server's lock wait runs out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_can_publish_into_its_own_table_while_it_holds_its_row() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<SendEmail>("emails")
            .route::<SendEmail>("followups");
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker, |b| {
            b.include(answer.batch(nonzero!(4)));
            b.include(follow);
        });
        // Far below the server's lock wait of 50 seconds, so a claim that blocks the insert fails
        // the test at once.
        let tb = TestApp::start_live_within(app, Duration::from_secs(5))
            .await
            .expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(200))
            .await
            .expect("nothing else runs");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called_once();
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("followups")
            .assert_called_once()
            .with(&Followup {
                to: "a@example.com".to_owned(),
            })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dead_letter_whose_delete_finds_no_row_moves_nothing() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::with_dialect(db.pool.clone(), Doctored::Missing)
            .connect()
            .await
            .expect("connects");
        let queue = SubscriptionSource::<ConnectedSqlxBroker<Db, Doctored>>::declare_retry(
            InboxQueue::<lease::Plain>::new("plain"),
            &RetryDeclaration::new()
                .with_max_attempts(nonzero!(1u32))
                .with_dead_letter("plain_jobs_dead"),
        );
        let mut subscriber = queue.subscribe(&connected).await.expect("opens");
        let delivery = {
            let mut deliveries = pin!(subscriber.stream());
            deliveries.next().await.expect("goes on").expect("claims")
        };
        // The retry is the declared move: a copy into `plain_jobs_dead`, then a delete that now
        // finds no row under the delivery's lease.
        let refused = delivery
            .nack(true)
            .await
            .expect_err("the move found its row under another lease");
        assert!(lost(&refused), "{refused:?}");
        assert_eq!(
            db.plain_rows("plain_jobs_dead").await,
            Vec::<Vec<u8>>::new(),
            "the copy rolled back with the delete"
        );
        assert_eq!(
            db.plain_rows("plain_jobs").await,
            [b"x".to_vec()],
            "the row stays in its queue, and only there"
        );
        drop(subscriber);
        connected.shutdown().await.expect("stops");
        db.finish().await;
    }

    // sqlx-mysql reads no answer to a statement it was preparing when its future dropped; a
    // rollback queued on that connection then takes the prepare's answer for its own, and the
    // pool's check of the connection waits for the rest forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_interrupted_claim_frees_its_connection() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .connect()
            .await
            .expect("connects");
        let mut subscriber = InboxQueue::<Interrupted>::new("plain")
            .subscribe(&connected)
            .await
            .expect("opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            // The stream drops in the middle of the claim, as the runtime drops it at shutdown.
            let interrupted =
                tokio::time::timeout(Duration::from_millis(200), deliveries.next()).await;
            assert!(interrupted.is_err(), "the claim never finishes");
        }
        drop(subscriber);
        connected.shutdown().await.expect("stops");
        let closed = tokio::time::timeout(Duration::from_secs(5), db.pool.close()).await;
        assert!(
            closed.is_ok(),
            "the interrupted claim's connection never came back to the pool"
        );
        db.finish().await;
    }
}

#[tokio::test]
async fn the_server_description_names_host_and_port_never_credentials() {
    let pool = MySqlPoolOptions::new()
        .connect_lazy("mysql://svc:s3cr3t@db.internal:3307/orders")
        .expect("a lazy pool is built without I/O");
    let server = SqlxBroker::new(pool).describe_server();
    assert_eq!(server.host.as_deref(), Some("db.internal:3307"));
    assert_eq!(server.protocol, "mysql");
    assert!(!format!("{server:?}").contains("s3cr3t"));
}
