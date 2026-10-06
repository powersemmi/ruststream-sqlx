//! What `shutdown` does to the advisory locks of a broker: it releases the lock of every delivery
//! in work, waits for the settlements in flight and for the connections still closing, and returns
//! once no lock of the broker is left. `ClosedSqlxBroker` reports the locks it released and the
//! connections closed instead. A delivery whose lock it released settles no more, and a claim
//! dropped midway, or a handler that panics under `fail_fast`, leaves no lock behind.
//!
//! MySQL and MariaDB name their locks server-wide, so the broker names each lock by the table's
//! database and the key, and the probes here ask the server for that name.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::future::{Future, ready};
use std::pin::pin;
use std::time::Duration;

use futures::{StreamExt, poll};
use ruststream::prelude::*;
use ruststream::runtime::RustStreamError;
use ruststream::testing::{InProcess, TestApp};
use ruststream::{
    AckError, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscriber,
    SubscriptionSource,
};
use ruststream_sqlx::{
    Inbox, InboxQueue, Insert, Publish, QueueDatabase, SqlxBroker, SqlxBrokerError,
};
use serde::{Deserialize, Serialize};
use sqlx::{Database, FromRow, Pool};

const POLL: Duration = Duration::from_millis(20);

/// The longest a test waits for a row its broker should claim at once.
const AT_ONCE: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Email {
    to: String,
}

fn email() -> Email {
    Email {
        to: "a@example.com".to_owned(),
    }
}

/// The tenant of a test's jobs: the process and the test, so no other test's key is the same.
fn tenant_of(test: &str) -> String {
    format!("{}-{test}", std::process::id())
}

/// A plain job locked by its tenant and its id.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "plain_jobs", advisory_lock = "shutdown-{tenant}-{id}")]
struct Job {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    tenant: String,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Job {
    fn of(tenant: String, payload: &[u8]) -> Self {
        Self {
            id: 0,
            attempt: 1,
            tenant,
            payload: payload.to_vec(),
        }
    }
}

/// A published job belongs to the tenant of the name it was published to.
impl<DB> Publish<DB> for Job
where
    DB: QueueDatabase,
    Self: Insert<DB::Connection>,
{
    async fn publish(
        conn: &mut DB::Connection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        Self::of(tenant_of(message.name()), message.payload())
            .insert(conn)
            .await
    }
}

/// The key of the first job of `tenant`, the one a test writes.
fn key(tenant: &str) -> String {
    format!("shutdown-{tenant}-1")
}

/// A stand's database, and the advisory locks it shows.
trait Locks: Database {
    /// Whether the database `pool` reaches shows a lock of the broker: on Postgres any advisory
    /// lock of the test's database, on MySQL and MariaDB the lock the broker takes on `key`, named
    /// by the database and the key. `None` where the database keeps no locks, as SQLite does,
    /// whose keys in work live in the process.
    fn locked(pool: &Pool<Self>, key: &str) -> impl Future<Output = Option<bool>> + Send;
}

#[cfg(feature = "postgres")]
impl Locks for sqlx::Postgres {
    async fn locked(pool: &Pool<Self>, _: &str) -> Option<bool> {
        let held: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND database = \
             (SELECT oid FROM pg_database WHERE datname = current_database())",
        )
        .fetch_one(pool)
        .await
        .expect("the locks read");
        Some(held > 0)
    }
}

#[cfg(feature = "mysql")]
impl Locks for sqlx::MySql {
    async fn locked(pool: &Pool<Self>, key: &str) -> Option<bool> {
        let name = live::mysql::lock_name(pool, key).await;
        Some(live::mysql::lock_held(pool, &name).await)
    }
}

#[cfg(feature = "sqlite")]
impl Locks for sqlx::Sqlite {
    fn locked(_: &Pool<Self>, _: &str) -> impl Future<Output = Option<bool>> + Send {
        ready(None)
    }
}

/// Whether `refused` is the error of a settlement whose lock `shutdown` released.
fn is_closed(refused: &AckError) -> bool {
    matches!(refused, AckError::Broker(source)
        if matches!(source.downcast_ref::<SqlxBrokerError>(), Some(SqlxBrokerError::Closed)))
}

/// One module per stand, each holding `$items`: Postgres, MySQL, MariaDB and SQLite.
macro_rules! stands {
    ($($items:item)*) => {
        server_stands! { $($items)* }

        #[cfg(feature = "sqlite")]
        mod sqlite {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::sqlite::{Db, database};
            $($items)*
        }
    };
}

/// One module per stand that runs as a server, each holding `$items`: for a test that holds a row
/// in a transaction of its own, which only a server lets run beside the broker's statements.
macro_rules! server_stands {
    ($($items:item)*) => {
        #[cfg(feature = "postgres")]
        mod postgres {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::postgres::{Db, database};
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mysql {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mysql::{Db, database};
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mariadb {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mariadb::{Db, database};
            $($items)*
        }
    };
}

/// Every stand: a delivery held while `shutdown` runs, and a handler that panics under the
/// default `fail_fast`.
mod in_work {
    use super::*;

    stands! {
        /// Writes the job of `tenant`.
        async fn write_job(pool: &Pool<Db>, tenant: &str) {
            let mut conn = pool.acquire().await.expect("a connection");
            Job::of(tenant.to_owned(), b"x")
                .insert(&mut *conn)
                .await
                .expect("the job writes");
        }

        /// A new broker on `pool` takes the job at once and acknowledges it: no lock of a broker
        /// before it holds the job's key, in the database or in the process.
        async fn claimed_at_once(pool: &Pool<Db>) {
            let again = SqlxBroker::new(pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("a second broker connects");
            let mut subscriber = InboxQueue::<Job>::new("jobs")
                .subscribe(&again)
                .await
                .expect("the subscription opens");
            {
                let mut deliveries = pin!(subscriber.stream());
                let taken = tokio::time::timeout(AT_ONCE, deliveries.next())
                    .await
                    .expect("the job is claimable at once")
                    .expect("the stream goes on")
                    .expect("the claim takes the job");
                taken.ack().await.expect("the job settles");
            }
            drop(subscriber);
            again.shutdown().await.expect("the broker shuts down");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn shutdown_releases_a_lock_in_work() {
            let Some(db) = database().await else { return };
            let tenant = tenant_of("in-work");
            write_job(&db.pool, &tenant).await;
            let connected = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<Job>::new("jobs")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let held = {
                let mut deliveries = pin!(subscriber.stream());
                deliveries
                    .next()
                    .await
                    .expect("the stream goes on")
                    .expect("the claim takes the job")
            };
            drop(subscriber);
            assert_ne!(
                Db::locked(&db.pool, &key(&tenant)).await,
                Some(false),
                "the delivery's session holds its job's lock"
            );
            let closed = connected.shutdown().await.expect("the broker shuts down");
            assert_eq!(
                (closed.locks_released(), closed.connections_closed()),
                (1, 0),
                "shutdown released the lock in work and returned its session to the pool: \
                 {closed:?}"
            );
            assert_ne!(
                Db::locked(&db.pool, &key(&tenant)).await,
                Some(true),
                "no lock of the broker is left"
            );
            claimed_at_once(&db.pool).await;
            let refused = held
                .ack()
                .await
                .expect_err("a settlement after shutdown released its lock fails");
            assert!(is_closed(&refused), "{refused:?}");
            assert_eq!(db.count("plain_jobs").await, 0, "the second broker settled the job");
            db.finish().await;
        }

        #[subscriber(InboxQueue::<Job>::new("fail-fast"))]
        async fn panics(_email: &Email) -> HandlerOutcome {
            panic!("the handler fails")
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_fail_fast_panic_leaves_no_lock() {
            let Some(db) = database().await else { return };
            let broker = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .route::<Job>("fail-fast");
            let app = RustStream::new(AppInfo::new("shutdown", "0.0.0")).with_broker(broker, |b| {
                b.include(panics);
            });
            let tb = TestApp::start_live(app).await.expect("the app starts");
            // The panic tears the service down, whatever the publish reports.
            let _ = tb
                .broker::<SqlxBroker<Db>>()
                .message(&email())
                .to("fail-fast")
                .publish()
                .await;
            let stopped = tb.shutdown().await;
            assert!(
                matches!(stopped, Err(RustStreamError::Dispatch(_))),
                "the panic failed the service fast: {stopped:?}"
            );
            assert_ne!(
                Db::locked(&db.pool, &key(&tenant_of("fail-fast"))).await,
                Some(true),
                "the delivery left unsettled holds no lock once the service stopped"
            );
            claimed_at_once(&db.pool).await;
            db.finish().await;
        }
    }
}

/// The server stands: a row the test holds in a transaction of its own keeps the broker's
/// statement on it waiting.
mod held_back {
    use super::*;

    server_stands! {
        /// Writes the job of `tenant`, and holds its row in a transaction of the test's own.
        async fn held_job(
            pool: &Pool<Db>,
            tenant: &str,
        ) -> sqlx::Transaction<'static, Db> {
            let mut conn = pool.acquire().await.expect("a connection");
            Job::of(tenant.to_owned(), b"x")
                .insert(&mut *conn)
                .await
                .expect("the job writes");
            drop(conn);
            let mut held = pool.begin().await.expect("a transaction");
            sqlx::query("SELECT id FROM plain_jobs WHERE id = 1 FOR UPDATE")
                .execute(&mut *held)
                .await
                .expect("the row locks");
            held
        }

        // A claim dropped while its take waits: its session took the key's lock, and closes after
        // releasing it. The close cannot end before the test lets the row go, so `shutdown` meets
        // it closing and waits for it.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_claim_cancelled_midway_leaves_no_lock() {
            let Some(db) = database().await else { return };
            let tenant = tenant_of("cancelled");
            let held = held_job(&db.pool, &tenant).await;
            let connected = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<Job>::new("jobs")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            {
                let mut deliveries = pin!(subscriber.stream());
                let waited =
                    tokio::time::timeout(Duration::from_millis(500), deliveries.next()).await;
                assert!(waited.is_err(), "the claim waits in its take: {waited:?}");
            }
            // The stream is gone, and the claim with it.
            drop(subscriber);
            assert_eq!(
                Db::locked(&db.pool, &key(&tenant)).await,
                Some(true),
                "the dropped claim's session holds the key's lock until its close ends"
            );
            let shutting_down = connected.shutdown();
            held.rollback().await.expect("the row is let go");
            let closed = shutting_down.await.expect("the broker shuts down");
            assert_eq!(
                (closed.locks_released(), closed.connections_closed()),
                (0, 1),
                "shutdown waited for the claim's session to close: {closed:?}"
            );
            assert_eq!(
                Db::locked(&db.pool, &key(&tenant)).await,
                Some(false),
                "no lock of the broker is left"
            );
            db.finish().await;
        }

        // A settlement whose statement waits on the row: `shutdown` leaves the session to it, and
        // returns once the settlement has released the lock itself. On a connection to the server
        // the settlement runs in the test's own task and takes its session in its first poll, so
        // each future is polled once, in order.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn shutdown_waits_for_a_settlement_in_flight() {
            let Some(db) = database().await else { return };
            let tenant = tenant_of("settling");
            let mut conn = db.pool.acquire().await.expect("a connection");
            Job::of(tenant.clone(), b"x")
                .insert(&mut *conn)
                .await
                .expect("the job writes");
            drop(conn);
            let connected = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<Job>::new("jobs")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let delivery = {
                let mut deliveries = pin!(subscriber.stream());
                deliveries
                    .next()
                    .await
                    .expect("the stream goes on")
                    .expect("the claim takes the job")
            };
            drop(subscriber);
            let mut held = db.pool.begin().await.expect("a transaction");
            sqlx::query("SELECT id FROM plain_jobs WHERE id = 1 FOR UPDATE")
                .execute(&mut *held)
                .await
                .expect("the row locks");
            let mut acking = pin!(delivery.ack());
            assert!(
                poll!(acking.as_mut()).is_pending(),
                "the acknowledgement's delete waits on the row"
            );
            let mut shutting_down = pin!(connected.shutdown());
            assert!(
                poll!(shutting_down.as_mut()).is_pending(),
                "shutdown waits for the settlement in flight"
            );
            held.rollback().await.expect("the row is let go");
            let (acked, closed) = tokio::join!(acking, shutting_down);
            acked.expect("the acknowledgement in flight settles");
            let closed = closed.expect("the broker shuts down");
            assert_eq!(
                (closed.locks_released(), closed.connections_closed()),
                (0, 0),
                "the settlement released its own lock: {closed:?}"
            );
            assert_eq!(
                Db::locked(&db.pool, &key(&tenant)).await,
                Some(false),
                "no lock of the broker is left"
            );
            assert_eq!(db.count("plain_jobs").await, 0, "the acknowledgement deleted the job");
            db.finish().await;
        }
    }
}

/// An unlock the database does not confirm: `shutdown` closes its session instead of returning it
/// to the pool, and counts it.
#[cfg(feature = "postgres")]
mod unconfirmed {
    use ruststream_sqlx::{Lock, Unlock};
    use sqlx::{PgConnection, Postgres};

    use super::*;
    use crate::live::postgres::database;

    /// A plain job whose unlock releases the key and answers that it did not.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(
        table = "plain_jobs",
        advisory_lock = "unconfirmed-{id}",
        custom(lock, unlock)
    )]
    struct Unconfirmed {
        #[field(id, generated)]
        id: i64,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(payload)]
        payload: Vec<u8>,
    }

    impl Lock<Postgres> for Unconfirmed {
        async fn lock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
            sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext($1))")
                .bind(key)
                .fetch_one(conn)
                .await
        }
    }

    impl Unlock<Postgres> for Unconfirmed {
        async fn unlock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
            sqlx::query_scalar("SELECT pg_advisory_unlock(hashtext($1)) AND false")
                .bind(key)
                .fetch_one(conn)
                .await
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_release_the_database_does_not_confirm_closes_its_session() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Unconfirmed>::new("jobs")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        let held = {
            let mut deliveries = pin!(subscriber.stream());
            deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the job")
        };
        drop(subscriber);
        let closed = connected.shutdown().await.expect("the broker shuts down");
        assert_eq!(
            (closed.locks_released(), closed.connections_closed()),
            (0, 1),
            "the unconfirmed release closed the session: {closed:?}"
        );
        assert_eq!(
            <Postgres as Locks>::locked(&db.pool, "").await,
            Some(false),
            "no lock of the broker is left"
        );
        let refused = held
            .ack()
            .await
            .expect_err("a settlement after shutdown released its lock fails");
        assert!(is_closed(&refused), "{refused:?}");
        db.finish().await;
    }
}
