//! The advisory lock form on a live database: a subscription opens with its candidate claim, its
//! lock, its unlock and its take prepared, and a table that does not match its struct stops it at
//! the statement the database refused. Its events run as the claim runs them: a session takes a
//! key another session cannot, and a take counts the attempt and reads the row only while the row
//! is still claimable.
//!
//! A delivery settles as in the other forms and then frees its key, so no lock outlives it: an
//! acknowledgement, a retry, a delayed retry, a dead letter and a handler's panic each leave the
//! database without a lock of the broker. A delivery dropped unsettled closes its session, and a
//! new broker takes its row at once. No transaction stays open while a handler works, and two
//! brokers on one table never deliver one row twice.
//!
//! A batch takes a session per delivery, as many as the pool spares at once. Each database keeps
//! its own locks, so two databases whose rows render one key lock their rows apart, and on MySQL
//! and MariaDB a lock name longer than the server takes is locked by its hash.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::collections::BTreeSet;
use std::future::{Future, ready};
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::prelude::*;
use ruststream::testing::{InProcess, Outcome, TestApp};
use ruststream::{
    BatchSubscriber, Broker, ConnectedBroker, IncomingMessage, Subscribe, Subscriber,
    SubscriptionSource,
};
use ruststream_sqlx::keys::Attempt;
use ruststream_sqlx::{InboxQueue, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::pool::PoolOptions;
use sqlx::{Database, Pool};

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

/// A stand's database, and where it keeps the advisory locks the broker's sessions hold.
trait DatabaseLocks: Database {
    /// How many locks the database `pool` reaches holds now, of those the broker takes on `keys`;
    /// `None` where it keeps none, as SQLite does, whose keys in work live in the process.
    ///
    /// Postgres lists every advisory lock of a database, so it counts them all. MySQL and MariaDB
    /// list none, and answer for the lock a key names.
    fn locks_held(pool: &Pool<Self>, keys: &[&str]) -> impl Future<Output = Option<i64>> + Send;
}

#[cfg(feature = "postgres")]
impl DatabaseLocks for sqlx::Postgres {
    async fn locks_held(pool: &Pool<Self>, _: &[&str]) -> Option<i64> {
        Some(live::postgres::advisory_locks(pool).await)
    }
}

#[cfg(feature = "mysql")]
impl DatabaseLocks for sqlx::MySql {
    async fn locks_held(pool: &Pool<Self>, keys: &[&str]) -> Option<i64> {
        let mut held = 0;
        for key in keys {
            let name = live::mysql::lock_name(pool, key).await;
            if live::mysql::lock_held(pool, &name).await {
                held += 1;
            }
        }
        Some(held)
    }
}

#[cfg(feature = "sqlite")]
impl DatabaseLocks for sqlx::Sqlite {
    fn locks_held(_: &Pool<Self>, _: &[&str]) -> impl Future<Output = Option<i64>> + Send {
        ready(None)
    }
}

/// Asserts the database `pool` reaches holds no lock the broker takes on `keys`, where it keeps
/// them.
async fn assert_no_lock<DB: DatabaseLocks>(pool: &Pool<DB>, keys: &[&str]) {
    if let Some(held) = DB::locks_held(pool, keys).await {
        assert_eq!(held, 0, "the database holds advisory locks of the broker");
    }
}

/// The id a delivery's payload names: each row of `plain_ids` carries its own id as text.
fn payload_id(delivery: &impl IncomingMessage) -> i64 {
    str::from_utf8(delivery.payload())
        .expect("the payload is text")
        .parse()
        .expect("the payload is an id")
}

live::advisory_stands! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone())
            .poll_interval(POLL)
            .route::<SendEmail>("emails")
            .route::<Plain>("plain")
            .route::<Keyed>("keyed")
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn acked(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<Keyed>::new("keyed"))]
    async fn keyed_acked(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_acks_and_frees_its_key() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(acked);
                b.include(keyed_acked);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        // Two jobs of one tenant share one lock key: the second goes into work once the first's
        // acknowledgement freed the key.
        for _ in 0..2 {
            tb.broker::<SqlxBroker<Db>>()
                .message(&email())
                .to("keyed")
                .publish()
                .await
                .expect("the publish settles");
        }
        tb.settle().await.expect("the deliveries settle");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let keyed = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("keyed")
            .assert_called(2)
            .outcomes();
        assert_eq!(keyed, [Outcome::Ack, Outcome::Ack]);
        assert_eq!(db.count("plain_jobs").await, 0, "both keyed jobs are done");
        assert_no_lock(&db.pool, &["email_jobs-1", "plain-acme"]).await;
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn retried_once(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        match attempt {
            Some(1) => HandlerOutcome::retry(),
            // The second delivery reads the attempt the first one's take counted; any other
            // reading drops the row, which the outcomes would show.
            Some(2) => HandlerOutcome::ack(),
            _ => HandlerOutcome::drop(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_returns_at_once_with_its_attempt_counted() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(retried_once);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(500))
            .await
            .expect("the retry settles");
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        assert_eq!(db.count("plain_jobs").await, 0, "the acknowledgement deleted the row");
        assert_no_lock(&db.pool, &["plain_jobs-1"]).await;
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // In process the harness counts each row the broker returns: the retried row is counted
    // before its key goes, so the publish waits for the second delivery too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_process_the_harness_waits_for_a_retried_row() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(retried_once);
            });
        let tb = TestApp::start(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        assert_eq!(db.count("plain_jobs").await, 0, "the acknowledgement deleted the row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn later(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        match attempt {
            Some(1) => HandlerOutcome::retry_after(Duration::from_millis(300)),
            Some(2) => HandlerOutcome::ack(),
            _ => HandlerOutcome::drop(),
        }
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn always_retried(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::retry()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_after_and_dead_letter_settle_as_in_the_other_forms() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(later);
                b.include(always_retried)
                    .max_attempts(nonzero!(2u32))
                    .dead_letter("plain_jobs_dead");
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        assert!(db.email_waits().await, "the delayed row waits for its time in the table");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(600))
            .await
            .expect("the redeliveries settle");
        let emails = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(2)
            .outcomes();
        assert_eq!(emails, [Outcome::Nack, Outcome::Ack]);
        let plain = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .outcomes();
        assert_eq!(plain, [Outcome::Nack, Outcome::Nack]);
        assert_eq!(db.count("plain_jobs").await, 0, "the spent row left the queue");
        assert_eq!(db.count("plain_jobs_dead").await, 1, "the spent row moved");
        assert_no_lock(&db.pool, &["email_jobs-1", "plain_jobs-1"]).await;
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"), on_failure(panic = retry))]
    async fn fails_first(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        assert_ne!(attempt, Some(1), "the first attempt fails");
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_panic_leaves_no_lock() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(fails_first);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(500))
            .await
            .expect("the retry settles");
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Panicked, Outcome::Ack]);
        assert_eq!(db.count("plain_jobs").await, 0, "the second attempt acknowledged the row");
        assert_no_lock(&db.pool, &["plain_jobs-1"]).await;
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // A batch takes a connection per delivery: what the pool spares at once, never a wait for a
    // delivery in work to settle.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_larger_than_the_pool_shrinks_instead_of_waiting() {
        let Some(db) = database().await else { return };
        db.plain(&[b"a".as_slice(), b"b", b"c"]).await;
        // A pool of two connections, one of them held: the claim has one place left. The pool
        // waits for a connection longer than the test waits for the batch.
        let small = PoolOptions::<Db>::new()
            .max_connections(2)
            .acquire_timeout(AT_ONCE * 6)
            .connect_with((*db.pool.connect_options()).clone())
            .await
            .expect("a second pool connects");
        let held = small.acquire().await.expect("a connection");
        let connected = SqlxBroker::new(small.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut batches = pin!(subscriber.batches(nonzero!(3_usize)));
            let batch = tokio::time::timeout(AT_ONCE, batches.next())
                .await
                .expect("the claim takes what the pool spares at once")
                .expect("the stream goes on")
                .expect("the claim takes a row");
            assert_eq!(batch.len(), 1, "the pool spares one connection");
            for delivery in batch {
                delivery.ack().await.expect("the row settles");
            }
        }
        drop((subscriber, held));
        connected.shutdown().await.expect("the broker shuts down");
        assert_eq!(db.count("plain_jobs").await, 2, "two rows wait for the next claim");
        small.close().await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_dropped_unsettled_closes_its_session() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let dropped = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(dropped.redelivery_count(), Some(1));
            drop(dropped);
        }
        drop(subscriber);
        // `shutdown` returns once the dropped delivery's session is closed.
        connected.shutdown().await.expect("the broker shuts down");
        assert_no_lock(&db.pool, &["plain_jobs-1"]).await;
        let again = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("a second broker connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&again)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let taken = tokio::time::timeout(AT_ONCE, deliveries.next())
                .await
                .expect("the row is claimable at once")
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(
                taken.redelivery_count(),
                Some(2),
                "the take of the dropped delivery counted its attempt"
            );
            taken.ack().await.expect("the row settles");
        }
        drop(subscriber);
        again.shutdown().await.expect("the broker shuts down");
        assert_eq!(db.count("plain_jobs").await, 0);
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_advisory_subscription_prepares_its_statements() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .route::<SendEmail>("routed")
            .connect()
            .await
            .expect("the broker connects");
        // Whole rows, ids for the service's own fetch, and the role columns a by-name
        // subscription reads: each take the dialect builds prepares.
        let rows = InboxQueue::<SendEmail>::new("emails")
            .subscribe(&connected)
            .await;
        assert!(rows.is_ok(), "{rows:?}");
        let ids = InboxQueue::<Fetched>::new("plain")
            .subscribe(&connected)
            .await;
        assert!(ids.is_ok(), "{ids:?}");
        let by_name = connected.subscribe("routed").await;
        assert!(by_name.is_ok(), "{by_name:?}");
        drop((rows, ids, by_name));
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    // Real concurrency: two brokers, each with its own claim loop, on a multi-threaded runtime.
    // Their candidate selects see the same rows; the lock, then the take that reads the row again
    // while it is still claimable, gives each row to one of them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_brokers_never_share_a_row() {
        let Some(db) = database().await else { return };
        let ids: BTreeSet<i64> = db.plain_ids(40).await;
        let claim = async move |pool: Pool<Db>| -> Vec<i64> {
            let connected = SqlxBroker::new(pool)
                .poll_interval(Duration::from_millis(10))
                .connect()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<Plain>::new("plain")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let mut taken = Vec::new();
            {
                let mut deliveries = pin!(subscriber.stream());
                while let Ok(Some(next)) =
                    tokio::time::timeout(Duration::from_millis(500), deliveries.next()).await
                {
                    let delivery = next.expect("a claim");
                    taken.push(payload_id(&delivery));
                    delivery.ack().await.expect("the acknowledgement");
                }
            }
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            taken
        };
        let (first, second) = tokio::join!(
            tokio::spawn(claim(db.pool.clone())),
            tokio::spawn(claim(db.pool.clone())),
        );
        let (first, second) = (first.expect("joins"), second.expect("joins"));
        let shared: Vec<_> = first.iter().filter(|id| second.contains(id)).collect();
        assert!(shared.is_empty(), "rows delivered twice: {shared:?}");
        let all: BTreeSet<i64> = first.into_iter().chain(second).collect();
        assert_eq!(all, ids, "every row was delivered once");
        let keys: Vec<String> = ids.iter().map(|id| format!("plain_jobs-{id}")).collect();
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        assert_no_lock(&db.pool, &keys).await;
        db.finish().await;
    }
}

/// What Postgres shows of a delivery in work: its session's lock and no open transaction.
#[cfg(feature = "postgres")]
mod on_postgres {
    use super::*;
    use crate::live::postgres::{advisory_locks, database, idle_in_transaction};
    use crate::live::rows::advisory::Plain;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_in_work_holds_no_transaction() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let held = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(
                idle_in_transaction(&db.pool).await,
                0,
                "no transaction stays open while the handler works"
            );
            assert_eq!(
                advisory_locks(&db.pool).await,
                1,
                "the delivery's session holds its row's lock"
            );
            held.ack().await.expect("the row settles");
            assert_eq!(
                advisory_locks(&db.pool).await,
                0,
                "the settlement freed the key"
            );
        }
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }
}

/// What MySQL and MariaDB show of a delivery in work: its session holds the lock its key names, and
/// a lock name longer than the server takes is locked by its hash.
#[cfg(feature = "mysql")]
mod on_mysql {
    use ruststream_sqlx::Inbox;
    use sqlx::{FromRow, MySqlPool};

    use super::*;
    use crate::live::mysql::{lock_held, lock_name};
    use crate::live::rows::advisory::Plain;

    /// The start of every key of `LongKeyed`, 40 characters: the database's name, a dot, this and
    /// an id of the right length make a lock name of 64 characters, or 65.
    const LONG_PREFIX: &str = "a-key-long-enough-to-pass-64-characters-";

    /// A job of `plain_jobs` whose lock name is as long as MySQL takes, or longer.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(
        table = "plain_jobs",
        advisory_lock = "a-key-long-enough-to-pass-64-characters-{id}"
    )]
    struct LongKeyed {
        #[field(id, generated)]
        id: i64,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(payload)]
        payload: Vec<u8>,
    }

    /// The SHA-256 of `key` in hex, as the server computes it.
    async fn sha2_hex(pool: &MySqlPool, key: &str) -> String {
        sqlx::query_scalar("SELECT SHA2(?, 256)")
            .bind(key)
            .fetch_one(pool)
            .await
            .expect("the hash reads")
    }

    /// Whether a session holds a lock named `name` as it is written: `None` where the server takes
    /// no such name, as MySQL takes none longer than 64 characters.
    async fn held_as_written(pool: &MySqlPool, name: &str) -> Option<bool> {
        // MySQL has no booleans: `IS NOT NULL` answers an integer, which sqlx reads as a `bool`.
        let held = sqlx::query_scalar("SELECT IS_USED_LOCK(?) IS NOT NULL")
            .bind(name)
            .fetch_one(pool)
            .await;
        match held {
            Ok(held) => Some(held),
            Err(sqlx::Error::Database(refused)) if refused.code().as_deref() == Some("42000") => {
                None
            }
            Err(failed) => panic!("the lock reads: {failed}"),
        }
    }

    crate::live::mysql_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_delivery_in_work_holds_its_key() {
            let Some(db) = database().await else { return };
            db.plain(&[b"x".as_slice()]).await;
            let connected = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<Plain>::new("plain")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            {
                let mut deliveries = pin!(subscriber.stream());
                let held = deliveries
                    .next()
                    .await
                    .expect("the stream goes on")
                    .expect("the claim takes the row");
                let name = lock_name(&db.pool, "plain_jobs-1").await;
                assert!(
                    lock_held(&db.pool, &name).await,
                    "the delivery's session holds its row's lock"
                );
                held.ack().await.expect("the row settles");
                assert!(!lock_held(&db.pool, &name).await, "the settlement freed the key");
            }
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            db.finish().await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_key_longer_than_64_characters_locks_by_its_hash() {
            let Some(db) = database().await else { return };
            // The ids whose lock names come to 64 characters and to 65.
            let named = lock_name(&db.pool, LONG_PREFIX).await;
            let digits = 64_usize
                .checked_sub(named.chars().count())
                .and_then(|digits| u32::try_from(digits).ok())
                .filter(|&digits| digits > 0)
                .expect("the name leaves room for an id");
            let (short_id, long_id) = (10_i64.pow(digits - 1), 10_i64.pow(digits));
            sqlx::query("INSERT INTO plain_jobs (id, payload) VALUES (?, 'x'), (?, 'y')")
                .bind(short_id)
                .bind(long_id)
                .execute(&db.pool)
                .await
                .expect("the rows write");
            let (exact, longer) = (format!("{named}{short_id}"), format!("{named}{long_id}"));
            assert_eq!((exact.chars().count(), longer.chars().count()), (64, 65));
            let (exact_hash, longer_hash) =
                (sha2_hex(&db.pool, &exact).await, sha2_hex(&db.pool, &longer).await);
            let connected = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<LongKeyed>::new("plain")
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            {
                let mut deliveries = pin!(subscriber.stream());
                let mut held = Vec::new();
                for _ in 0..2 {
                    held.push(
                        deliveries
                            .next()
                            .await
                            .expect("the stream goes on")
                            .expect("the claim takes the row"),
                    );
                }
                // A name of 64 characters is the lock's own; a longer one is locked by its hash.
                assert!(lock_held(&db.pool, &exact).await, "the name of 64 is the lock's own");
                assert!(!lock_held(&db.pool, &exact_hash).await);
                assert!(lock_held(&db.pool, &longer_hash).await, "the longer name is hashed");
                assert_ne!(held_as_written(&db.pool, &longer).await, Some(true));
                for delivery in held {
                    delivery.ack().await.expect("the row settles");
                }
                for name in [&exact, &exact_hash, &longer_hash] {
                    assert!(!lock_held(&db.pool, name).await, "the settlement freed {name}");
                }
                assert_ne!(held_as_written(&db.pool, &longer).await, Some(true));
            }
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            assert_eq!(db.count("plain_jobs").await, 0, "both rows are done");
            db.finish().await;
            let _ = |row: LongKeyed| (row.id, row.attempt, row.payload);
        }
    }
}

/// Two services, each in a database of its own on one stand, whose rows render the same key: each
/// locks its own row, as each database keeps its own locks.
mod per_database {
    use super::*;

    crate::live::advisory_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn two_databases_lock_their_rows_apart() {
            let Some(one) = database().await else { return };
            let Some(other) = database().await else { return };
            one.plain(&[b"one".as_slice()]).await;
            other.plain(&[b"other".as_slice()]).await;
            let first = SqlxBroker::new(one.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let second = SqlxBroker::new(other.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let mut holding = InboxQueue::<Plain>::new("plain")
                .subscribe(&first)
                .await
                .expect("the subscription opens");
            let mut taking = InboxQueue::<Plain>::new("plain")
                .subscribe(&second)
                .await
                .expect("the subscription opens");
            let held = pin!(holding.stream())
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(held.payload(), b"one");
            {
                let taken = tokio::time::timeout(AT_ONCE, pin!(taking.stream()).next())
                    .await
                    .expect("the other database's row is claimable at once")
                    .expect("the stream goes on")
                    .expect("the claim takes the row");
                assert_eq!(taken.payload(), b"other");
                taken.ack().await.expect("the row settles");
            }
            held.ack().await.expect("the row settles");
            drop((holding, taking));
            first.shutdown().await.expect("the broker shuts down");
            second.shutdown().await.expect("the broker shuts down");
            one.finish().await;
            other.finish().await;
        }
    }
}

/// A table that does not match its struct, refused when the subscription starts: every stand
/// refuses a name that matches no column, SQLite too, which quotes it as a name.
mod refused {
    use ruststream::{Broker, ConnectedBroker, SubscriptionSource};
    use ruststream_sqlx::dialect::Advisory;
    use ruststream_sqlx::{Inbox, InboxQueue, InboxRow, SqlxBroker, SqlxBrokerError};
    use sqlx::FromRow;

    /// A job of `email_jobs` whose lock key names `tenant`, a column the table does not have.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "email_jobs", advisory_lock = "email_jobs-{tenant}")]
    struct Tenanted {
        #[field(id, generated)]
        job_id: i64,
        #[field(group)]
        name: String,
        tenant: String,
        #[field(payload)]
        payload: Vec<u8>,
    }

    crate::live::advisory_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_lock_key_of_a_column_the_table_lacks_stops_the_subscription_at_its_claim() {
            let Some(db) = database().await else { return };
            let connected = SqlxBroker::new(db.pool.clone())
                .connect()
                .await
                .expect("the broker connects");
            let refused = InboxQueue::<Tenanted>::new("tenants")
                .subscribe(&connected)
                .await;
            // The candidate claim renders the key, and the startup check prepares it first.
            let claim = DIALECT
                .advisory_claim(&Tenanted::SPEC)
                .expect("the dialect builds the candidate claim");
            assert!(
                matches!(&refused, Err(SqlxBrokerError::Schema { statement, .. })
                    if *statement == claim.sql()),
                "{refused:?}"
            );
            connected.shutdown().await.expect("the broker shuts down");
            db.finish().await;
            let _ = |row: Tenanted| (row.job_id, row.name, row.tenant, row.payload);
        }
    }
}

/// The advisory lock form's events on their own, for a subscription whose statements the test
/// prepares: the lock, the unlock and the take on Postgres, and the take on SQLite in memory,
/// written as SQLite and as MySQL write it.
#[cfg(any(feature = "postgres", all(feature = "sqlite", feature = "mysql")))]
mod events {
    use std::time::Duration;

    use chrono::{DateTime, Utc};
    use ruststream_sqlx::__private::{Claiming, IdAt, Now, Prepared, Queue, Stmt};
    use ruststream_sqlx::Inbox;
    use ruststream_sqlx::dialect::{Statement, TableSpec};
    use sqlx::FromRow;

    /// A job in the advisory lock form: the take counts its attempt, and a finished job is no
    /// longer claimable.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "jobs", advisory_lock = "jobs-{id}")]
    struct Job {
        #[field(id)]
        id: i64,
        #[field(attempt)]
        attempt: i16,
        #[field(processed_at)]
        processed_at: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
    }

    fn interned(statement: &Statement) -> Stmt {
        Stmt {
            sql: Box::leak(statement.sql().into()),
            params: Box::leak(statement.params().into()),
        }
    }

    /// A subscription to the jobs `spec` describes, which prepared `prepared`.
    fn queue(spec: &TableSpec<'static>, prepared: &Prepared) -> &'static Queue {
        Box::leak(Box::new(Queue {
            name: "jobs",
            table: "jobs",
            row: "Job",
            spec: *spec,
            id_at: IdAt::First,
            native_retry_after: false,
            kinds: None,
            prepared: *prepared,
            begin_claim: None,
            counted_attempt: false,
            poll_interval: Duration::from_secs(1),
            lease: None,
            cap: None,
        }))
    }

    /// The claim in progress of `queue`.
    fn claiming(queue: &'static Queue) -> Claiming {
        Claiming {
            queue,
            limit: 1,
            now: Now::default(),
        }
    }

    #[cfg(feature = "postgres")]
    mod on_postgres {
        use ruststream_sqlx::__private::{Claimed, Events, Prepared, Settling};
        use ruststream_sqlx::InboxRow;
        use ruststream_sqlx::dialect::{Advisory, ClaimShape};
        use sqlx::{
            AssertSqlSafe, Column, Error, Executor, PgConnection, Postgres, SqlSafeStr, Statement,
            TypeInfo,
        };

        use super::{Job, claiming, interned, queue};
        use crate::live::postgres::{DIALECT, database};
        use crate::live::rows::advisory::Plain;

        /// The type of the `attempt` column the statement `sql` returns, as Postgres describes it.
        async fn attempt_type(conn: &mut PgConnection, sql: &str) -> Result<String, Error> {
            let statement = conn
                .prepare(AssertSqlSafe(sql.to_owned()).into_sql_str())
                .await?;
            let attempt = statement
                .columns()
                .iter()
                .find(|column| column.name() == "attempt")
                .expect("the take returns the attempt");
            Ok(attempt.type_info().name().to_owned())
        }

        // `plain_jobs` keeps its attempt as `smallint`, which the struct reads as `i16`: the
        // take's read of the attempt as it was before its count keeps that type, for the struct
        // and for a reader by role alike.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_take_reads_the_attempt_in_its_columns_own_type() -> Result<(), Error> {
            let Some(db) = database().await else {
                return Ok(());
            };
            db.plain(&[b"x".as_slice()]).await;
            let id: i64 = sqlx::query_scalar("SELECT id FROM plain_jobs")
                .fetch_one(&db.pool)
                .await?;
            let take = DIALECT
                .take(&Plain::SPEC, ClaimShape::Rows)
                .expect("Postgres takes");
            let cx = claiming(queue(
                &Plain::SPEC,
                &Prepared {
                    take: take.first().map(interned),
                    ..Prepared::default()
                },
            ));
            let mut conn = db.pool.acquire().await?;
            let mut out = Vec::new();
            assert!(<Plain as Events<Postgres>>::take(&mut conn, &cx, &id, &mut out).await?);
            assert!(
                matches!(out.as_slice(), [Claimed::Row(Plain { attempt: 1, .. })]),
                "{out:?}"
            );
            assert_eq!(attempt_type(&mut conn, take[0].sql()).await?, "INT2");
            let roles = DIALECT
                .take(&Plain::SPEC, ClaimShape::Roles)
                .expect("Postgres takes");
            assert_eq!(attempt_type(&mut conn, roles[0].sql()).await?, "INT2");
            drop(conn);
            db.finish().await;
            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_session_takes_a_key_another_one_does_not_and_releases_it() -> Result<(), Error> {
            let Some(db) = database().await else {
                return Ok(());
            };
            let queue = queue(
                &Job::SPEC,
                &Prepared {
                    lock: DIALECT.lock().as_ref().map(interned),
                    unlock: DIALECT.unlock().as_ref().map(interned),
                    ..Prepared::default()
                },
            );
            let cx = claiming(queue);
            let settling = Settling { queue, now: cx.now };
            let mut first = db.pool.acquire().await?;
            let mut second = db.pool.acquire().await?;
            assert!(<Job as Events<Postgres>>::lock(&mut first, &cx, "jobs-1").await?);
            assert!(!<Job as Events<Postgres>>::lock(&mut second, &cx, "jobs-1").await?);
            assert!(<Job as Events<Postgres>>::lock(&mut second, &cx, "jobs-2").await?);
            assert!(<Job as Events<Postgres>>::unlock(&mut first, &settling, "jobs-1").await?);
            // A key the session no longer holds is not released twice.
            assert!(!<Job as Events<Postgres>>::unlock(&mut first, &settling, "jobs-1").await?);
            assert!(<Job as Events<Postgres>>::lock(&mut second, &cx, "jobs-1").await?);
            for key in ["jobs-1", "jobs-2"] {
                assert!(<Job as Events<Postgres>>::unlock(&mut second, &settling, key).await?);
            }
            drop((first, second));
            db.finish().await;
            Ok(())
        }
    }

    #[cfg(all(feature = "sqlite", feature = "mysql"))]
    mod on_sqlite {
        use chrono::{DateTime, Utc};
        use ruststream_sqlx::__private::{Claimed, Events, Prepared, Queue, Settling};
        use ruststream_sqlx::dialect::{self, Advisory, ClaimShape, Statement, TableSpec};
        use ruststream_sqlx::{Fetch, Inbox, InboxRow};
        use sqlx::{Connection, Error, FromRow, Sqlite, SqliteConnection};

        use super::{Job, claiming, interned, queue};

        /// The same jobs, each read by the service's own fetch after its take.
        #[derive(Debug, Inbox, FromRow)]
        #[inbox(table = "jobs", advisory_lock = "jobs-{id}", custom(fetch))]
        struct Fetched {
            #[field(id)]
            id: i64,
            #[field(attempt)]
            attempt: i16,
            #[field(processed_at)]
            processed_at: Option<DateTime<Utc>>,
            #[field(payload)]
            payload: Vec<u8>,
        }

        /// Jobs 1 and 2 waiting at their first attempt, job 2 with no payload, and job 3
        /// finished.
        async fn jobs() -> Result<SqliteConnection, Error> {
            let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
            sqlx::raw_sql(
                "CREATE TABLE jobs (id INTEGER PRIMARY KEY, attempt INTEGER NOT NULL, \
                 processed_at TEXT, payload BLOB NOT NULL); \
                 INSERT INTO jobs VALUES (1, 1, NULL, x'01'), (2, 1, NULL, x''), \
                 (3, 1, '2026-10-06T00:00:00Z', x'03')",
            )
            .execute(&mut conn)
            .await?;
            Ok(conn)
        }

        async fn attempt(conn: &mut SqliteConnection, id: i64) -> Result<i16, Error> {
            sqlx::query_scalar("SELECT attempt FROM jobs WHERE id = ?")
                .bind(id)
                .fetch_one(conn)
                .await
        }

        /// The subscription to `spec` whose take is `take`, in one statement or two.
        fn taking(spec: &TableSpec<'static>, take: &[Statement]) -> &'static Queue {
            queue(
                spec,
                &Prepared {
                    take: take.first().map(interned),
                    take_then: take.get(1).map(interned),
                    ..Prepared::default()
                },
            )
        }

        /// The take of `spec` in `shape`, as SQLite writes it in one statement and as MySQL
        /// writes it in two, which SQLite runs too.
        fn takes(spec: &TableSpec<'static>, shape: ClaimShape) -> [Vec<Statement>; 2] {
            let one = dialect::Sqlite.take(spec, shape).expect("SQLite takes");
            let two = dialect::MySql.take(spec, shape).expect("MySQL takes");
            assert_eq!((one.len(), two.len()), (1, 2));
            [one, two]
        }

        impl Fetch<Sqlite> for Fetched {
            async fn fetch(conn: &mut SqliteConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
                // A job without a payload is one the service's fetch does not find.
                let mut rows = Vec::new();
                for id in ids {
                    rows.extend(
                        sqlx::query_as::<_, Self>(
                            "SELECT id, attempt, processed_at, payload FROM jobs \
                             WHERE id = ? AND length(payload) > 0",
                        )
                        .bind(id)
                        .fetch_optional(&mut *conn)
                        .await?,
                    );
                }
                Ok(rows)
            }
        }

        #[tokio::test]
        async fn a_take_counts_the_attempt_and_reads_the_row_while_it_is_claimable()
        -> Result<(), Error> {
            for take in takes(&Job::SPEC, ClaimShape::Rows) {
                let cx = claiming(taking(&Job::SPEC, &take));
                let mut conn = jobs().await?;
                let mut out = Vec::new();
                assert!(<Job as Events<Sqlite>>::take(&mut conn, &cx, &1, &mut out).await?);
                // The row comes as it was before the count, which the table now holds.
                assert!(
                    matches!(
                        out.as_slice(),
                        [Claimed::Row(Job {
                            id: 1,
                            attempt: 1,
                            ..
                        })]
                    ),
                    "{out:?}"
                );
                assert_eq!(attempt(&mut conn, 1).await?, 2);
                // A finished job is no longer claimable: its take counts and reads nothing.
                assert!(!<Job as Events<Sqlite>>::take(&mut conn, &cx, &3, &mut out).await?);
                assert_eq!(out.len(), 1, "{out:?}");
                assert_eq!(attempt(&mut conn, 3).await?, 1);
            }
            let _ = |job: Job| (job.processed_at, job.payload);
            Ok(())
        }

        #[tokio::test]
        async fn a_row_the_service_fetches_is_read_by_its_fetch_after_the_take() -> Result<(), Error>
        {
            for take in takes(&Fetched::SPEC, ClaimShape::Ids) {
                let cx = claiming(taking(&Fetched::SPEC, &take));
                let mut conn = jobs().await?;
                let mut out = Vec::new();
                for id in [1, 2] {
                    assert!(
                        <Fetched as Events<Sqlite>>::take(&mut conn, &cx, &id, &mut out).await?
                    );
                }
                assert!(!<Fetched as Events<Sqlite>>::take(&mut conn, &cx, &3, &mut out).await?);
                // The fetch reads the counted row; a taken row it does not find is missing.
                assert!(
                    matches!(
                        out.as_slice(),
                        [
                            Claimed::Row(Fetched {
                                id: 1,
                                attempt: 2,
                                ..
                            }),
                            Claimed::Missing(2)
                        ]
                    ),
                    "{out:?}"
                );
                assert_eq!(
                    (attempt(&mut conn, 2).await?, attempt(&mut conn, 3).await?),
                    (2, 1)
                );
            }
            let _ = |row: Fetched| (row.processed_at, row.payload);
            Ok(())
        }

        #[tokio::test]
        async fn a_dialect_that_keeps_no_locks_prepares_none_to_run() -> Result<(), Error> {
            let take = dialect::Sqlite
                .take(&Job::SPEC, ClaimShape::Rows)
                .expect("SQLite takes");
            assert_eq!(dialect::Sqlite.lock(), None);
            let queue = taking(&Job::SPEC, &take);
            let cx = claiming(queue);
            let mut conn = jobs().await?;
            let locked = <Job as Events<Sqlite>>::lock(&mut conn, &cx, "jobs-1").await;
            assert!(
                matches!(&locked, Err(Error::Configuration(message))
                    if message.to_string() == "the subscription prepared no lock statement"),
                "{locked:?}"
            );
            let settling = Settling { queue, now: cx.now };
            let unlocked = <Job as Events<Sqlite>>::unlock(&mut conn, &settling, "jobs-1").await;
            assert!(
                matches!(&unlocked, Err(Error::Configuration(message))
                    if message.to_string() == "the subscription prepared no unlock statement"),
                "{unlocked:?}"
            );
            Ok(())
        }
    }
}
