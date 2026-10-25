//! A tracked publish from a transactional handler on SQLite, in each form SQLite serves: before the
//! handler's first write through the delivery's transaction the record is written; after it, or in
//! a transaction that took the write lock as it began, the publish fails naming the write lock
//! instead of waiting for the handler that waits for it.

use std::env;
use std::error::Error as StdError;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::testing::TestApp;
use ruststream_sqlx::outbox::{Nil, Outbox, Registered};
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{Inbox, Insert, Publish, QueueDatabase};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Error, FromRow, Pool, Sqlite, SqliteConnection};
use tokio::time::timeout;

use crate::records::OrderRecord;
use crate::tracking_on;

/// How long a test lets its delivery take before it reports the hang: past the stand pool's
/// acquire timeout, which bounds the record's wait where nothing tells the lock up front.
const HANG: Duration = Duration::from_secs(12);

/// Shorter than the stand pool's acquire timeout: a publish refused this fast did not wait.
const AT_ONCE: Duration = Duration::from_secs(1);

/// How long a connection of a database in a file waits for a lock before SQLite refuses its
/// statement: well short of the pool's acquire timeout.
const BUSY: Duration = Duration::from_millis(300);

/// How often a subscription polls its table.
const POLL: Duration = Duration::from_millis(20);

/// A job the service takes from its inbox.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Job {
    n: i64,
}

/// An order: published under `orders`, a name the outbox tracks.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    n: i64,
}

/// The plain queue by lease, its transactions opened with the write lock taken.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "plain_jobs", mode = immediate)]
struct LeasedImmediately {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl<DB> Publish<DB> for LeasedImmediately
where
    DB: QueueDatabase,
    Self: Insert<DB::Connection>,
{
    async fn publish(
        conn: &mut DB::Connection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), Error> {
        let job = Self {
            id: 0,
            attempt: 1,
            locked_until: None,
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

/// The plain queue by advisory lock, its transactions opened with the write lock taken.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "plain_jobs", advisory_lock = "plain_jobs-{id}", mode = immediate)]
struct AdvisedImmediately {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(payload)]
    payload: Vec<u8>,
}

impl<DB> Publish<DB> for AdvisedImmediately
where
    DB: QueueDatabase,
    Self: Insert<DB::Connection>,
{
    async fn publish(
        conn: &mut DB::Connection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), Error> {
        let job = Self {
            id: 0,
            attempt: 1,
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

type Tracking = Outbox<Sqlite, Registered<OrderRecord, Nil>>;

/// What a test's handler does.
#[derive(Debug, Clone, Copy)]
enum Handler {
    /// Publishes, then writes through `Tx`.
    PublishesFirst,
    /// Writes through `Tx`, then publishes.
    WritesFirst,
    /// Publishes in a transaction that took the write lock as it began.
    PublishesInImmediate,
}

/// `error` and every error under it, one per line.
fn chain(error: &(dyn StdError + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(next) = source {
        text.push('\n');
        text.push_str(&next.to_string());
        source = next.source();
    }
    text
}

/// Writes an audit row of `job` noting `note` through `conn`.
async fn note(conn: &mut SqliteConnection, job: &Job, note: &str) {
    sqlx::query("INSERT INTO audit (job_id, note) VALUES (?, ?)")
        .bind(job.n)
        .bind(note)
        .execute(conn)
        .await
        .expect("the audit row writes");
}

/// The audit rows' notes, in the order they were written.
async fn notes(pool: &Pool<Sqlite>) -> Vec<String> {
    sqlx::query_scalar("SELECT note FROM audit ORDER BY rowid")
        .fetch_all(pool)
        .await
        .expect("the audit reads")
}

/// How many records `outbox` holds.
async fn records(pool: &Pool<Sqlite>) -> usize {
    let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM outbox")
        .fetch_all(pool)
        .await
        .expect("the outbox reads");
    ids.len()
}

/// A database of the test's own in a file, with the stand's schema applied: SQLite refuses a
/// statement that waited [`BUSY`] for a lock, where the stand's database in memory waits on.
struct FileDatabase {
    pool: Pool<Sqlite>,
    path: PathBuf,
}

impl FileDatabase {
    async fn open() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = env::temp_dir().join(format!(
            "rs_sqlx_outbox_{}_{}.db",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .busy_timeout(BUSY);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options)
            .await
            .expect("the database file opens");
        sqlx::raw_sql(include_str!("../schema/sqlite.sql"))
            .execute(&pool)
            .await
            .expect("the test schema applies");
        Self { pool, path }
    }

    /// Closes the pool and removes the file, with the journals beside it.
    async fn finish(self) {
        self.pool.close().await;
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut file = self.path.clone().into_os_string();
            file.push(suffix);
            let _ = fs::remove_file(file);
        }
    }
}

macro_rules! sqlite_forms {
    ($($items:item)*) => {
        mod sqlite_lease {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::rows::lease::*;
            #[allow(unused_imports)]
            use crate::live::sqlite::database;
            type Immediate = LeasedImmediately;
            $($items)*
        }

        mod sqlite_advisory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::rows::advisory::*;
            #[allow(unused_imports)]
            use crate::live::sqlite::database;
            type Immediate = AdvisedImmediately;
            $($items)*
        }
    };
}

sqlite_forms! {
    /// The service: the inbox on `pool` with `handler`, `orders` routed to `email_jobs`, its
    /// publishes tracked in the same database.
    fn app(pool: &Pool<Sqlite>, handler: Handler) -> impl App<State = ()> {
        let tracking: Tracking = Outbox::new(pool.clone()).register::<OrderRecord>("orders");
        let broker = SqlxBroker::new(pool.clone())
            .poll_interval(POLL)
            .route::<Plain>("jobs")
            .route::<Immediate>("immediate")
            .route::<SendEmail>("orders");
        RustStream::new(AppInfo::new("shop", "0.0.0"))
            .publish_layer(tracking.publish_layer())
            .with_broker(broker, move |b| match handler {
                Handler::PublishesFirst => {
                    b.include(publishes_first.transactional()).out(DefaultSlot, Routed).build();
                }
                Handler::WritesFirst => {
                    b.include(writes_first.transactional()).out(DefaultSlot, Routed).build();
                }
                Handler::PublishesInImmediate => {
                    b.include(publishes_in_immediate.transactional())
                        .out(DefaultSlot, Routed)
                        .build();
                }
            })
    }

    /// Delivers `job` to `queue` and returns the audit rows once the delivery settled.
    async fn delivered(pool: &Pool<Sqlite>, handler: Handler, queue: &str, job: Job) -> Vec<String> {
        let tb = TestApp::start_live(app(pool, handler)).await.expect("the app starts");
        let delivered = tb.broker::<SqlxBroker<Sqlite>>().message(&job).to(queue).publish();
        timeout(HANG, delivered)
            .await
            .expect("the delivery settles instead of hanging")
            .expect("the publish settles");
        tb.shutdown().await.expect("the app stops");
        notes(pool).await
    }

    #[subscriber(InboxQueue::<Plain>::new("jobs"))]
    async fn publishes_first(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
        Out(orders): Out<impl Publisher>,
    ) -> HandlerOutcome {
        let published = orders.message(&Order { n: job.n }).publish().await;
        note(&mut tx, job, "written").await;
        if let Err(error) = published {
            note(&mut tx, job, &chain(&error)).await;
        }
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<Plain>::new("jobs"))]
    async fn writes_first(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
        Out(orders): Out<impl Publisher>,
    ) -> HandlerOutcome {
        note(&mut tx, job, "written").await;
        match orders.message(&Order { n: job.n }).publish().await {
            Ok(()) => note(&mut tx, job, "published").await,
            Err(error) => note(&mut tx, job, &chain(&error)).await,
        }
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<Immediate>::new("immediate"))]
    async fn publishes_in_immediate(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
        Out(orders): Out<impl Publisher>,
    ) -> HandlerOutcome {
        let started = Instant::now();
        let published = orders.message(&Order { n: job.n }).publish().await;
        let waited = started.elapsed();
        match published {
            Ok(()) => note(&mut tx, job, "published").await,
            Err(error) if waited < AT_ONCE => note(&mut tx, job, &chain(&error)).await,
            Err(_) => note(&mut tx, job, &format!("refused after {waited:?}")).await,
        }
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tracked_publish_before_the_first_write_is_recorded() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let notes = delivered(&db.pool, Handler::PublishesFirst, "jobs", Job { n: 1 }).await;
        assert_eq!(notes, ["written"], "the handler published, then wrote");
        assert_eq!(records(&db.pool).await, 1, "the publish was recorded");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tracked_publish_after_the_first_write_fails_naming_the_write_lock() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let notes = delivered(&db.pool, Handler::WritesFirst, "jobs", Job { n: 2 }).await;
        assert_eq!(notes.len(), 2, "the handler noted its write and the publish's error: {notes:?}");
        assert_eq!(notes[0], "written");
        assert!(
            notes[1].contains("holds SQLite's write lock"),
            "the error names the write lock the handler's transaction holds: {}",
            notes[1]
        );
        assert_eq!(records(&db.pool).await, 0, "nothing was recorded");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tracked_publish_in_an_immediate_transaction_fails_at_once() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let notes =
            delivered(&db.pool, Handler::PublishesInImmediate, "immediate", Job { n: 3 }).await;
        assert_eq!(notes.len(), 1, "the handler noted the publish's error: {notes:?}");
        assert!(
            notes[0].contains("holds SQLite's write lock"),
            "the publish failed at once, naming the write lock: {}",
            notes[0]
        );
        assert_eq!(records(&db.pool).await, 0, "nothing was recorded");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_a_file_a_tracked_publish_after_the_first_write_fails_naming_the_write_lock() {
        if !tracking_on() {
            return;
        }
        let db = FileDatabase::open().await;
        let notes = delivered(&db.pool, Handler::WritesFirst, "jobs", Job { n: 4 }).await;
        assert_eq!(notes.len(), 2, "the handler noted its write and the publish's error: {notes:?}");
        assert!(
            notes[1].contains("holds SQLite's write lock"),
            "SQLite's refusal names the write lock the handler's transaction holds: {}",
            notes[1]
        );
        assert_eq!(records(&db.pool).await, 0, "nothing was recorded");
        db.finish().await;
    }
}
