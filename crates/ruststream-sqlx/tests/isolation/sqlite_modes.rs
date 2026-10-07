//! SQLite's modes, on a database in a file: a table in `immediate` mode opens its delivery's
//! transaction with the database's write lock taken, so a second writer that asks for the lock
//! meets it held while the handler runs; without a mode the transaction takes the lock at its
//! first write.

#![cfg(feature = "sqlite")]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::testing::TestApp;
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{Inbox, Insert, Publish, QueueDatabase, Tx};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Connection, Error, FromRow, Sqlite, SqliteConnection, SqlitePool};

use super::{JOB, Job, POLL, audit};

/// How long the second writer waits for the write lock: longer than any claim holds it, far
/// shorter than the test waits for its delivery.
const SECOND_WRITER_WAITS: Duration = Duration::from_millis(500);

/// The plain queue by lease, its transactions opened with the write lock taken.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "plain_jobs", mode = immediate)]
pub(crate) struct LeasedImmediately {
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
pub(crate) struct AdvisedImmediately {
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

/// A database of the test's own in a file, with the stand's schema applied.
struct FileDatabase {
    pool: SqlitePool,
    path: PathBuf,
}

impl FileDatabase {
    async fn open() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "rs_sqlx_modes_{}_{}.db",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .expect("the database file opens");
        sqlx::raw_sql(include_str!("../schema/sqlite.sql"))
            .execute(&pool)
            .await
            .expect("the test schema applies");
        Self { pool, path }
    }

    async fn notes(&self) -> Vec<String> {
        sqlx::query_scalar("SELECT note FROM audit ORDER BY note")
            .fetch_all(&self.pool)
            .await
            .expect("the audit reads")
    }

    /// Closes the pool and removes the file, with the journals beside it.
    async fn finish(self) {
        self.pool.close().await;
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut file = self.path.clone().into_os_string();
            file.push(suffix);
            let _ = std::fs::remove_file(file);
        }
    }
}

/// What a second connection meets when it asks for the database's write lock: `free` when it
/// took the lock, which it gives back at once, or the database's refusal.
async fn second_writer(pool: &SqlitePool) -> String {
    let options = (*pool.connect_options())
        .clone()
        .busy_timeout(SECOND_WRITER_WAITS);
    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .expect("a second connection opens");
    let met = match sqlx::raw_sql("BEGIN IMMEDIATE").execute(&mut conn).await {
        Ok(_) => {
            sqlx::raw_sql("ROLLBACK")
                .execute(&mut conn)
                .await
                .expect("the second writer gives the lock back");
            "free".to_owned()
        }
        Err(error) => error
            .as_database_error()
            .map_or_else(|| error.to_string(), |refused| refused.message().to_owned()),
    };
    conn.close().await.expect("the second connection closes");
    met
}

/// Notes through the delivery's transaction what a second writer met, and acknowledges.
async fn note_the_second_writer(
    job: &Job,
    tx: &mut Tx<Sqlite>,
    pool: &SqlitePool,
) -> HandlerOutcome {
    let met = second_writer(pool).await;
    sqlx::raw_sql(audit(job, &met))
        .execute(&mut **tx)
        .await
        .expect("the note writes");
    HandlerOutcome::ack()
}

/// Runs `app` on its own, publishes the job to it, and returns the notes its handler wrote.
async fn noted(db: &FileDatabase, app: RustStream) -> Vec<String> {
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Sqlite>>()
        .message(&JOB)
        .to("plain")
        .publish()
        .await
        .expect("the publish settles");
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("plain")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("the app stops");
    db.notes().await
}

mod lease {
    use super::*;
    use crate::live::rows::lease::Plain;

    #[subscriber(InboxQueue::<LeasedImmediately>::new("plain"))]
    async fn immediate(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
        Ctx(pool): Ctx<keys::Pool<Sqlite>>,
    ) -> HandlerOutcome {
        note_the_second_writer(job, &mut tx, &pool).await
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn deferred(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
        Ctx(pool): Ctx<keys::Pool<Sqlite>>,
    ) -> HandlerOutcome {
        note_the_second_writer(job, &mut tx, &pool).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_immediate_mode_takes_the_write_lock_at_begin() {
        let db = FileDatabase::open().await;
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<LeasedImmediately>("plain");
        let app = RustStream::new(AppInfo::new("modes", "0.0.0")).with_broker(broker, |b| {
            b.include(immediate.transactional());
        });
        assert_eq!(
            noted(&db, app).await,
            ["database is locked"],
            "the delivery's transaction held the write lock from its start"
        );
        db.finish().await;

        let db = FileDatabase::open().await;
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<Plain>("plain");
        let app = RustStream::new(AppInfo::new("modes", "0.0.0")).with_broker(broker, |b| {
            b.include(deferred.transactional());
        });
        assert_eq!(
            noted(&db, app).await,
            ["free"],
            "without a mode, the transaction takes the lock at its first write"
        );
        db.finish().await;
    }
}

mod advisory {
    use super::*;
    use crate::live::rows::advisory::Plain;

    #[subscriber(InboxQueue::<AdvisedImmediately>::new("plain"))]
    async fn immediate(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
        Ctx(pool): Ctx<keys::Pool<Sqlite>>,
    ) -> HandlerOutcome {
        note_the_second_writer(job, &mut tx, &pool).await
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn deferred(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
        Ctx(pool): Ctx<keys::Pool<Sqlite>>,
    ) -> HandlerOutcome {
        note_the_second_writer(job, &mut tx, &pool).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_immediate_mode_takes_the_write_lock_at_begin() {
        let db = FileDatabase::open().await;
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<AdvisedImmediately>("plain");
        let app = RustStream::new(AppInfo::new("modes", "0.0.0")).with_broker(broker, |b| {
            b.include(immediate.transactional());
        });
        assert_eq!(
            noted(&db, app).await,
            ["database is locked"],
            "the delivery's transaction held the write lock from its start"
        );
        db.finish().await;

        let db = FileDatabase::open().await;
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<Plain>("plain");
        let app = RustStream::new(AppInfo::new("modes", "0.0.0")).with_broker(broker, |b| {
            b.include(deferred.transactional());
        });
        assert_eq!(
            noted(&db, app).await,
            ["free"],
            "without a mode, the transaction takes the lock at its first write"
        );
        db.finish().await;
    }
}
