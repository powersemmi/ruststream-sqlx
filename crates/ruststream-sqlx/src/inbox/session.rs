//! The sessions of the advisory lock form: the pool connection a delivery holds, which may hold an
//! advisory lock and an open transaction, and the closes of sessions that end while they hold
//! either.

use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use sqlx::pool::PoolConnection;
use sqlx::{Connection, Database, Error, Pool};
use tokio::runtime::Handle;
use tokio::sync::Notify;

use super::advisory::{ProcessKey, ProcessLocks};
use super::engine::Settling;
#[cfg(feature = "testing")]
use super::testing::off_clock;

/// How long a session's close, or the release `shutdown` runs on it, may take. Past it the
/// connection drops, which closes its socket, and the server ends the session all the same.
pub(crate) const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// A row's unlock of a key, erased from the row's type: what a session about to close runs to
/// release the lock it holds.
pub(crate) type Unlock<DB> = for<'a> fn(
    &'a mut <DB as Database>::Connection,
    Settling,
    &'a str,
) -> BoxFuture<'a, Result<bool, Error>>;

/// The release of the lock a session holds, run before the session closes: the row's unlock, the
/// settlement it runs in, and the key.
pub(crate) struct Unlocking<DB: Database> {
    pub(crate) unlock: Unlock<DB>,
    pub(crate) cx: Settling,
    pub(crate) key: String,
}

/// A pool connection a delivery holds, which may hold an advisory lock and an open transaction.
/// Released clean it goes back to the pool; dropped while it holds either, it is detached from the
/// pool and closed on the broker's runtime, so the server ends the session and frees the lock.
///
/// A key the process keeps for the session, where the dialect keeps its locks in the process, is
/// freed when the session ends, and the connection goes back to the pool.
pub(crate) struct Session<DB: Database> {
    /// The connection, until the session ends.
    conn: Option<PoolConnection<DB>>,
    /// Whether the session may hold an advisory lock in the database: set before a lock statement
    /// leaves, cleared once an unlock confirmed the release.
    locked: bool,
    /// Whether the session may hold an open transaction: set before the transaction begins,
    /// cleared once it ended.
    open: bool,
    /// The key the process keeps for the session, where the dialect leaves its locks to the
    /// process.
    process: Option<ProcessKey>,
    closing: &'static Closing,
    /// In process, the session among those `shutdown` waits for until the book holds it: a claim
    /// there runs on a thread of its own, which goes on after the stream that started it is gone.
    #[cfg(feature = "testing")]
    claiming: Option<Counted>,
}

impl<DB: Database> Session<DB> {
    /// A session on a connection of `pool`, waiting for one when none is idle.
    ///
    /// # Errors
    ///
    /// The pool's error.
    pub(crate) async fn acquire(pool: &Pool<DB>, closing: &'static Closing) -> Result<Self, Error> {
        Ok(Self::on(pool.acquire().await?, closing))
    }

    /// A session on an idle connection of `pool`; `None` when the pool spares none at once.
    pub(crate) fn try_acquire(pool: &Pool<DB>, closing: &'static Closing) -> Option<Self> {
        pool.try_acquire().map(|conn| Self::on(conn, closing))
    }

    fn on(conn: PoolConnection<DB>, closing: &'static Closing) -> Self {
        Self {
            conn: Some(conn),
            locked: false,
            open: false,
            process: None,
            closing,
            #[cfg(feature = "testing")]
            claiming: closing.in_process().then(|| Counted::new(closing, false)),
        }
    }

    /// Whether the session may hold an advisory lock in the database.
    pub(crate) const fn locked(&self) -> bool {
        self.locked
    }

    /// Records that the book holds the session, which `shutdown` releases from there.
    #[cfg(feature = "testing")]
    pub(crate) fn entered(&mut self) {
        self.claiming = None;
    }

    /// The connection, to run a statement on.
    ///
    /// # Panics
    ///
    /// Never: the connection leaves the session only when the session ends.
    pub(crate) fn conn(&mut self) -> &mut DB::Connection {
        self.conn
            .as_mut()
            .expect("a session holds its connection until it ends")
    }

    /// Records whether the session may hold an advisory lock in the database.
    pub(crate) const fn set_locked(&mut self, locked: bool) {
        self.locked = locked;
    }

    /// Records whether the session may hold an open transaction.
    pub(crate) const fn set_open(&mut self, open: bool) {
        self.open = open;
    }

    /// Takes `key` of the database `database` names in the process's registry for the session:
    /// `false` while a key of its hash is in work.
    pub(crate) fn take_in_process(&mut self, database: u64, key: &str) -> bool {
        self.process = ProcessLocks::try_take(database, key);
        self.process.is_some()
    }

    /// Frees the key the process keeps for the session, if it keeps one.
    pub(crate) fn free_in_process(&mut self) {
        self.process = None;
    }

    /// Ends the session: its connection goes back to the pool when it holds nothing, and closes
    /// when it holds a lock or a transaction.
    pub(crate) fn release(mut self) {
        self.end(None);
    }

    /// Ends the session as [`release`](Self::release) does; where it closes holding a lock in the
    /// database, the close first releases the lock with `unlocking`, so the lock is gone once the
    /// close ends, and the close still ends the session where the release fails.
    pub(crate) fn release_unlocking(mut self, unlocking: Unlocking<DB>) {
        self.end(Some(unlocking));
    }

    fn end(&mut self, unlocking: Option<Unlocking<DB>>) {
        // A key the process keeps is the process's alone: its connection holds nothing of it.
        self.process = None;
        let Some(conn) = self.conn.take() else {
            return;
        };
        // A pool connection that leaves its pool, back or detached, spawns the pool's upkeep, which
        // needs a runtime, and a session may end where none runs.
        let _entered = self.closing.runtime.enter();
        if self.locked || self.open {
            self.closing
                .close(conn, self.locked, unlocking.filter(|_| self.locked));
        } else {
            drop(conn);
        }
    }
}

impl<DB: Database> Drop for Session<DB> {
    fn drop(&mut self) {
        self.end(None);
    }
}

/// The sessions of one broker being closed: `shutdown` waits until none is, and counts the closes
/// of sessions that held a lock it waited for.
pub(crate) struct Closing {
    /// The closes started and not yet ended; in process, also the sessions of claims the book
    /// does not hold yet.
    in_flight: AtomicUsize,
    /// Wakes `settled` when the last close in flight ends.
    done: Notify,
    /// The runtime the broker connected on: the closes run there.
    runtime: Handle,
    /// Set once `shutdown` began: a close of a session that held a lock and ends from then on is
    /// one `shutdown` waited for.
    shutting: AtomicBool,
    /// The closes of sessions that held a lock, among those `shutdown` waited for.
    forced: AtomicUsize,
    /// Whether the broker's connection runs in process: a close then runs off a paused clock.
    #[cfg(feature = "testing")]
    in_process: AtomicBool,
}

impl Closing {
    /// The closes of a broker connected on `runtime`, for the life of the process: a session
    /// reaches them through a `'static` reference, with no reference count per message.
    pub(crate) fn leak(runtime: Handle) -> &'static Self {
        Box::leak(Box::new(Self {
            in_flight: AtomicUsize::new(0),
            done: Notify::new(),
            runtime,
            shutting: AtomicBool::new(false),
            forced: AtomicUsize::new(0),
            #[cfg(feature = "testing")]
            in_process: AtomicBool::new(false),
        }))
    }

    /// Marks the start of `shutdown`: from here on, a session that held a lock and ends its close
    /// counts as closed by force.
    pub(crate) fn begin_shutdown(&self) {
        self.shutting.store(true, Ordering::SeqCst);
    }

    /// The sessions that held a lock and closed while `shutdown` ran: read once every close has
    /// ended, it counts each one `shutdown` waited for.
    pub(crate) fn forced(&self) -> usize {
        self.forced.load(Ordering::Acquire)
    }

    /// Whether the broker's connection runs in process, where its database calls run off a
    /// paused clock.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(&self) -> bool {
        self.in_process.load(Ordering::Acquire)
    }

    /// Returns once no close is in flight.
    pub(crate) async fn settled(&self) {
        loop {
            let mut notified = pin!(self.done.notified());
            // Enabled before the count is read, so a close that ends in between still wakes it.
            notified.as_mut().enable();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Marks the broker's connection as running in process.
    #[cfg(feature = "testing")]
    pub(crate) fn run_in_process(&self) {
        self.in_process.store(true, Ordering::Release);
    }

    /// Detaches `conn` from its pool, so it never serves another delivery, and closes it on the
    /// broker's runtime, after `unlocking` where one is given. One timeout bounds the unlock and
    /// the close together; past it the connection drops, which closes its socket, and the server
    /// ends the session and its lock all the same. Counted until it ends, as the close of a
    /// session that held a lock where `locked` says so; with that runtime gone the task drops
    /// unrun, and the connection drops with it.
    fn close<DB: Database>(
        &'static self,
        conn: PoolConnection<DB>,
        locked: bool,
        unlocking: Option<Unlocking<DB>>,
    ) {
        let mut raw = conn.detach();
        let counted = Counted::new(self, locked);
        let work = async move {
            // Inside the work, so the count drops when the close ends, and when the work is
            // dropped midway or never runs.
            let _counted = counted;
            // Why a timeout: a session that ended while a statement was in flight may never
            // answer another one (sqlx-mysql reads a queued statement's reply as the cut one's).
            let closed = async move {
                if let Some(Unlocking { unlock, cx, key }) = unlocking {
                    // A release that fails or finds no lock leaves the close to end the session.
                    let _ = unlock(&mut raw, cx, &key).await;
                }
                raw.close().await
            };
            let _ = tokio::time::timeout(CLOSE_TIMEOUT, closed).await;
        };
        // In process the close runs off a paused clock: its timeout runs out on the test's clock,
        // which stands still while the database answers.
        #[cfg(feature = "testing")]
        if self.in_process.load(Ordering::Acquire) {
            drop(self.runtime.spawn(async move {
                let _ = off_clock(work).await;
            }));
            return;
        }
        drop(self.runtime.spawn(work));
    }
}

/// One close in flight, counted while it lives; `locked` where its session held a lock.
struct Counted {
    closing: &'static Closing,
    locked: bool,
}

impl Counted {
    fn new(closing: &'static Closing, locked: bool) -> Self {
        closing.in_flight.fetch_add(1, Ordering::AcqRel);
        Self { closing, locked }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        let closing = self.closing;
        // Counted before the close leaves the count, so `shutdown` reads it once nothing is in
        // flight.
        if self.locked && closing.shutting.load(Ordering::SeqCst) {
            closing.forced.fetch_add(1, Ordering::AcqRel);
        }
        if closing.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            closing.done.notify_waiters();
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::future::pending;
    use std::pin::pin;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use futures::future::BoxFuture;
    use futures::poll;
    use ruststream_sqlx_dialect::{Column, Form, KeyPart, TableSpec};
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Error, SqliteConnection};
    use tokio::runtime::{Builder, Handle};
    use tokio::time::Instant;

    use super::{CLOSE_TIMEOUT, Closing, Counted, Session, Unlocking};
    use crate::inbox::engine::{IdAt, Now, Prepared, Settling};
    use crate::inbox::queue::Queue;

    const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("id")];

    /// A subscription to the jobs of an advisory table, for the settlement an unlock runs in.
    fn settling() -> Settling {
        let queue: &'static Queue = Box::leak(Box::new(Queue {
            name: "jobs",
            table: "jobs",
            row: "Job",
            spec: TableSpec::new("jobs", Column::new("id"), Form::Advisory(KEY))
                .payload(Column::new("payload")),
            id_at: IdAt::First,
            native_retry_after: false,
            kinds: None,
            prepared: Prepared::default(),
            begin_claim: None,
            counted_attempt: false,
            poll_interval: Duration::from_secs(1),
            lease: None,
            cap: None,
        }));
        Settling {
            queue,
            now: Now::default(),
        }
    }

    /// An unlock that never answers, as a connection whose statement was cut midway may not.
    fn unanswered<'a>(
        _: &'a mut SqliteConnection,
        _: Settling,
        _: &'a str,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        Box::pin(pending())
    }

    #[tokio::test]
    async fn settled_waits_for_every_close_in_flight() {
        let closing = Closing::leak(Handle::current());
        let first = Counted::new(closing, false);
        let second = Counted::new(closing, false);
        let mut settled = pin!(closing.settled());
        assert!(poll!(settled.as_mut()).is_pending());
        drop(first);
        assert!(
            poll!(settled.as_mut()).is_pending(),
            "a close is still in flight"
        );
        drop(second);
        assert!(poll!(settled.as_mut()).is_ready());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_session_that_may_hold_a_lock_leaves_its_pool_and_closes() -> Result<(), Error> {
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect("sqlite::memory:")
            .await?;
        let closing = Closing::leak(Handle::current());
        let mut session = Session::acquire(&pool, closing).await?;
        assert_eq!(pool.size(), 1);
        session.set_locked(true);
        session.release();
        assert_eq!(pool.size(), 0, "the connection left its pool at once");
        closing.settled().await;
        assert_eq!(closing.in_flight.load(Ordering::Acquire), 0);
        pool.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn an_unlock_that_never_answers_ends_with_the_close_at_its_timeout() -> Result<(), Error>
    {
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect("sqlite::memory:")
            .await?;
        let closing = Closing::leak(Handle::current());
        let mut session = Session::acquire(&pool, closing).await?;
        session.set_locked(true);
        // From here the clock moves only when nothing else runs: to the close's timeout.
        tokio::time::pause();
        let started = Instant::now();
        session.release_unlocking(Unlocking {
            unlock: unanswered,
            cx: settling(),
            key: "jobs-1".to_owned(),
        });
        closing.settled().await;
        // The timer's tick rounds the deadline up to the next millisecond.
        let ended = started.elapsed();
        assert!(
            ended >= CLOSE_TIMEOUT && ended < CLOSE_TIMEOUT + Duration::from_millis(10),
            "one timeout ended the unlock and the close together: {ended:?}"
        );
        assert_eq!(closing.in_flight.load(Ordering::Acquire), 0);
        tokio::time::resume();
        pool.close().await;
        Ok(())
    }

    #[test]
    fn a_close_whose_runtime_is_gone_drops_with_its_connection() -> Result<(), Error> {
        // A runtime that ran and is gone: its handle spawns nothing that ever runs.
        let gone = Builder::new_current_thread()
            .build()
            .map_err(Error::Io)?
            .handle()
            .clone();
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(Error::Io)?;
        runtime.block_on(async move {
            let pool = SqlitePoolOptions::new()
                .max_connections(2)
                .connect("sqlite::memory:")
                .await?;
            let closing = Closing::leak(gone);
            let mut session = Session::acquire(&pool, closing).await?;
            session.set_open(true);
            session.release();
            assert_eq!(pool.size(), 0, "the connection never went back to its pool");
            assert_eq!(
                closing.in_flight.load(Ordering::Acquire),
                0,
                "the close dropped unrun and left the count"
            );
            pool.close().await;
            Ok(())
        })
    }
}
