//! The broker's place in the test harness: the in-process transition, the view the harness
//! drives, and what an in-process connection does differently.
//!
//! The inbox has no model of a database to run in process: its in-process mode reaches the
//! database the test's pool names, as `connect` does. Every database call of an in-process
//! connection runs through [`off_clock`], so a test on a paused tokio clock does not see its
//! timers fire while a reply is on its way, and the system clock follows the tokio clock.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::mem;
use std::panic::resume_unwind;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, SystemTime};

use ruststream::testing::{Backlog, Coordinator, InProcess, TestableBroker};
use ruststream::{HeaderMap, OutgoingMessage, RawMessage};
use ruststream_sqlx_dialect::{Dialect, Role};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use tokio::time::Instant;

use super::broker::{ConnectedSqlxBroker, Shared, SqlxBroker};
use super::database::QueueDatabase;
use super::engine::{Claiming, Event, Events, Now, Values, arguments, intern};
use super::error::SqlxBrokerError;
use super::publish::{TableWake, insert_routed};
use super::queue::Queue;

/// What an in-process connection reads in place of [`SystemClock`](crate::SystemClock): the tokio
/// clock, anchored to the wall clock when the broker connected, so a test that moves a paused
/// clock moves the queue's time with it. A clock of the service's own and the database's clock
/// are read as they are live.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TestClock {
    system: SystemTime,
    instant: Instant,
}

impl TestClock {
    fn start() -> Self {
        Self {
            system: SystemTime::now(),
            instant: Instant::now(),
        }
    }

    pub(crate) fn now(self) -> SystemTime {
        self.system + self.instant.elapsed()
    }
}

/// Runs `work` where a paused tokio clock stands still while it waits on the database.
///
/// Tokio does not auto-advance a paused clock while a blocking task runs, and the database's reply
/// arrives in real time. `None` when the runtime shut down before the work finished.
pub(crate) async fn off_clock<T, Work>(work: Work) -> Option<T>
where
    T: Send + 'static,
    Work: Future<Output = T> + Send + 'static,
{
    let runtime = Handle::current();
    match spawn_blocking(move || runtime.block_on(work)).await {
        Ok(output) => Some(output),
        Err(join) if join.is_panic() => resume_unwind(join.into_panic()),
        Err(_) => None,
    }
}

/// The error of database work the runtime dropped before it finished.
pub(crate) fn cancelled() -> sqlx::Error {
    sqlx::Error::Io(io::Error::other(
        "the runtime shut down during a database call",
    ))
}

/// A queue the harness counts rows for: a table, and the group of a table that keeps groups.
///
/// A row counts for the subscription that reads where it landed, whatever name it was written
/// under: a table without groups is one queue, and its subscription and its routes name it
/// freely.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueueAt<'a> {
    /// The table, qualified with its schema.
    table: &'a str,
    /// The group; empty for a table without groups.
    group: &'a str,
}

impl<'a> QueueAt<'a> {
    /// Where a write of `name` into the table of `wake` lands.
    pub(crate) fn written(wake: &'a TableWake, name: &'a str) -> Self {
        Self {
            table: wake.table(),
            group: wake.group(name),
        }
    }

    /// The queue the subscription `queue` reads.
    pub(crate) fn read(queue: &'a Queue) -> Self {
        Self {
            table: queue.table,
            group: queue.spec.column(Role::Group).map_or("", |_| queue.name),
        }
    }

    /// Where a spent row of `queue` moves to `destination`: that group of its table, or, from a
    /// table without groups, that table.
    pub(crate) fn dead_letter(queue: &'a Queue, destination: &'a str) -> Self {
        match queue.spec.column(Role::Group) {
            Some(_) => Self {
                table: queue.table,
                group: destination,
            },
            None => Self {
                table: destination,
                group: "",
            },
        }
    }

    /// The queue a subscription registers for: `group` is `None` in a table without groups.
    pub(crate) fn registered(table: &'a str, group: Option<&'a str>) -> Self {
        Self {
            table,
            group: group.unwrap_or_default(),
        }
    }
}

/// The books of one open subscription.
#[derive(Debug, Default)]
struct Tally {
    /// The messages counted ahead of a claim.
    ahead: usize,
    /// The rows delayed retries hold back, not counted yet. Each is counted once, by its timer or
    /// by the claim that takes it first: on a paused clock the subscription's poll and the timer
    /// come due at the same instant, in either order.
    returning: Vec<Return>,
}

impl Tally {
    /// Whether the row `ticket` booked was still held back; it is not any more.
    fn take_return(&mut self, ticket: Ticket) -> bool {
        let Some(at) = self.returning.iter().position(|back| back.ticket == ticket) else {
            return false;
        };
        self.returning.swap_remove(at);
        true
    }

    /// The held-back row due first leaves the books: a claim took it.
    fn take_earliest_return(&mut self) {
        if let Some(at) = (0..self.returning.len()).min_by_key(|&at| self.returning[at].due) {
            self.returning.swap_remove(at);
        }
    }
}

/// A row a delayed retry holds back until `due`.
#[derive(Debug, Clone, Copy)]
struct Return {
    due: Instant,
    ticket: Ticket,
}

/// Which held-back row a redelivery timer counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ticket(u64);

/// The open subscriptions of a connection by table, then by group, each with its books.
type Credits = HashMap<Box<str>, HashMap<Box<str>, Tally>>;

/// The harness's books of one connection: the coordinator a test installed, and every message the
/// broker's own publishers and the test's injections wrote.
#[derive(Debug, Default)]
pub(crate) struct Harness {
    coordinator: OnceLock<Coordinator>,
    log: Mutex<Vec<RawMessage>>,
    /// The clock of an in-process connection; unset on a connection to the server.
    clock: OnceLock<TestClock>,
    /// The connection's open subscriptions by queue, with the messages counted ahead of a claim
    /// and the rows delayed retries hold back.
    credits: Mutex<Credits>,
    /// The last ticket a held-back row took.
    tickets: AtomicU64,
    /// The last injection's write, which the next one waits for.
    writes: Mutex<Option<oneshot::Receiver<()>>>,
}

impl Harness {
    /// Whether the connection runs in process.
    pub(crate) fn in_process(&self) -> bool {
        self.clock.get().is_some()
    }

    /// Where "now" comes from for the connection's statements.
    pub(crate) fn now(&self) -> Now {
        Now::test(self.clock.get().copied())
    }

    /// The coordinator, on an in-process connection: only that one keeps the books.
    fn books(&self) -> Option<&Coordinator> {
        self.clock.get().and_then(|_| self.coordinator.get())
    }

    fn credits(&self) -> MutexGuard<'_, Credits> {
        self.credits.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The books of the open subscription that reads `queue`, if one does.
    fn credit<'books>(
        credits: &'books mut Credits,
        queue: QueueAt<'_>,
    ) -> Option<&'books mut Tally> {
        credits.get_mut(queue.table)?.get_mut(queue.group)
    }

    /// A subscription to `queue` opened on the connection, which found `waiting` rows it can claim
    /// already there: each is counted now, so the harness waits for its delivery.
    pub(crate) fn opened(&self, queue: QueueAt<'_>, waiting: usize) {
        self.credits()
            .entry(queue.table.into())
            .or_default()
            .entry(queue.group.into())
            .or_default()
            .ahead += waiting;
        if let Some(coordinator) = self.books() {
            for _ in 0..waiting {
                coordinator.enqueued();
            }
        }
    }

    /// The subscription to `queue` closed.
    pub(crate) fn closed(&self, queue: QueueAt<'_>) {
        let mut credits = self.credits();
        if let Some(groups) = credits.get_mut(queue.table) {
            groups.remove(queue.group);
            if groups.is_empty() {
                credits.remove(queue.table);
            }
        }
    }

    /// A message reached `queue`: counted now when a subscription of the connection reads it, so
    /// the harness waits for its delivery.
    pub(crate) fn expect(&self, queue: QueueAt<'_>) {
        let Some(coordinator) = self.books() else {
            return;
        };
        if let Some(tally) = Self::credit(&mut self.credits(), queue) {
            tally.ahead += 1;
            coordinator.enqueued();
        }
    }

    /// A message counted for `queue` never reached the table.
    pub(crate) fn refused(&self, queue: QueueAt<'_>) {
        let Some(coordinator) = self.books() else {
            return;
        };
        if let Some(tally) = Self::credit(&mut self.credits(), queue)
            && tally.ahead > 0
        {
            tally.ahead -= 1;
            coordinator.consumed();
        }
    }

    /// A delayed retry is about to hold a row of `queue` back for `delay`: the row is booked as
    /// returning, uncounted until its timer fires or a claim takes it. `None` when no subscription
    /// of the connection reads `queue`.
    pub(crate) fn holds_back(&self, queue: QueueAt<'_>, delay: Duration) -> Option<Ticket> {
        self.books()?;
        let ticket = Ticket(self.tickets.fetch_add(1, Ordering::Relaxed));
        let due = Instant::now() + delay;
        Self::credit(&mut self.credits(), queue)?
            .returning
            .push(Return { due, ticket });
        Some(ticket)
    }

    /// The row `ticket` booked stays where it was: the statement that would hold it back failed.
    pub(crate) fn withdraw(&self, queue: QueueAt<'_>, ticket: Ticket) {
        if let Some(tally) = Self::credit(&mut self.credits(), queue) {
            tally.take_return(ticket);
        }
    }

    /// The timer of the row `ticket` booked fired: the row is counted now, unless a claim took it
    /// first.
    fn comes_due(&self, queue: QueueAt<'_>, ticket: Ticket) {
        let Some(coordinator) = self.books() else {
            return;
        };
        if let Some(tally) = Self::credit(&mut self.credits(), queue)
            && tally.take_return(ticket)
        {
            tally.ahead += 1;
            coordinator.enqueued();
        }
    }

    /// A claim of `queue` took `count` rows: each was counted ahead, or is counted now. A row
    /// counted now may be one a delayed retry held back, taken before its timer fired: the
    /// earliest one due leaves the books, so its timer counts nothing.
    pub(crate) fn claimed(&self, queue: QueueAt<'_>, count: usize) {
        let Some(coordinator) = self.books() else {
            return;
        };
        let covered = Self::credit(&mut self.credits(), queue).map_or(0, |tally| {
            let covered = tally.ahead.min(count);
            tally.ahead -= covered;
            for _ in covered..count {
                tally.take_earliest_return();
            }
            covered
        });
        for _ in covered..count {
            coordinator.enqueued();
        }
    }

    /// A delivery settled.
    pub(crate) fn released(&self) {
        if let Some(coordinator) = self.books() {
            coordinator.consumed();
        }
    }

    /// A delivery of `queue` dropped unsettled: its row returns, and the delivery leaves the books.
    pub(crate) fn returned(&self, queue: QueueAt<'_>) {
        self.expect(queue);
        self.released();
    }

    /// A message one of the broker's own publishers wrote; it was counted before its insert, so
    /// a claim that lands right after the insert finds it counted.
    pub(crate) fn published(&self, message: &OutgoingMessage<'_>) {
        self.record(message);
    }

    fn record(&self, message: &OutgoingMessage<'_>) {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(
                RawMessage::new(message.name().to_owned(), message.payload().to_vec())
                    .with_headers(message.headers().clone()),
            );
    }
}

/// The dead-letter moves the harness counted for the deliveries of one row lock batch, where each
/// row would move. The batch's transaction decides whether they took place: a commit keeps them,
/// and a rollback leaves every row where it was, so the harness counts none of them.
pub(crate) struct BatchMoves<DB: QueueDatabase> {
    /// The in-process connection whose books counted the moves; unset outside the harness.
    connection: OnceLock<Arc<Shared<DB>>>,
    counted: Mutex<Vec<QueueAt<'static>>>,
}

impl<DB: QueueDatabase> Default for BatchMoves<DB> {
    fn default() -> Self {
        Self {
            connection: OnceLock::new(),
            counted: Mutex::default(),
        }
    }
}

impl<DB: QueueDatabase> BatchMoves<DB> {
    /// Keeps the moves for the books of `connection`, where it keeps them.
    pub(crate) fn watch(&self, connection: &Arc<Shared<DB>>) {
        if connection.harness.books().is_some() {
            let _ = self.connection.set(Arc::clone(connection));
        }
    }

    fn counted(&self) -> MutexGuard<'_, Vec<QueueAt<'static>>> {
        self.counted.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A delivery of the batch counted its row at `at`, where it moves; the move stands once the
    /// batch commits.
    pub(crate) fn counted_at(&self, at: QueueAt<'static>) {
        if self.connection.get().is_some() {
            self.counted().push(at);
        }
    }

    /// The batch's transaction ended: committed, so every move took place, or not.
    pub(crate) fn ended(&self, committed: bool) {
        if committed {
            self.counted().clear();
        } else {
            self.undone();
        }
    }

    /// The batch's transaction rolled back: no row moved, and none is counted where it would have.
    pub(crate) fn undone(&self) {
        let Some(connection) = self.connection.get() else {
            return;
        };
        let undone = mem::take(&mut *self.counted());
        for at in undone {
            connection.harness.refused(at);
        }
    }
}

/// The row of an in-process delivery of `queue`, booked under `ticket`, comes back after `delay`:
/// it is counted when the delay runs out, as `TestApp::advance` fires the timer, unless a claim
/// took it first.
pub(crate) fn returns_after<DB: QueueDatabase>(
    connection: &Arc<Shared<DB>>,
    queue: &'static Queue,
    delay: Duration,
    ticket: Ticket,
) {
    let Some(coordinator) = connection.harness.books() else {
        return;
    };
    let connection = Arc::clone(connection);
    coordinator.schedule_redelivery(delay, move || {
        connection.harness.comes_due(QueueAt::read(queue), ticket);
    });
}

/// Opens the harness's books for the subscription `queue`, which reads `group` (`None` in a table
/// without groups), with the rows it can claim already counted.
///
/// The rows are counted before the books open: a message published meanwhile is counted once, by
/// this count or by the claim that takes it, never by both.
///
/// # Errors
///
/// As [`claimable`].
pub(crate) async fn open_books<DB, Row>(
    connection: &Arc<Shared<DB>>,
    dialect: &dyn Dialect,
    queue: &'static Queue,
    group: Option<&str>,
) -> Result<(), SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let waiting = claimable::<DB, Row>(connection, dialect, queue).await?;
    connection
        .harness
        .opened(QueueAt::registered(queue.table, group), waiting);
    Ok(())
}

/// The rows the subscription `queue` of an in-process connection can claim as it opens: rows the
/// service wrote with its own SQL before the test, which the harness waits for as for published
/// messages. Counted with the dialect's [`count_claimable`](Dialect::count_claimable), bound as the
/// claim binds; none on a connection that keeps no books, for a claim of the service's own, or
/// where the dialect counts none.
///
/// # Errors
///
/// The dialect's refusal, or the database's error.
async fn claimable<DB, Row>(
    connection: &Arc<Shared<DB>>,
    dialect: &dyn Dialect,
    queue: &'static Queue,
) -> Result<usize, SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    // A claim of the service's own reads the rows its way, which a count cannot follow.
    if connection.harness.books().is_none() || Row::SHAPE.custom_claim {
        return Ok(0);
    }
    let counting =
        dialect
            .count_claimable(&queue.spec)
            .map_err(|source| SqlxBrokerError::Dialect {
                subscription: queue.name.to_owned(),
                table: queue.table.to_owned(),
                row: queue.row,
                source,
            })?;
    let Some(counting) = counting else {
        return Ok(0);
    };
    let statement = intern(&counting);
    let (pool, now) = (connection.pool.clone(), connection.harness.now());
    let counted = off_clock(async move {
        // Bound as the claim binds: a lease table's rows are free by the lease's own "now".
        let lease = queue.lease.map(|_| Row::lease(queue, now)).transpose()?;
        let cx = Claiming {
            queue,
            limit: 0,
            now,
        };
        let values = Values::claiming(cx, Event::Claim, lease.as_ref());
        let arguments = arguments::<DB, Row>(statement, &values)?;
        let mut conn = pool.acquire().await?;
        DB::fetch_count(&mut conn, statement.sql, arguments).await
    })
    .await
    .unwrap_or_else(|| Err(cancelled()))
    .map_err(|source| SqlxBrokerError::Sqlx {
        subscription: queue.name.to_owned(),
        table: queue.table.to_owned(),
        row: queue.row,
        statement: statement.sql,
        source: Box::new(source),
    })?;
    Ok(usize::try_from(counted).unwrap_or_default())
}

impl<DB: QueueDatabase, D: Dialect + 'static> InProcess for SqlxBroker<DB, D> {
    /// Connects to the database the test's pool reaches, as [`connect`](ruststream::Broker::connect)
    /// does, with every database call of the connection kept off a paused clock.
    async fn connect_in_process(self) -> Result<ConnectedSqlxBroker<DB, D>, SqlxBrokerError> {
        let (pool, choice) = (self.pool.clone(), self.dialect.clone());
        let dialect = off_clock(async move {
            let conn = pool
                .acquire()
                .await
                .map_err(|source| SqlxBrokerError::Connect { source })?;
            choice.resolve(&conn)
        })
        .await
        .unwrap_or_else(|| {
            Err(SqlxBrokerError::Connect {
                source: cancelled(),
            })
        })?;
        let listening = match self.listen {
            Some(start) => {
                let pool = self.pool.clone();
                Some(
                    off_clock(start(pool))
                        .await
                        .unwrap_or_else(|| Err(cancelled()))
                        .map_err(|source| SqlxBrokerError::Connect { source })?,
                )
            }
            None => None,
        };
        let connected = ConnectedSqlxBroker::new(self, dialect, Handle::current(), listening);
        let _ = connected.shared.harness.clock.set(TestClock::start());
        connected.shared.closing.run_in_process();
        Ok(connected)
    }
}

impl<DB: QueueDatabase, D: Dialect + 'static> TestableBroker for ConnectedSqlxBroker<DB, D> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.shared.harness.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let harness = &self.shared.harness;
        harness.record(&message);
        // A name no route leads to a table is refused below, and counts nowhere.
        let wake = self
            .shared
            .routes
            .find_with_wake(message.name())
            .map(|(_, wake)| wake);
        if let Some(wake) = wake {
            harness.expect(QueueAt::written(wake, message.name()));
        }
        let (name, payload, headers) = message.into_parts();
        let (name, payload) = (name.to_owned(), payload.to_vec());
        // Each injection waits for the one before it, so they reach the table in the order the
        // test made them.
        let (written, next) = oneshot::channel();
        let previous = harness
            .writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(next);
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            inject(&shared, &name, &payload, headers).await;
            let _ = written.send(());
        });
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.shared
            .harness
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|message| message.name() == name)
            .cloned()
            .collect()
    }

    fn backlog(&self) -> Backlog {
        // A queue table keeps what is written before a subscription opens.
        Backlog::Delivered
    }
}

/// Writes an injected message into the table its route leads to, as an external producer would.
async fn inject<DB: QueueDatabase>(
    shared: &Shared<DB>,
    name: &str,
    payload: &[u8],
    headers: HeaderMap,
) {
    let message = OutgoingMessage::new(name, payload).with_headers(headers);
    let written = match shared.routes.find_with_wake(name) {
        Some((route, wake)) => insert_routed(shared, route, wake, &message).await,
        None => Err(SqlxBrokerError::NoRoute {
            name: name.to_owned(),
        }),
    };
    if let Err(error) = written {
        if let Some((_, wake)) = shared.routes.find_with_wake(name) {
            shared.harness.refused(QueueAt::written(wake, name));
        }
        tracing::warn!(target: "ruststream_sqlx", %error, name, "an injected message was refused");
    }
}

#[cfg(feature = "postgres")]
ruststream::register_testable_broker!(SqlxBroker<sqlx::Postgres>);
#[cfg(feature = "mysql")]
ruststream::register_testable_broker!(SqlxBroker<sqlx::MySql>);
#[cfg(feature = "sqlite")]
ruststream::register_testable_broker!(SqlxBroker<sqlx::Sqlite>);
#[cfg(feature = "any")]
ruststream::register_testable_broker!(SqlxBroker<sqlx::Any>);
