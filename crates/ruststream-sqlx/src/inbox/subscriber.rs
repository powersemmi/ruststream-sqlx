//! `InboxSubscriber`: the claim loop a subscription's stream runs.

use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use ruststream::{BatchSubscriber, Subscriber};
use sqlx::{Database, Pool, SqlStr};
use sqlx_core::transaction::TransactionManager;
use sync_wrapper::SyncWrapper;
use tokio_util::sync::DropGuard;

use super::PayloadRow;
use super::advisory::{LockBook, LockHold};
use super::broker::Shared;
use super::database::QueueDatabase;
use super::delivery::{BatchTx, InboxDelivery};
use super::engine::{
    self, Candidates, Claimed, Claiming, Events, Leasing, Now, Prepared, Settling,
};
use super::error::SqlxBrokerError;
use super::lease::LeaseBook;
use super::queue::{Queue, Registration};
use super::session::{Closing, Session};
#[cfg(feature = "testing")]
use super::testing::{cancelled, off_clock};
use super::transactional::{InboxMode, Plain, TxBook};
use super::tx::PoolTx;

/// How long a subscription waits after a claim failed, so a persistent failure cannot spin the
/// loop.
const CLAIM_RETRY: Duration = Duration::from_secs(1);

/// The subscriber an [`InboxQueue`](crate::InboxQueue) opens: a stream of deliveries claimed from
/// the table.
///
/// The stream claims when it is polled: up to one row for a single-message handler, up to the
/// batch size for a batch handler. After a claim that filled its limit the next one runs at once;
/// after one that found fewer rows it waits the poll interval. A failed claim reaches the stream
/// as an error item and the next claim waits one second. After `shutdown` the stream ends.
///
/// In the row lock form a message in work holds one connection, and a batch one for all its
/// messages. A batch's settlements take effect together, when the last of its deliveries
/// finishes: a settlement whose statement fails rolls the whole batch back, and its rows return.
/// A batch that holds a row whose statement always fails therefore rolls back and returns all of
/// its rows on every attempt, until the service's SQL or schema is fixed.
///
/// In the lease form the claim commits at once and holds no connection: each delivery, in a batch
/// or alone, takes a connection only while it settles, and its settlement takes effect on its own;
/// in transactional mode a delivery holds its transaction's connection until it settles.
/// A task on the runtime the broker connected on extends the lease of every delivery in work each
/// half lease, on one connection, until the subscriber drops or the broker shuts down.
///
/// In the advisory lock form each message in work holds a connection of its own, in a batch too,
/// and its session holds the lock on the row's key: outside transactional mode no transaction
/// stays open while the handler works. A claim selects candidates, locks each key on a session
/// and takes the row while it is still claimable; a batch takes idle connections first and opens
/// new ones while the pool has room, so a batch larger than the pool shrinks instead of waiting. A
/// settlement runs its statement on the session, then releases the lock and returns the connection
/// to the pool. A delivery dropped unsettled closes its connection on the runtime the broker
/// connected on, after releasing the lock, and its row returns at once. SQLite keeps no such
/// locks: the process keeps the keys in work instead, so one process serves a database file.
///
/// In transactional mode, a registration mounted with
/// [`transactional`](crate::InboxSettings::transactional), each delivery lends its handler the
/// transaction it settles in, opened at the table's isolation level or SQLite mode. In the row lock
/// form that is the claim's: the subscription sets a savepoint after each claim that took a row, so
/// a settlement other than acknowledgement can discard what the handler wrote. In the lease form
/// the subscription opens a transaction for each delivery once its claim committed; in the advisory
/// lock form, on the delivery's session once its row is taken. Transactional mode serves single
/// deliveries: the subscriber delivers no batches in it.
///
/// A table whose groups keep their order (`#[field(group, fifo = true)]`) has one row of a group
/// in work at a time. A claim takes the group's head, its first unfinished row in claim order, and
/// takes nothing while another row of the group is in work, a row that entered the group ahead of
/// it included. The claim first takes the group in its transaction with the guard its dialect
/// builds, so subscriptions to one group on separate brokers keep the order too. A batch of such a
/// table holds one row, and a handler mounted with `workers(n)` handles the group's rows one after
/// another.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// # use ruststream_sqlx::Inbox;
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(payload)] payload: Vec<u8> }
/// use futures::StreamExt;
/// use ruststream::{Broker, IncomingMessage, Subscriber, SubscriptionSource};
/// use ruststream_sqlx::{InboxQueue, SqlxBroker};
///
/// // What the runtime does for a mounted handler, written out.
/// pub async fn drain(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
///     let connected = SqlxBroker::new(pool).connect().await?;
///     let mut subscriber = InboxQueue::<Job>::new("jobs").subscribe(&connected).await?;
///     let mut deliveries = std::pin::pin!(subscriber.stream());
///     while let Some(delivery) = deliveries.next().await {
///         delivery?.ack().await?;
///     }
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
pub struct InboxSubscriber<DB: QueueDatabase, Row: Events<DB>, Mode = Plain> {
    shared: Arc<Shared<DB>>,
    queue: &'static Queue,
    holding: Holding<DB, Row>,
    /// The subscription's handle on the pool, which its deliveries lend their handlers.
    pool: &'static Pool<DB>,
    /// Where a transactional delivery's transaction waits while its handler does not hold it, in
    /// the row lock and lease forms; `None` in the plain mode, and in the advisory lock form, whose
    /// book keeps the session that holds the transaction.
    lending: Option<&'static TxBook<DB>>,
    /// What the last claim took, its storage reused from claim to claim.
    claimed: ClaimBuffers<DB, Row>,
    /// What the next claim waits for first.
    wait: Option<Duration>,
    _registration: Registration<DB>,
    /// Stops the lease keeper when the subscriber drops; `None` outside the lease form.
    _keeper: Option<DropGuard>,
    _mode: PhantomData<fn() -> Mode>,
}

/// How a subscription holds the rows it claimed until they settle.
pub(crate) enum Holding<DB: QueueDatabase, Row: Events<DB>> {
    /// In the claim's transaction: the row lock form.
    Transaction,
    /// By the lease the claim wrote and committed, each delivery's kept in the subscription's
    /// book: the lease form.
    Leases(&'static LeaseBook<DB, Row>),
    /// By the lock on each row's key, held by the session of its delivery, which the
    /// subscription's book keeps: the advisory lock form.
    Advisory(&'static LockBook<DB>),
}

impl<DB: QueueDatabase, Row: Events<DB>> Clone for Holding<DB, Row> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<DB: QueueDatabase, Row: Events<DB>> Copy for Holding<DB, Row> {}

/// What a claim leaves the deliveries of its rows.
pub(crate) enum Taken<DB: QueueDatabase, Row: Events<DB>> {
    /// The claim's transaction, which holds the rows until they settle.
    Locked(PoolTx<DB>),
    /// The lease the claim wrote and committed, and the book its deliveries enter.
    Leased(&'static LeaseBook<DB, Row>, Row::Token),
    /// Each row's place in the book, whose session holds the row's lock, next to the row.
    Advised,
}

/// What a claim took, kept by the subscriber from claim to claim so their storage is reused.
pub(crate) struct ClaimBuffers<DB: QueueDatabase, Row: Events<DB>> {
    /// The claim's rows, oldest first.
    rows: Vec<Claimed<Row>>,
    /// What only the advisory lock form keeps, made at its first claim: a subscription in another
    /// form carries the pointer alone.
    advised: Option<Box<Advised<DB, Row>>>,
    /// In the lease form's transactional mode, the transaction of each row's delivery, opened once
    /// the claim committed its leases; empty otherwise. Wrapped because a connection is not
    /// `Sync`, and only the subscriber's own `&mut` reaches it.
    transactions: SyncWrapper<Vec<PoolTx<DB>>>,
}

/// What an advisory claim keeps beside its rows.
struct Advised<DB: QueueDatabase, Row: Events<DB>> {
    /// The place of each row in the subscription's book, beside the row.
    holds: Vec<LockHold<DB>>,
    /// The candidates of the last claim, whose keys' buffers the next one writes over.
    candidates: Candidates<Row::Id>,
    /// The candidates the last claim took, by their place among the candidates.
    taken: Vec<usize>,
}

impl<DB: QueueDatabase, Row: Events<DB>> Default for ClaimBuffers<DB, Row> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            advised: None,
            transactions: SyncWrapper::new(Vec::new()),
        }
    }
}

impl<DB: QueueDatabase, Row: Events<DB>> Default for Advised<DB, Row> {
    fn default() -> Self {
        Self {
            holds: Vec::new(),
            candidates: Candidates::default(),
            taken: Vec::new(),
        }
    }
}

impl<DB: QueueDatabase, Row: Events<DB>> ClaimBuffers<DB, Row> {
    /// Empties them for the next claim. A hold left from a claim that failed midway drops here,
    /// which ends its session, and so does a transaction, which closes its connection.
    fn clear(&mut self) {
        self.rows.clear();
        self.transactions.get_mut().clear();
        if let Some(advised) = self.advised.as_deref_mut() {
            advised.holds.clear();
            advised.taken.clear();
        }
    }
}

impl<DB: QueueDatabase, Row: Events<DB>, Mode> fmt::Debug for InboxSubscriber<DB, Row, Mode> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxSubscriber")
            .field("subscription", &self.queue.name)
            .field("table", &self.queue.table)
            .field("row", &self.queue.row)
            .finish_non_exhaustive()
    }
}

impl<DB, Row, Mode> InboxSubscriber<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
    Mode: InboxMode,
{
    pub(crate) fn new(
        shared: Arc<Shared<DB>>,
        queue: &'static Queue,
        holding: Holding<DB, Row>,
        registration: Registration<DB>,
        keeper: Option<DropGuard>,
        pool: &'static Pool<DB>,
        lending: Option<&'static TxBook<DB>>,
    ) -> Self {
        Self {
            shared,
            queue,
            holding,
            pool,
            lending,
            claimed: ClaimBuffers::default(),
            wait: None,
            _registration: registration,
            _keeper: keeper,
            _mode: PhantomData,
        }
    }

    fn failed(&self, statement: &'static str, source: sqlx::Error) -> SqlxBrokerError {
        SqlxBrokerError::Sqlx {
            subscription: self.queue.name.to_owned(),
            table: self.queue.table.to_owned(),
            row: self.queue.row,
            statement,
            source: Box::new(source),
        }
    }

    /// Waits for what the last claim asked, then claims up to `limit` rows. `None` once the
    /// broker is shut down.
    pub(crate) async fn claim(
        &mut self,
        limit: usize,
    ) -> Option<Result<(Taken<DB, Row>, usize), SqlxBrokerError>> {
        loop {
            if let Some(wait) = self.wait.take() {
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = self.shared.stopping.cancelled() => return None,
                }
            }
            if self.shared.is_closed() {
                return None;
            }
            let claimed = self.claim_once(limit).await;
            match claimed {
                Ok((taken, 0)) => {
                    self.end_empty(taken).await;
                    self.wait = Some(self.queue.poll_interval);
                }
                Ok((taken, count)) => {
                    self.wait = (count < limit).then_some(self.queue.poll_interval);
                    return Some(Ok((taken, count)));
                }
                Err(error) => {
                    self.wait = Some(CLAIM_RETRY);
                    return Some(Err(error));
                }
            }
        }
    }

    async fn claim_once(
        &mut self,
        limit: usize,
    ) -> Result<(Taken<DB, Row>, usize), SqlxBrokerError> {
        #[cfg(feature = "testing")]
        if self.shared.harness.in_process() {
            return self.claim_in_process(limit).await;
        }
        let taken = claim_rows(
            &self.shared.pool,
            self.queue,
            self.holding,
            limit,
            Now::default(),
            &mut self.claimed,
        )
        .await
        .map_err(|(statement, source)| self.failed(statement, source))?;
        Ok((taken, self.claimed.rows.len()))
    }

    /// Ends the transaction of a claim that took no row; a claim by lease committed already.
    async fn end_empty(&self, taken: Taken<DB, Row>) {
        let Taken::Locked(tx) = taken else {
            return;
        };
        // A rollback that fails leaves the transaction to its drop, which closes the connection.
        #[cfg(feature = "testing")]
        if self.shared.harness.in_process() {
            let _ = off_clock(tx.rollback()).await;
            return;
        }
        let _ = tx.rollback().await;
    }

    /// The claim of an in-process connection: on the test's clock, off a paused one, and in the
    /// harness's books.
    #[cfg(feature = "testing")]
    async fn claim_in_process(
        &mut self,
        limit: usize,
    ) -> Result<(Taken<DB, Row>, usize), SqlxBrokerError> {
        let pool = self.shared.pool.clone();
        let queue = self.queue;
        let holding = self.holding;
        let now = self.shared.harness.now();
        let mut buffers = std::mem::take(&mut self.claimed);
        let (claimed, buffers) = off_clock(async move {
            let claimed = claim_rows(&pool, queue, holding, limit, now, &mut buffers).await;
            (claimed, buffers)
        })
        .await
        .unwrap_or_else(|| (Err(("BEGIN", cancelled())), ClaimBuffers::default()));
        self.claimed = buffers;
        let taken = claimed.map_err(|(statement, source)| self.failed(statement, source))?;
        self.shared
            .harness
            .claimed(queue.name, self.claimed.rows.len());
        Ok((taken, self.claimed.rows.len()))
    }

    /// The deliveries of the last claim, oldest first.
    pub(crate) fn take_rows(&mut self) -> impl Iterator<Item = Claimed<Row>> + '_ {
        self.claimed.rows.drain(..)
    }

    /// The deliveries of the last advisory claim, oldest first, each with its place in the book.
    fn take_advised(&mut self) -> impl Iterator<Item = (Claimed<Row>, LockHold<DB>)> + '_ {
        let ClaimBuffers { rows, advised, .. } = &mut self.claimed;
        let holds = advised
            .as_deref_mut()
            .map(|advised| advised.holds.drain(..));
        rows.drain(..).zip(holds.into_iter().flatten())
    }

    pub(crate) const fn queue(&self) -> &'static Queue {
        self.queue
    }

    async fn next_one(&mut self) -> Option<Result<InboxDelivery<DB, Row, Mode>, SqlxBrokerError>> {
        let (taken, _) = match self.claim(1).await? {
            Ok(claimed) => claimed,
            Err(error) => return Some(Err(error)),
        };
        let (queue, pool) = (self.queue, self.pool);
        let delivery = match taken {
            Taken::Locked(tx) => {
                let claimed = self.claimed.rows.pop()?;
                match self.lending {
                    // The claim's transaction waits in the book for the handler to borrow it.
                    Some(book) => InboxDelivery::lent(claimed, book.enter(tx), queue, pool),
                    None => InboxDelivery::own(claimed, tx, queue, pool),
                }
            }
            Taken::Leased(book, lease) => {
                let claimed = self.claimed.rows.pop()?;
                // In transactional mode the delivery's own transaction waits in the book for the
                // handler to borrow it.
                let lent = self
                    .lending
                    .zip(self.claimed.transactions.get_mut().pop())
                    .map(|(lending, tx)| lending.enter(tx));
                InboxDelivery::leased(claimed, book, lease, lent, queue, pool)
            }
            Taken::Advised => {
                let (claimed, hold) = self.take_advised().next()?;
                InboxDelivery::advised(claimed, hold, queue, pool)
            }
        };
        #[cfg(feature = "testing")]
        let delivery = delivery.on(&self.shared);
        Some(Ok(delivery))
    }

    /// The subscription's deliveries, as a stream that owns it.
    pub(crate) fn into_stream(
        self,
    ) -> impl Stream<Item = Result<InboxDelivery<DB, Row, Mode>, SqlxBrokerError>> + Send + 'static
    {
        futures::stream::unfold(self, |mut subscriber| async move {
            let next = subscriber.next_one().await?;
            Some((next, subscriber))
        })
    }
}

/// A statement that failed, and why.
type Failed = (&'static str, sqlx::Error);

/// Claims up to `limit` rows of `queue` into `claimed`, holding them as `holding` says: in a
/// transaction of `pool` it returns open, by a lease it commits, or by the lock on each row's key,
/// held by a session of its own.
///
/// The claim of a table whose groups keep their order takes the group first, in the claim's
/// transaction, and takes nothing while another transaction holds it.
async fn claim_rows<DB, Row>(
    pool: &Pool<DB>,
    queue: &'static Queue,
    holding: Holding<DB, Row>,
    limit: usize,
    now: Now,
    claimed: &mut ClaimBuffers<DB, Row>,
) -> Result<Taken<DB, Row>, Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let cx = Claiming {
        queue,
        limit: i64::try_from(limit).unwrap_or(i64::MAX),
        now,
    };
    let claim_failed = |source| -> Failed {
        let statement = queue.prepared.claim.map_or("claim", |claim| claim.sql);
        (statement, source)
    };
    claimed.clear();
    let ClaimBuffers {
        rows,
        advised,
        transactions,
    } = claimed;
    let book = match holding {
        Holding::Transaction => {
            let mut tx = begin(pool, queue).await?;
            let claimed = async {
                // The transaction keeps the group until the delivery settles. A claim that finds
                // it kept takes nothing, and the claim loop ends its transaction.
                if take_group::<DB, Row>(&mut tx, &cx, None).await? {
                    Row::claim(&mut tx, &cx, None, rows)
                        .await
                        .map_err(claim_failed)?;
                }
                // A handler that writes in the claim's transaction writes after the savepoint, so
                // a settlement can discard what it wrote and keep the claim.
                if let Some(savepoint) = queue.prepared.savepoint
                    && !rows.is_empty()
                {
                    DB::execute_text(&mut tx, savepoint.set)
                        .await
                        .map_err(|source| (savepoint.set, source))?;
                }
                Ok::<_, Failed>(())
            }
            .await;
            return match claimed {
                Ok(()) => Ok(Taken::Locked(tx)),
                Err(failed) => {
                    // A rollback that fails leaves the transaction to its drop, which closes the
                    // connection.
                    let _ = tx.rollback().await;
                    Err(failed)
                }
            };
        }
        Holding::Leases(book) => book,
        Holding::Advisory(book) => {
            let advised = advised.get_or_insert_with(Box::default);
            claim_advised::<DB, Row>(pool, book, &cx, limit, rows, advised).await?;
            return Ok(Taken::Advised);
        }
    };
    let lease = claim_leased::<DB, Row>(pool, &cx, now, rows).await?;
    // In transactional mode each row's delivery writes in a transaction of its own, opened once
    // the claim committed: the lease, not this transaction, keeps other claims off the row. A
    // transaction that fails to open fails the claim, and the rows it took return once their
    // leases run out, as after a crash.
    if queue.prepared.transactional {
        for _ in 0..rows.len() {
            transactions.get_mut().push(begin_work(pool, queue).await?);
        }
    }
    Ok(Taken::Leased(book, lease))
}

/// Claims up to `cx.limit` rows of a lease subscription into `rows`, and returns the lease they
/// hold: committed before it returns, so the rows hold their leases and no transaction does.
async fn claim_leased<DB, Row>(
    pool: &Pool<DB>,
    cx: &Claiming,
    now: Now,
    rows: &mut Vec<Claimed<Row>>,
) -> Result<Row::Token, Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let queue = cx.queue;
    let claim_failed = |source| -> Failed {
        let statement = queue.prepared.claim.map_or("claim", |claim| claim.sql);
        (statement, source)
    };
    // The lease is read once the connection is in hand, so a wait for the pool does not shorten
    // it. The claim reads "now" there once: the rows it finds due, the leases it finds ended and
    // the expiry it writes start from that instant.
    if queue.prepared.stamps || queue.prepared.fifo_guard.is_some() {
        // The claim only selects, and its transaction stamps each row it took; or it takes its
        // group first, and the group stays taken until the transaction commits its lease. Either
        // way the transaction commits before the handlers run, so the rows hold their leases, not
        // the transaction.
        let mut tx = begin(pool, queue).await?;
        let claimed = async {
            let lease = Row::lease(queue, now).map_err(claim_failed)?;
            let taken = take_group::<DB, Row>(&mut tx, cx, Some(&lease)).await?;
            if taken {
                Row::claim(&mut tx, cx, Some(&lease), rows)
                    .await
                    .map_err(claim_failed)?;
                if queue.prepared.stamps {
                    stamp_rows::<DB, Row>(&mut tx, cx, &lease, rows).await?;
                }
            }
            Ok::<_, Failed>((lease.expiry, taken))
        }
        .await;
        return match claimed {
            Ok((lease, true)) => {
                tx.commit().await.map_err(|source| ("COMMIT", source))?;
                Ok(lease)
            }
            // Another transaction holds the group: the claim took nothing, and the rollback lets
            // go of what the guard read.
            Ok((lease, false)) => {
                let _ = tx.rollback().await;
                Ok(lease)
            }
            Err(failed) => {
                let _ = tx.rollback().await;
                Err(failed)
            }
        };
    }
    // The claim writes the lease itself, in one statement that commits on its own.
    let mut conn = pool.acquire().await.map_err(|source| ("acquire", source))?;
    let lease = Row::lease(queue, now).map_err(claim_failed)?;
    Row::claim(&mut conn, cx, Some(&lease), rows)
        .await
        .map_err(claim_failed)?;
    Ok(lease.expiry)
}

/// Claims up to `limit` rows of an advisory subscription: each candidate's key locked on a session
/// of its own, and its row taken while it is still claimable. The rows go into `rows`, and each
/// row's place in `book` beside it into `advised`.
///
/// The first session waits for the pool; each next one is an idle connection, or a new one while
/// the pool is below its size, and the claim ends where the pool is full. A key this claim took
/// already is passed over, and so is a key another session holds. The candidate select reads as
/// many rows past `limit` as there are keys in work it cannot leave out. A session left over holds
/// nothing and goes back to the pool. A claim that fails or is dropped midway leaves no lock: what
/// it took drops, so each session ends, one that may hold a lock closed after an unlock of its key,
/// and each row returns.
async fn claim_advised<DB, Row>(
    pool: &Pool<DB>,
    book: &'static LockBook<DB>,
    cx: &Claiming,
    limit: usize,
    rows: &mut Vec<Claimed<Row>>,
    advised: &mut Advised<DB, Row>,
) -> Result<(), Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let prepared = &cx.queue.prepared;
    let named = |statement: Option<engine::Stmt>, event: &'static str| {
        move |source| -> Failed { (statement.map_or(event, |statement| statement.sql), source) }
    };
    let settling = Settling {
        queue: cx.queue,
        now: cx.now,
    };
    let Advised {
        holds,
        candidates,
        taken,
    } = advised;
    let mut taking = Taking {
        rows,
        holds,
        done: false,
    };
    let mut session = Session::acquire(pool, book.closing())
        .await
        .map_err(|source| ("acquire", source))?;
    select_candidates::<DB, Row>(&mut session, book, cx, candidates)
        .await
        .map_err(named(prepared.claim, "claim"))?;
    let mut spare = Some(session);
    for index in 0..candidates.len() {
        let Some((id, key)) = candidates.get(index) else {
            continue;
        };
        let held_already = taken.iter().any(|&earlier| {
            candidates
                .get(earlier)
                .is_some_and(|(_, other)| other == key)
        });
        if held_already {
            continue;
        }
        if spare.is_none() {
            spare = next_session(pool, book.closing()).await;
        }
        let Some(session) = spare.take() else {
            break;
        };
        // From its lock until the book holds it, the session goes with the candidate's key: a
        // claim dropped or failed midway closes it after an unlock of the key, so the lock is gone
        // once the close ends.
        let mut locked = book.locked(session, key);
        let took = if book.process() {
            locked.take_in_process()
        } else {
            // Marked before the statement leaves: a lock statement dropped midway may have taken
            // the lock, and a session that may hold one closes instead of going back to the pool.
            locked.session().set_locked(true);
            let took = Row::lock(locked.session().conn(), cx, key)
                .await
                .map_err(named(prepared.lock, "lock"))?;
            if !took {
                locked.session().set_locked(false);
            }
            took
        };
        if !took {
            spare = Some(locked.into_session());
            continue;
        }
        let before = taking.rows.len();
        let found = Row::take(locked.session().conn(), cx, id, taking.rows)
            .await
            .map_err(named(prepared.take, "take"))?;
        if !found {
            // Another holder settled the row between the select and the lock: its key goes, and
            // the session tries the next candidate.
            let freed = free_key::<DB, Row>(locked.session(), book, &settling, key).await?;
            let session = locked.into_session();
            if freed {
                spare = Some(session);
            } else {
                // The database did not confirm the release: the session closes, and the claim
                // goes on with another one.
                drop(session);
                spare = None;
            }
            continue;
        }
        let read = taking.rows.len() - before;
        if read != 1 {
            return Err((
                prepared.take.map_or("take", |take| take.sql),
                sqlx::Error::Protocol(format!("the take of one candidate read {read} rows")),
            ));
        }
        if prepared.transactional {
            begin_on(locked.session(), prepared).await?;
        }
        taking.holds.push(book.enter(key, locked.into_session()));
        taken.push(index);
        if taking.holds.len() >= limit {
            break;
        }
    }
    // A session left over holds nothing, and goes back to the pool.
    drop(spare);
    taking.done = true;
    Ok(())
}

/// Selects the candidates of an advisory claim on `session` into `candidates`: up to `cx.limit`
/// rows, and as many more as there are keys in work the select cannot leave out. The select leaves
/// out the keys in work only where it probes the locks that hold them, so past the keys it cannot
/// see, it reads further, and a key in work at the head of the claim order does not hold back the
/// rows behind it.
async fn select_candidates<DB, Row>(
    session: &mut Session<DB>,
    book: &LockBook<DB>,
    cx: &Claiming,
    candidates: &mut Candidates<Row::Id>,
) -> Result<(), sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let unseen = i64::try_from(book.unseen_in_work()).unwrap_or(i64::MAX);
    let reach = Claiming {
        limit: cx.limit.saturating_add(unseen),
        ..*cx
    };
    Row::candidates(session.conn(), &reach, candidates).await
}

/// The session an advisory claim locks its next candidate on: an idle connection of `pool`, or a
/// new one while the pool is below its size. `None` where the pool is full, so a batch larger than
/// the pool shrinks instead of waiting for a delivery in work to settle.
async fn next_session<DB: Database>(
    pool: &Pool<DB>,
    closing: &'static Closing,
) -> Option<Session<DB>> {
    if let Some(session) = Session::try_acquire(pool, closing) {
        return Some(session);
    }
    if pool.size() >= pool.options().get_max_connections() {
        return None;
    }
    // Why a wait remains: the pool has no call that opens a connection only while it has room.
    // Another task may take the last place between the count above and this acquire, which then
    // waits for a connection to come back, at most the pool's acquire timeout; a claim whose
    // acquire fails ends with what it took.
    Session::acquire(pool, closing).await.ok()
}

/// The rows and the holds of an advisory claim in progress. Dropped before the claim is done, by
/// an error or a cancellation, it drops what the claim took.
struct Taking<'a, DB: QueueDatabase, Row: Events<DB>> {
    rows: &'a mut Vec<Claimed<Row>>,
    holds: &'a mut Vec<LockHold<DB>>,
    done: bool,
}

impl<DB: QueueDatabase, Row: Events<DB>> Drop for Taking<'_, DB, Row> {
    fn drop(&mut self) {
        if !self.done {
            self.holds.clear();
            self.rows.clear();
        }
    }
}

/// Frees `key`, which `session` took for a candidate the take found gone: from the process, or
/// with the row's unlock. `false` when the database did not confirm the release, and the session
/// then ends closed.
async fn free_key<DB, Row>(
    session: &mut Session<DB>,
    book: &LockBook<DB>,
    cx: &Settling,
    key: &str,
) -> Result<bool, Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    if book.process() {
        session.free_in_process();
        return Ok(true);
    }
    let unlocked = Row::unlock(session.conn(), cx, key)
        .await
        .map_err(|source| {
            let unlock = cx.queue.prepared.unlock;
            (unlock.map_or("unlock", |unlock| unlock.sql), source)
        })?;
    if unlocked {
        session.set_locked(false);
    } else {
        tracing::warn!(
            target: "ruststream_sqlx",
            subscription = cx.queue.name,
            table = cx.queue.table,
            row = cx.queue.row,
            key,
            "the unlock of a candidate's key found the session not holding it; the session \
             closes",
        );
    }
    Ok(unlocked)
}

/// Takes the group of `cx` for the claim's transaction on `conn`, where its table keeps its groups
/// in order: `false` when another transaction holds the group. A queue without a guard takes no
/// group and claims at once.
async fn take_group<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    lease: Option<&Leasing<Row::Token>>,
) -> Result<bool, Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let Some(guard) = cx.queue.prepared.fifo_guard else {
        return Ok(true);
    };
    engine::take_group::<DB, Row>(conn, cx, guard, lease)
        .await
        .map_err(|source| (guard.sql, source))
}

/// Opens a claim's transaction on a connection of `pool`, with the statement `queue`'s dialect
/// opens it with, or `BEGIN`.
async fn begin<DB: QueueDatabase>(pool: &Pool<DB>, queue: &Queue) -> Result<PoolTx<DB>, Failed> {
    PoolTx::begin(pool, queue.begin_claim)
        .await
        .map_err(|source| (queue.begin_claim.unwrap_or("BEGIN"), source))
}

/// Opens the handler's transaction on `session`, whose lock holds a taken row's key, at the
/// table's isolation level or SQLite mode, or with `BEGIN` where the table names neither:
/// transactional mode in the advisory lock form.
async fn begin_on<DB: QueueDatabase>(
    session: &mut Session<DB>,
    prepared: &Prepared,
) -> Result<(), Failed> {
    // Open before the statement leaves: a begin dropped midway may have started the transaction,
    // and a session that may hold one closes instead of going back to the pool.
    session.set_open(true);
    let begin = prepared.begin_work;
    DB::TransactionManager::begin(session.conn(), begin.map(SqlStr::from_static))
        .await
        .map_err(|source| (begin.unwrap_or("BEGIN"), source))
}

/// Opens a transactional delivery's own transaction on a connection of `pool`, at the table's
/// isolation level or SQLite mode, or with `BEGIN` where the table names neither.
async fn begin_work<DB: QueueDatabase>(
    pool: &Pool<DB>,
    queue: &Queue,
) -> Result<PoolTx<DB>, Failed> {
    let begin = queue.prepared.begin_work;
    PoolTx::begin(pool, begin)
        .await
        .map_err(|source| (begin.unwrap_or("BEGIN"), source))
}

/// Leases each of `rows` with `lease` inside the claim's transaction, and drops from the claim
/// each row another lease holds.
async fn stamp_rows<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    lease: &Leasing<Row::Token>,
    rows: &mut Vec<Claimed<Row>>,
) -> Result<(), Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let mut index = 0;
    while let Some(claimed) = rows.get(index) {
        let stamped = engine::stamp::<DB, Row>(conn, cx, claimed.id::<DB>(), lease)
            .await
            .map_err(|source| {
                let statement = cx.queue.prepared.stamp.map_or("stamp", |stamp| stamp.sql);
                (statement, source)
            })?;
        if stamped {
            index += 1;
        } else {
            rows.remove(index);
        }
    }
    Ok(())
}

impl<DB, Row, Mode> Subscriber for InboxSubscriber<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
    Mode: InboxMode,
{
    type Message = InboxDelivery<DB, Row, Mode>;
    type Error = SqlxBrokerError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        futures::stream::unfold(self, |subscriber| async move {
            let next = subscriber.next_one().await?;
            Some((next, subscriber))
        })
    }
}

// Transactional mode serves single deliveries: a batch handler reads no delivery's context, so
// it could take no delivery's transaction.
impl<DB, Row> BatchSubscriber for InboxSubscriber<DB, Row, Plain>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    type Batch = Vec<InboxDelivery<DB, Row>>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        futures::stream::unfold(self, move |subscriber| async move {
            let (taken, count) = match subscriber.claim(size.get()).await? {
                Ok(claimed) => claimed,
                Err(error) => return Some((Err(error), subscriber)),
            };
            let (queue, pool) = (subscriber.queue(), subscriber.pool);
            #[cfg(feature = "testing")]
            let shared = Arc::clone(&subscriber.shared);
            let on = |delivery: InboxDelivery<DB, Row>| {
                #[cfg(feature = "testing")]
                let delivery = delivery.on(&shared);
                delivery
            };
            let deliveries = match taken {
                Taken::Locked(tx) => {
                    let batch = BatchTx::new(tx, count);
                    subscriber
                        .take_rows()
                        .map(|claimed| {
                            on(InboxDelivery::batched(
                                claimed,
                                Arc::clone(&batch),
                                queue,
                                pool,
                            ))
                        })
                        .collect()
                }
                // Each delivery of a leased batch holds its own lease and settles on its own.
                Taken::Leased(book, lease) => subscriber
                    .take_rows()
                    .map(|claimed| {
                        on(InboxDelivery::leased(
                            claimed, book, lease, None, queue, pool,
                        ))
                    })
                    .collect(),
                // Each delivery of an advisory batch holds its own session and settles on its own.
                Taken::Advised => subscriber
                    .take_advised()
                    .map(|(claimed, hold)| on(InboxDelivery::advised(claimed, hold, queue, pool)))
                    .collect(),
            };
            Some((Ok(deliveries), subscriber))
        })
    }
}
