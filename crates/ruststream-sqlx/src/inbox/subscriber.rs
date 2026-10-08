//! `InboxSubscriber`: the claim loop a subscription's stream runs.

use std::fmt;
use std::future::{Future, poll_fn};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use ruststream::{BatchSubscriber, Subscriber};
use ruststream_sqlx_dialect::Role;
use sqlx::Pool;
use sync_wrapper::SyncWrapper;
use tokio::sync::Notify;
use tokio_util::sync::DropGuard;

use super::batch::{BatchClaim, BatchLane};
use super::broker::Shared;
use super::claims::{Claims, Flow, InWork, Settled};
use super::database::QueueDatabase;
use super::delivery::InboxDelivery;
use super::engine::{self, Claimed, Claiming, Events, Leasing, Now};
use super::error::SqlxBrokerError;
use super::form::advisory::claim::{Advised, claim_advised};
use super::form::advisory::{LockBook, LockHold};
use super::form::lease::LeaseBook;
use super::form::lease::claim::{begin_work, claim_leased};
use super::form::row_lock::claim_locked;
use super::queue::{Queue, Registration};
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
/// The stream claims when it is polled: up to the batch size for a batch handler, and one row for
/// a single-message handler, with a claim in flight for each free worker of a handler mounted with
/// `workers(n)`. The subscription holds at most the pool's size less one connection, and a claim
/// beside another one starts only while the pool has a connection to spare, so the pool keeps one
/// for the handlers. It keeps one claim in flight on SQLite, which takes one writer at a time, and
/// on a table whose groups keep their order or that has a `partition_key` column, whose order claims
/// that run at once could break. After a claim that filled its limit the next one runs at once;
/// after one that found fewer rows it waits the poll interval, or until a publisher of the same
/// broker writes a row of its table and group (a row the service writes through its own SQL or a
/// handler's transaction waits for the interval). A write that lands while the subscription
/// claims ends its next wait at once. A failed claim reaches the stream as an error item and the
/// next claim waits one second. After `shutdown` the stream ends.
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
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// /// Changes to push to a CRM, one group per customer, each group in order.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "crm_jobs")]
/// pub struct SyncChange {
///     #[field(id, generated)]
///     id: i64,
///     #[field(group, fifo = true)]
///     customer: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Change {
///     field: String,
/// }
///
/// // The mount's subscription is an `InboxSubscriber<Postgres, SyncChange>` reading group `acme`.
/// #[subscriber(InboxQueue::<SyncChange>::new("acme"))]
/// async fn push(change: &Change) -> HandlerOutcome {
///     tracing::info!(field = %change.field, "pushing");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("crm-sync", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         // Four workers, and still one change of the group at a time, in order.
///         b.include(push.workers(nonzero!(4)));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct InboxSubscriber<DB: QueueDatabase, Row: Events<DB>, Mode = Plain> {
    pub(super) shared: Arc<Shared<DB>>,
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
    wait: Option<Wait>,
    /// How many claims of single deliveries the subscription keeps in flight.
    flow: Flow,
    /// The deliveries of single claims made so far; less those `settled` counts, the ones in work.
    made: usize,
    /// The deliveries of single claims settled or dropped so far.
    settled: &'static Settled,
    /// What the connection's publishers wake the subscription with after they write a row of its
    /// table and group.
    wake: &'static Notify,
    _registration: Registration<DB>,
    /// Stops the lease keeper when the subscriber drops; `None` outside the lease form.
    _keeper: Option<DropGuard>,
    _mode: PhantomData<fn() -> Mode>,
}

/// What a claim loop waits for before its next claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wait {
    /// The poll interval, or a publish of the connection into the subscription's group.
    Interval,
    /// A second after a failed claim, whatever is published meanwhile: a persistent failure
    /// cannot spin the loop at the rate of the publishes.
    Failed,
}

/// What opening a subscription made for its subscriber.
pub(crate) struct Opened<DB: QueueDatabase, Row: Events<DB>> {
    pub(crate) queue: &'static Queue,
    pub(crate) holding: Holding<DB, Row>,
    pub(crate) registration: Registration<DB>,
    pub(crate) keeper: Option<DropGuard>,
    pub(crate) pool: &'static Pool<DB>,
    pub(crate) lending: Option<&'static TxBook<DB>>,
    pub(crate) wake: &'static Notify,
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
    /// The ids a claim of ids for a fetch of the service's own reads, before the fetch reads their
    /// rows; nothing for a table whose claim reads its rows whole.
    ids: Row::Ids,
    /// What only the advisory lock form keeps, made at its first claim: a subscription in another
    /// form carries the pointer alone.
    advised: Option<Box<Advised<DB, Row>>>,
    /// In the lease form's transactional mode, the transaction of each row's delivery, opened once
    /// the claim committed its leases; empty otherwise. Wrapped because a connection is not
    /// `Sync`, and only the subscriber's own `&mut` reaches it.
    transactions: SyncWrapper<Vec<PoolTx<DB>>>,
}

impl<DB: QueueDatabase, Row: Events<DB>> Default for ClaimBuffers<DB, Row> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            ids: Row::Ids::default(),
            advised: None,
            transactions: SyncWrapper::new(Vec::new()),
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
    Row: Events<DB>,
    Mode: InboxMode,
{
    pub(crate) fn new(shared: Arc<Shared<DB>>, opened: Opened<DB, Row>) -> Self {
        Self {
            shared,
            queue: opened.queue,
            holding: opened.holding,
            pool: opened.pool,
            lending: opened.lending,
            claimed: ClaimBuffers::default(),
            wait: None,
            flow: Flow::new(
                opened.pool.options().get_max_connections(),
                !matches!(opened.holding, Holding::Leases(_))
                    || opened.queue.prepared.transactional,
                one_claim(opened.queue),
            ),
            made: 0,
            // Once per subscription, as its queue and books are: deliveries outlive the borrow of
            // the subscriber.
            settled: Box::leak(Box::default()),
            wake: opened.wake,
            _registration: opened.registration,
            _keeper: opened.keeper,
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
            match self.wait.take() {
                Some(Wait::Interval) => tokio::select! {
                    () = tokio::time::sleep(self.queue.poll_interval) => {}
                    () = self.wake.notified() => {}
                    () = self.shared.stopping.cancelled() => return None,
                },
                Some(Wait::Failed) => tokio::select! {
                    () = tokio::time::sleep(CLAIM_RETRY) => {}
                    () = self.shared.stopping.cancelled() => return None,
                },
                None => {}
            }
            if self.shared.is_closed() {
                return None;
            }
            let claimed = self.claim_once(limit).await;
            match claimed {
                Ok((taken, 0)) => {
                    self.end_empty(taken).await;
                    self.wait = Some(Wait::Interval);
                }
                Ok((taken, count)) => {
                    self.wait = (count < limit).then_some(Wait::Interval);
                    return Some(Ok((taken, count)));
                }
                Err(error) => {
                    self.wait = Some(Wait::Failed);
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
            0,
            Now::default(),
            &mut self.claimed,
        )
        .await
        .map_err(|(statement, source)| self.failed(statement, source))?;
        Ok((taken, self.claimed.rows.len()))
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
            let claimed = claim_rows(&pool, queue, holding, limit, 0, now, &mut buffers).await;
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
    pub(crate) fn take_advised(
        &mut self,
    ) -> impl Iterator<Item = (Claimed<Row>, LockHold<DB>)> + '_ {
        let ClaimBuffers { rows, advised, .. } = &mut self.claimed;
        let holds = advised
            .as_deref_mut()
            .map(|advised| advised.holds.drain(..));
        rows.drain(..).zip(holds.into_iter().flatten())
    }

    pub(crate) const fn queue(&self) -> &'static Queue {
        self.queue
    }

    /// The subscription's handle on the pool, which its deliveries lend their handlers.
    pub(crate) const fn pool(&self) -> &'static Pool<DB> {
        self.pool
    }

    /// The deliveries of single claims in work.
    fn in_work(&self) -> usize {
        self.made.wrapping_sub(self.settled.count())
    }

    /// What a single claim of the subscription runs with, beside `running` claims in flight.
    fn one(&self, running: usize) -> ClaimOne<DB, Row> {
        ClaimOne {
            pool: self.pool,
            queue: self.queue,
            holding: self.holding,
            beside: running,
            #[cfg(feature = "testing")]
            in_process: self
                .shared
                .harness
                .in_process()
                .then(|| Arc::clone(&self.shared)),
        }
    }

    /// The next single delivery: a claim in flight for each free worker, within the connections
    /// the subscription may hold, and the first one that took a row. `None` once the broker is
    /// shut down.
    async fn next_one<Make, Fut>(
        &mut self,
        claims: &mut Claims<ClaimOne<DB, Row>, ClaimBuffers<DB, Row>, Make, Fut>,
    ) -> Option<Result<InboxDelivery<DB, Row, Mode>, SqlxBrokerError>>
    where
        Make: Fn(ClaimOne<DB, Row>, ClaimBuffers<DB, Row>) -> Fut,
        Fut: Future<Output = OneClaimed<DB, Row>>,
    {
        self.flow.polled(self.in_work());
        loop {
            if claims.running() == 0 {
                match self.wait.take() {
                    Some(Wait::Interval) => tokio::select! {
                        () = tokio::time::sleep(self.queue.poll_interval) => {}
                        () = self.wake.notified() => {}
                        () = self.shared.stopping.cancelled() => return None,
                    },
                    Some(Wait::Failed) => tokio::select! {
                        () = tokio::time::sleep(CLAIM_RETRY) => {}
                        () = self.shared.stopping.cancelled() => return None,
                    },
                    None => {}
                }
                if self.shared.is_closed() {
                    return None;
                }
                let in_work = self.in_work();
                if self.flow.waits_for_a_settlement(0, in_work) {
                    let seen = self.settled.count();
                    tokio::select! {
                        () = self.settled.change(seen) => {}
                        () = self.shared.stopping.cancelled() => return None,
                    }
                    continue;
                }
            }
            let in_work = self.in_work();
            let (claimed, mut buffers) = if claims.running() == 0 && self.flow.wanted(in_work) == 1
            {
                // One claim and nothing beside it, as a plain mount always claims: awaited in place,
                // so it costs no slot.
                claim_one(self.one(0), claims.buffers()).await
            } else {
                let Some(done) = poll_fn(|cx| self.turn(claims, cx)).await else {
                    continue;
                };
                done
            };
            match claimed {
                Ok(taken) if buffers.rows.is_empty() => {
                    self.end_empty(taken).await;
                    claims.put_back(buffers);
                    self.flow.found_nothing();
                    if claims.running() == 0 {
                        self.wait = Some(Wait::Interval);
                    }
                }
                Ok(taken) => {
                    self.flow.took();
                    let delivery = self.deliver(taken, &mut buffers);
                    claims.put_back(buffers);
                    return delivery.map(Ok);
                }
                Err((statement, source)) => {
                    claims.put_back(buffers);
                    self.flow.found_nothing();
                    self.wait = Some(Wait::Failed);
                    return Some(Err(self.failed(statement, source)));
                }
            }
        }
    }

    /// Starts the claims the flow allows, then polls every claim in flight. `None` when none runs.
    fn turn<Make, Fut>(
        &self,
        claims: &mut Claims<ClaimOne<DB, Row>, ClaimBuffers<DB, Row>, Make, Fut>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<OneClaimed<DB, Row>>>
    where
        Make: Fn(ClaimOne<DB, Row>, ClaimBuffers<DB, Row>) -> Fut,
        Fut: Future<Output = OneClaimed<DB, Row>>,
    {
        if self.wait.is_none() && !self.shared.is_closed() {
            let in_work = self.in_work();
            while self
                .flow
                .may_start(claims.running(), in_work, || spare(self.pool))
            {
                // Each claim is polled once as it starts, so it takes its connection before the
                // pool is read for the next one.
                let started = claims.start(self.one(claims.running()));
                if let Poll::Ready(done) = claims.poll_slot(started, cx) {
                    return Poll::Ready(Some(done));
                }
            }
        }
        claims.poll_any(cx)
    }

    /// The delivery of the row a single claim took into `buffers`.
    fn deliver(
        &mut self,
        taken: Taken<DB, Row>,
        buffers: &mut ClaimBuffers<DB, Row>,
    ) -> Option<InboxDelivery<DB, Row, Mode>> {
        let (queue, pool) = (self.queue, self.pool);
        let delivery = match taken {
            Taken::Locked(tx) => {
                let claimed = buffers.rows.pop()?;
                match self.lending {
                    // The claim's transaction waits in the book for the handler to borrow it.
                    Some(book) => InboxDelivery::lent(claimed, book.enter(tx), queue, pool),
                    None => InboxDelivery::own(claimed, tx, queue, pool),
                }
            }
            Taken::Leased(book, lease) => {
                let claimed = buffers.rows.pop()?;
                // In transactional mode the delivery's own transaction waits in the book for the
                // handler to borrow it.
                let lent = self
                    .lending
                    .zip(buffers.transactions.get_mut().pop())
                    .map(|(lending, tx)| lending.enter(tx));
                InboxDelivery::leased(claimed, book, lease, lent, queue, pool)
            }
            Taken::Advised => {
                let claimed = buffers.rows.pop()?;
                let hold = buffers.advised.as_deref_mut()?.holds.pop()?;
                InboxDelivery::advised(claimed, hold, queue, pool)
            }
        };
        self.made = self.made.wrapping_add(1);
        let delivery = delivery.counted(InWork::new(self.settled));
        #[cfg(feature = "testing")]
        let delivery = delivery.on(&self.shared);
        Some(delivery)
    }

    /// The subscription's deliveries, as a stream that owns it.
    pub(crate) fn into_stream(
        self,
    ) -> impl Stream<Item = Result<InboxDelivery<DB, Row, Mode>, SqlxBrokerError>> + Send + 'static
    {
        let claims = Claims::new(claim_one::<DB, Row>);
        futures::stream::unfold((self, claims), |(mut subscriber, mut claims)| async move {
            let next = subscriber.next_one(&mut claims).await?;
            Some((next, (subscriber, claims)))
        })
    }
}

/// What a single claim runs with.
pub(crate) struct ClaimOne<DB: QueueDatabase, Row: Events<DB>> {
    pool: &'static Pool<DB>,
    queue: &'static Queue,
    holding: Holding<DB, Row>,
    /// The other claims of the subscription in flight.
    beside: usize,
    /// The connection, where it runs in process.
    #[cfg(feature = "testing")]
    in_process: Option<Arc<Shared<DB>>>,
}

/// What a single claim ends with: its outcome, and the buffers it hands back.
pub(crate) type OneClaimed<DB, Row> = (Result<Taken<DB, Row>, Failed>, ClaimBuffers<DB, Row>);

/// Claims one row with `one`, into `buffers` it owns while it runs, so several run at once.
async fn claim_one<DB, Row>(
    one: ClaimOne<DB, Row>,
    mut buffers: ClaimBuffers<DB, Row>,
) -> OneClaimed<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let ClaimOne {
        pool,
        queue,
        holding,
        beside,
        ..
    } = one;
    // In process the claim runs on the test's clock, off a paused one, and in the harness's books.
    #[cfg(feature = "testing")]
    if let Some(shared) = one.in_process {
        let now = shared.harness.now();
        let (claimed, buffers) = off_clock(async move {
            let claimed = claim_rows(pool, queue, holding, 1, beside, now, &mut buffers).await;
            (claimed, buffers)
        })
        .await
        .unwrap_or_else(|| (Err(("BEGIN", cancelled())), ClaimBuffers::default()));
        if claimed.is_ok() {
            shared.harness.claimed(queue.name, buffers.rows.len());
        }
        return (claimed, buffers);
    }
    let claimed = claim_rows(
        pool,
        queue,
        holding,
        1,
        beside,
        Now::default(),
        &mut buffers,
    )
    .await;
    (claimed, buffers)
}

/// Whether a subscription to `queue` keeps one claim in flight: claims that run at once may
/// finish out of their claim order, which a table whose groups keep their order and a keyed table
/// read their order from; and on a database of one writer they only collide.
fn one_claim(queue: &Queue) -> bool {
    queue.prepared.fifo_guard.is_some()
        || queue.spec.column(Role::PartitionKey).is_some()
        || queue.one_writer
}

/// The connections `pool` can lend at once: its idle ones, and room for new ones.
fn spare<DB: sqlx::Database>(pool: &Pool<DB>) -> usize {
    let room = pool
        .options()
        .get_max_connections()
        .saturating_sub(pool.size());
    pool.num_idle()
        .saturating_add(usize::try_from(room).unwrap_or(usize::MAX))
}

/// A statement that failed, and why.
pub(crate) type Failed = (&'static str, sqlx::Error);

/// Claims up to `limit` rows of `queue` into `claimed`, holding them as `holding` says: in a
/// transaction of `pool` it returns open, by a lease it commits, or by the lock on each row's key,
/// held by a session of its own. An advisory claim reads a candidate more for each of `beside`
/// claims of the subscription in flight.
///
/// The claim of a table whose groups keep their order takes the group first, in the claim's
/// transaction, and takes nothing while another transaction holds it.
async fn claim_rows<DB, Row>(
    pool: &Pool<DB>,
    queue: &'static Queue,
    holding: Holding<DB, Row>,
    limit: usize,
    beside: usize,
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
    claimed.clear();
    let ClaimBuffers {
        rows,
        ids,
        advised,
        transactions,
    } = claimed;
    let book = match holding {
        Holding::Transaction => {
            return claim_locked::<DB, Row>(pool, &cx, ids, rows)
                .await
                .map(Taken::Locked);
        }
        Holding::Leases(book) => book,
        Holding::Advisory(book) => {
            let advised = advised.get_or_insert_with(Box::default);
            claim_advised::<DB, Row>(pool, book, &cx, limit, beside, rows, advised).await?;
            return Ok(Taken::Advised);
        }
    };
    let lease = claim_leased::<DB, Row>(pool, &cx, now, ids, rows).await?;
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

/// Takes the group of `cx` for the claim's transaction on `conn`, where its table keeps its groups
/// in order: `false` when another transaction holds the group. A queue without a guard takes no
/// group and claims at once.
pub(crate) async fn take_group<DB, Row>(
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
pub(crate) async fn begin<DB: QueueDatabase>(
    pool: &Pool<DB>,
    queue: &Queue,
) -> Result<PoolTx<DB>, Failed> {
    PoolTx::begin(pool, queue.begin_claim)
        .await
        .map_err(|source| (queue.begin_claim.unwrap_or("BEGIN"), source))
}

impl<DB, Row, Mode> Subscriber for InboxSubscriber<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Mode: InboxMode,
{
    type Message = InboxDelivery<DB, Row, Mode>;
    type Error = SqlxBrokerError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let claims = Claims::new(claim_one::<DB, Row>);
        futures::stream::unfold((self, claims), |(subscriber, mut claims)| async move {
            let next = subscriber.next_one(&mut claims).await?;
            Some((next, (subscriber, claims)))
        })
    }
}

// Transactional mode serves single deliveries: a batch handler reads no delivery's context, so
// it could take no delivery's transaction. The table's mode picks the batch: a payload-mode
// table's is its deliveries, a row-mode table's lends its rows as one slice.
impl<DB, Row> BatchSubscriber for InboxSubscriber<DB, Row, Plain>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Lane: BatchLane<DB, Row>,
{
    type Batch = <Row::Lane as BatchLane<DB, Row>>::Batch;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        futures::stream::unfold(self, move |subscriber| async move {
            let (taken, count) = match subscriber.claim(size.get()).await? {
                Ok(claimed) => claimed,
                Err(error) => return Some((Err(error), subscriber)),
            };
            let batch =
                <Row::Lane as BatchLane<DB, Row>>::batch(BatchClaim::new(subscriber, taken, count));
            Some((Ok(batch), subscriber))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ruststream_sqlx_dialect::{Column, Form, TableSpec};

    use super::one_claim;
    use crate::inbox::engine::{IdAt, Prepared};
    use crate::inbox::queue::Queue;

    fn queue(spec: &TableSpec<'static>, one_writer: bool) -> Queue {
        Queue {
            name: "jobs",
            table: "jobs",
            row: "app::Job",
            spec: *spec,
            id_at: IdAt::First,
            native_retry_after: false,
            kinds: None,
            prepared: Prepared::default(),
            begin_claim: None,
            counted_attempt: false,
            one_writer,
            poll_interval: Duration::from_secs(1),
            lease: None,
            cap: None,
        }
    }

    const PLAIN: TableSpec<'static> = TableSpec::new("jobs", Column::new("id"), Form::RowLock);

    #[test]
    fn a_plain_table_on_a_server_claims_for_every_free_worker() {
        assert!(!one_claim(&queue(&PLAIN, false)));
    }

    #[test]
    fn a_keyed_table_keeps_its_claim_order() {
        let keyed = PLAIN.partition_key(Column::new("customer"));
        assert!(one_claim(&queue(&keyed, false)));
    }

    #[test]
    fn a_database_of_one_writer_keeps_one_claim() {
        assert!(one_claim(&queue(&PLAIN, true)));
    }
}
