//! `InboxSubscriber`: the claim loop a subscription's stream runs.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use ruststream::{BatchSubscriber, Subscriber};
use sqlx::Pool;
use tokio_util::sync::DropGuard;

use super::PayloadRow;
use super::broker::Shared;
use super::database::QueueDatabase;
use super::delivery::{BatchTx, InboxDelivery};
use super::engine::{self, Claimed, Claiming, Events, Leasing, Now};
use super::error::SqlxBrokerError;
use super::lease::LeaseBook;
use super::queue::{Queue, Registration};
#[cfg(feature = "testing")]
use super::testing::{cancelled, off_clock};
use super::tx::Tx;

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
/// or alone, takes a connection only while it settles, and its settlement takes effect on its own.
/// A task on the runtime the broker connected on extends the lease of every delivery in work each
/// half lease, on one connection, until the subscriber drops or the broker shuts down.
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
pub struct InboxSubscriber<DB: QueueDatabase, Row: Events<DB>> {
    shared: Arc<Shared<DB>>,
    queue: &'static Queue,
    holding: Holding<DB, Row>,
    /// The claim's rows, reused from claim to claim.
    rows: Vec<Claimed<Row>>,
    /// What the next claim waits for first.
    wait: Option<Duration>,
    _registration: Registration<DB>,
    /// Stops the lease keeper when the subscriber drops; `None` outside the lease form.
    _keeper: Option<DropGuard>,
}

/// How a subscription holds the rows it claimed until they settle.
pub(crate) enum Holding<DB: QueueDatabase, Row: Events<DB>> {
    /// In the claim's transaction: the row lock form.
    Locks,
    /// By the lease the claim wrote and committed, each delivery's kept in the subscription's
    /// book: the lease form.
    Leases(&'static LeaseBook<DB, Row>),
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
    Locked(Tx<DB>),
    /// The lease the claim wrote and committed, and the book its deliveries enter.
    Leased(&'static LeaseBook<DB, Row>, Row::Token),
}

impl<DB: QueueDatabase, Row: Events<DB>> fmt::Debug for InboxSubscriber<DB, Row> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxSubscriber")
            .field("subscription", &self.queue.name)
            .field("table", &self.queue.table)
            .field("row", &self.queue.row)
            .finish_non_exhaustive()
    }
}

impl<DB: QueueDatabase, Row: Events<DB> + PayloadRow> InboxSubscriber<DB, Row> {
    pub(crate) const fn new(
        shared: Arc<Shared<DB>>,
        queue: &'static Queue,
        holding: Holding<DB, Row>,
        registration: Registration<DB>,
        keeper: Option<DropGuard>,
    ) -> Self {
        Self {
            shared,
            queue,
            holding,
            rows: Vec::new(),
            wait: None,
            _registration: registration,
            _keeper: keeper,
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
            &mut self.rows,
        )
        .await
        .map_err(|(statement, source)| self.failed(statement, source))?;
        Ok((taken, self.rows.len()))
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
        let mut rows = std::mem::take(&mut self.rows);
        let (claimed, rows) = off_clock(async move {
            let claimed = claim_rows(&pool, queue, holding, limit, now, &mut rows).await;
            (claimed, rows)
        })
        .await
        .unwrap_or_else(|| (Err(("BEGIN", cancelled())), Vec::new()));
        self.rows = rows;
        let taken = claimed.map_err(|(statement, source)| self.failed(statement, source))?;
        self.shared.harness.claimed(queue.name, self.rows.len());
        Ok((taken, self.rows.len()))
    }

    /// The deliveries of the last claim, oldest first.
    pub(crate) fn take_rows(&mut self) -> impl Iterator<Item = Claimed<Row>> + '_ {
        self.rows.drain(..)
    }

    pub(crate) const fn queue(&self) -> &'static Queue {
        self.queue
    }

    async fn next_one(&mut self) -> Option<Result<InboxDelivery<DB, Row>, SqlxBrokerError>> {
        let (taken, _) = match self.claim(1).await? {
            Ok(claimed) => claimed,
            Err(error) => return Some(Err(error)),
        };
        let queue = self.queue;
        let claimed = self.rows.pop()?;
        let delivery = match taken {
            Taken::Locked(tx) => InboxDelivery::own(claimed, tx, queue),
            Taken::Leased(book, lease) => InboxDelivery::leased(claimed, book, lease, queue),
        };
        #[cfg(feature = "testing")]
        let delivery = delivery.on(&self.shared);
        Some(Ok(delivery))
    }

    /// The subscription's deliveries, as a stream that owns it.
    pub(crate) fn into_stream(
        self,
    ) -> impl Stream<Item = Result<InboxDelivery<DB, Row>, SqlxBrokerError>> + Send + 'static {
        futures::stream::unfold(self, |mut subscriber| async move {
            let next = subscriber.next_one().await?;
            Some((next, subscriber))
        })
    }
}

/// A statement that failed, and why.
type Failed = (&'static str, sqlx::Error);

/// Claims up to `limit` rows of `queue` into `rows`, holding them as `holding` says: in a
/// transaction of `pool` it returns open, or by a lease it commits.
async fn claim_rows<DB, Row>(
    pool: &Pool<DB>,
    queue: &'static Queue,
    holding: Holding<DB, Row>,
    limit: usize,
    now: Now,
    rows: &mut Vec<Claimed<Row>>,
) -> Result<Taken<DB, Row>, Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let mut cx = Claiming {
        queue,
        limit: i64::try_from(limit).unwrap_or(i64::MAX),
        now,
    };
    let claim_failed = |source| -> Failed {
        let statement = queue.prepared.claim.map_or("claim", |claim| claim.sql);
        (statement, source)
    };
    rows.clear();
    let book = match holding {
        Holding::Locks => {
            let mut tx = begin(pool, queue).await?;
            return match Row::claim(&mut tx, &cx, None, rows).await {
                Ok(()) => Ok(Taken::Locked(tx)),
                Err(source) => {
                    // A rollback that fails leaves the transaction to its drop, which closes the
                    // connection.
                    let _ = tx.rollback().await;
                    Err(claim_failed(source))
                }
            };
        }
        Holding::Leases(book) => book,
    };
    // The lease is read once the connection is in hand, so a wait for the pool does not shorten
    // it. The claim reads "now" there once: the rows it finds due, the leases it finds ended and
    // the expiry it writes start from that instant.
    if queue.prepared.stamps {
        // The claim only selects: its transaction stamps each row it took and commits, so the
        // rows hold their leases, not the transaction.
        let mut tx = begin(pool, queue).await?;
        let claimed = async {
            let lease = Row::lease(queue, &mut cx.now).map_err(claim_failed)?;
            Row::claim(&mut tx, &cx, Some(&lease), rows)
                .await
                .map_err(claim_failed)?;
            stamp_rows::<DB, Row>(&mut tx, &cx, &lease, rows).await?;
            Ok::<_, Failed>(lease.expiry)
        }
        .await;
        let lease = match claimed {
            Ok(lease) => lease,
            Err(failed) => {
                let _ = tx.rollback().await;
                return Err(failed);
            }
        };
        tx.commit().await.map_err(|source| ("COMMIT", source))?;
        return Ok(Taken::Leased(book, lease));
    }
    // The claim writes the lease itself, in one statement that commits on its own.
    let mut conn = pool.acquire().await.map_err(|source| ("acquire", source))?;
    let lease = Row::lease(queue, &mut cx.now).map_err(claim_failed)?;
    Row::claim(&mut conn, &cx, Some(&lease), rows)
        .await
        .map_err(claim_failed)?;
    Ok(Taken::Leased(book, lease.expiry))
}

/// Opens a claim's transaction on a connection of `pool`, with the statement `queue`'s dialect
/// opens it with, or `BEGIN`.
async fn begin<DB: QueueDatabase>(pool: &Pool<DB>, queue: &Queue) -> Result<Tx<DB>, Failed> {
    Tx::begin(pool, queue.begin_claim)
        .await
        .map_err(|source| (queue.begin_claim.unwrap_or("BEGIN"), source))
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

impl<DB, Row> Subscriber for InboxSubscriber<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    type Message = InboxDelivery<DB, Row>;
    type Error = SqlxBrokerError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        futures::stream::unfold(self, |subscriber| async move {
            let next = subscriber.next_one().await?;
            Some((next, subscriber))
        })
    }
}

impl<DB, Row> BatchSubscriber for InboxSubscriber<DB, Row>
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
            let queue = subscriber.queue();
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
                            on(InboxDelivery::batched(claimed, Arc::clone(&batch), queue))
                        })
                        .collect()
                }
                // Each delivery of a leased batch holds its own lease and settles on its own.
                Taken::Leased(book, lease) => subscriber
                    .take_rows()
                    .map(|claimed| on(InboxDelivery::leased(claimed, book, lease, queue)))
                    .collect(),
            };
            Some((Ok(deliveries), subscriber))
        })
    }
}
