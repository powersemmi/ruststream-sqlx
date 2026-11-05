//! What a batch handler is handed: a payload-mode table's deliveries, or a row-mode table's rows
//! as one slice ([`RowBatch`]), chosen by the table's lane.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, mem, vec};

use futures::ready;
use pin_project_lite::pin_project;
use ruststream::{BatchSlot, BatchStream, CarriesBatch};

#[cfg(feature = "testing")]
use super::broker::Shared;
use super::database::QueueDatabase;
use super::delivery::{Hold, InboxDelivery};
use super::engine::{Claimed, Events};
use super::error::SqlxBrokerError;
use super::form::advisory::LockHold;
use super::form::lease::{LeaseBook, Slot};
use super::form::row_lock::BatchTx;
use super::headers::HeaderCell;
use super::queue::Queue;
use super::subscriber::{InboxSubscriber, Taken};
use super::{Lane, PayloadLane, PayloadRow, QueueRow, RowLane};
use crate::inbox::pools::ServicePool;

/// How a table's lane fills a batch handler's slot with what one claim took. Machinery: a
/// subscription in the plain mode fills its batches through it, so a table's mode picks its batch
/// type.
#[doc(hidden)]
pub trait BatchLane<DB: QueueDatabase, Row: Events<DB>>: Lane<Row> {
    /// The slot a subscription to the table fills, which the runtime lends and reuses.
    type Batch: BatchSlot<Message = InboxDelivery<DB, Row>> + Send;

    /// Moves what `claim` took into the empty `slot`.
    fn fill(claim: BatchClaim<'_, DB, Row>, slot: &mut Self::Batch);
}

/// What one claim of a batch subscription took, and the subscription it took it for. Machinery.
#[doc(hidden)]
pub struct BatchClaim<'s, DB: QueueDatabase, Row: Events<DB>> {
    subscriber: &'s mut InboxSubscriber<DB, Row>,
    taken: Taken<DB, Row>,
    count: usize,
}

impl<'s, DB: QueueDatabase, Row: Events<DB>> BatchClaim<'s, DB, Row> {
    /// The `count` rows `subscriber`'s last claim took, held as `taken` says.
    pub(crate) const fn new(
        subscriber: &'s mut InboxSubscriber<DB, Row>,
        taken: Taken<DB, Row>,
        count: usize,
    ) -> Self {
        Self {
            subscriber,
            taken,
            count,
        }
    }
}

impl<DB: QueueDatabase, Row: Events<DB>> fmt::Debug for BatchClaim<'_, DB, Row> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchClaim")
            .field("subscription", &self.subscriber.queue().name)
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
}

pin_project! {
    /// The batches of a subscription in the plain mode: each claim fills the slot the runtime lent,
    /// and the claim in flight lives here between polls. Machinery.
    #[doc(hidden)]
    pub struct Batches<'s, DB, Row, Start, Claim>
    where
        DB: QueueDatabase,
        Row: Events<DB>,
    {
        // The subscription while no claim runs; the claim in flight holds it otherwise, and it
        // is gone once the broker shut down.
        idle: Option<&'s mut InboxSubscriber<DB, Row>>,
        #[pin]
        claim: Option<Claim>,
        start: Start,
        size: NonZeroUsize,
    }
}

/// The batches of at most `size` rows `subscriber` claims.
pub(crate) fn batches<DB, Row>(
    subscriber: &mut InboxSubscriber<DB, Row>,
    size: NonZeroUsize,
) -> impl BatchStream<<Row::Lane as BatchLane<DB, Row>>::Batch, Error = SqlxBrokerError> + Send + '_
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Lane: BatchLane<DB, Row>,
{
    Batches {
        idle: Some(subscriber),
        claim: None,
        start: claim_batch::<DB, Row>,
        size,
    }
}

/// One claim of up to `size` rows; the claim hands the subscription back beside what it took, so
/// the batch is filled from the subscription's buffers.
async fn claim_batch<DB: QueueDatabase, Row: Events<DB>>(
    subscriber: &mut InboxSubscriber<DB, Row>,
    size: NonZeroUsize,
) -> (
    &mut InboxSubscriber<DB, Row>,
    Option<Result<(Taken<DB, Row>, usize), SqlxBrokerError>>,
) {
    let claimed = subscriber.claim(size.get()).await;
    (subscriber, claimed)
}

impl<'s, DB, Row, Start, Claim> BatchStream<<Row::Lane as BatchLane<DB, Row>>::Batch>
    for Batches<'s, DB, Row, Start, Claim>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Lane: BatchLane<DB, Row>,
    Start: FnMut(&'s mut InboxSubscriber<DB, Row>, NonZeroUsize) -> Claim,
    Claim: Future<Output = ClaimOutcome<'s, DB, Row>>,
{
    type Error = SqlxBrokerError;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut <Row::Lane as BatchLane<DB, Row>>::Batch,
    ) -> Poll<Option<Result<(), Self::Error>>> {
        let mut this = self.project();
        loop {
            if let Some(claim) = this.claim.as_mut().as_pin_mut() {
                let (subscriber, claimed) = ready!(claim.poll(cx));
                this.claim.set(None);
                return Poll::Ready(match claimed {
                    // The broker shut down: the subscription stays gone.
                    None => None,
                    Some(Err(error)) => {
                        *this.idle = Some(subscriber);
                        Some(Err(error))
                    }
                    Some(Ok((taken, count))) => {
                        <Row::Lane as BatchLane<DB, Row>>::fill(
                            BatchClaim::new(subscriber, taken, count),
                            slot,
                        );
                        *this.idle = Some(subscriber);
                        Some(Ok(()))
                    }
                });
            }
            let Some(subscriber) = this.idle.take() else {
                return Poll::Ready(None);
            };
            let claim = (this.start)(subscriber, *this.size);
            this.claim.set(Some(claim));
        }
    }
}

/// What [`claim_batch`] resolves to.
type ClaimOutcome<'s, DB, Row> = (
    &'s mut InboxSubscriber<DB, Row>,
    Option<Result<(Taken<DB, Row>, usize), SqlxBrokerError>>,
);

/// A payload-mode batch is its deliveries, each holding its row, as a codec decodes them.
impl<DB, Row> BatchLane<DB, Row> for PayloadLane
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    type Batch = Vec<InboxDelivery<DB, Row>>;

    fn fill(claim: BatchClaim<'_, DB, Row>, slot: &mut Self::Batch) {
        let BatchClaim {
            subscriber,
            taken,
            count,
        } = claim;
        let (queue, pool) = (subscriber.queue(), subscriber.pool());
        #[cfg(feature = "testing")]
        let shared = Arc::clone(&subscriber.shared);
        let on = |delivery: InboxDelivery<DB, Row>| {
            #[cfg(feature = "testing")]
            let delivery = delivery.on(&shared);
            delivery
        };
        match taken {
            Taken::Locked(tx) => {
                let batch = BatchTx::new(tx, count);
                #[cfg(feature = "testing")]
                batch.watch(&shared);
                slot.extend(subscriber.take_rows().map(|claimed| {
                    on(InboxDelivery::batched(
                        claimed,
                        Arc::clone(&batch),
                        queue,
                        pool,
                    ))
                }));
            }
            // Each delivery of a leased batch holds its own lease and settles on its own.
            Taken::Leased(book, lease) => slot.extend(subscriber.take_rows().map(|claimed| {
                on(InboxDelivery::leased(
                    claimed, book, lease, None, queue, pool,
                ))
            })),
            // Each delivery of an advisory batch holds its own session and settles on its own.
            Taken::Advised => slot.extend(
                subscriber
                    .take_advised()
                    .map(|(claimed, hold)| on(InboxDelivery::advised(claimed, hold, queue, pool))),
            ),
        }
    }
}

/// A row-mode batch lends its rows as one slice.
impl<DB, Row> BatchLane<DB, Row> for RowLane
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = Self>,
{
    type Batch = RowBatch<DB, Row>;

    fn fill(claim: BatchClaim<'_, DB, Row>, slot: &mut Self::Batch) {
        slot.fill(claim);
    }
}

/// The rows one claim of a table in row mode took, which a batch handler borrows as one slice,
/// `&[Row]`, and the claims that settle them.
///
/// The rows lie in one vector in claim order, as the driver read them: the handler reads them
/// where they lie, with no codec and no copy. Each row's headers column was taken into its
/// delivery's headers when the batch was built, so the handler reads the column empty. A message
/// assembled from a headers struct keeps its headers struct whole, and each delivery builds its
/// header map from it on the first read. A claimed id without a row, and a row the driver could
/// not read, follow the slice: the runtime settles them by the subscription's decode policy, its
/// error in the log. Every delivery of the batch
/// holds its row in the table's form, as a payload-mode batch's deliveries do: one transaction
/// for the whole batch in the row lock form, a lease the subscription extends while the handler
/// runs in the lease form, a session that holds the row's key in the advisory lock form.
///
/// A batch dropped before its deliveries settle (its handler panicked, the app shut down)
/// returns its rows at once, as a dropped delivery does.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::PgPool;
///
/// /// A mail to send: the table has no payload column, so a handler takes the rows themselves.
/// #[derive(Debug, Clone, Inbox, sqlx::FromRow)]
/// #[inbox(table = "mail_jobs")]
/// pub struct SendEmail {
///     #[field(id)]
///     job_id: i64,
///     recipient: String,
/// }
///
/// # async fn deliver(_: &str) -> bool { true }
/// /// Each claim of up to 50 rows reaches the handler as one slice; each mail settles on its own.
/// #[subscriber(InboxQueue::<SendEmail>::new("mail"))]
/// async fn send(mails: &[SendEmail]) -> Vec<HandlerOutcome> {
///     let mut outcomes = Vec::with_capacity(mails.len());
///     for mail in mails {
///         outcomes.push(if deliver(&mail.recipient).await {
///             HandlerOutcome::ack()
///         } else {
///             HandlerOutcome::retry()
///         });
///     }
///     outcomes
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(send.batch(nonzero!(50)))
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    /// The rows the handler borrows, in claim order.
    rows: Vec<Row>,
    /// The headers taken from the rows, beside them; empty and unallocated while every row's are
    /// unset (a message assembled from a headers struct builds its own on the first read), and
    /// shorter than the rows when the last rows' are.
    headers: Vec<Row::Headers>,
    /// What holds the rows, by the table's form.
    claims: Claims<DB, Row>,
    /// Each row's place in the lease book, beside the rows, in the lease form.
    slots: Vec<Slot>,
    /// Each row's place in the book whose session holds its key, beside the rows, in the advisory
    /// lock form.
    holds: Vec<LockHold<DB>>,
    /// The deliveries with no row to lend, in claim order; empty and unallocated while every row
    /// was read.
    gone: Vec<InboxDelivery<DB, Row>>,
    /// The subscription the rows were claimed for; set by the first claim.
    from: Option<Origin<DB>>,
    /// The batch size the runtime allocated the slot for.
    room: NonZeroUsize,
}

/// The subscription a [`RowBatch`] was filled for.
struct Origin<DB: QueueDatabase> {
    queue: &'static Queue,
    pool: &'static ServicePool<DB>,
    /// The connection the batch was claimed on: a delivery claimed in process keeps the harness's
    /// books.
    #[cfg(feature = "testing")]
    shared: Arc<Shared<DB>>,
}

/// What holds a row-mode batch's rows, by the table's form.
enum Claims<DB: QueueDatabase, Row: Events<DB>> {
    /// The slot holds no rows.
    Empty,
    /// The claim's transaction, which every delivery of the batch shares.
    Locked(Arc<BatchTx<DB>>),
    /// The book that keeps each row's lease, entered when the batch was filled so the keeper
    /// extends the leases while the handler runs.
    Leased(&'static LeaseBook<DB, Row>),
    /// The book whose sessions hold the rows' keys.
    Advised,
}

/// The deliveries a [`RowBatch`] yields as the runtime drains it: the rows' first, in claim order,
/// then those of the ids without a row and the rows the driver could not read. Machinery.
///
/// Dropped before it yielded them all, it builds and drops the rest, which returns their rows as
/// dropped deliveries return theirs. The batch keeps its storage for the next claim.
#[doc(hidden)]
pub struct RowDeliveries<'b, DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    rows: vec::Drain<'b, Row>,
    headers: vec::Drain<'b, Row::Headers>,
    claims: Claims<DB, Row>,
    slots: vec::Drain<'b, Slot>,
    holds: vec::Drain<'b, LockHold<DB>>,
    gone: vec::Drain<'b, InboxDelivery<DB, Row>>,
    from: Option<&'b Origin<DB>>,
}

impl<DB, Row> RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    /// Moves what `claim` took into the empty batch: each row once, into the slice, its headers out
    /// of it, and its claim beside it.
    fn fill(&mut self, claim: BatchClaim<'_, DB, Row>) {
        let BatchClaim {
            subscriber,
            taken,
            count,
        } = claim;
        let (queue, pool) = (subscriber.queue(), subscriber.pool());
        // The connection the batch was claimed on keeps the harness's books in process.
        #[cfg(feature = "testing")]
        let shared = &self
            .from
            .get_or_insert_with(|| Origin {
                queue,
                pool,
                shared: Arc::clone(&subscriber.shared),
            })
            .shared;
        #[cfg(not(feature = "testing"))]
        self.from.get_or_insert(Origin { queue, pool });
        let on = |delivery: InboxDelivery<DB, Row>| {
            #[cfg(feature = "testing")]
            let delivery = delivery.on(shared);
            delivery
        };
        let room = self.room.get();
        let (rows, headers, gone) = (&mut self.rows, &mut self.headers, &mut self.gone);
        self.claims = match taken {
            Taken::Locked(tx) => {
                let batch = BatchTx::new(tx, count);
                #[cfg(feature = "testing")]
                batch.watch(shared);
                for claimed in subscriber.take_rows() {
                    match claimed {
                        Claimed::Row(row) => take(row, rows, headers, room),
                        claimed => gone.push(on(InboxDelivery::batched(
                            claimed,
                            Arc::clone(&batch),
                            queue,
                            pool,
                        ))),
                    }
                }
                Claims::Locked(batch)
            }
            Taken::Leased(book, lease) => {
                self.slots.reserve_exact(room);
                for claimed in subscriber.take_rows() {
                    match claimed {
                        Claimed::Row(row) => {
                            self.slots.push(book.enter(Row::id(&row), lease));
                            take(row, rows, headers, room);
                        }
                        claimed => gone.push(on(InboxDelivery::leased(
                            claimed, book, lease, None, queue, pool,
                        ))),
                    }
                }
                Claims::Leased(book)
            }
            Taken::Advised => {
                self.holds.reserve_exact(room);
                for (claimed, hold) in subscriber.take_advised() {
                    match claimed {
                        Claimed::Row(row) => {
                            self.holds.push(hold);
                            take(row, rows, headers, room);
                        }
                        claimed => {
                            gone.push(on(InboxDelivery::advised(claimed, hold, queue, pool)));
                        }
                    }
                }
                Claims::Advised
            }
        };
    }
}

/// Moves `row` into `rows` and its headers into `headers`, which allocates for the first row that
/// carries any, room for a whole batch of `room` rows.
fn take<DB: QueueDatabase, Row: Events<DB>>(
    mut row: Row,
    rows: &mut Vec<Row>,
    headers: &mut Vec<Row::Headers>,
    room: usize,
) {
    let taken = Row::Headers::take(&mut row);
    if !headers.is_empty() || !taken.is_unset() {
        if headers.is_empty() {
            headers.reserve_exact(room);
            headers.resize_with(rows.len(), Row::Headers::default);
        }
        headers.push(taken);
    }
    rows.push(row);
}

/// The runtime allocates one batch per worker with room for the registration's `batch(n)`, and
/// every claim of the worker fills it again: a batch allocates no storage of its own after the
/// first claim of each form.
impl<DB, Row> BatchSlot for RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    type Message = InboxDelivery<DB, Row>;

    fn with_capacity(size: NonZeroUsize) -> Self {
        Self {
            rows: Vec::with_capacity(size.get()),
            headers: Vec::new(),
            claims: Claims::Empty,
            slots: Vec::new(),
            holds: Vec::new(),
            gone: Vec::new(),
            from: None,
            room: size,
        }
    }

    fn drain(&mut self) -> impl Iterator<Item = Self::Message> + '_ {
        RowDeliveries {
            rows: self.rows.drain(..),
            headers: self.headers.drain(..),
            claims: mem::replace(&mut self.claims, Claims::Empty),
            slots: self.slots.drain(..),
            holds: self.holds.drain(..),
            gone: self.gone.drain(..),
            from: self.from.as_ref(),
        }
    }
}

impl<DB, Row> CarriesBatch<Row> for RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    type Item = Row;

    fn carried(&self) -> &[Row] {
        &self.rows
    }
}

impl<DB, Row> Iterator for RowDeliveries<'_, DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    type Item = InboxDelivery<DB, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rows.len() == 0 {
            return self.gone.next();
        }
        // A batch that holds rows was filled, so it knows its subscription.
        let from = self.from?;
        let hold = match &self.claims {
            Claims::Locked(batch) => Hold::Batch(Arc::clone(batch)),
            Claims::Leased(book) => Hold::Lease {
                book: *book,
                slot: self.slots.next()?,
                tx: None,
            },
            Claims::Advised => Hold::Advisory(self.holds.next()?),
            Claims::Empty => return None,
        };
        // The batch filled one claim per row, so a row is here whenever its claim is.
        let row = self.rows.next()?;
        let headers = self.headers.next().unwrap_or_default();
        let delivery = InboxDelivery::of_batch(row, headers, hold, from.queue, from.pool);
        #[cfg(feature = "testing")]
        let delivery = delivery.on(&from.shared);
        Some(delivery)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.rows.len() + self.gone.len();
        (len, Some(len))
    }
}

impl<DB, Row> ExactSizeIterator for RowDeliveries<'_, DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
}

impl<DB, Row> Drop for RowDeliveries<'_, DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    fn drop(&mut self) {
        // Each delivery not yet taken returns its row as it drops: the batch transaction's last
        // one rolls back, a lease is released, a session closes, and the harness counts it.
        self.for_each(drop);
    }
}

/// A batch the runtime drops with rows still in it (the subscription ended mid-batch) returns them
/// as its deliveries would.
impl<DB, Row> Drop for RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    fn drop(&mut self) {
        self.drain().for_each(drop);
    }
}

impl<DB, Row> fmt::Debug for RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowBatch")
            .field(
                "subscription",
                &self.from.as_ref().map(|from| from.queue.name),
            )
            .field("rows", &self.rows.len())
            .field("gone", &self.gone.len())
            .finish_non_exhaustive()
    }
}

impl<DB, Row> fmt::Debug for RowDeliveries<'_, DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowDeliveries")
            .field("subscription", &self.from.map(|from| from.queue.name))
            .field("left", &self.len())
            .finish_non_exhaustive()
    }
}
