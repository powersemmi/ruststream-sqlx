//! What a batch handler is handed: a payload-mode table's deliveries, or a row-mode table's rows
//! as one slice ([`RowBatch`]), chosen by the table's lane.

use std::fmt;
use std::sync::Arc;
use std::vec;

use ruststream::CarriesBatch;

#[cfg(feature = "testing")]
use super::broker::Shared;
use super::database::QueueDatabase;
use super::delivery::{Hold, InboxDelivery};
use super::engine::{Claimed, Events};
use super::form::advisory::LockHold;
use super::form::lease::{LeaseBook, Slot};
use super::form::row_lock::BatchTx;
use super::headers::HeaderCell;
use super::queue::Queue;
use super::subscriber::{InboxSubscriber, Taken};
use super::{Lane, PayloadLane, PayloadRow, QueueRow, RowLane};
use crate::inbox::pools::ServicePool;

/// How a table's lane hands a batch handler what one claim took. Machinery: a subscription in the
/// plain mode builds its batches through it, so a table's mode picks its batch type.
#[doc(hidden)]
pub trait BatchLane<DB: QueueDatabase, Row: Events<DB>>: Lane<Row> {
    /// The batch a subscription to the table yields.
    type Batch: IntoIterator<Item = InboxDelivery<DB, Row>> + Send;

    /// The batch of what `claim` took.
    fn batch(claim: BatchClaim<'_, DB, Row>) -> Self::Batch;
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

/// A payload-mode batch is its deliveries, each holding its row, as a codec decodes them.
impl<DB, Row> BatchLane<DB, Row> for PayloadLane
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    type Batch = Vec<InboxDelivery<DB, Row>>;

    fn batch(claim: BatchClaim<'_, DB, Row>) -> Self::Batch {
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

    fn batch(claim: BatchClaim<'_, DB, Row>) -> Self::Batch {
        RowBatch::new(claim)
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
///         b.include(send.batch(nonzero!(50)));
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
    deliveries: RowDeliveries<DB, Row>,
}

/// The deliveries of a [`RowBatch`], built as the runtime takes them: the rows' first, in claim
/// order, then those of the ids without a row and the rows the driver could not read. Machinery.
///
/// Dropped before it yielded them all, it builds and drops the rest, which returns their rows as
/// dropped deliveries return theirs.
#[doc(hidden)]
pub struct RowDeliveries<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    /// The rows the handler borrows, in claim order.
    rows: vec::IntoIter<Row>,
    /// The headers taken from the rows, beside them; empty and unallocated while every row's are
    /// unset (a message assembled from a headers struct builds its own on the first read), and
    /// shorter than the rows when the last rows' are.
    headers: vec::IntoIter<Row::Headers>,
    /// What holds the rows, beside them.
    claims: Claims<DB, Row>,
    /// The deliveries with no row to lend, in claim order; empty and unallocated when every row
    /// was read.
    gone: vec::IntoIter<InboxDelivery<DB, Row>>,
    queue: &'static Queue,
    pool: &'static ServicePool<DB>,
    /// The connection the batch was claimed on: a delivery claimed in process keeps the harness's
    /// books.
    #[cfg(feature = "testing")]
    shared: Arc<Shared<DB>>,
}

/// What holds a row-mode batch's rows, by the table's form.
enum Claims<DB: QueueDatabase, Row: Events<DB>> {
    /// The claim's transaction, which every delivery of the batch shares.
    Locked(Arc<BatchTx<DB>>),
    /// The book that keeps each row's lease, and each row's place in it, entered when the batch
    /// was built so the keeper extends the leases while the handler runs.
    Leased(&'static LeaseBook<DB, Row>, vec::IntoIter<Slot>),
    /// The place of each row in the book whose session holds the row's key.
    Advised(vec::IntoIter<LockHold<DB>>),
}

impl<DB, Row> RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    /// Moves what `claim` took into the batch: each row once, into the slice, its headers out of
    /// it, and its claim beside it.
    fn new(claim: BatchClaim<'_, DB, Row>) -> Self {
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
        let mut rows = Vec::with_capacity(count);
        let mut headers = Vec::new();
        let mut gone = Vec::new();
        let claims = match taken {
            Taken::Locked(tx) => {
                let batch = BatchTx::new(tx, count);
                for claimed in subscriber.take_rows() {
                    match claimed {
                        Claimed::Row(row) => take(row, &mut rows, &mut headers, count),
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
                let mut slots = Vec::with_capacity(count);
                for claimed in subscriber.take_rows() {
                    match claimed {
                        Claimed::Row(row) => {
                            slots.push(book.enter(Row::id(&row), lease));
                            take(row, &mut rows, &mut headers, count);
                        }
                        claimed => gone.push(on(InboxDelivery::leased(
                            claimed, book, lease, None, queue, pool,
                        ))),
                    }
                }
                Claims::Leased(book, slots.into_iter())
            }
            Taken::Advised => {
                let mut holds = Vec::with_capacity(count);
                for (claimed, hold) in subscriber.take_advised() {
                    match claimed {
                        Claimed::Row(row) => {
                            holds.push(hold);
                            take(row, &mut rows, &mut headers, count);
                        }
                        claimed => {
                            gone.push(on(InboxDelivery::advised(claimed, hold, queue, pool)));
                        }
                    }
                }
                Claims::Advised(holds.into_iter())
            }
        };
        Self {
            deliveries: RowDeliveries {
                rows: rows.into_iter(),
                headers: headers.into_iter(),
                claims,
                gone: gone.into_iter(),
                queue,
                pool,
                #[cfg(feature = "testing")]
                shared,
            },
        }
    }
}

/// Moves `row` into `rows` and its headers into `headers`, which allocates for the first row that
/// carries any, room for all `count` rows of the claim.
fn take<DB: QueueDatabase, Row: Events<DB>>(
    mut row: Row,
    rows: &mut Vec<Row>,
    headers: &mut Vec<Row::Headers>,
    count: usize,
) {
    let taken = Row::Headers::take(&mut row);
    if !headers.is_empty() || !taken.is_unset() {
        if headers.is_empty() {
            headers.reserve_exact(count);
            headers.resize_with(rows.len(), Row::Headers::default);
        }
        headers.push(taken);
    }
    rows.push(row);
}

impl<DB, Row> CarriesBatch<Row> for RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    fn carried(&self) -> &[Row] {
        self.deliveries.rows.as_slice()
    }
}

impl<DB, Row> IntoIterator for RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    type Item = InboxDelivery<DB, Row>;
    type IntoIter = RowDeliveries<DB, Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.deliveries
    }
}

impl<DB, Row> Iterator for RowDeliveries<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    type Item = InboxDelivery<DB, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rows.len() == 0 {
            return self.gone.next();
        }
        let hold = match &mut self.claims {
            Claims::Locked(batch) => Hold::Batch(Arc::clone(batch)),
            Claims::Leased(book, slots) => Hold::Lease {
                book: *book,
                slot: slots.next()?,
                tx: None,
            },
            Claims::Advised(holds) => Hold::Advisory(holds.next()?),
        };
        // The batch built one claim per row, so a row is here whenever its claim is.
        let row = self.rows.next()?;
        let headers = self.headers.next().unwrap_or_default();
        let delivery = InboxDelivery::of_batch(row, headers, hold, self.queue, self.pool);
        #[cfg(feature = "testing")]
        let delivery = delivery.on(&self.shared);
        Some(delivery)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.rows.len() + self.gone.len();
        (len, Some(len))
    }
}

impl<DB, Row> ExactSizeIterator for RowDeliveries<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
}

impl<DB, Row> Drop for RowDeliveries<DB, Row>
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

impl<DB, Row> fmt::Debug for RowBatch<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowBatch")
            .field("subscription", &self.deliveries.queue.name)
            .field("rows", &self.deliveries.rows.len())
            .field("gone", &self.deliveries.gone.len())
            .finish_non_exhaustive()
    }
}

impl<DB, Row> fmt::Debug for RowDeliveries<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowDeliveries")
            .field("subscription", &self.queue.name)
            .field("left", &self.len())
            .finish_non_exhaustive()
    }
}
