//! `InboxDelivery`: one claimed row in a handler's hands, and how it settles.

use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use ruststream::{AckError, HeaderMap, IncomingMessage};
use sqlx::Pool;
use sync_wrapper::SyncWrapper;
use tokio::runtime::Handle;

use super::PayloadRow;
#[cfg(feature = "testing")]
use super::broker::Shared;
use super::database::QueueDatabase;
use super::engine::{Claimed, Events, Now};
use super::form::advisory::LockHold;
use super::form::lease::settle::release;
#[cfg(feature = "testing")]
use super::form::lease::settle::release_in_process;
use super::form::lease::{LeaseBook, Slot};
use super::form::row_lock::BatchTx;
use super::queue::Queue;
#[cfg(feature = "testing")]
use super::testing::off_clock;
use super::transactional::{InboxMode, Plain, TxHold};
use super::tx::PoolTx;

pub(crate) mod settle;

use settle::Outcome;

/// What holds a delivery's row until it settles.
pub(super) enum Hold<DB: QueueDatabase, Row: Events<DB>> {
    /// The delivery's own transaction: one claim, one row.
    Own(SyncWrapper<PoolTx<DB>>),
    /// A batch's transaction: one claim, its rows sharing the transaction.
    Batch(Arc<BatchTx<DB>>),
    /// The lease the claim wrote, in the subscription's book; in transactional mode with the
    /// delivery's own transaction, in the subscription's book of transactions while the handler
    /// does not borrow it.
    Lease {
        book: &'static LeaseBook<DB, Row>,
        slot: Slot,
        tx: Option<TxHold<DB>>,
    },
    /// The lock on the row's key, held by the delivery's own session in the subscription's book.
    Advisory(LockHold<DB>),
    /// The delivery's own transaction, in the subscription's book while the handler does not
    /// borrow it: one claim, one row, in transactional mode.
    Lent(TxHold<DB>),
}

/// One claimed row in a handler's hands.
///
/// The payload is lent from the row, without a copy. In the row lock form the delivery holds the
/// claim's transaction: settling it runs one statement and commits, and dropping it unsettled
/// rolls the transaction back, which returns the row to the queue at once. In the lease form the
/// delivery holds the lease its claim wrote, which its subscription extends each half lease:
/// settling it runs one statement on a connection of its own, which takes effect only while the
/// row still holds that lease. A lease delivery dropped unsettled releases its row at once, on the
/// runtime the broker connected on; with that runtime gone, the row returns once the lease runs
/// out.
///
/// In the advisory lock form the delivery holds a connection whose session holds the lock on the
/// row's key: settling it runs one statement on that connection, then releases the lock and
/// returns the connection to the pool. A delivery dropped unsettled closes its connection on the
/// runtime the broker connected on, after releasing the lock, and its row returns at once; with
/// that runtime gone the connection closes as it drops, and the server ends its session and its
/// lock. A settlement after [`shutdown`](ruststream::ConnectedBroker::shutdown) released its lock
/// fails with [`SqlxBrokerError::Closed`](crate::SqlxBrokerError::Closed).
///
/// In transactional mode the delivery lends its handler the transaction it settles in, and the
/// handler's writes go with the settlement: acknowledgement runs its statement and commits them
/// together; every other settlement first discards what the handler wrote, then settles as outside
/// transactional mode. A statement that fails rolls the whole transaction back, and the row
/// returns at once. In the lease form the acknowledgement runs in the delivery's own transaction by
/// the lease it holds: a lease lost meanwhile rolls everything back and the settlement fails with
/// [`SqlxBrokerError::LeaseLost`](crate::SqlxBrokerError::LeaseLost). In the advisory lock form
/// the transaction runs on the session that holds the key, and ends before the key's release. A
/// delivery dropped unsettled closes its transaction's connection, and the server rolls the
/// transaction back.
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
/// use ruststream::IncomingMessage;
/// use ruststream_sqlx::InboxDelivery;
///
/// // A delivery whose payload is not JSON goes back to the queue for a later attempt.
/// pub async fn settle(delivery: InboxDelivery<sqlx::Postgres, Job>) -> Result<(), ruststream::AckError> {
///     if serde_json::from_slice::<serde_json::Value>(delivery.payload()).is_ok() {
///         delivery.ack().await
///     } else {
///         delivery.nack(true).await
///     }
/// }
/// # }
/// # fn main() {}
/// ```
pub struct InboxDelivery<DB: QueueDatabase, Row: Events<DB>, Mode = Plain> {
    claimed: Claimed<Row>,
    headers: HeaderMap,
    pub(super) hold: Option<Hold<DB, Row>>,
    queue: &'static Queue,
    /// The subscription's handle on the pool, which the delivery lends its handler.
    pub(super) pool: &'static Pool<DB>,
    /// The connection of a delivery claimed in process: its settlement keeps the harness's books.
    #[cfg(feature = "testing")]
    in_process: Option<Arc<Shared<DB>>>,
    _mode: PhantomData<fn() -> Mode>,
}

impl<DB: QueueDatabase, Row: Events<DB>, Mode> Debug for InboxDelivery<DB, Row, Mode> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxDelivery")
            .field("subscription", &self.queue.name)
            .field("id", self.claimed.id::<DB>())
            .finish_non_exhaustive()
    }
}

impl<DB, Row, Mode> InboxDelivery<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
    Mode: InboxMode,
{
    /// A delivery that owns its claim's transaction.
    pub(crate) fn own(
        claimed: Claimed<Row>,
        tx: PoolTx<DB>,
        queue: &'static Queue,
        pool: &'static Pool<DB>,
    ) -> Self {
        Self::held(claimed, Hold::Own(SyncWrapper::new(tx)), queue, pool)
    }

    /// A delivery whose claim's transaction waits at `hold` in its subscription's book, for its
    /// handler to borrow: transactional mode.
    pub(crate) fn lent(
        claimed: Claimed<Row>,
        hold: TxHold<DB>,
        queue: &'static Queue,
        pool: &'static Pool<DB>,
    ) -> Self {
        Self::held(claimed, Hold::Lent(hold), queue, pool)
    }

    /// A delivery of a batch, sharing its claim's transaction.
    pub(crate) fn batched(
        claimed: Claimed<Row>,
        batch: Arc<BatchTx<DB>>,
        queue: &'static Queue,
        pool: &'static Pool<DB>,
    ) -> Self {
        Self::held(claimed, Hold::Batch(batch), queue, pool)
    }

    /// A delivery that holds the lease its claim wrote, entered in its subscription's `book`; in
    /// transactional mode with its own transaction, which waits at `tx` for its handler to borrow.
    pub(crate) fn leased(
        claimed: Claimed<Row>,
        book: &'static LeaseBook<DB, Row>,
        lease: Row::Token,
        tx: Option<TxHold<DB>>,
        queue: &'static Queue,
        pool: &'static Pool<DB>,
    ) -> Self {
        let slot = book.enter(claimed.id::<DB>(), lease);
        Self::held(claimed, Hold::Lease { book, slot, tx }, queue, pool)
    }

    /// A delivery whose session holds the lock on its row's key, at `hold` in its subscription's
    /// book.
    pub(crate) fn advised(
        claimed: Claimed<Row>,
        hold: LockHold<DB>,
        queue: &'static Queue,
        pool: &'static Pool<DB>,
    ) -> Self {
        Self::held(claimed, Hold::Advisory(hold), queue, pool)
    }

    /// The subscription's handle on the pool, which the delivery lends its handler.
    pub(crate) const fn pool(&self) -> &'static Pool<DB> {
        self.pool
    }

    fn held(
        mut claimed: Claimed<Row>,
        hold: Hold<DB, Row>,
        queue: &'static Queue,
        pool: &'static Pool<DB>,
    ) -> Self {
        let headers = match &mut claimed {
            Claimed::Row(row) => Row::take_headers(row),
            Claimed::Missing(id) => {
                tracing::warn!(
                    target: "ruststream_sqlx",
                    subscription = queue.name,
                    table = queue.table,
                    row = queue.row,
                    ?id,
                    "the fetch returned no row for a claimed id; its delivery carries no payload \
                     and fails to decode",
                );
                HeaderMap::new()
            }
            Claimed::Undecodable { id, attempt, error } => {
                tracing::warn!(
                    target: "ruststream_sqlx",
                    subscription = queue.name,
                    table = queue.table,
                    row = queue.row,
                    ?id,
                    attempt,
                    %error,
                    "the row does not decode into its struct; its delivery carries no payload and \
                     the decode-failure policy settles it",
                );
                HeaderMap::new()
            }
        };
        Self {
            claimed,
            headers,
            hold: Some(hold),
            queue,
            pool,
            #[cfg(feature = "testing")]
            in_process: None,
            _mode: PhantomData,
        }
    }

    /// The delivery of `connection`, which keeps it when the connection runs in process.
    #[cfg(feature = "testing")]
    pub(crate) fn on(mut self, connection: &Arc<Shared<DB>>) -> Self {
        self.in_process = connection
            .harness
            .in_process()
            .then(|| Arc::clone(connection));
        self
    }
}

impl<DB, Row, Mode> IncomingMessage for InboxDelivery<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
    Mode: InboxMode,
{
    fn payload(&self) -> &[u8] {
        match &self.claimed {
            Claimed::Row(row) => row.payload(),
            Claimed::Missing(_) | Claimed::Undecodable { .. } => &[],
        }
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn partition_key(&self) -> Option<&[u8]> {
        match &self.claimed {
            Claimed::Row(row) => Row::partition_key(row),
            Claimed::Missing(_) | Claimed::Undecodable { .. } => None,
        }
    }

    fn redelivery_count(&self) -> Option<u64> {
        let carried = match &self.claimed {
            Claimed::Row(row) => Row::attempt(row),
            Claimed::Undecodable { attempt, .. } => *attempt,
            Claimed::Missing(_) => None,
        };
        // A claim that returns its rows after counting reports the attempt before its count.
        if self.queue.counted_attempt {
            carried.map(|attempt| attempt.saturating_sub(1))
        } else {
            carried
        }
    }

    async fn ack(self) -> Result<(), AckError> {
        self.settle(Outcome::Ack).await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.settle(if requeue {
            Outcome::Retry
        } else {
            Outcome::Discard
        })
        .await
    }

    fn supports_nack_after(&self) -> bool {
        // A delivery whose attempts are spent has no delayed redelivery to offer: the runtime then
        // settles it with `nack(true)`, which moves the row, and expects nothing back.
        self.queue.native_retry_after && self.spent().is_none()
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        if !self.supports_nack_after() {
            return Err(AckError::Unsupported);
        }
        self.settle(Outcome::RetryAfter(delay)).await
    }
}

impl<DB: QueueDatabase, Row: Events<DB>, Mode> Drop for InboxDelivery<DB, Row, Mode> {
    fn drop(&mut self) {
        let Some(hold) = self.hold.take() else {
            return;
        };
        #[cfg(feature = "testing")]
        let in_process = self.in_process.take();
        match hold {
            // In process the rollback runs off a paused clock too.
            #[cfg(feature = "testing")]
            Hold::Own(tx) if in_process.is_some() => {
                if let Ok(runtime) = Handle::try_current() {
                    drop(runtime.spawn(off_clock(tx.into_inner().rollback())));
                }
            }
            // An unsettled delivery's transaction rolls back on the runtime, which returns the
            // row to the queue at once. Without a runtime the transaction drops here: its
            // connection closes, and the server rolls it back.
            Hold::Own(tx) => {
                if let Ok(runtime) = Handle::try_current() {
                    drop(runtime.spawn(tx.into_inner().rollback()));
                }
            }
            Hold::Batch(batch) => batch.release(self.queue),
            // Nothing async runs in `drop`: the transaction ends as its hold drops, its connection
            // closed rather than rolled back, as the handler may have left a statement midway on
            // it; the server rolls the transaction back and the row returns at once.
            Hold::Lent(hold) => drop(hold),
            Hold::Advisory(hold) => {
                #[cfg(feature = "testing")]
                if let Some(connection) = in_process {
                    // The row comes back once its session ends; it is counted first, so a claim
                    // that takes it at once finds it counted.
                    connection.harness.expect(self.queue.name);
                    drop(hold);
                    connection.harness.released();
                    return;
                }
                // Nothing async runs in `drop`: the session closes on the runtime the broker
                // connected on, after releasing the lock, and the row returns at once.
                drop(hold);
            }
            Hold::Lease { book, slot, tx } => {
                // The delivery's own transaction ends first: its connection closes, and the server
                // rolls back what the handler wrote, so the release below waits for nothing of it.
                drop(tx);
                let id = self.claimed.id::<DB>().clone();
                #[cfg(feature = "testing")]
                if let Some(connection) = in_process {
                    release_in_process(connection, book, slot, self.queue, id);
                    return;
                }
                // Nothing async runs in `drop`: the release runs on the runtime the broker
                // connected on, and returns the row to the queue at once. With that runtime gone
                // the task never runs, and the row returns once its lease runs out.
                drop(book.runtime().spawn(release::<DB, Row>(
                    book,
                    slot,
                    self.queue,
                    id,
                    Now::default(),
                )));
            }
        }
        #[cfg(feature = "testing")]
        if let Some(connection) = in_process {
            connection.harness.returned(self.queue.name);
        }
    }
}
