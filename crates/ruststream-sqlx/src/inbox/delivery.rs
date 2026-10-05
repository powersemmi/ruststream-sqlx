//! `InboxDelivery`: one claimed row in a handler's hands, and how it settles.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ruststream::{AckError, HeaderMap, IncomingMessage};
use sqlx::Transaction;
use sync_wrapper::SyncWrapper;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use super::PayloadRow;
#[cfg(feature = "testing")]
use super::broker::Shared;
use super::database::QueueDatabase;
use super::engine::{Claimed, Events, Now, Released, Settling};
use super::error::SqlxBrokerError;
use super::queue::Queue;
#[cfg(feature = "testing")]
use super::testing::{off_clock, returns_after};

/// What a handler's outcome asks of the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Ack,
    Discard,
    Retry,
    RetryAfter(Duration),
}

/// The statement an outcome runs, once the cap is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    Ack,
    Discard,
    Retry,
    RetryAfter(Duration),
    DeadLetter(&'static str),
}

impl Step {
    const fn event(self) -> &'static str {
        match self {
            Self::Ack => "ack",
            Self::Discard => "discard",
            Self::Retry => "retry",
            Self::RetryAfter(_) => "retry_after",
            Self::DeadLetter(_) => "dead_letter",
        }
    }
}

/// Where a delivery's transaction lives.
enum Hold<DB: QueueDatabase> {
    /// The delivery's own: one claim, one row.
    Own(SyncWrapper<Transaction<'static, DB>>),
    /// A batch's: one claim, its rows sharing the transaction.
    Batch(Arc<BatchTx<DB>>),
}

/// The transaction a batch's deliveries share: each settles its own row on it, and the last one to
/// finish ends it.
pub(crate) struct BatchTx<DB: QueueDatabase> {
    tx: Mutex<Option<Transaction<'static, DB>>>,
    open: AtomicUsize,
    wrote: AtomicBool,
}

impl<DB: QueueDatabase> BatchTx<DB> {
    pub(crate) fn new(tx: Transaction<'static, DB>, deliveries: usize) -> Arc<Self> {
        Arc::new(Self {
            tx: Mutex::new(Some(tx)),
            open: AtomicUsize::new(deliveries),
            wrote: AtomicBool::new(false),
        })
    }

    /// Marks one delivery finished; the last one commits what the batch wrote, or rolls back a
    /// batch that wrote nothing.
    async fn finish(&self, wrote: bool) -> Result<(), sqlx::Error> {
        if wrote {
            self.wrote.store(true, Ordering::Release);
        }
        if self.open.fetch_sub(1, Ordering::AcqRel) != 1 {
            return Ok(());
        }
        let Some(tx) = self.tx.lock().await.take() else {
            return Ok(());
        };
        if self.wrote.load(Ordering::Acquire) {
            tx.commit().await
        } else {
            tx.rollback().await
        }
    }

    /// Marks one delivery dropped unsettled: its row stays in the table, and the last delivery
    /// to finish still commits what the others wrote.
    fn release(self: Arc<Self>) {
        if self.open.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        // Every other delivery has finished, so nothing else holds the lock.
        let Ok(mut held) = self.tx.try_lock() else {
            return;
        };
        let Some(tx) = held.take() else {
            return;
        };
        drop(held);
        if !self.wrote.load(Ordering::Acquire) {
            // Dropping the transaction rolls it back.
            return;
        }
        if let Ok(runtime) = Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = tx.commit().await {
                    tracing::warn!(target: "ruststream_sqlx", %error, "a batch's commit failed after a delivery was dropped unsettled");
                }
            });
        }
    }
}

/// One claimed row in a handler's hands.
///
/// The payload is lent from the row, without a copy. The delivery holds the claim's transaction;
/// settling it runs one statement and commits, and dropping it unsettled rolls the transaction
/// back, which returns the row to the queue at once.
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
pub struct InboxDelivery<DB: QueueDatabase, Row: Events<DB>> {
    claimed: Claimed<Row>,
    headers: HeaderMap,
    hold: Option<Hold<DB>>,
    queue: &'static Queue,
    /// The connection of a delivery claimed in process: its settlement keeps the harness's books.
    #[cfg(feature = "testing")]
    in_process: Option<Arc<Shared<DB>>>,
}

impl<DB: QueueDatabase, Row: Events<DB>> fmt::Debug for InboxDelivery<DB, Row> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxDelivery")
            .field("subscription", &self.queue.name)
            .field("id", self.claimed.id::<DB>())
            .finish_non_exhaustive()
    }
}

impl<DB: QueueDatabase, Row: Events<DB> + PayloadRow> InboxDelivery<DB, Row> {
    /// A delivery that owns its claim's transaction.
    pub(crate) fn own(
        claimed: Claimed<Row>,
        tx: Transaction<'static, DB>,
        queue: &'static Queue,
    ) -> Self {
        Self::held(claimed, Hold::Own(SyncWrapper::new(tx)), queue)
    }

    /// A delivery of a batch, sharing its claim's transaction.
    pub(crate) fn batched(
        claimed: Claimed<Row>,
        batch: Arc<BatchTx<DB>>,
        queue: &'static Queue,
    ) -> Self {
        Self::held(claimed, Hold::Batch(batch), queue)
    }

    fn held(claimed: Claimed<Row>, hold: Hold<DB>, queue: &'static Queue) -> Self {
        let headers = match &claimed {
            Claimed::Row(row) => Row::headers(row),
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
        };
        Self {
            claimed,
            headers,
            hold: Some(hold),
            queue,
            #[cfg(feature = "testing")]
            in_process: None,
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

    /// Whether the row's attempts are spent: its `attempt` has reached the declared cap.
    fn spent(&self) -> bool {
        let attempt = self.redelivery_count();
        self.queue
            .max_attempts
            .is_some_and(|cap| attempt.is_some_and(|attempt| attempt >= u64::from(cap.get())))
    }

    /// Where a retry of this delivery goes instead of back to its queue: the declared
    /// destination, or the discard once the attempts are spent with none declared.
    fn redirected(&self) -> Option<Step> {
        match (self.queue.dead_letter, self.queue.max_attempts) {
            (Some(destination), None) => Some(Step::DeadLetter(destination)),
            (Some(destination), Some(_)) if self.spent() => Some(Step::DeadLetter(destination)),
            (None, Some(_)) if self.spent() => Some(Step::Discard),
            _ => None,
        }
    }

    /// What `outcome` runs: its own statement, or the declared move once the attempts are spent.
    fn step(&self, outcome: Outcome) -> Step {
        let asked = match outcome {
            Outcome::Ack => return Step::Ack,
            Outcome::Discard => return Step::Discard,
            Outcome::Retry => Step::Retry,
            Outcome::RetryAfter(delay) => Step::RetryAfter(delay),
        };
        if self.spent() {
            tracing::warn!(
                target: "ruststream_sqlx",
                subscription = self.queue.name,
                table = self.queue.table,
                row = self.queue.row,
                id = ?self.claimed.id::<DB>(),
                attempt = self.redelivery_count(),
                dead_letter = self.queue.dead_letter,
                "the row's attempts are spent",
            );
        }
        self.redirected().unwrap_or(asked)
    }

    async fn settle(self, outcome: Outcome) -> Result<(), AckError> {
        let step = self.step(outcome);
        #[cfg(feature = "testing")]
        if let Some(connection) = self.in_process.clone() {
            return self.settle_in_process(connection, step).await;
        }
        self.run(step, Now::default()).await
    }

    /// The settlement of an in-process delivery: on the test's clock, off a paused one, and in
    /// the harness's books.
    #[cfg(feature = "testing")]
    async fn settle_in_process(
        self,
        connection: Arc<Shared<DB>>,
        step: Step,
    ) -> Result<(), AckError> {
        let harness = &connection.harness;
        let name = self.queue.name;
        let settled = off_clock(self.run(step, harness.now()))
            .await
            .unwrap_or_else(|| Err(AckError::Broker(Box::new(SqlxBrokerError::Closed))));
        match step {
            // The row is back in the table: counted again before this delivery leaves the books.
            Step::Retry => harness.expect(name),
            // The row comes back once its delay runs out, which `TestApp::advance` fires.
            Step::RetryAfter(delay) if settled.is_ok() => returns_after(&connection, name, delay),
            _ => {}
        }
        harness.released();
        settled
    }

    /// Runs the statement of `step` and ends the transaction it ran on.
    async fn run(mut self, step: Step, now: Now) -> Result<(), AckError> {
        // A delivery holds its transaction until it settles, and settling consumes it.
        let Some(hold) = self.hold.take() else {
            return Ok(());
        };
        let queue = self.queue;
        let failed = |source: sqlx::Error| {
            AckError::Broker(Box::new(SqlxBrokerError::Sqlx {
                subscription: queue.name.to_owned(),
                table: queue.table.to_owned(),
                row: queue.row,
                statement: step.event(),
                source: Box::new(source),
            }))
        };
        let cx = Settling {
            queue: queue.name,
            prepared: &queue.prepared,
            now,
        };
        let id = self.claimed.id::<DB>();
        match hold {
            Hold::Own(tx) => {
                let mut tx = tx.into_inner();
                let released = run_step::<DB, Row>(&mut tx, &cx, id, step)
                    .await
                    .map_err(failed)?;
                match released {
                    Released::Written => tx.commit().await.map_err(failed),
                    Released::Untouched => tx.rollback().await.map_err(failed),
                }
            }
            Hold::Batch(batch) => {
                let outcome = {
                    let mut guard = batch.tx.lock().await;
                    match guard.as_mut() {
                        Some(tx) => run_step::<DB, Row>(tx, &cx, id, step).await,
                        None => Err(sqlx::Error::Protocol(
                            "the batch's transaction ended before this delivery settled".to_owned(),
                        )),
                    }
                };
                // The batch counts this delivery finished whatever its statement did, so the last
                // one still ends the transaction.
                let wrote = matches!(outcome, Ok(Released::Written));
                let finished = batch.finish(wrote).await;
                outcome.map_err(failed)?;
                finished.map_err(failed)
            }
        }
    }
}

/// Runs the statement of `step` for the row `id` on `conn`.
async fn run_step<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Settling<'_>,
    id: &Row::Id,
    step: Step,
) -> Result<Released, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    match step {
        Step::Ack => Row::ack(conn, cx, id).await.map(|()| Released::Written),
        Step::Discard => Row::discard(conn, cx, id).await.map(|()| Released::Written),
        Step::Retry => Row::retry(conn, cx, id).await,
        Step::RetryAfter(delay) => Row::retry_after(conn, cx, id, delay)
            .await
            .map(|()| Released::Written),
        Step::DeadLetter(destination) => Row::dead_letter(conn, cx, id, destination)
            .await
            .map(|()| Released::Written),
    }
}

impl<DB, Row> IncomingMessage for InboxDelivery<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    fn payload(&self) -> &[u8] {
        match &self.claimed {
            Claimed::Row(row) => row.payload(),
            Claimed::Missing(_) => &[],
        }
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn partition_key(&self) -> Option<&[u8]> {
        match &self.claimed {
            Claimed::Row(row) => Row::partition_key(row),
            Claimed::Missing(_) => None,
        }
    }

    fn redelivery_count(&self) -> Option<u64> {
        match &self.claimed {
            Claimed::Row(row) => Row::attempt(row),
            Claimed::Missing(_) => None,
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
        // A delivery whose retry moves the row elsewhere has no delayed redelivery to offer: the
        // runtime then settles it with `nack(true)`, which moves it, and expects nothing back.
        Row::SHAPE.native_retry_after() && self.redirected().is_none()
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        if !self.supports_nack_after() {
            return Err(AckError::Unsupported);
        }
        self.settle(Outcome::RetryAfter(delay)).await
    }
}

impl<DB: QueueDatabase, Row: Events<DB>> Drop for InboxDelivery<DB, Row> {
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
            // An unsettled delivery's transaction rolls back as it drops, which returns the row
            // to the queue at once.
            Hold::Own(_) => {}
            Hold::Batch(batch) => batch.release(),
        }
        #[cfg(feature = "testing")]
        if let Some(connection) = in_process {
            connection.harness.returned(self.queue.name);
        }
    }
}
