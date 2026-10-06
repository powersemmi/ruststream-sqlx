//! `InboxDelivery`: one claimed row in a handler's hands, and how it settles.

use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ruststream::{AckError, HeaderMap, IncomingMessage};
use sync_wrapper::SyncWrapper;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use super::PayloadRow;
#[cfg(feature = "testing")]
use super::broker::Shared;
use super::database::QueueDatabase;
use super::engine::{Claimed, Events, Now, Settled, Settling, Shape};
use super::error::SqlxBrokerError;
use super::lease::{LeaseBook, Slot};
use super::queue::Queue;
#[cfg(feature = "testing")]
use super::testing::{off_clock, returns_after};
use super::tx::Tx;

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

    /// Whether the service implements the event this step runs.
    const fn overridden(self, shape: Shape) -> bool {
        match self {
            Self::Ack => shape.custom_ack,
            Self::Discard => shape.custom_discard,
            Self::Retry => shape.custom_retry,
            Self::RetryAfter(_) => shape.custom_retry_after,
            Self::DeadLetter(_) => shape.custom_dead_letter,
        }
    }
}

/// What holds a delivery's row until it settles.
enum Hold<DB: QueueDatabase, Row: Events<DB>> {
    /// The delivery's own transaction: one claim, one row.
    Own(SyncWrapper<Tx<DB>>),
    /// A batch's transaction: one claim, its rows sharing the transaction.
    Batch(Arc<BatchTx<DB>>),
    /// The lease the claim wrote, in the subscription's book.
    Lease {
        book: &'static LeaseBook<DB, Row>,
        slot: Slot,
    },
}

/// The transaction a batch's deliveries share: each settles its own row on it, and the last one to
/// finish ends it. The settlements become durable together: a statement that fails rolls the
/// whole batch back.
pub(crate) struct BatchTx<DB: QueueDatabase> {
    tx: Mutex<Option<Tx<DB>>>,
    open: AtomicUsize,
    /// The settlements that wrote something for the commit to keep.
    written: AtomicUsize,
    /// Set by the first statement that fails: the transaction can only roll back from then on.
    failed: AtomicBool,
}

impl<DB: QueueDatabase> BatchTx<DB> {
    pub(crate) fn new(tx: Tx<DB>, deliveries: usize) -> Arc<Self> {
        Arc::new(Self {
            tx: Mutex::new(Some(tx)),
            open: AtomicUsize::new(deliveries),
            written: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
        })
    }

    /// Marks one delivery finished; the last one commits what the batch wrote, or rolls back a
    /// batch that wrote nothing or failed.
    async fn finish(&self, wrote: bool, queue: &'static Queue) -> Result<(), sqlx::Error> {
        if wrote {
            self.written.fetch_add(1, Ordering::AcqRel);
        }
        if self.open.fetch_sub(1, Ordering::AcqRel) != 1 {
            return Ok(());
        }
        let Some(tx) = self.tx.lock().await.take() else {
            return Ok(());
        };
        if self.failed.load(Ordering::Acquire) {
            self.undone(queue);
            return tx.rollback().await;
        }
        if self.written.load(Ordering::Acquire) > 0 {
            tx.commit().await
        } else {
            tx.rollback().await
        }
    }

    /// Marks one delivery dropped unsettled: its row stays in the table, and the last delivery
    /// to finish still commits what the others wrote.
    fn release(self: Arc<Self>, queue: &'static Queue) {
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
        let failed = self.failed.load(Ordering::Acquire);
        if failed {
            self.undone(queue);
        }
        let commit = !failed && self.written.load(Ordering::Acquire) > 0;
        // Without a runtime the transaction drops here: its connection closes, and the server rolls
        // it back.
        if let Ok(runtime) = Handle::try_current() {
            runtime.spawn(async move {
                let ended = if commit {
                    tx.commit().await
                } else {
                    tx.rollback().await
                };
                if let Err(error) = ended {
                    tracing::warn!(
                        target: "ruststream_sqlx",
                        subscription = queue.name,
                        table = queue.table,
                        row = queue.row,
                        commit,
                        %error,
                        "a batch's transaction failed to end after a delivery was dropped \
                         unsettled; its connection closes",
                    );
                }
            });
        }
    }

    /// Says how many settlements of a failed batch its rollback undoes.
    fn undone(&self, queue: &'static Queue) {
        let undone = self.written.load(Ordering::Acquire);
        if undone > 0 {
            tracing::warn!(
                target: "ruststream_sqlx",
                subscription = queue.name,
                table = queue.table,
                row = queue.row,
                undone,
                "a settlement of the batch failed, so the batch rolls back: settlements that \
                 succeeded are undone and their rows return",
            );
        }
    }

    /// Records that a statement failed; the transaction can only roll back now.
    fn fail(&self) {
        self.failed.store(true, Ordering::Release);
    }

    fn has_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
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
    hold: Option<Hold<DB, Row>>,
    queue: &'static Queue,
    /// The connection of a delivery claimed in process: its settlement keeps the harness's books.
    #[cfg(feature = "testing")]
    in_process: Option<Arc<Shared<DB>>>,
}

impl<DB: QueueDatabase, Row: Events<DB>> Debug for InboxDelivery<DB, Row> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxDelivery")
            .field("subscription", &self.queue.name)
            .field("id", self.claimed.id::<DB>())
            .finish_non_exhaustive()
    }
}

impl<DB: QueueDatabase, Row: Events<DB> + PayloadRow> InboxDelivery<DB, Row> {
    /// A delivery that owns its claim's transaction.
    pub(crate) fn own(claimed: Claimed<Row>, tx: Tx<DB>, queue: &'static Queue) -> Self {
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

    /// A delivery that holds the lease its claim wrote, entered in its subscription's `book`.
    pub(crate) fn leased(
        claimed: Claimed<Row>,
        book: &'static LeaseBook<DB, Row>,
        lease: Row::Token,
        queue: &'static Queue,
    ) -> Self {
        let slot = book.enter(claimed.id::<DB>(), lease);
        Self::held(claimed, Hold::Lease { book, slot }, queue)
    }

    fn held(mut claimed: Claimed<Row>, hold: Hold<DB, Row>, queue: &'static Queue) -> Self {
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

    /// The declared destination, where the row's `attempt` has reached the declared cap.
    fn spent(&self) -> Option<&'static str> {
        let attempt = self.redelivery_count()?;
        self.queue
            .cap
            .filter(|cap| attempt >= u64::from(cap.attempts.get()))
            .map(|cap| cap.dead_letter)
    }

    /// What `outcome` runs: its own statement, or the declared move once the attempts are spent.
    fn step(&self, outcome: Outcome) -> Step {
        let asked = match outcome {
            Outcome::Ack => return Step::Ack,
            Outcome::Discard => return Step::Discard,
            Outcome::Retry => Step::Retry,
            Outcome::RetryAfter(delay) => Step::RetryAfter(delay),
        };
        let Some(dead_letter) = self.spent() else {
            return asked;
        };
        tracing::warn!(
            target: "ruststream_sqlx",
            subscription = self.queue.name,
            table = self.queue.table,
            row = self.queue.row,
            id = ?self.claimed.id::<DB>(),
            attempt = self.redelivery_count(),
            dead_letter,
            "the row's attempts are spent",
        );
        Step::DeadLetter(dead_letter)
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
        let leased = matches!(self.hold, Some(Hold::Lease { .. }));
        // A retry's row is counted again before the statement that returns it, so a claim that
        // takes it at once finds it counted.
        if step == Step::Retry {
            harness.expect(name);
        }
        let settled = off_clock(self.run(step, harness.now()))
            .await
            .unwrap_or_else(|| Err(AckError::Broker(Box::new(SqlxBrokerError::Closed))));
        match step {
            // A retry of a row lock delivery returns the row whatever its statement did, through
            // the rollback; a leased row comes back only from a release that took effect.
            Step::Retry if leased && settled.is_err() => harness.refused(name),
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
        let cx = Settling { queue, now };
        let id = self.claimed.id::<DB>();
        match hold {
            Hold::Own(tx) => {
                let mut tx = tx.into_inner();
                let settled = match run_step::<DB, Row>(&mut tx, &cx, id, None, step).await {
                    Ok(settled) => settled,
                    Err(source) => {
                        // A rollback that fails leaves the transaction to its drop, which closes
                        // the connection.
                        let _ = tx.rollback().await;
                        return Err(failed(source));
                    }
                };
                match settled {
                    Settled::Written => tx.commit().await.map_err(failed),
                    Settled::Untouched => tx.rollback().await.map_err(failed),
                    Settled::Lost => {
                        tx.rollback().await.map_err(failed)?;
                        Err(lease_lost(queue, id))
                    }
                }
            }
            Hold::Lease { book, slot } => {
                match settle_leased::<DB, Row>(book, slot, &cx, id, step)
                    .await
                    .map_err(failed)?
                {
                    Settled::Written | Settled::Untouched => Ok(()),
                    Settled::Lost => Err(lease_lost(queue, id)),
                }
            }
            Hold::Batch(batch) => {
                let outcome = if batch.has_failed() {
                    // The transaction is aborted: a statement now would only fail too.
                    None
                } else {
                    let mut guard = batch.tx.lock().await;
                    Some(match guard.as_mut() {
                        Some(tx) => run_step::<DB, Row>(tx, &cx, id, None, step).await,
                        None => Err(sqlx::Error::Protocol(
                            "the batch's transaction ended before this delivery settled".to_owned(),
                        )),
                    })
                };
                if matches!(outcome, Some(Err(_))) {
                    batch.fail();
                }
                // The batch counts this delivery finished whatever its statement did, so the last
                // one still ends the transaction.
                let wrote = matches!(outcome, Some(Ok(Settled::Written)));
                let finished = batch.finish(wrote, queue).await;
                let Some(outcome) = outcome else {
                    return Err(AckError::Broker(Box::new(
                        SqlxBrokerError::BatchRolledBack {
                            subscription: queue.name.to_owned(),
                            table: queue.table.to_owned(),
                            row: queue.row,
                        },
                    )));
                };
                let settled = outcome.map_err(failed)?;
                finished.map_err(failed)?;
                match settled {
                    Settled::Written | Settled::Untouched => Ok(()),
                    Settled::Lost => Err(lease_lost(queue, id)),
                }
            }
        }
    }
}

/// The error of a settlement that found its row under another lease.
fn lease_lost<Id: Debug>(queue: &Queue, id: &Id) -> AckError {
    AckError::Broker(Box::new(SqlxBrokerError::LeaseLost {
        subscription: queue.name.to_owned(),
        table: queue.table.to_owned(),
        row: queue.row,
        id: format!("{id:?}"),
    }))
}

/// Settles the delivery in `slot` of `book`'s subscription on a connection of its own, committed
/// at once, by the lease it holds once no extension of that lease is in flight.
///
/// A statement that fails leaves the row under its lease, which returns it once it runs out, as
/// after a crash.
async fn settle_leased<DB, Row>(
    book: &LeaseBook<DB, Row>,
    slot: Slot,
    cx: &Settling,
    id: &Row::Id,
    step: Step,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    // The delivery leaves the book before anything else, so a settlement dropped midway leaves
    // the keeper no lease to extend.
    let Ok(held) = book.settling(slot).await else {
        // The keeper found the row under another lease: nothing this settlement runs takes effect.
        return Ok(Settled::Lost);
    };
    let held = &held;
    let overridden = step.overridden(Row::SHAPE);
    let split = matches!(step, Step::DeadLetter(_)) && cx.queue.prepared.dead_letter_then.is_some();
    if !overridden && !split {
        let mut conn = book.pool().acquire().await?;
        return run_step::<DB, Row>(&mut conn, cx, id, Some(held), step).await;
    }
    // The service's own SQL names no lease, so its transaction first confirms the delivery still
    // holds one: the lease written over itself. A move the dialect splits in two runs in one
    // transaction too, so a half-moved row never shows.
    let mut tx = Tx::begin(book.pool(), None).await?;
    let settled = async {
        if overridden && Row::extend(&mut tx, cx, id, held, held).await? == Settled::Lost {
            return Ok(Settled::Lost);
        }
        run_step::<DB, Row>(&mut tx, cx, id, Some(held), step).await
    }
    .await;
    match settled {
        Ok(Settled::Written | Settled::Untouched) => tx.commit().await?,
        Ok(Settled::Lost) => tx.rollback().await?,
        Err(error) => {
            // A rollback that fails leaves the transaction to its drop, which closes the
            // connection.
            let _ = tx.rollback().await;
            return Err(error);
        }
    }
    settled
}

/// Runs the statement of `step` for the row `id` on `conn`; `held` is the delivery's lease in the
/// lease form.
async fn run_step<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Settling,
    id: &Row::Id,
    held: Option<&Row::Token>,
    step: Step,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    match step {
        Step::Ack => Row::ack(conn, cx, id, held).await,
        Step::Discard => Row::discard(conn, cx, id, held).await,
        Step::Retry => Row::retry(conn, cx, id, held).await,
        Step::RetryAfter(delay) => Row::retry_after(conn, cx, id, held, delay).await,
        Step::DeadLetter(destination) => Row::dead_letter(conn, cx, id, held, destination).await,
    }
}

/// Releases the row of the lease delivery in `slot`, dropped unsettled, while it still holds the
/// delivery's lease, so the row returns to the queue at once. `true` when the release took effect.
async fn release<DB, Row>(
    book: &'static LeaseBook<DB, Row>,
    slot: Slot,
    queue: &'static Queue,
    id: Row::Id,
    now: Now,
) -> bool
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let cx = Settling { queue, now };
    match settle_leased::<DB, Row>(book, slot, &cx, &id, Step::Retry).await {
        Ok(settled) => settled == Settled::Written,
        Err(error) => {
            tracing::warn!(
                target: "ruststream_sqlx",
                subscription = queue.name,
                table = queue.table,
                row = queue.row,
                ?id,
                %error,
                "a delivery dropped unsettled could not release its row; the row returns once its \
                 lease runs out",
            );
            false
        }
    }
}

/// The release of an in-process lease delivery dropped unsettled: off a paused clock, and in the
/// harness's books, where the row is counted again before it is back.
#[cfg(feature = "testing")]
fn release_in_process<DB, Row>(
    connection: Arc<Shared<DB>>,
    book: &'static LeaseBook<DB, Row>,
    slot: Slot,
    queue: &'static Queue,
    id: Row::Id,
) where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    connection.harness.expect(queue.name);
    drop(book.runtime().spawn(async move {
        let now = connection.harness.now();
        let released = off_clock(release::<DB, Row>(book, slot, queue, id, now))
            .await
            .unwrap_or(false);
        if !released {
            connection.harness.refused(queue.name);
        }
        connection.harness.released();
    }));
}

impl<DB, Row> IncomingMessage for InboxDelivery<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
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
            // An unsettled delivery's transaction rolls back on the runtime, which returns the
            // row to the queue at once. Without a runtime the transaction drops here: its
            // connection closes, and the server rolls it back.
            Hold::Own(tx) => {
                if let Ok(runtime) = Handle::try_current() {
                    drop(runtime.spawn(tx.into_inner().rollback()));
                }
            }
            Hold::Batch(batch) => batch.release(self.queue),
            Hold::Lease { book, slot } => {
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
