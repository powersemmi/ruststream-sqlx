//! The row lock form: the claim's transaction holds the rows it took until they settle. A single
//! delivery owns its claim's transaction; a batch's deliveries share one.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use sqlx::Pool;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use crate::inbox::FormDialect;
use crate::inbox::database::QueueDatabase;
use crate::inbox::delivery::settle::{Step, run_step};
use crate::inbox::engine::{Claimed, Claiming, Events, Savepoint, Settled, Settling};
use crate::inbox::queue::Queue;
use crate::inbox::subscriber::{Failed, InboxSubscriber, Taken, begin, take_group};
#[cfg(feature = "testing")]
use crate::inbox::testing::off_clock;
use crate::inbox::transactional::InboxMode;
use crate::inbox::tx::PoolTx;

impl<DB, Row, Mode> InboxSubscriber<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Mode: InboxMode,
{
    /// Ends the transaction of a claim that took no row; a claim by lease committed already.
    pub(crate) async fn end_empty(&self, taken: Taken<DB, Row>) {
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
}

/// Claims up to `cx.limit` rows of a row lock subscription into `rows` (a claim of ids reads them
/// into `ids`), in a transaction of `pool`
/// it returns open: the transaction holds the rows until they settle.
pub(crate) async fn claim_locked<DB, Row>(
    pool: &Pool<DB>,
    cx: &Claiming,
    ids: &mut Row::Ids,
    rows: &mut Vec<Claimed<Row>>,
) -> Result<PoolTx<DB>, Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let queue = cx.queue;
    let claim_failed = |source| -> Failed {
        let statement = queue.prepared.claim.map_or("claim", |claim| claim.sql);
        (statement, source)
    };
    let mut tx = begin(pool, queue).await?;
    let claimed = async {
        // The transaction keeps the group until the delivery settles. A claim that finds
        // it kept takes nothing, and the claim loop ends its transaction.
        if take_group::<DB, Row>(&mut tx, cx, None).await? {
            Row::claim(&mut tx, cx, None, ids, rows)
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
    match claimed {
        Ok(()) => Ok(tx),
        Err(failed) => {
            // A rollback that fails leaves the transaction to its drop, which closes the
            // connection.
            let _ = tx.rollback().await;
            Err(failed)
        }
    }
}

/// The transaction a batch's deliveries share: each settles its own row on it, and the last one to
/// finish ends it. The settlements become durable together: a statement that fails rolls the
/// whole batch back.
pub(crate) struct BatchTx<DB: QueueDatabase> {
    tx: Mutex<Option<PoolTx<DB>>>,
    open: AtomicUsize,
    /// The settlements that wrote something for the commit to keep.
    written: AtomicUsize,
    /// Set by the first statement that fails: the transaction can only roll back from then on.
    failed: AtomicBool,
}

impl<DB: QueueDatabase> BatchTx<DB> {
    pub(crate) fn new(tx: PoolTx<DB>, deliveries: usize) -> Arc<Self> {
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
    pub(crate) fn release(self: Arc<Self>, queue: &'static Queue) {
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

/// Settles a delivery of the row `id` in the claim's transaction `tx`, which it owns: the
/// statement of `step`, then the commit, or the rollback where the statement wrote nothing the
/// commit should keep.
///
/// A statement that fails rolls the transaction back, and the row returns at once; a rollback that
/// fails leaves the transaction to its drop, which closes the connection.
pub(crate) async fn settle_own<DB, Row>(
    mut tx: PoolTx<DB>,
    cx: &Settling,
    id: &Row::Id,
    step: Step,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let settled = match run_step::<DB, Row>(&mut tx, cx, id, None, step).await {
        Ok(settled) => settled,
        Err(error) => {
            let _ = tx.rollback().await;
            return Err(error);
        }
    };
    match settled {
        Settled::Written => tx.commit().await?,
        Settled::Untouched | Settled::Lost => tx.rollback().await?,
    }
    Ok(settled)
}

/// Settles a delivery of the row `id` in the transaction its batch shares: the statement of `step`,
/// after which the last delivery of the batch to finish ends the transaction. `None` where a
/// statement of the batch failed before: the transaction can only roll back, and this delivery
/// runs nothing.
pub(crate) async fn settle_batched<DB, Row>(
    batch: &BatchTx<DB>,
    cx: &Settling,
    id: &Row::Id,
    step: Step,
) -> Option<Result<Settled, sqlx::Error>>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let outcome = if batch.has_failed() {
        // The transaction is aborted: a statement now would only fail too.
        None
    } else {
        let mut guard = batch.tx.lock().await;
        Some(match guard.as_mut() {
            Some(tx) => run_step::<DB, Row>(tx, cx, id, None, step).await,
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
    let finished = batch.finish(wrote, cx.queue).await;
    Some(outcome?.and_then(|settled| finished.map(|()| settled)))
}

/// Where a handler's writes start in the claim's transaction, for a subscription in `Mode` through
/// the dialect `form` shows: the row lock form's savepoint in transactional mode. `None` in the
/// plain mode, and in the lease and advisory lock forms, whose deliveries open transactions of
/// their own after the claim, so a rollback discards the handler's writes alone.
pub(crate) fn savepoint_of<Mode: InboxMode>(form: &FormDialect) -> Option<Savepoint> {
    match form {
        FormDialect::RowLock(dialect) if Mode::TRANSACTIONAL => Some(Savepoint {
            set: dialect.savepoint(),
            rollback_to: dialect.rollback_to_savepoint(),
        }),
        FormDialect::RowLock(_) | FormDialect::Lease(_) | FormDialect::Advisory(_) => None,
    }
}
