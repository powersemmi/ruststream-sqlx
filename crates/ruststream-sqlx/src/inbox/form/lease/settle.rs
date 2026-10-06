//! The settlement of a lease delivery on a connection of its own, by the lease it holds, and the
//! release of a lease delivery dropped unsettled.

use std::fmt::Debug;
#[cfg(feature = "testing")]
use std::sync::Arc;

use ruststream::AckError;
use sqlx::Pool;

use super::{LeaseBook, Slot};
#[cfg(feature = "testing")]
use crate::inbox::broker::Shared;
use crate::inbox::database::QueueDatabase;
use crate::inbox::delivery::settle::{Step, run_step};
use crate::inbox::engine::{Events, Now, Settled, Settling};
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::queue::Queue;
#[cfg(feature = "testing")]
use crate::inbox::testing::off_clock;
use crate::inbox::tx::PoolTx;

/// Settles the delivery in `slot` of `book`'s subscription on a connection of its own, committed
/// at once, by the lease it holds once no extension of that lease is in flight.
///
/// A statement that fails leaves the row under its lease, which returns it once it runs out, as
/// after a crash.
pub(crate) async fn settle_leased<DB, Row>(
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
    run_leased::<DB, Row>(book.pool(), cx, id, &held, step).await
}

/// Runs the statement of `step` for the row `id` on a connection of `pool`, committed at once,
/// by the lease `held` the delivery held when it left its book.
pub(crate) async fn run_leased<DB, Row>(
    pool: &Pool<DB>,
    cx: &Settling,
    id: &Row::Id,
    held: &Row::Token,
    step: Step,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let overridden = step.overridden(Row::SHAPE);
    let split = matches!(step, Step::DeadLetter(_)) && cx.queue.prepared.dead_letter_then.is_some();
    if !overridden && !split {
        let mut conn = pool.acquire().await?;
        return run_step::<DB, Row>(&mut conn, cx, id, Some(held), step).await;
    }
    // The service's own SQL names no lease, so its transaction first confirms the delivery still
    // holds one: the lease written over itself. A move the dialect splits in two runs in one
    // transaction too, so a half-moved row never shows.
    let mut tx = PoolTx::begin(pool, None).await?;
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

/// Releases the row of the lease delivery in `slot`, dropped unsettled, while it still holds the
/// delivery's lease, so the row returns to the queue at once. `true` when the release took effect.
pub(crate) async fn release<DB, Row>(
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
pub(crate) fn release_in_process<DB, Row>(
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

/// The error of a settlement that found its row under another lease.
pub(crate) fn lease_lost<Id: Debug>(queue: &Queue, id: &Id) -> AckError {
    AckError::Broker(Box::new(SqlxBrokerError::LeaseLost {
        subscription: queue.name.to_owned(),
        table: queue.table.to_owned(),
        row: queue.row,
        id: format!("{id:?}"),
    }))
}
