//! The settlement of a transactional delivery: in the transaction it lent its handler, which
//! acknowledgement commits with the handler's writes and every other outcome discards first.

use std::fmt::Debug;

use ruststream::{AckError, IncomingMessage};
use sqlx_core::transaction::TransactionManager;

use super::{Lender, Returned, Transactional};
use crate::inbox::database::QueueDatabase;
use crate::inbox::delivery::settle::{Step, run_step};
use crate::inbox::delivery::{Hold, InboxDelivery};
use crate::inbox::engine::{Events, Settled, Settling};
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::form::advisory::session::Session;
use crate::inbox::form::advisory::settle::run_advised;
use crate::inbox::form::lease::settle::{run_leased, settle_leased};
use crate::inbox::form::lease::{LeaseBook, Slot};
use crate::inbox::keys::{TransactionalDelivery, TxContext};
use crate::inbox::queue::Queue;
use crate::inbox::tx::PoolTx;

impl<DB, Row> TransactionalDelivery<DB> for InboxDelivery<DB, Row, Transactional>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    /// Copies two references, where the transaction waits and the pool, and the attempt.
    ///
    /// # Panics
    ///
    /// Never: a transactional subscription builds each delivery with a hold that lends its
    /// transaction, and keeps it until the delivery settles, which consumes the delivery.
    fn tx_context(&self) -> TxContext<DB> {
        let lender = match &self.hold {
            Some(Hold::Lent(hold) | Hold::Lease { tx: Some(hold), .. }) => hold.lender(),
            Some(Hold::Advisory(hold)) => Lender::Lock(hold.book(), hold.slot()),
            Some(Hold::Own(_) | Hold::Batch(_) | Hold::Lease { tx: None, .. }) | None => {
                unreachable!(
                    "a transactional delivery holds a transaction it lends until it settles"
                )
            }
        };
        TxContext::new(lender, self.pool, self.redelivery_count())
    }
}

/// Settles a transactional delivery of the row `id` in the transaction it lent its handler, as
/// `returned` holds it: the statement of `step`, then the commit, or the rollback where the
/// statement wrote nothing the commit should keep.
///
/// A statement that fails rolls the whole transaction back, the handler's writes with it, and the
/// row returns at once; a rollback that fails leaves the transaction to its drop, which closes the
/// connection.
pub(crate) async fn settle_lent<DB, Row>(
    returned: Returned<DB>,
    cx: &Settling,
    id: &Row::Id,
    step: Step,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let Returned { mut tx, panicked } = returned;
    // Only an acknowledgement of a handler that ended keeps what the handler wrote.
    let discard = step != Step::Ack || panicked;
    let settled = match run_lent::<DB, Row>(&mut tx, cx, id, step, discard).await {
        Ok(settled) => settled,
        Err(error) => {
            let _ = tx.rollback().await;
            return Err(error);
        }
    };
    match settled {
        Settled::Written => tx.commit().await?,
        // An acknowledgement commits what the handler wrote, whatever its own statement changed.
        Settled::Untouched if !discard => tx.commit().await?,
        Settled::Untouched | Settled::Lost => tx.rollback().await?,
    }
    Ok(settled)
}

/// Runs the statement of `step` for the row `id` in the transaction `tx` a transactional delivery
/// lent its handler, after discarding what the handler wrote where `discard` says so: rolled back
/// to the savepoint the claim set.
async fn run_lent<DB, Row>(
    tx: &mut PoolTx<DB>,
    cx: &Settling,
    id: &Row::Id,
    step: Step,
    discard: bool,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    if discard && let Some(savepoint) = cx.queue.prepared.savepoint {
        DB::execute_text(tx, savepoint.rollback_to).await?;
    }
    run_step::<DB, Row>(tx, cx, id, None, step).await
}

/// Settles a transactional lease delivery of the row `id`, whose lease waits at `slot` of `book`,
/// in its own transaction, which it lent its handler and which `returned` holds.
///
/// Only an acknowledgement of a handler that ended keeps what the handler wrote: it runs by the
/// lease the delivery holds, inside the transaction, and commits; it finds the row under another
/// lease and rolls everything back as [`Settled::Lost`]. It takes the lease without waiting for an
/// extension in flight, which may wait for what the transaction holds. Every other step rolls the
/// transaction back first, then settles as outside transactional mode.
///
/// An acknowledgement that fails rolls the transaction back and returns the row at once, by its
/// lease; where that release fails too, the row returns once the lease runs out.
pub(crate) async fn settle_leased_lent<DB, Row>(
    book: &'static LeaseBook<DB, Row>,
    slot: Slot,
    returned: Returned<DB>,
    cx: &Settling,
    id: &Row::Id,
    step: Step,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let Returned { mut tx, panicked } = returned;
    if step != Step::Ack || panicked {
        // A rollback that fails leaves the transaction to its drop, which closes the connection.
        let _ = tx.rollback().await;
        return settle_leased::<DB, Row>(book, slot, cx, id, step).await;
    }
    let Ok(held) = book.take_ahead(slot) else {
        // The keeper found the row under another lease.
        tx.rollback().await?;
        return Ok(Settled::Lost);
    };
    let acknowledged = match ack_by_lease::<DB, Row>(&mut tx, cx, id, &held).await {
        Ok(Settled::Lost) => return tx.rollback().await.map(|()| Settled::Lost),
        Ok(settled) => tx.commit().await.map(|()| settled),
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    };
    if acknowledged.is_err() {
        // The release matches the lease the delivery holds, so it changes nothing where the
        // acknowledgement took effect after all.
        let _ =
            run_leased::<DB, Row>(book.pool(), book.runtime(), cx, id, &held, Step::Retry).await;
    }
    acknowledged
}

/// Runs the acknowledgement of the row `id` in `tx`, by the lease `held`: a service's own
/// statement, which names no lease, after the lease written over itself confirms the row still
/// holds it.
async fn ack_by_lease<DB, Row>(
    tx: &mut PoolTx<DB>,
    cx: &Settling,
    id: &Row::Id,
    held: &Row::Token,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    if Step::Ack.overridden(Row::SHAPE)
        && Row::extend(tx, cx, id, held, held).await? == Settled::Lost
    {
        return Ok(Settled::Lost);
    }
    run_step::<DB, Row>(tx, cx, id, Some(held), Step::Ack).await
}

/// Settles a transactional advisory delivery of the row `id` on its `session`, whose transaction
/// the handler wrote in: an acknowledgement of a handler that ended runs its statement in the
/// transaction and commits; every other step, and any after a handler that `panicked`, rolls the
/// transaction back first, then runs as outside transactional mode.
///
/// A statement that fails rolls the transaction back. A rollback that fails leaves the session's
/// transaction open, and the session closes instead of going back to the pool.
pub(crate) async fn settle_in_session<DB, Row>(
    session: &mut Session<DB>,
    cx: &Settling,
    id: &Row::Id,
    step: Step,
    panicked: bool,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    if step != Step::Ack || panicked {
        DB::TransactionManager::rollback(session.conn()).await?;
        session.set_open(false);
        return run_advised::<DB, Row>(session, cx, id, step).await;
    }
    match run_step::<DB, Row>(session.conn(), cx, id, None, step).await {
        Ok(settled) => {
            DB::TransactionManager::commit(session.conn()).await?;
            session.set_open(false);
            Ok(settled)
        }
        Err(error) => {
            if DB::TransactionManager::rollback(session.conn())
                .await
                .is_ok()
            {
                session.set_open(false);
            }
            Err(error)
        }
    }
}

/// The error of a settlement that found the delivery's transaction still lent to its handler.
pub(crate) fn transaction_held<Id: Debug>(queue: &Queue, id: &Id) -> AckError {
    AckError::Broker(Box::new(SqlxBrokerError::TransactionHeld {
        subscription: queue.name.to_owned(),
        table: queue.table.to_owned(),
        row: queue.row,
        id: format!("{id:?}"),
    }))
}
