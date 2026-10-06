//! The settlement of a transactional delivery: in the transaction it lent its handler, which
//! acknowledgement commits with the handler's writes and every other outcome discards first.

use std::fmt::Debug;

use ruststream::{AckError, IncomingMessage};

use super::{Hold, InboxDelivery, Step, run_step};
use crate::inbox::PayloadRow;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{Events, Settled, Settling};
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::keys::{TransactionalDelivery, TxContext};
use crate::inbox::queue::Queue;
use crate::inbox::transactional::{Returned, Transactional};
use crate::inbox::tx::PoolTx;

impl<DB, Row> TransactionalDelivery<DB> for InboxDelivery<DB, Row, Transactional>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    /// Copies two references, where the transaction waits and the pool, and the attempt.
    ///
    /// # Panics
    ///
    /// Never: a transactional subscription builds each delivery with a hold that lends its
    /// transaction, and keeps it until the delivery settles, which consumes the delivery.
    fn tx_context(&self) -> TxContext<DB> {
        let lender = match &self.hold {
            Some(Hold::Lent(hold)) => hold.lender(),
            Some(Hold::Own(_) | Hold::Batch(_) | Hold::Lease { .. } | Hold::Advisory(_)) | None => {
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
pub(super) async fn settle_lent<DB, Row>(
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

/// The error of a settlement that found the delivery's transaction still lent to its handler.
pub(super) fn transaction_held<Id: Debug>(queue: &Queue, id: &Id) -> AckError {
    AckError::Broker(Box::new(SqlxBrokerError::TransactionHeld {
        subscription: queue.name.to_owned(),
        table: queue.table.to_owned(),
        row: queue.row,
        id: format!("{id:?}"),
    }))
}
