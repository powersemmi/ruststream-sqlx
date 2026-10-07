//! The advisory lock form's settlement: the statement on the session that holds the row's key,
//! then the key's release on that same session.

use sqlx_core::transaction::TransactionManager;

use super::session::Session;
use super::{LockHold, Locked};
use crate::inbox::database::QueueDatabase;
use crate::inbox::delivery::settle::{Step, run_step};
use crate::inbox::engine::{Events, Settled, Settling};
use crate::inbox::transactional::settle::settle_in_session;

/// Settles the delivery of `hold` on its `lent` session, which holds the lock on its row's key: the
/// statement of `step`, then the release of the key, never before the statement returned, on the
/// same session. The session then goes back to the pool, or closes where the database did not
/// confirm the release, and the delivery leaves the book. Dropped midway, the settlement closes
/// the session after an unlock of the key.
///
/// In transactional mode the step settles in the transaction the handler wrote in, which ends
/// before the release: committed by an acknowledgement, rolled back first by any other step, and
/// by any step after a handler that `panicked`.
///
/// A step that changes no row has still settled: the lock, not a token, holds the row.
pub(crate) async fn settle_advised<DB, Row>(
    hold: LockHold<DB>,
    mut lent: Locked<'static, DB>,
    panicked: bool,
    cx: &Settling,
    id: &Row::Id,
    step: Step,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let settled = if cx.queue.prepared.transactional {
        settle_in_session::<DB, Row>(lent.session(), cx, id, step, panicked).await
    } else {
        run_advised::<DB, Row>(lent.session(), cx, id, step).await
    };
    if hold.book().process() {
        lent.session().free_in_process();
    } else {
        let unlocked = {
            let (conn, key) = lent.conn_and_key();
            Row::unlock(conn, cx, key).await
        };
        let key = lent.key();
        match unlocked {
            Ok(true) => lent.session().set_locked(false),
            Ok(false) => tracing::warn!(
                target: "ruststream_sqlx",
                subscription = cx.queue.name,
                table = cx.queue.table,
                row = cx.queue.row,
                ?id,
                key,
                "a settlement's unlock found the session not holding the row's key; the session \
                 closes",
            ),
            Err(error) => tracing::warn!(
                target: "ruststream_sqlx",
                subscription = cx.queue.name,
                table = cx.queue.table,
                row = cx.queue.row,
                ?id,
                key,
                %error,
                "a settlement's unlock failed; the session closes, which ends its lock",
            ),
        }
    }
    let (session, key) = lent.into_parts();
    session.release();
    hold.leave(key.into_owned());
    settled
}

/// Runs the statement of `step` for the row `id` on an advisory delivery's `session`. A dead letter
/// the dialect splits in two runs in one transaction on the session, so a half-moved row never
/// shows.
pub(crate) async fn run_advised<DB, Row>(
    session: &mut Session<DB>,
    cx: &Settling,
    id: &Row::Id,
    step: Step,
) -> Result<Settled, sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let split = matches!(step, Step::DeadLetter(_)) && cx.queue.prepared.dead_letter_then.is_some();
    if !split {
        return run_step::<DB, Row>(session.conn(), cx, id, None, step).await;
    }
    // Open before the statement leaves: a begin dropped midway may have started the transaction,
    // and a session that may hold one closes instead of going back to the pool.
    session.set_open(true);
    DB::TransactionManager::begin(session.conn(), None).await?;
    match run_step::<DB, Row>(session.conn(), cx, id, None, step).await {
        Ok(settled) => {
            DB::TransactionManager::commit(session.conn()).await?;
            session.set_open(false);
            Ok(settled)
        }
        Err(error) => {
            // A rollback that fails leaves the transaction open, and the session closes.
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
