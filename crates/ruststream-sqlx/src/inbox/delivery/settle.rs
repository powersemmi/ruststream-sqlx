//! How an [`InboxDelivery`] settles: the statement a handler's outcome runs once the cap on
//! attempts is read, and the dispatch of that statement to the form that holds the row.

#[cfg(feature = "testing")]
use std::sync::Arc;
use std::time::Duration;

use ruststream::{AckError, IncomingMessage};

use super::{Hold, InboxDelivery};
#[cfg(feature = "testing")]
use crate::inbox::broker::Shared;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{Events, Now, Settled, Settling, Shape};
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::form::advisory::Unlent;
use crate::inbox::form::advisory::settle::settle_advised;
use crate::inbox::form::lease::settle::{lease_lost, settle_leased};
use crate::inbox::form::row_lock::{settle_batched, settle_own};
#[cfg(feature = "testing")]
use crate::inbox::testing::{off_clock, returns_after};
use crate::inbox::transactional::InboxMode;
use crate::inbox::transactional::settle::{settle_leased_lent, settle_lent, transaction_held};

/// What a handler's outcome asks of the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome {
    Ack,
    Discard,
    Retry,
    RetryAfter(Duration),
}

/// The statement an outcome runs, once the cap is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    Ack,
    Discard,
    Retry,
    RetryAfter(Duration),
    DeadLetter(&'static str),
}

impl Step {
    pub(crate) const fn event(self) -> &'static str {
        match self {
            Self::Ack => "ack",
            Self::Discard => "discard",
            Self::Retry => "retry",
            Self::RetryAfter(_) => "retry_after",
            Self::DeadLetter(_) => "dead_letter",
        }
    }

    /// Whether the service implements the event this step runs.
    pub(crate) const fn overridden(self, shape: Shape) -> bool {
        match self {
            Self::Ack => shape.custom_ack,
            Self::Discard => shape.custom_discard,
            Self::Retry => shape.custom_retry,
            Self::RetryAfter(_) => shape.custom_retry_after,
            Self::DeadLetter(_) => shape.custom_dead_letter,
        }
    }
}

impl<DB, Row, Mode> InboxDelivery<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Mode: InboxMode,
{
    /// The declared destination, where the row's `attempt` has reached the declared cap.
    pub(super) fn spent(&self) -> Option<&'static str> {
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

    pub(super) async fn settle(self, outcome: Outcome) -> Result<(), AckError> {
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
        let advised = matches!(self.hold, Some(Hold::Advisory(_)));
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
            // the rollback, and an advisory one through the unlock or the close of its session; a
            // leased row comes back only from a release that took effect, and a row whose session
            // `shutdown` released never comes back to this connection.
            Step::Retry if (leased || (advised && is_closed(&settled))) && settled.is_err() => {
                harness.refused(name);
            }
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
        let settled = match hold {
            Hold::Own(tx) => settle_own::<DB, Row>(tx.into_inner(), &cx, id, step).await,
            Hold::Lent(hold) => {
                let Some(returned) = hold.settle() else {
                    // Why a runtime error: the framework hands a handler its `Ctx` values owned
                    // and `'static`, so a handler may move its `Tx` somewhere that outlives it.
                    return Err(transaction_held(queue, id));
                };
                settle_lent::<DB, Row>(returned, &cx, id, step).await
            }
            Hold::Lease {
                book,
                slot,
                tx: None,
            } => settle_leased::<DB, Row>(book, slot, &cx, id, step).await,
            Hold::Lease {
                book,
                slot,
                tx: Some(hold),
            } => {
                let Some(returned) = hold.settle() else {
                    // Why a runtime error: as in the row lock form, a handler may move its `Tx`
                    // somewhere that outlives it. The row goes back once its lease runs out, which
                    // the keeper extends no more; nothing here waits for an extension the kept
                    // transaction may hold up.
                    let _ = book.take_ahead(slot);
                    return Err(transaction_held(queue, id));
                };
                settle_leased_lent::<DB, Row>(book, slot, returned, &cx, id, step).await
            }
            Hold::Advisory(hold) => match hold.lend() {
                Ok((lent, panicked)) => {
                    settle_advised::<DB, Row>(hold, lent, panicked, &cx, id, step).await
                }
                // Why a runtime error: `shutdown` releases the lock of a delivery whose handler
                // still works, a race between two tasks no type can order.
                Err(Unlent::Released) => {
                    return Err(AckError::Broker(Box::new(SqlxBrokerError::Closed)));
                }
                // Why a runtime error: as in the row lock form, a handler may move its `Tx`
                // somewhere that outlives it. The slot waits for the session the handler kept, and
                // ends it when it comes back.
                Err(Unlent::Borrowed) => return Err(transaction_held(queue, id)),
            },
            Hold::Batch(batch) => {
                let Some(settled) = settle_batched::<DB, Row>(&batch, &cx, id, step).await else {
                    return Err(AckError::Broker(Box::new(
                        SqlxBrokerError::BatchRolledBack {
                            subscription: queue.name.to_owned(),
                            table: queue.table.to_owned(),
                            row: queue.row,
                        },
                    )));
                };
                settled
            }
        };
        match settled.map_err(failed)? {
            Settled::Written | Settled::Untouched => Ok(()),
            Settled::Lost => Err(lease_lost(queue, id)),
        }
    }
}

/// Whether a settlement failed because the broker shut down.
#[cfg(feature = "testing")]
fn is_closed(settled: &Result<(), AckError>) -> bool {
    matches!(settled, Err(AckError::Broker(source))
        if matches!(source.downcast_ref::<SqlxBrokerError>(), Some(SqlxBrokerError::Closed)))
}

/// Runs the statement of `step` for the row `id` on `conn`; `held` is the delivery's lease in the
/// lease form.
pub(crate) async fn run_step<DB, Row>(
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
