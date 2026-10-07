//! The lease book and its keeper: the deliveries of one lease subscription in work, each with the
//! lease it holds, and the task that extends every one of those leases each half lease.

use std::future::Future;

use sqlx::Pool;
use tokio::runtime::Handle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::inbox::broker::Shared;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{Events, Now, Settled, Settling};
use crate::inbox::queue::Queue;
#[cfg(feature = "testing")]
use crate::inbox::testing::off_clock;

pub(crate) mod claim;
pub(crate) mod settle;
mod slots;

use slots::{Extended, Leases, Resolving, Round};

/// The deliveries of one lease subscription in work, the pool they settle on, and what its keeper
/// extends their leases with.
///
/// A subscription leaks its book once, so a delivery reaches the book and the pool through a
/// `&'static` reference, with no reference count per message.
pub(crate) struct LeaseBook<DB: QueueDatabase, Row: Events<DB>> {
    leases: Leases<Row::Id, Row::Token>,
    /// The service's pool: a lease delivery settles on a connection of its own, committed at once,
    /// except an acknowledgement in transactional mode, which runs in the delivery's transaction.
    pool: Pool<DB>,
    /// The runtime the broker connected on: the keeper runs there, and so does the release of a
    /// delivery dropped unsettled.
    runtime: Handle,
    /// The subscription: the lease the keeper extends by, and its statements.
    queue: &'static Queue,
    /// Where "now" comes from for the keeper's extensions.
    now: Now,
    /// Whether the connection runs in process: the keeper's database work then runs off a paused
    /// clock.
    #[cfg(feature = "testing")]
    in_process: bool,
}

/// A delivery's place in its subscription's book: taken at the claim, given back once, when the
/// delivery settles or its release runs.
#[derive(Debug)]
pub(crate) struct Slot(usize);

impl Slot {
    /// The slot's index, given up with the slot itself.
    const fn into_index(self) -> usize {
        self.0
    }
}

/// The keeper found a delivery's row no longer under its lease: the lease ran out before an
/// extension reached it and another claim took the row, or the row is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaseGone;

impl<DB: QueueDatabase, Row: Events<DB>> LeaseBook<DB, Row> {
    /// The book of a subscription to `queue` on `shared`'s connection, for the life of the process.
    pub(crate) fn leak(shared: &Shared<DB>, queue: &'static Queue) -> &'static Self {
        Box::leak(Box::new(Self {
            leases: Leases::default(),
            pool: shared.pool.clone(),
            runtime: shared.runtime.clone(),
            queue,
            #[cfg(feature = "testing")]
            now: shared.harness.now(),
            #[cfg(not(feature = "testing"))]
            now: Now::default(),
            #[cfg(feature = "testing")]
            in_process: shared.harness.in_process(),
        }))
    }

    /// Enters a delivery of the row `id` that holds `lease`: one lock round trip, and an
    /// allocation only when the subscription has more deliveries in work than ever before.
    pub(crate) fn enter(&self, id: &Row::Id, lease: Row::Token) -> Slot {
        self.leases.enter(id, lease)
    }

    /// Takes the delivery in `slot` out of the book for its settlement, and hands back the lease
    /// it holds: at once, or once an extension of that lease in flight has ended. [`LeaseGone`]
    /// when the keeper found the row under another lease.
    ///
    /// Dropping the future before it finishes takes the delivery out of the book as well.
    pub(crate) fn settling(
        &self,
        slot: Slot,
    ) -> impl Future<Output = Result<Row::Token, LeaseGone>> + Send + '_ {
        self.leases.settling(slot)
    }

    /// Takes the delivery in `slot` out of the book without waiting for an extension in flight,
    /// and hands back the lease it held before that extension, which the keeper extends no more.
    /// [`LeaseGone`] when the keeper found the row under another lease.
    ///
    /// For a delivery whose transaction may hold what an extension in flight waits for: the
    /// acknowledgement inside a handler's transaction, which waiting would deadlock, and a
    /// settlement that gives the delivery up while its handler keeps the transaction.
    pub(crate) fn take_ahead(&self, slot: Slot) -> Result<Row::Token, LeaseGone> {
        self.leases.take_ahead(slot)
    }

    /// The pool the deliveries settle on.
    pub(crate) const fn pool(&self) -> &Pool<DB> {
        &self.pool
    }

    /// The runtime the broker connected on.
    pub(crate) const fn runtime(&self) -> &Handle {
        &self.runtime
    }

    /// One round of extensions, a failure logged; in process, off a paused clock.
    async fn round(&'static self, round: Round<Row::Id, Row::Token>) -> Round<Row::Id, Row::Token> {
        #[cfg(feature = "testing")]
        if self.in_process {
            return off_clock(self.logged(round)).await.unwrap_or_default();
        }
        self.logged(round).await
    }

    async fn logged(&self, mut round: Round<Row::Id, Row::Token>) -> Round<Row::Id, Row::Token> {
        if let Err(error) = self.extend(&mut round).await {
            tracing::warn!(
                target: "ruststream_sqlx",
                subscription = self.queue.name,
                table = self.queue.table,
                row = self.queue.row,
                %error,
                "a round of lease extensions failed: the deliveries in work keep the leases they \
                 hold until the next round",
            );
        }
        round
    }

    /// Writes a lease from now into every row whose delivery holds one, while the row still holds
    /// it: one statement per row, on one connection.
    async fn extend(&self, round: &mut Round<Row::Id, Row::Token>) -> Result<(), sqlx::Error> {
        // An idle subscription's round ends here, without a connection.
        if !self.leases.any_held() {
            return Ok(());
        }
        // The connection first: a settlement that waits for this round holds a connection of its
        // own, and this round never waits for one while leases are in flight.
        let mut conn = self.pool.acquire().await?;
        // Read once the connection is in hand, so a wait for the pool does not shorten the lease.
        let next = Row::lease(self.queue, self.now)?.expiry;
        if self.leases.mark(next, round) == 0 {
            return Ok(());
        }
        let cx = Settling {
            queue: self.queue,
            now: self.now,
        };
        let resolving = Resolving {
            leases: &self.leases,
            round,
        };
        // Only transactional mode takes a lease ahead of its extension, so only its rounds ask.
        let transactional = self.queue.prepared.transactional;
        for target in resolving.round.marked_mut() {
            // An extension after an acknowledgement that took the lease ahead could only race it.
            if transactional && self.leases.taken_ahead(target.slot) {
                continue;
            }
            let extended = Row::extend(&mut conn, &cx, &target.id, &target.held, &next).await;
            target.outcome = Some(match extended {
                Ok(Settled::Written | Settled::Untouched) => Extended::Written,
                // The acknowledgement that took the lease ahead settled the row meanwhile.
                Ok(Settled::Lost) if transactional && self.leases.taken_ahead(target.slot) => {
                    Extended::Lost
                }
                Ok(Settled::Lost) => {
                    tracing::warn!(
                        target: "ruststream_sqlx",
                        subscription = self.queue.name,
                        table = self.queue.table,
                        row = self.queue.row,
                        id = ?target.id,
                        "a lease in work was gone when the keeper came to extend it: it ran out \
                         and another claim took the row, or the row is gone; the delivery's \
                         settlement will not take effect",
                    );
                    Extended::Lost
                }
                Err(error) => {
                    tracing::warn!(
                        target: "ruststream_sqlx",
                        subscription = self.queue.name,
                        table = self.queue.table,
                        row = self.queue.row,
                        id = ?target.id,
                        %error,
                        "a lease extension failed: the delivery keeps the lease it holds until the \
                         next round",
                    );
                    Extended::Failed
                }
            });
        }
        Ok(())
    }
}

/// Extends the lease of every delivery of `book`'s subscription in work each half lease, until
/// `stop` is cancelled.
pub(crate) async fn keep<DB, Row>(book: &'static LeaseBook<DB, Row>, stop: CancellationToken)
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    // Every half lease: an extended lease has half of itself left when the next round comes.
    let Some(period) = book.queue.lease.map(|lease| lease / 2) else {
        return;
    };
    let mut ticks = tokio::time::interval_at(Instant::now() + period, period);
    // A round that ran late moves the next one, instead of a burst of rounds that catch up.
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut round = Round::default();
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = ticks.tick() => {}
        }
        // A round runs to its end once started, so no extension is left in flight.
        round = book.round(round).await;
    }
}
