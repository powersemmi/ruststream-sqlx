//! The lease book and its keeper: the deliveries of one lease subscription in work, each with the
//! lease it holds, and the task that extends every one of those leases each half lease.

use std::future::Future;
use std::sync::{Mutex, MutexGuard, PoisonError};

use sqlx::Pool;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::broker::Shared;
use super::database::QueueDatabase;
use super::engine::{Events, Now, Settled, Settling};
use super::queue::Queue;
#[cfg(feature = "testing")]
use super::testing::off_clock;

/// The deliveries of one lease subscription in work, the pool they settle on, and what its keeper
/// extends their leases with.
///
/// A subscription leaks its book once, so a delivery reaches the book and the pool through a
/// `&'static` reference, with no reference count per message.
pub(crate) struct LeaseBook<DB: QueueDatabase, Row: Events<DB>> {
    leases: Leases<Row::Id, Row::Token>,
    /// The service's pool: a lease delivery settles on a connection of its own, committed at once.
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
        for target in resolving.round.marked_mut() {
            let extended = Row::extend(&mut conn, &cx, &target.id, &target.held, &next).await;
            target.outcome = Some(match extended {
                Ok(Settled::Written | Settled::Untouched) => Extended::Written,
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

/// The slots of one book, and the settlements that wait on its keeper.
pub(crate) struct Leases<Id, Token> {
    slots: Mutex<Slots<Id, Token>>,
    /// Wakes the settlements that wait for an extension, once a round has resolved its slots.
    extended: Notify,
}

/// Every slot the book has held, and which of them are free.
struct Slots<Id, Token> {
    entries: Vec<Entry<Id, Token>>,
    /// The free slots, by index; a claim takes one of them before it adds a slot.
    free: Vec<usize>,
}

/// One slot: the id of its delivery's row, and where its lease stands.
struct Entry<Id, Token> {
    /// The row's id. A free slot keeps the last one, so the next delivery copies its id into the
    /// same storage.
    id: Option<Id>,
    standing: Standing<Token>,
}

/// Where a delivery's lease stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing<Token> {
    /// No delivery holds the slot.
    Free,
    /// A delivery in work holds the lease; the keeper extends it.
    Held(Token),
    /// The keeper extends the lease from `held` to `next`. `settling` once the delivery's
    /// settlement waits for the outcome, which then goes to it instead of back to the keeper.
    Extending {
        held: Token,
        next: Token,
        settling: bool,
    },
    /// The lease an extension left a settlement that waited for it; the keeper extends it no more.
    Settling(Token),
    /// The keeper found the row no longer under the delivery's lease.
    Lost,
}

/// What one extension did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Extended {
    /// The row holds the new lease.
    Written,
    /// The row no longer held the delivery's lease.
    Lost,
    /// The statement failed; whether it took effect is unknown.
    Failed,
}

/// The deliveries one round extends, kept from round to round, so each id is copied into the
/// storage the round before left.
pub(crate) struct Round<Id, Token> {
    targets: Vec<Target<Id, Token>>,
    /// How many of `targets` this round extends; the rest keep their storage for a later round.
    marked: usize,
}

/// One delivery a round extends.
struct Target<Id, Token> {
    slot: usize,
    id: Id,
    held: Token,
    /// What its extension did; `None` until it ran.
    outcome: Option<Extended>,
}

/// Resolves the slots a round marked when it drops: when the round ends, and when it is dropped
/// midway, so no settlement waits on an extension that never finishes.
struct Resolving<'a, Id, Token: Copy> {
    leases: &'a Leases<Id, Token>,
    round: &'a mut Round<Id, Token>,
}

impl<Id, Token: Copy> Drop for Resolving<'_, Id, Token> {
    fn drop(&mut self) {
        self.leases.resolve(self.round);
    }
}

/// A settlement's hold on its slot while it waits for an extension. Dropped with the settlement,
/// it takes the slot out of the book, so the keeper keeps no lease of a delivery that is gone.
struct Waiting<'a, Id, Token> {
    leases: &'a Leases<Id, Token>,
    /// The slot, until the settlement has its lease.
    slot: Option<usize>,
}

impl<Id, Token> Drop for Waiting<'_, Id, Token> {
    fn drop(&mut self) {
        let Some(index) = self.slot.take() else {
            return;
        };
        let mut slots = self.leases.slots();
        // A slot still being extended leaves the book now; the round's resolution passes over it.
        if !matches!(slots.entries[index].standing, Standing::Free) {
            slots.release(index);
        }
    }
}

/// What a settlement finds in its slot.
enum Found<'a, Token> {
    /// The lease, or that it is gone; the slot has left the book.
    Done(Result<Token, LeaseGone>),
    /// An extension in flight, whose end this wakes.
    Wait(Notified<'a>),
}

impl<Id, Token> Default for Leases<Id, Token> {
    fn default() -> Self {
        Self {
            slots: Mutex::new(Slots {
                entries: Vec::new(),
                free: Vec::new(),
            }),
            extended: Notify::new(),
        }
    }
}

impl<Id, Token> Default for Round<Id, Token> {
    fn default() -> Self {
        Self {
            targets: Vec::new(),
            marked: 0,
        }
    }
}

impl<Id, Token> Round<Id, Token> {
    fn marked_mut(&mut self) -> &mut [Target<Id, Token>] {
        &mut self.targets[..self.marked]
    }
}

impl<Id, Token> Slots<Id, Token> {
    /// Frees the slot at `index` for the next delivery.
    fn release(&mut self, index: usize) {
        self.entries[index].standing = Standing::Free;
        self.free.push(index);
    }
}

impl<Id, Token> Leases<Id, Token> {
    fn slots(&self) -> MutexGuard<'_, Slots<Id, Token>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<Id: Clone, Token: Copy> Leases<Id, Token> {
    /// Enters a delivery of the row `id` that holds `lease`.
    fn enter(&self, id: &Id, lease: Token) -> Slot {
        let mut slots = self.slots();
        let Some(index) = slots.free.pop() else {
            slots.entries.push(Entry {
                id: Some(id.clone()),
                standing: Standing::Held(lease),
            });
            let index = slots.entries.len() - 1;
            drop(slots);
            return Slot(index);
        };
        let entry = &mut slots.entries[index];
        // Into the storage the slot's last id left: an id with storage of its own costs no
        // allocation once its slot has held one as long.
        match &mut entry.id {
            Some(kept) => kept.clone_from(id),
            empty => *empty = Some(id.clone()),
        }
        entry.standing = Standing::Held(lease);
        drop(slots);
        Slot(index)
    }

    /// Takes the delivery in `slot` out of the book, with its lease, once no extension of it is in
    /// flight.
    async fn settling(&self, slot: Slot) -> Result<Token, LeaseGone> {
        let Slot(index) = slot;
        let mut waiting = Waiting {
            leases: self,
            slot: Some(index),
        };
        loop {
            match self.find(index) {
                Found::Done(lease) => {
                    waiting.slot = None;
                    return lease;
                }
                Found::Wait(extended) => extended.await,
            }
        }
    }

    /// What the settlement of the slot at `index` finds there, under one lock.
    fn find(&self, index: usize) -> Found<'_, Token> {
        let mut slots = self.slots();
        let found = match slots.entries[index].standing {
            Standing::Held(lease) | Standing::Settling(lease) => {
                slots.release(index);
                Found::Done(Ok(lease))
            }
            Standing::Lost => {
                slots.release(index);
                Found::Done(Err(LeaseGone))
            }
            Standing::Extending { held, next, .. } => {
                slots.entries[index].standing = Standing::Extending {
                    held,
                    next,
                    settling: true,
                };
                // Created under the lock the keeper resolves the slot under: a `Notified` hears
                // every `notify_waiters` after its creation, so the resolution cannot slip past.
                Found::Wait(self.extended.notified())
            }
            // Unreachable: a slot settles once, for `Slot` is neither `Clone` nor `Copy`.
            Standing::Free => Found::Done(Err(LeaseGone)),
        };
        drop(slots);
        found
    }

    /// Whether any delivery holds a lease the keeper extends.
    fn any_held(&self) -> bool {
        self.slots()
            .entries
            .iter()
            .any(|entry| matches!(entry.standing, Standing::Held(_)))
    }

    /// Starts a round: every delivery that holds a lease is now being extended to `next`, and
    /// `round` lists them. Returns how many it lists.
    fn mark(&self, next: Token, round: &mut Round<Id, Token>) -> usize {
        round.marked = 0;
        let mut slots = self.slots();
        for (slot, entry) in slots.entries.iter_mut().enumerate() {
            let (Standing::Held(held), Some(id)) = (entry.standing, &entry.id) else {
                continue;
            };
            entry.standing = Standing::Extending {
                held,
                next,
                settling: false,
            };
            match round.targets.get_mut(round.marked) {
                Some(target) => {
                    target.slot = slot;
                    target.id.clone_from(id);
                    target.held = held;
                    target.outcome = None;
                }
                None => round.targets.push(Target {
                    slot,
                    id: id.clone(),
                    held,
                    outcome: None,
                }),
            }
            round.marked += 1;
        }
        drop(slots);
        round.marked
    }
}

impl<Id, Token: Copy> Leases<Id, Token> {
    /// Ends a round: each slot it marked holds what its extension left, and the settlements that
    /// waited for it wake.
    fn resolve(&self, round: &Round<Id, Token>) {
        let mut slots = self.slots();
        for target in &round.targets[..round.marked] {
            let entry = &mut slots.entries[target.slot];
            let Standing::Extending {
                held,
                next,
                settling,
            } = entry.standing
            else {
                // The delivery left the book while its lease was extended; the slot may hold
                // another one by now.
                continue;
            };
            entry.standing = match (target.outcome, settling) {
                (Some(Extended::Lost), _) => Standing::Lost,
                (Some(Extended::Written), false) => Standing::Held(next),
                (Some(Extended::Written), true) => Standing::Settling(next),
                // Whether a failed or unfinished extension took effect is unknown: the delivery
                // keeps the lease it held.
                (Some(Extended::Failed) | None, false) => Standing::Held(held),
                (Some(Extended::Failed) | None, true) => Standing::Settling(held),
            };
        }
        drop(slots);
        self.extended.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::poll;

    use super::{Extended, LeaseGone, Leases, Round, Standing};

    /// A book of text ids, which hold storage of their own, and leases that are plain numbers.
    type Book = Leases<String, u32>;

    fn standing(book: &Book, slot: usize) -> Standing<u32> {
        book.slots().entries[slot].standing
    }

    /// Ends `round` with `outcome` for every delivery it marked.
    fn resolve(book: &Book, round: &mut Round<String, u32>, outcome: Extended) {
        for target in round.marked_mut() {
            target.outcome = Some(outcome);
        }
        book.resolve(round);
    }

    #[tokio::test]
    async fn an_extension_racing_a_settlement_never_loses_the_lease() {
        let book = Book::default();
        let slot = book.enter(&"job-1".to_owned(), 10);
        let mut round = Round::default();
        assert_eq!(book.mark(20, &mut round), 1);
        let mut settling = pin!(book.settling(slot));
        assert!(
            poll!(settling.as_mut()).is_pending(),
            "the settlement waits while its lease is extended"
        );
        resolve(&book, &mut round, Extended::Written);
        assert_eq!(
            settling.await,
            Ok(20),
            "the settlement holds the lease the extension wrote, not the one it replaced"
        );
        assert_eq!(standing(&book, 0), Standing::Free);
    }

    #[tokio::test]
    async fn a_settlement_that_waited_for_a_lost_lease_learns_it_is_gone() {
        let book = Book::default();
        let lost = book.enter(&"job-1".to_owned(), 10);
        let mut round = Round::default();
        book.mark(20, &mut round);
        let mut settling = pin!(book.settling(lost));
        assert!(poll!(settling.as_mut()).is_pending());
        resolve(&book, &mut round, Extended::Lost);
        assert_eq!(settling.await, Err(LeaseGone));
        // A failed extension may not have taken effect: the delivery keeps the lease it held.
        let failed = book.enter(&"job-2".to_owned(), 30);
        book.mark(40, &mut round);
        let mut settling = pin!(book.settling(failed));
        assert!(poll!(settling.as_mut()).is_pending());
        resolve(&book, &mut round, Extended::Failed);
        assert_eq!(settling.await, Ok(30));
    }

    #[tokio::test]
    async fn a_lease_lost_between_settlements_is_gone_for_the_next_one() {
        let book = Book::default();
        let slot = book.enter(&"job-1".to_owned(), 10);
        let mut round = Round::default();
        book.mark(20, &mut round);
        resolve(&book, &mut round, Extended::Lost);
        assert_eq!(standing(&book, 0), Standing::Lost);
        assert!(!book.any_held(), "the keeper extends a lost lease no more");
        assert_eq!(book.settling(slot).await, Err(LeaseGone));
        assert_eq!(standing(&book, 0), Standing::Free);
    }

    #[tokio::test]
    async fn a_settling_slot_is_skipped_by_the_next_round() {
        let book = Book::default();
        let settled = book.enter(&"job-1".to_owned(), 10);
        let kept = book.enter(&"job-2".to_owned(), 10);
        let mut round = Round::default();
        assert_eq!(book.mark(20, &mut round), 2);
        let mut settling = pin!(book.settling(settled));
        assert!(poll!(settling.as_mut()).is_pending());
        resolve(&book, &mut round, Extended::Written);
        assert_eq!(standing(&book, 0), Standing::Settling(20));
        assert_eq!(standing(&book, 1), Standing::Held(20));
        // The settlement has not run again yet; the next round extends the other delivery alone.
        assert_eq!(book.mark(30, &mut round), 1);
        assert_eq!(round.marked_mut()[0].slot, 1);
        assert_eq!(standing(&book, 0), Standing::Settling(20));
        assert_eq!(settling.await, Ok(20));
        resolve(&book, &mut round, Extended::Written);
        assert_eq!(book.settling(kept).await, Ok(30));
        // A round with nothing held marks nothing.
        assert!(!book.any_held());
        assert_eq!(book.mark(40, &mut round), 0);
    }

    #[tokio::test]
    async fn a_settlement_dropped_while_it_waits_frees_its_slot() {
        let book = Book::default();
        let slot = book.enter(&"job-1".to_owned(), 10);
        let mut round = Round::default();
        book.mark(20, &mut round);
        {
            let mut settling = pin!(book.settling(slot));
            assert!(poll!(settling.as_mut()).is_pending());
        }
        assert_eq!(standing(&book, 0), Standing::Free);
        // The slot holds another delivery before the round ends; the round leaves it alone.
        let next = book.enter(&"job-2".to_owned(), 15);
        resolve(&book, &mut round, Extended::Written);
        assert_eq!(standing(&book, 0), Standing::Held(15));
        assert_eq!(book.settling(next).await, Ok(15));
    }

    #[tokio::test]
    async fn enter_after_leave_reuses_the_slot_and_its_id_allocation() {
        let book = Book::default();
        let first = book.enter(&"job-0001".to_owned(), 10);
        let storage = book.slots().entries[0].id.as_deref().map(str::as_ptr);
        assert_eq!(book.settling(first).await, Ok(10));
        let second = book.enter(&"job-0002".to_owned(), 20);
        {
            let slots = book.slots();
            assert_eq!(slots.entries.len(), 1, "the delivery took the free slot");
            assert_eq!(slots.entries[0].id.as_deref(), Some("job-0002"));
            assert_eq!(
                slots.entries[0].id.as_deref().map(str::as_ptr),
                storage,
                "the id was copied into the storage the last one left"
            );
        }
        assert_eq!(book.settling(second).await, Ok(20));
    }

    #[tokio::test]
    async fn a_round_copies_each_id_into_the_storage_the_round_before_left() {
        let book = Book::default();
        let first = book.enter(&"job-0001".to_owned(), 10);
        let mut round = Round::default();
        book.mark(20, &mut round);
        let storage = round.targets[0].id.as_ptr();
        resolve(&book, &mut round, Extended::Written);
        assert_eq!(book.settling(first).await, Ok(20));
        let second = book.enter(&"job-0002".to_owned(), 30);
        assert_eq!(book.mark(40, &mut round), 1);
        assert_eq!(round.targets[0].id, "job-0002");
        assert_eq!(round.targets[0].id.as_ptr(), storage);
        resolve(&book, &mut round, Extended::Written);
        assert_eq!(book.settling(second).await, Ok(40));
    }
}
