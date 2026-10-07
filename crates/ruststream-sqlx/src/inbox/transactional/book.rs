//! The book a transactional subscription lends its deliveries' transactions from, in the row lock
//! and lease forms: each transaction waits there while its handler does not hold it.

use std::fmt;
use std::mem::{self, ManuallyDrop};
use std::sync::{Mutex, MutexGuard, PoisonError};

use sqlx::Database;

use super::tx::Lender;
use crate::inbox::tx::PoolTx;

/// A delivery's place in a [`TxBook`]: the slot, and which of the deliveries that held the slot it
/// is, so a lender of an earlier one never reaches a later one's transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TxSlot {
    index: usize,
    generation: u64,
}

/// The transactions of one transactional subscription's deliveries in work, each in the slot its
/// delivery holds, from the claim until the settlement; the handler borrows it from there.
///
/// A subscription leaks its book once, so a delivery and its handler's [`Tx`](super::Tx) reach
/// the book through a `'static` reference, with no reference count per message. Entering,
/// lending, giving back and leaving each take the book's lock once; a delivery allocates only when
/// the subscription has more deliveries in work than ever before.
pub(crate) struct TxBook<DB: Database> {
    slots: Mutex<TxSlots<DB>>,
}

/// Every slot the book has held, and which of them are free.
struct TxSlots<DB: Database> {
    entries: Vec<TxEntry<DB>>,
    /// The free slots, by index; a delivery takes one of them before it adds a slot.
    free: Vec<usize>,
}

/// One slot: which delivery holds it, and where its transaction is.
struct TxEntry<DB: Database> {
    /// Counts the deliveries that held the slot; the current one's lender carries it.
    generation: u64,
    standing: TxStanding<DB>,
}

/// Where a slot's transaction is.
enum TxStanding<DB: Database> {
    /// No delivery holds the slot.
    Free,
    /// The transaction waits in the slot while the handler does not hold it.
    Held(Returned<DB>),
    /// The handler holds the transaction.
    Lent,
}

/// A delivery's transaction as its settlement finds it.
pub(crate) struct Returned<DB: Database> {
    /// The transaction.
    pub(crate) tx: PoolTx<DB>,
    /// Whether a panic of the handler gave it back: what the handler wrote is rolled back,
    /// whatever the outcome.
    pub(crate) panicked: bool,
}

impl<DB: Database> TxBook<DB> {
    /// A book for a subscription, for the life of the process.
    pub(crate) fn leak() -> &'static Self {
        Box::leak(Box::new(Self {
            slots: Mutex::new(TxSlots {
                entries: Vec::new(),
                free: Vec::new(),
            }),
        }))
    }

    /// Records a delivery's transaction `tx` until the delivery settles.
    pub(crate) fn enter(&'static self, tx: PoolTx<DB>) -> TxHold<DB> {
        let mut slots = self.slots();
        let TxSlots { entries, free } = &mut *slots;
        let reused = free
            .pop()
            .and_then(|index| Some((index, entries.get_mut(index)?)));
        let held = TxStanding::Held(Returned {
            tx,
            panicked: false,
        });
        let slot = if let Some((index, entry)) = reused {
            entry.generation = entry.generation.wrapping_add(1);
            entry.standing = held;
            TxSlot {
                index,
                generation: entry.generation,
            }
        } else {
            entries.push(TxEntry {
                generation: 0,
                standing: held,
            });
            TxSlot {
                index: entries.len() - 1,
                generation: 0,
            }
        };
        drop(slots);
        TxHold { book: self, slot }
    }

    /// The transaction in `slot`, out of the book for the handler; `None` unless it waits there for
    /// the delivery `slot` names.
    pub(super) fn lend(&self, slot: TxSlot) -> Option<PoolTx<DB>> {
        let mut slots = self.slots();
        let lent = slots
            .entries
            .get_mut(slot.index)
            .filter(|entry| entry.generation == slot.generation)
            .and_then(
                |entry| match mem::replace(&mut entry.standing, TxStanding::Lent) {
                    TxStanding::Held(held) => Some(held.tx),
                    other => {
                        entry.standing = other;
                        None
                    }
                },
            );
        drop(slots);
        lent
    }

    /// `tx` back into `slot` after the handler, `panicked` when a panic dropped it; a slot that no
    /// longer waits for it leaves it to end here.
    pub(super) fn give_back(&self, slot: TxSlot, tx: PoolTx<DB>, panicked: bool) {
        let mut slots = self.slots();
        let refused = match slots.entries.get_mut(slot.index) {
            Some(entry)
                if entry.generation == slot.generation
                    && matches!(entry.standing, TxStanding::Lent) =>
            {
                entry.standing = TxStanding::Held(Returned { tx, panicked });
                None
            }
            _ => Some(tx),
        };
        drop(slots);
        // The transaction ends outside the book's lock: its connection closes.
        drop(refused);
    }

    /// Frees `slot` as its delivery settles or drops, and hands over the transaction where it waits
    /// in the slot; `None` while the handler holds it.
    fn leave(&self, slot: TxSlot) -> Option<Returned<DB>> {
        let mut slots = self.slots();
        let TxSlots { entries, free } = &mut *slots;
        let entry = entries
            .get_mut(slot.index)
            .filter(|entry| entry.generation == slot.generation)?;
        let left = mem::replace(&mut entry.standing, TxStanding::Free);
        if matches!(left, TxStanding::Free) {
            return None;
        }
        free.push(slot.index);
        drop(slots);
        match left {
            TxStanding::Held(held) => Some(held),
            TxStanding::Free | TxStanding::Lent => None,
        }
    }

    fn slots(&self) -> MutexGuard<'_, TxSlots<DB>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A transactional delivery's place in its subscription's book: entered at the claim, left once,
/// when the delivery settles. Dropped unconsumed, the unsettled drop, it ends the transaction
/// waiting in the slot: the connection closes, the server rolls the transaction back, and the row
/// returns at once.
pub(crate) struct TxHold<DB: Database> {
    book: &'static TxBook<DB>,
    slot: TxSlot,
}

impl<DB: Database> TxHold<DB> {
    /// Where the delivery's handler borrows the transaction from.
    pub(crate) const fn lender(&self) -> Lender<DB> {
        Lender::Tx(self.book, self.slot)
    }

    /// The transaction for the delivery's settlement, its slot freed; `None` while the handler
    /// still holds it, which then gives it back to no one and ends it.
    pub(crate) fn settle(self) -> Option<Returned<DB>> {
        let hold = ManuallyDrop::new(self);
        hold.book.leave(hold.slot)
    }
}

impl<DB: Database> Drop for TxHold<DB> {
    fn drop(&mut self) {
        // Nothing async runs here: a transaction still open closes its connection as it drops.
        drop(self.book.leave(self.slot));
    }
}

impl<DB: Database> fmt::Debug for TxHold<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxHold")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Error, Sqlite, SqlitePool};

    use super::{Lender, TxBook, TxSlot};
    use crate::inbox::tx::PoolTx;

    async fn pool() -> Result<SqlitePool, Error> {
        SqlitePoolOptions::new()
            .max_connections(4)
            .connect("sqlite::memory:")
            .await
    }

    fn slot(lender: Lender<Sqlite>) -> TxSlot {
        let Lender::Tx(_, slot) = lender else {
            panic!("a transaction book lends from its own slots")
        };
        slot
    }

    #[tokio::test]
    async fn the_handler_borrows_the_transaction_once_and_gives_it_back() -> Result<(), Error> {
        let pool = pool().await?;
        let book = TxBook::<Sqlite>::leak();
        let hold = book.enter(PoolTx::begin(&pool, None).await?);
        let lender = hold.lender();
        let lent = lender
            .lend()
            .expect("the book lends the delivery's transaction");
        assert!(lender.lend().is_none(), "a second borrower finds it lent");
        lent.give_back(false);
        let returned = hold.settle().expect("the settlement finds it back");
        assert!(!returned.panicked);
        returned.tx.rollback().await
    }

    #[tokio::test]
    async fn a_panic_marks_the_transaction_it_gives_back() -> Result<(), Error> {
        let pool = pool().await?;
        let book = TxBook::<Sqlite>::leak();
        let hold = book.enter(PoolTx::begin(&pool, None).await?);
        let lent = hold.lender().lend().expect("lends");
        lent.give_back(true);
        let returned = hold.settle().expect("the settlement finds it back");
        assert!(
            returned.panicked,
            "the settlement discards what the handler wrote"
        );
        returned.tx.rollback().await
    }

    #[tokio::test]
    async fn a_transaction_kept_past_its_settlement_never_reaches_the_next_delivery()
    -> Result<(), Error> {
        let pool = pool().await?;
        let book = TxBook::<Sqlite>::leak();
        let first = book.enter(PoolTx::begin(&pool, None).await?);
        let stale = first.lender();
        let kept = stale.lend().expect("lends");
        assert!(
            first.settle().is_none(),
            "a settlement finds the transaction still lent"
        );
        // The next delivery takes the freed slot, and its handler borrows its own transaction.
        let second = book.enter(PoolTx::begin(&pool, None).await?);
        assert_eq!(slot(second.lender()).index, slot(stale).index);
        let borrowed = second.lender().lend().expect("lends");
        // The first transaction comes back late: the slot it left refuses it, so it ends there.
        kept.give_back(false);
        assert!(
            second.lender().lend().is_none(),
            "the slot still waits for the second delivery's own transaction"
        );
        borrowed.give_back(false);
        let returned = second
            .settle()
            .expect("the settlement finds its own transaction");
        returned.tx.rollback().await
    }

    #[tokio::test]
    async fn a_hold_dropped_unsettled_frees_its_slot() -> Result<(), Error> {
        let pool = pool().await?;
        let book = TxBook::<Sqlite>::leak();
        let dropped = book.enter(PoolTx::begin(&pool, None).await?);
        let stale = dropped.lender();
        drop(dropped);
        assert!(
            stale.lend().is_none(),
            "the dropped delivery's transaction ended"
        );
        let next = book.enter(PoolTx::begin(&pool, None).await?);
        assert_eq!(
            slot(next.lender()),
            TxSlot {
                index: slot(stale).index,
                generation: slot(stale).generation + 1,
            },
            "the next delivery takes the slot"
        );
        let returned = next.settle().expect("held");
        returned.tx.rollback().await
    }
}
