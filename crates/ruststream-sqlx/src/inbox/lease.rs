//! The lease book: the deliveries of one lease subscription in work, each with the lease it holds.

use std::sync::{Mutex, MutexGuard, PoisonError};

use sqlx::Pool;

use super::database::QueueDatabase;
use super::engine::Events;

/// The deliveries of one lease subscription in work, and the pool they settle on.
///
/// Each slot holds the lease of one delivery: the expiry its claim wrote, which its settlement
/// matches. A subscription leaks its book once, so a delivery reaches the book and the pool
/// through a `&'static` reference, with no reference count per message.
pub(crate) struct LeaseBook<DB: QueueDatabase, Row: Events<DB>> {
    slots: Mutex<Slots<Row::Token>>,
    /// The service's pool: a lease delivery settles on a connection of its own, committed at once.
    pool: Pool<DB>,
}

/// The slots of a book: the lease each one holds, and which of them are free.
struct Slots<Token> {
    /// Every slot the book has held; a free one keeps its last lease until the next delivery
    /// takes it.
    leases: Vec<Token>,
    /// The free slots, by index; a claim takes one of them before it adds a slot.
    free: Vec<usize>,
}

/// A delivery's place in its subscription's book: taken at the claim, given back once, when the
/// delivery settles or drops.
#[derive(Debug)]
pub(crate) struct Slot(usize);

impl<DB: QueueDatabase, Row: Events<DB>> LeaseBook<DB, Row> {
    /// A book on `pool`, for the life of the process.
    pub(crate) fn leak(pool: Pool<DB>) -> &'static Self {
        Box::leak(Box::new(Self {
            slots: Mutex::new(Slots {
                leases: Vec::new(),
                free: Vec::new(),
            }),
            pool,
        }))
    }

    fn slots(&self) -> MutexGuard<'_, Slots<Row::Token>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Enters a delivery that holds `lease`: one lock round trip, and an allocation only when the
    /// subscription has more deliveries in work than ever before.
    pub(crate) fn enter(&self, lease: Row::Token) -> Slot {
        let mut slots = self.slots();
        if let Some(index) = slots.free.pop() {
            slots.leases[index] = lease;
            return Slot(index);
        }
        slots.leases.push(lease);
        Slot(slots.leases.len() - 1)
    }

    /// Takes the delivery in `slot` out of the book, and hands back the lease it held.
    // Why by value: a delivery gives its slot back once, and a second `leave` of it does not
    // compile.
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn leave(&self, slot: Slot) -> Row::Token {
        let mut slots = self.slots();
        slots.free.push(slot.0);
        slots.leases[slot.0]
    }

    /// The pool the deliveries settle on.
    pub(crate) const fn pool(&self) -> &Pool<DB> {
        &self.pool
    }
}
