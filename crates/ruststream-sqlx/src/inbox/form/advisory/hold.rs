//! The handles of a delivery in the advisory lock form's book: its place in the book, entered at
//! the claim and left once, and a session that may hold the lock on its key outside the book.

use std::borrow::Cow;
use std::mem::{self, ManuallyDrop};

use sqlx::Database;

use super::session::Session;
use super::{Borrower, LockBook, LockSlot, Slots, Standing};

/// Why a settlement found no session in its delivery's slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unlent {
    /// `shutdown` released the delivery's lock and ended its session.
    Released,
    /// The handler's transaction still has the session: the handler kept its `Tx` past its end.
    Borrowed,
}

/// A delivery's place in the book: entered at the claim, left once, when the delivery settles.
/// Dropped unconsumed, it ends its session (the unsettled drop): a session that holds a lock in
/// the database closes, the lock released first, and a key the process keeps is freed.
pub(crate) struct LockHold<DB: Database> {
    pub(super) book: &'static LockBook<DB>,
    pub(super) slot: LockSlot,
}

/// A session that may hold the lock on its key outside the book: in a claim, from its lock until
/// the book holds it, and lent to a settlement or a handler's transaction. Dropped before it is let
/// go, by an error or a cancellation, it ends its session: one that may hold the lock in the
/// database closes after an unlock of the key, so the lock is gone once the close ends, and a key
/// the process keeps is freed.
pub(crate) struct Locked<'k, DB: Database> {
    pub(super) session: Option<Session<DB>>,
    /// Borrowed from the claim's candidates in a claim, owned once lent from the book: a dropped
    /// claim copies it for the unlock, off the path of a message.
    pub(super) key: Cow<'k, str>,
    pub(super) book: &'static LockBook<DB>,
}

impl<DB: Database> LockHold<DB> {
    /// The book the delivery is in.
    pub(crate) const fn book(&self) -> &'static LockBook<DB> {
        self.book
    }

    /// Where the delivery is in its book, for its handler's transaction to borrow the session.
    pub(crate) const fn slot(&self) -> LockSlot {
        self.slot
    }

    /// The session with its key, out of the book for the delivery's settlement, and whether a panic
    /// of the handler gave it back. [`Unlent::Released`] once `shutdown` released it,
    /// [`Unlent::Borrowed`] while the handler's transaction still has it.
    pub(crate) fn lend(&self) -> Result<(Locked<'static, DB>, bool), Unlent> {
        let mut slots = self.book.slots();
        let lent = slots
            .entries
            .get_mut(self.slot.index)
            .map_or(Err(Unlent::Released), |entry| {
                match mem::replace(&mut entry.standing, Standing::Lent(Borrower::Settlement)) {
                    Standing::Held(session) => {
                        Ok((session, mem::take(&mut entry.key), entry.panicked))
                    }
                    other => {
                        let unlent = if matches!(other, Standing::Lent(Borrower::Handler)) {
                            Unlent::Borrowed
                        } else {
                            Unlent::Released
                        };
                        entry.standing = other;
                        Err(unlent)
                    }
                }
            });
        drop(slots);
        lent.map(|(session, key, panicked)| {
            let locked = Locked {
                session: Some(session),
                key: Cow::Owned(key),
                book: self.book,
            };
            (locked, panicked)
        })
    }

    /// Frees the slot after a settlement, keeping the key's buffer for the next delivery.
    pub(crate) fn leave(self, key: String) {
        let hold = ManuallyDrop::new(self);
        let mut slots = hold.book.slots();
        let Slots { entries, free, .. } = &mut *slots;
        let left = entries.get_mut(hold.slot.index).and_then(|entry| {
            if matches!(entry.standing, Standing::Free) {
                return None;
            }
            entry.key = key;
            Some(mem::replace(&mut entry.standing, Standing::Free))
        });
        if left.is_some() {
            free.push(hold.slot.index);
        }
        drop(slots);
        hold.book.returned.notify_waiters();
        // A session still in the slot ends outside the book's lock.
        drop(left);
    }
}

impl<DB: Database> Drop for LockHold<DB> {
    fn drop(&mut self) {
        let book = self.book;
        let mut slots = book.slots();
        let Slots { entries, free, .. } = &mut *slots;
        let Some(entry) = entries.get_mut(self.slot.index) else {
            return;
        };
        let (session, key) = match mem::replace(&mut entry.standing, Standing::Free) {
            // A hold frees its slot once.
            Standing::Free => return,
            // The handler's transaction has the session, which may hold the lock: the slot waits
            // for it, so `shutdown` waits for it too, and ends it when it comes back.
            Standing::Lent(Borrower::Handler) | Standing::Abandoned => {
                entry.standing = Standing::Abandoned;
                drop(slots);
                book.returned.notify_waiters();
                return;
            }
            // A settlement dropped midway: its session ends with it. A released one has ended.
            Standing::Lent(Borrower::Settlement) | Standing::Released => (None, None),
            // The process's key needs no release, so the slot keeps the key's buffer.
            Standing::Held(session) if book.process() => (Some(session), None),
            Standing::Held(session) => (Some(session), Some(mem::take(&mut entry.key))),
        };
        free.push(self.slot.index);
        drop(slots);
        book.returned.notify_waiters();
        let Some(session) = session else {
            return;
        };
        // The unsettled drop. Nothing async runs here: a session that holds a lock in the database
        // closes on the broker's runtime, after an unlock that makes the lock go at once.
        match key {
            Some(key) => book.end(session, Cow::Owned(key)),
            None => session.release(),
        }
    }
}

impl<'k, DB: Database> Locked<'k, DB> {
    /// The session, to run a statement on.
    ///
    /// # Panics
    ///
    /// Never: the session leaves only when it is let go, which consumes this.
    pub(crate) fn session(&mut self) -> &mut Session<DB> {
        self.session
            .as_mut()
            .expect("a locked session stays until it is let go")
    }

    /// The key whose lock the session may hold.
    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    /// The session's connection, to read through.
    ///
    /// # Panics
    ///
    /// As [`session`](Self::session).
    pub(crate) fn conn_ref(&self) -> &DB::Connection {
        self.session
            .as_ref()
            .expect("a locked session stays until it is let go")
            .conn_ref()
    }

    /// The session's connection, to run a statement on.
    ///
    /// # Panics
    ///
    /// As [`session`](Self::session).
    pub(crate) fn conn(&mut self) -> &mut DB::Connection {
        self.session().conn()
    }

    /// Takes the key in the process's registry, under the book's database: `false` while it is in
    /// work.
    ///
    /// # Panics
    ///
    /// As [`session`](Self::session).
    pub(crate) fn take_in_process(&mut self) -> bool {
        let database = self.book.database;
        self.session
            .as_mut()
            .expect("a locked session stays until it is let go")
            .take_in_process(database, &self.key)
    }

    /// The connection and the key, to run the key's unlock on.
    ///
    /// # Panics
    ///
    /// As [`session`](Self::session).
    pub(crate) fn conn_and_key(&mut self) -> (&mut DB::Connection, &str) {
        let session = self
            .session
            .as_mut()
            .expect("a locked session stays until it is let go");
        (session.conn(), &self.key)
    }

    /// Lets the session go, for the caller to end it or to hand it on.
    pub(crate) fn into_session(self) -> Session<DB> {
        self.into_parts().0
    }

    /// Lets the session go with its key.
    ///
    /// # Panics
    ///
    /// As [`session`](Self::session).
    pub(crate) fn into_parts(mut self) -> (Session<DB>, Cow<'k, str>) {
        let session = self
            .session
            .take()
            .expect("a locked session stays until it is let go");
        (session, mem::take(&mut self.key))
    }
}

impl<DB: Database> Drop for Locked<'_, DB> {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            self.book.end(session, mem::take(&mut self.key));
        }
    }
}
