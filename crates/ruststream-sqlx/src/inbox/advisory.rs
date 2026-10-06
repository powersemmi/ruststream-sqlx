//! The advisory lock form's book: the deliveries of one subscription in work, each with its key and
//! the session that holds the key's lock, and their release when the broker shuts down; and the
//! process's registry of the keys in work, for a dialect whose locks the process keeps.

use std::borrow::Cow;
use std::mem::{self, ManuallyDrop};
use std::pin::pin;
use std::sync::{Mutex, MutexGuard, PoisonError};

use futures::future::{BoxFuture, join_all};
use sqlx::{Database, Error};
use tokio::sync::Notify;

use super::broker::Shared;
use super::database::QueueDatabase;
use super::engine::{Events, Now, Prepared, Settling, Shape};
use super::queue::Queue;
use super::session::{CLOSE_TIMEOUT, Closing, Session, Unlock, Unlocking};
#[cfg(feature = "testing")]
use super::testing::off_clock;

mod process;
#[cfg(all(test, feature = "sqlite"))]
mod tests;

use process::database_of;
pub(crate) use process::{ProcessKey, ProcessLocks};

/// The deliveries of one advisory subscription in work, each with its key and its session.
///
/// A subscription leaks its book once and the broker registers it, so a delivery reaches its book
/// through a `'static` reference, with no reference count per message, and `shutdown` reaches every
/// book the broker's subscriptions opened.
pub(crate) struct LockBook<DB: Database> {
    slots: Mutex<Slots<DB>>,
    /// Wakes whoever waits for a lent session to come back to its slot or to leave the book.
    returned: Notify,
    /// The closes of the broker's sessions.
    closing: &'static Closing,
    /// The subscription: its statements, and its names for messages.
    queue: &'static Queue,
    /// Who keeps the keys in work, and so whether the claim's select can leave them out.
    kept_by: KeptBy,
    /// The database the subscription reads, as the process tells databases apart: the keys it
    /// keeps are each database's own.
    database: u64,
    /// The row's unlock, for a session that closes while it holds a lock.
    unlock: Unlock<DB>,
    /// Where "now" comes from for that unlock.
    now: Now,
}

/// Who keeps a subscription's keys in work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeptBy {
    /// The database, in the lock statement the dialect builds: the claim's select probes the same
    /// locks and leaves out the keys in work.
    Database,
    /// The service, in a lock of its own (`custom(lock, unlock)`), which the dialect's select
    /// cannot see.
    Service,
    /// The process, for a dialect whose database keeps no locks: its registry, which no select
    /// sees either.
    Process,
}

impl KeptBy {
    /// Who keeps the keys of a subscription that `prepared` its statements for a row of `shape`:
    /// the dialect built no lock statement where the process keeps them, or where the row runs a
    /// lock of the service's own.
    const fn of(prepared: &Prepared, shape: Shape) -> Self {
        if shape.custom_lock {
            Self::Service
        } else if prepared.lock.is_none() {
            Self::Process
        } else {
            Self::Database
        }
    }
}

/// Every slot the book has held, and which of them are free.
struct Slots<DB: Database> {
    entries: Vec<Entry<DB>>,
    /// The free slots, by index; a claim takes one of them before it adds a slot.
    free: Vec<usize>,
    /// Set when `shutdown` releases the book: a delivery that enters from then on finds its lock
    /// released at once.
    released: bool,
}

/// One slot: its delivery's key, and where its session is.
struct Entry<DB: Database> {
    /// The key whose lock the delivery's session holds. A free slot keeps the last key's buffer, so
    /// the next delivery's key is copied into it.
    key: String,
    /// Counts the deliveries that held the slot; the current one's [`LockSlot`] carries it.
    generation: u64,
    /// Whether a panic of the delivery's handler gave its session back: in transactional mode the
    /// settlement then rolls back what the handler wrote, whatever the outcome.
    panicked: bool,
    standing: Standing<DB>,
}

/// Where a slot's session is.
enum Standing<DB: Database> {
    /// No delivery holds the slot.
    Free,
    /// The delivery's session waits in the slot while its handler does not hold it.
    Held(Session<DB>),
    /// The delivery's settlement, or its handler's transaction, has the session.
    Lent(Borrower),
    /// The delivery settled, or dropped, while its handler's transaction had the session: the slot
    /// waits for the session to come back, then ends it and frees itself.
    Abandoned,
    /// `shutdown` released the delivery's lock and ended its session: the delivery settles no
    /// more, and the slot frees when the delivery leaves.
    Released,
}

/// Who has a slot's session out of the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Borrower {
    /// The delivery's settlement, which ends the session and leaves the slot.
    Settlement,
    /// The handler's transaction in transactional mode, which gives the session back when the
    /// handler ends.
    Handler,
}

/// A delivery's place in a [`LockBook`]: the slot, and which of the deliveries that held the slot
/// it is, so a handler's transaction kept past its delivery never reaches a later delivery's
/// session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LockSlot {
    index: usize,
    generation: u64,
}

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
    book: &'static LockBook<DB>,
    slot: LockSlot,
}

/// A session that may hold the lock on its key outside the book: in a claim, from its lock until
/// the book holds it, and lent to a settlement or a handler's transaction. Dropped before it is let
/// go, by an error or a cancellation, it ends its session: one that may hold the lock in the
/// database closes after an unlock of the key, so the lock is gone once the close ends, and a key
/// the process keeps is freed.
pub(crate) struct Locked<'k, DB: Database> {
    session: Option<Session<DB>>,
    /// Borrowed from the claim's candidates in a claim, owned once lent from the book: a dropped
    /// claim copies it for the unlock, off the path of a message.
    key: Cow<'k, str>,
    book: &'static LockBook<DB>,
}

impl<DB: QueueDatabase> LockBook<DB> {
    /// The book of a subscription to `queue` on `shared`'s connection, whose rows read as `Row`,
    /// registered with the connection for the life of the process.
    pub(crate) fn leak<Row: Events<DB>>(
        shared: &Shared<DB>,
        queue: &'static Queue,
    ) -> &'static Self {
        let book: &'static Self = Box::leak(Box::new(Self {
            slots: Mutex::new(Slots {
                entries: Vec::new(),
                free: Vec::new(),
                released: false,
            }),
            returned: Notify::new(),
            closing: shared.closing,
            queue,
            kept_by: KeptBy::of(&queue.prepared, Row::SHAPE),
            database: database_of(&shared.pool),
            unlock: unlock::<DB, Row>,
            #[cfg(feature = "testing")]
            now: shared.harness.now(),
            #[cfg(not(feature = "testing"))]
            now: Now::default(),
        }));
        shared
            .locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(book);
        book
    }
}

impl<DB: Database> LockBook<DB> {
    /// Records a session that holds `key`'s lock: the key is copied into the slot's kept buffer,
    /// so a delivery allocates only when the subscription has more in work than ever before, or a
    /// longer key.
    ///
    /// Once `shutdown` released the book, the delivery enters released: its session ends here, the
    /// lock released first, and its settlement fails.
    pub(crate) fn enter(&'static self, key: &str, session: Session<DB>) -> LockHold<DB> {
        #[cfg(feature = "testing")]
        let session = {
            let mut session = session;
            session.entered();
            session
        };
        let mut slots = self.slots();
        let Slots {
            entries,
            free,
            released,
        } = &mut *slots;
        let (standing, late) = if *released {
            (Standing::Released, Some(session))
        } else {
            (Standing::Held(session), None)
        };
        let reused = free
            .pop()
            .and_then(|index| Some((index, entries.get_mut(index)?)));
        let slot = if let Some((index, entry)) = reused {
            entry.key.clear();
            entry.key.push_str(key);
            entry.generation = entry.generation.wrapping_add(1);
            entry.panicked = false;
            entry.standing = standing;
            LockSlot {
                index,
                generation: entry.generation,
            }
        } else {
            entries.push(Entry {
                key: key.to_owned(),
                generation: 0,
                panicked: false,
                standing,
            });
            LockSlot {
                index: entries.len() - 1,
                generation: 0,
            }
        };
        drop(slots);
        if let Some(session) = late {
            self.end(session, Cow::Borrowed(key));
        }
        LockHold { book: self, slot }
    }

    /// The session of the delivery in `slot` with its key, out of the book for its handler's
    /// transaction; `None` unless it waits there for the delivery `slot` names.
    pub(crate) fn lend(&'static self, slot: LockSlot) -> Option<Locked<'static, DB>> {
        let mut slots = self.slots();
        let lent = slots
            .entries
            .get_mut(slot.index)
            .filter(|entry| entry.generation == slot.generation)
            .and_then(|entry| {
                match mem::replace(&mut entry.standing, Standing::Lent(Borrower::Handler)) {
                    Standing::Held(session) => Some((session, mem::take(&mut entry.key))),
                    other => {
                        entry.standing = other;
                        None
                    }
                }
            });
        drop(slots);
        lent.map(|(session, key)| Locked {
            session: Some(session),
            key: Cow::Owned(key),
            book: self,
        })
    }

    /// `lent` back into `slot` after the handler, `panicked` when a panic dropped it. A slot whose
    /// delivery settled or dropped meanwhile ends the session here, the lock released first, and
    /// frees itself; so does a slot that holds another delivery by now, the session ending alone.
    pub(crate) fn give_back(&self, slot: LockSlot, lent: Locked<'static, DB>, panicked: bool) {
        let (session, key) = lent.into_parts();
        let mut slots = self.slots();
        let Slots { entries, free, .. } = &mut *slots;
        let ends = match entries
            .get_mut(slot.index)
            .filter(|entry| entry.generation == slot.generation)
        {
            Some(entry) if matches!(entry.standing, Standing::Lent(Borrower::Handler)) => {
                entry.key = key.into_owned();
                entry.panicked = panicked;
                entry.standing = Standing::Held(session);
                None
            }
            Some(entry) if matches!(entry.standing, Standing::Abandoned) => {
                entry.standing = Standing::Free;
                free.push(slot.index);
                Some((session, key))
            }
            _ => Some((session, key)),
        };
        drop(slots);
        self.returned.notify_waiters();
        // A session no delivery waits for ends here.
        if let Some((session, key)) = ends {
            self.end(session, key);
        }
    }

    /// `session`, which may hold the lock on `key`, out of the book: a claim's, from its lock on.
    pub(crate) const fn locked<'k>(
        &'static self,
        session: Session<DB>,
        key: &'k str,
    ) -> Locked<'k, DB> {
        Locked {
            session: Some(session),
            key: Cow::Borrowed(key),
            book: self,
        }
    }

    /// The closes of the broker's sessions.
    pub(crate) const fn closing(&self) -> &'static Closing {
        self.closing
    }

    /// Whether the process keeps the keys in work, rather than the database.
    pub(crate) const fn process(&self) -> bool {
        matches!(self.kept_by, KeptBy::Process)
    }

    /// How many keys in work a claim's select may meet without leaving them out, so the claim
    /// reads that many candidates past its limit: the keys the process keeps for the book's
    /// database, in every subscription and broker of the process; the subscription's deliveries in
    /// work under a lock of the service's own; none where the database keeps the locks the select
    /// probes. A key held elsewhere under a service's lock is passed over only as far as that
    /// margin reaches.
    pub(crate) fn unseen_in_work(&self) -> usize {
        match self.kept_by {
            KeptBy::Database => 0,
            KeptBy::Process => ProcessLocks::held(self.database),
            KeptBy::Service => {
                let slots = self.slots();
                slots.entries.len() - slots.free.len()
            }
        }
    }

    /// Releases the lock of every delivery in the book, once `shutdown` began, and returns how many
    /// releases the database confirmed.
    ///
    /// A session waiting in its slot unlocks its key and goes back to the pool, or closes where
    /// the database does not confirm the release, which ends its lock; the delivery settles no
    /// more. A session lent to a settlement or a handler's transaction is waited for: a settlement
    /// ends and leaves its slot, and a session given back is released then. From the first look
    /// on, a delivery that enters finds its lock released.
    pub(crate) async fn release_all(&'static self) -> usize {
        let mut released = 0;
        loop {
            let mut returned = pin!(self.returned.notified());
            // Enabled before the slots are read, so a session that comes back or leaves in between
            // still wakes the wait.
            returned.as_mut().enable();
            let (held, lent) = self.take_held();
            let confirmed = join_all(held.into_iter().map(|locked| self.release(locked))).await;
            released += confirmed.into_iter().filter(|&confirmed| confirmed).count();
            if !lent {
                return released;
            }
            returned.await;
        }
    }

    /// Takes every session waiting in its slot out of the book, its slot released, and says whether
    /// a session is lent; from here on, a delivery that enters finds its lock released.
    fn take_held(&'static self) -> (Vec<Locked<'static, DB>>, bool) {
        let mut slots = self.slots();
        slots.released = true;
        let mut held = Vec::new();
        let mut lent = false;
        for entry in &mut slots.entries {
            match mem::replace(&mut entry.standing, Standing::Released) {
                Standing::Held(session) => held.push(Locked {
                    session: Some(session),
                    key: Cow::Owned(mem::take(&mut entry.key)),
                    book: self,
                }),
                // A settlement in flight ends its session; a handler's transaction gives it back,
                // and an abandoned slot ends it then.
                standing @ (Standing::Lent(_) | Standing::Abandoned) => {
                    entry.standing = standing;
                    lent = true;
                }
                other => entry.standing = other,
            }
        }
        drop(slots);
        (held, lent)
    }

    /// Releases the lock `locked` holds: `true` when the database confirmed the release and the
    /// session went back to the pool, or the process freed the key.
    async fn release(&'static self, locked: Locked<'static, DB>) -> bool {
        // In process the release runs off a paused clock: its timeout runs out on the test's clock,
        // which stands still while the database answers.
        #[cfg(feature = "testing")]
        if self.closing.in_process() {
            return off_clock(self.unlock_and_release(locked))
                .await
                .unwrap_or(false);
        }
        self.unlock_and_release(locked).await
    }

    async fn unlock_and_release(&'static self, mut locked: Locked<'static, DB>) -> bool {
        if self.process() {
            let mut session = locked.into_session();
            session.free_in_process();
            session.release();
            return true;
        }
        let cx = self.settling();
        let answered = {
            let (conn, key) = locked.conn_and_key();
            // Why a timeout: a session whose statement was cut midway may never answer another.
            tokio::time::timeout(CLOSE_TIMEOUT, (self.unlock)(conn, cx, key)).await
        };
        let queue = self.queue;
        let key = locked.key();
        match &answered {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => tracing::warn!(
                target: "ruststream_sqlx",
                subscription = queue.name,
                table = queue.table,
                row = queue.row,
                key,
                "shutdown's release found the session not holding the delivery's key; the session \
                 closes",
            ),
            Ok(Err(error)) => tracing::warn!(
                target: "ruststream_sqlx",
                subscription = queue.name,
                table = queue.table,
                row = queue.row,
                key,
                %error,
                "shutdown's release of a delivery's lock failed; the session closes, which ends its \
                 lock",
            ),
            Err(_) => tracing::warn!(
                target: "ruststream_sqlx",
                subscription = queue.name,
                table = queue.table,
                row = queue.row,
                key,
                "shutdown's release of a delivery's lock did not answer in time; the session \
                 closes, which ends its lock",
            ),
        }
        let confirmed = matches!(answered, Ok(Ok(true)));
        let mut session = locked.into_session();
        if confirmed {
            session.set_locked(false);
        }
        // A release the database did not confirm leaves the session to close.
        session.release();
        confirmed
    }

    /// The settlement the subscription's unlock runs in.
    const fn settling(&self) -> Settling {
        Settling {
            queue: self.queue,
            now: self.now,
        }
    }

    /// Ends `session`, which may hold the lock on `key`: one that may hold it in the database
    /// closes after an unlock of the key; any other goes back to the pool, its process key freed.
    fn end(&self, session: Session<DB>, key: Cow<'_, str>) {
        if self.process() || !session.locked() {
            session.release();
            return;
        }
        session.release_unlocking(Unlocking {
            unlock: self.unlock,
            cx: self.settling(),
            key: key.into_owned(),
        });
    }

    fn slots(&self) -> MutexGuard<'_, Slots<DB>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }
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

/// `Row`'s unlock of `key` on `conn`, boxed so the book runs it without the row's type: for a
/// session that closes while it holds a lock, off the path of a message.
fn unlock<'a, DB, Row>(
    conn: &'a mut DB::Connection,
    cx: Settling,
    key: &'a str,
) -> BoxFuture<'a, Result<bool, Error>>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    Box::pin(async move { Row::unlock(conn, &cx, key).await })
}
