//! The advisory lock form's book: the deliveries of one subscription in work, each with its key and
//! the session that holds the key's lock, and their release when the broker shuts down; and the
//! process's registry of the keys in work, for a dialect whose locks the process keeps.

use std::borrow::Cow;
use std::mem;
use std::pin::pin;
use std::sync::{Mutex, MutexGuard, PoisonError};

use futures::future::{BoxFuture, join_all};
use sqlx::{Database, Error};
use tokio::sync::Notify;

use crate::inbox::broker::Shared;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{Events, Now, Prepared, Settling, Shape};
use crate::inbox::queue::Queue;
#[cfg(feature = "testing")]
use crate::inbox::testing::off_clock;
use session::{CLOSE_TIMEOUT, Closing, Session, Unlock, Unlocking};

pub(crate) mod claim;
pub(crate) mod events;
mod hold;
mod process;
pub(crate) mod session;
pub(crate) mod settle;
#[cfg(all(test, feature = "sqlite"))]
mod tests;

pub(crate) use hold::{LockHold, Locked, Unlent};
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
