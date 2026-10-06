//! The advisory lock form's book: the deliveries of one subscription in work, each with its key and
//! the session that holds the key's lock; and the process's registry of the keys in work, for a
//! dialect whose locks the process keeps.

use std::collections::HashSet;
use std::hash::BuildHasher;
use std::mem::{self, ManuallyDrop};
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use foldhash::fast::FixedState;
use futures::future::BoxFuture;
use sqlx::{Database, Error};
use tokio::sync::Notify;

use super::broker::Shared;
use super::database::QueueDatabase;
use super::engine::{Events, Now, Settling};
use super::queue::Queue;
use super::session::{Closing, Session, Unlock, Unlocking};

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
    /// Whether the process keeps the keys in work: the subscription prepared no lock statement.
    process: bool,
    /// The row's unlock, for a session that closes while it holds a lock.
    unlock: Unlock<DB>,
    /// Where "now" comes from for that unlock.
    now: Now,
}

/// Every slot the book has held, and which of them are free.
struct Slots<DB: Database> {
    entries: Vec<Entry<DB>>,
    /// The free slots, by index; a claim takes one of them before it adds a slot.
    free: Vec<usize>,
}

/// One slot: its delivery's key, and where its session is.
struct Entry<DB: Database> {
    /// The key whose lock the delivery's session holds. A free slot keeps the last key's buffer, so
    /// the next delivery's key is copied into it.
    key: String,
    standing: Standing<DB>,
}

/// Where a slot's session is.
enum Standing<DB: Database> {
    /// No delivery holds the slot.
    Free,
    /// The delivery's session waits in the slot while its handler works.
    Held(Session<DB>),
    /// A settlement has the session.
    Lent,
}

/// A delivery's place in the book: entered at the claim, left once, when the delivery settles.
/// Dropped unconsumed, it ends its session (the unsettled drop): a session that holds a lock in
/// the database closes, the lock released first, and a key the process keeps is freed.
pub(crate) struct LockHold<DB: Database> {
    book: &'static LockBook<DB>,
    slot: usize,
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
            }),
            returned: Notify::new(),
            closing: shared.closing,
            queue,
            process: queue.prepared.lock.is_none(),
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
    pub(crate) fn enter(&'static self, key: &str, session: Session<DB>) -> LockHold<DB> {
        let mut slots = self.slots();
        let Slots { entries, free } = &mut *slots;
        let reused = free
            .pop()
            .and_then(|slot| Some((slot, entries.get_mut(slot)?)));
        let slot = if let Some((slot, entry)) = reused {
            entry.key.clear();
            entry.key.push_str(key);
            entry.standing = Standing::Held(session);
            slot
        } else {
            entries.push(Entry {
                key: key.to_owned(),
                standing: Standing::Held(session),
            });
            entries.len() - 1
        };
        drop(slots);
        LockHold { book: self, slot }
    }

    /// The closes of the broker's sessions.
    pub(crate) const fn closing(&self) -> &'static Closing {
        self.closing
    }

    /// Whether the process keeps the keys in work, rather than the database.
    pub(crate) const fn process(&self) -> bool {
        self.process
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

    /// The session and its key, out of the book while a settlement (or a handler's transaction)
    /// uses it; `None` when the session is not in its slot.
    pub(crate) fn lend(&self) -> Option<(Session<DB>, String)> {
        let mut slots = self.book.slots();
        let lent = slots.entries.get_mut(self.slot).and_then(|entry| {
            match mem::replace(&mut entry.standing, Standing::Lent) {
                Standing::Held(session) => Some((session, mem::take(&mut entry.key))),
                other => {
                    entry.standing = other;
                    None
                }
            }
        });
        drop(slots);
        lent
    }

    /// The session back into its slot, with its key, after a handler's transaction used it.
    #[cfg_attr(
        not(all(test, feature = "sqlite")),
        expect(
            dead_code,
            reason = "a handler's transaction in transactional mode returns its session this way"
        )
    )]
    pub(crate) fn give_back(&self, session: Session<DB>, key: String) {
        let mut slots = self.book.slots();
        let refused = match slots.entries.get_mut(self.slot) {
            Some(entry) if matches!(entry.standing, Standing::Lent) => {
                entry.key = key;
                entry.standing = Standing::Held(session);
                None
            }
            _ => Some(session),
        };
        drop(slots);
        self.book.returned.notify_waiters();
        // A slot that no longer waits for its session leaves the session to end here.
        drop(refused);
    }

    /// Frees the slot after a settlement, keeping the key's buffer for the next delivery.
    pub(crate) fn leave(self, key: String) {
        let hold = ManuallyDrop::new(self);
        let mut slots = hold.book.slots();
        let Slots { entries, free } = &mut *slots;
        let left = entries.get_mut(hold.slot).and_then(|entry| {
            if matches!(entry.standing, Standing::Free) {
                return None;
            }
            entry.key = key;
            Some(mem::replace(&mut entry.standing, Standing::Free))
        });
        if left.is_some() {
            free.push(hold.slot);
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
        let Slots { entries, free } = &mut *slots;
        let Some(entry) = entries.get_mut(self.slot) else {
            return;
        };
        let (session, key) = match mem::replace(&mut entry.standing, Standing::Free) {
            // A hold frees its slot once.
            Standing::Free => return,
            // A settlement dropped midway: its session ends with it.
            Standing::Lent => (None, None),
            // The process's key needs no release, so the slot keeps the key's buffer.
            Standing::Held(session) if book.process => (Some(session), None),
            Standing::Held(session) => (Some(session), Some(mem::take(&mut entry.key))),
        };
        free.push(self.slot);
        drop(slots);
        book.returned.notify_waiters();
        let Some(session) = session else {
            return;
        };
        // The unsettled drop. Nothing async runs here: a session that holds a lock in the database
        // closes on the broker's runtime, after an unlock that makes the lock go at once.
        match key {
            Some(key) => session.release_unlocking(Unlocking {
                unlock: book.unlock,
                cx: Settling {
                    queue: book.queue,
                    now: book.now,
                },
                key,
            }),
            None => session.release(),
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

/// The process's registry of advisory keys in work, for a dialect whose locks the process keeps
/// (SQLite): the 64-bit hash of each key.
///
/// Two keys with one hash wait for each other, a delay and never a double delivery. Keys are
/// process-wide, as a database's locks are database-wide.
pub(crate) struct ProcessLocks;

/// The seed of the key hashes, fixed so a key hashes alike in every run.
const SEED: u64 = 0x5d1c_9e37_79b9_7f4a;

/// The hashes of the keys in work. The set keeps its capacity, so a key in work allocates only
/// when the process has more keys in work than ever before.
static KEYS: LazyLock<Mutex<HashSet<u64, FixedState>>> =
    LazyLock::new(|| Mutex::new(HashSet::with_hasher(FixedState::default())));

impl ProcessLocks {
    /// Takes `key` for the process: `None` while a key of its hash is in work.
    pub(crate) fn try_take(key: &str) -> Option<ProcessKey> {
        let hash = FixedState::with_seed(SEED).hash_one(key);
        // The key is made only when taken: a key dropped here would free its hash, which another
        // holder keeps.
        let taken = Self::keys().insert(hash);
        taken.then(|| ProcessKey(hash))
    }

    fn keys() -> MutexGuard<'static, HashSet<u64, FixedState>> {
        KEYS.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A key the process keeps in work, by its hash; dropping it frees the key.
#[derive(Debug)]
pub(crate) struct ProcessKey(u64);

impl Drop for ProcessKey {
    fn drop(&mut self) {
        ProcessLocks::keys().remove(&self.0);
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::future::BoxFuture;
    use ruststream_sqlx_dialect::{Column, Form, KeyPart, TableSpec};
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Error, Sqlite, SqliteConnection, SqlitePool};
    use tokio::runtime::Handle;
    use tokio::sync::Notify;

    use super::{LockBook, ProcessLocks, Slots, Standing};
    use crate::inbox::engine::{IdAt, Now, Prepared, Settling};
    use crate::inbox::queue::Queue;
    use crate::inbox::session::{Closing, Session};

    const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("id")];

    /// A subscription to the jobs of an advisory table.
    fn queue() -> &'static Queue {
        Box::leak(Box::new(Queue {
            name: "jobs",
            table: "jobs",
            row: "Job",
            spec: TableSpec::new("jobs", Column::new("id"), Form::Advisory(KEY))
                .payload(Column::new("payload")),
            id_at: IdAt::First,
            native_retry_after: false,
            kinds: None,
            prepared: Prepared::default(),
            begin_claim: None,
            counted_attempt: false,
            poll_interval: Duration::from_secs(1),
            lease: None,
            cap: None,
        }))
    }

    /// An unlock that always finds its lock.
    fn unlocked<'a>(
        _: &'a mut SqliteConnection,
        _: Settling,
        _: &'a str,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        Box::pin(async { Ok(true) })
    }

    /// A book whose keys the process keeps, or the database.
    fn book(process: bool) -> &'static LockBook<Sqlite> {
        Box::leak(Box::new(LockBook {
            slots: Mutex::new(Slots {
                entries: Vec::new(),
                free: Vec::new(),
            }),
            returned: Notify::new(),
            closing: Closing::leak(Handle::current()),
            queue: queue(),
            process,
            unlock: unlocked,
            now: Now::default(),
        }))
    }

    async fn pool() -> Result<SqlitePool, Error> {
        SqlitePoolOptions::new()
            .max_connections(2)
            .connect("sqlite::memory:")
            .await
    }

    /// Where the key of the slot at `index` keeps its bytes.
    fn key_storage(book: &LockBook<Sqlite>, index: usize) -> *const u8 {
        book.slots().entries[index].key.as_ptr()
    }

    fn is_free(book: &LockBook<Sqlite>, index: usize) -> bool {
        matches!(book.slots().entries[index].standing, Standing::Free)
    }

    #[tokio::test]
    async fn a_free_slot_keeps_its_key_buffer_for_the_next_delivery() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(false);
        let hold = book.enter("jobs-0001", Session::acquire(&pool, book.closing).await?);
        let storage = key_storage(book, 0);
        let (session, key) = hold.lend().expect("the session waits in its slot");
        assert_eq!(key, "jobs-0001");
        assert!(hold.lend().is_none(), "a lent session is lent once");
        session.release();
        hold.leave(key);
        assert!(is_free(book, 0));
        let next = book.enter("jobs-0002", Session::acquire(&pool, book.closing).await?);
        {
            let slots = book.slots();
            assert_eq!(slots.entries.len(), 1, "the delivery took the free slot");
            assert_eq!(slots.entries[0].key, "jobs-0002");
        }
        assert_eq!(
            key_storage(book, 0),
            storage,
            "the key was copied into the buffer the last one left"
        );
        let (session, key) = next.lend().expect("the session waits in its slot");
        session.release();
        next.leave(key);
        Ok(())
    }

    #[tokio::test]
    async fn a_lent_session_comes_back_to_its_slot() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(false);
        let hold = book.enter("jobs-7", Session::acquire(&pool, book.closing).await?);
        let (session, key) = hold.lend().expect("the session waits in its slot");
        hold.give_back(session, key);
        let (session, key) = hold.lend().expect("the session is back in its slot");
        assert_eq!(key, "jobs-7");
        session.release();
        hold.leave(key);
        assert!(is_free(book, 0));
        Ok(())
    }

    #[tokio::test]
    async fn a_hold_dropped_unsettled_frees_its_slot_and_its_process_key() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(true);
        let mut session = Session::acquire(&pool, book.closing).await?;
        assert!(session.take_in_process("unit-dropped-hold"));
        let hold = book.enter("unit-dropped-hold", session);
        assert!(
            ProcessLocks::try_take("unit-dropped-hold").is_none(),
            "the delivery holds its key"
        );
        drop(hold);
        assert!(is_free(book, 0));
        assert_eq!(book.slots().free, [0]);
        assert!(
            ProcessLocks::try_take("unit-dropped-hold").is_some(),
            "the unsettled drop freed the key"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_settlement_dropped_midway_frees_its_slot() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(false);
        let hold = book.enter("jobs-9", Session::acquire(&pool, book.closing).await?);
        let lent = hold.lend().expect("the session waits in its slot");
        // The settlement's future drops with the hold it owns, and the session it lent ends on
        // its own.
        drop(hold);
        assert!(is_free(book, 0));
        assert_eq!(book.slots().free, [0]);
        drop(lent);
        Ok(())
    }

    #[test]
    fn a_refused_take_keeps_the_holders_key() {
        let held = ProcessLocks::try_take("unit-refused-take").expect("a free key is taken");
        assert!(ProcessLocks::try_take("unit-refused-take").is_none());
        assert!(
            ProcessLocks::try_take("unit-refused-take").is_none(),
            "the refused take left the key with its holder"
        );
        drop(held);
        assert!(
            ProcessLocks::try_take("unit-refused-take").is_some(),
            "a freed key is taken again"
        );
    }
}
