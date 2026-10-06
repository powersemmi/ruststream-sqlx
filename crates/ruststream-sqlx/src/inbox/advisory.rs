//! The advisory lock form's book: the deliveries of one subscription in work, each with its key and
//! the session that holds the key's lock, and their release when the broker shuts down; and the
//! process's registry of the keys in work, for a dialect whose locks the process keeps.

use std::borrow::Cow;
use std::collections::HashSet;
use std::hash::BuildHasher;
use std::mem::{self, ManuallyDrop};
use std::pin::pin;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use foldhash::fast::FixedState;
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
    /// Whether the process keeps the keys in work: the dialect builds no lock statement and the
    /// service runs no lock of its own.
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
    /// Set when `shutdown` releases the book: a delivery that enters from then on finds its lock
    /// released at once.
    released: bool,
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
    /// A settlement, or a handler's transaction, has the session.
    Lent,
    /// `shutdown` released the delivery's lock and ended its session: the delivery settles no
    /// more, and the slot frees when the delivery leaves.
    Released,
}

/// A delivery's place in the book: entered at the claim, left once, when the delivery settles.
/// Dropped unconsumed, it ends its session (the unsettled drop): a session that holds a lock in
/// the database closes, the lock released first, and a key the process keeps is freed.
pub(crate) struct LockHold<DB: Database> {
    book: &'static LockBook<DB>,
    slot: usize,
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
            process: in_process(&queue.prepared, Row::SHAPE),
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
            .and_then(|slot| Some((slot, entries.get_mut(slot)?)));
        let slot = if let Some((slot, entry)) = reused {
            entry.key.clear();
            entry.key.push_str(key);
            entry.standing = standing;
            slot
        } else {
            entries.push(Entry {
                key: key.to_owned(),
                standing,
            });
            entries.len() - 1
        };
        drop(slots);
        if let Some(session) = late {
            self.end(session, Cow::Borrowed(key));
        }
        LockHold { book: self, slot }
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
        self.process
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
                Standing::Lent => {
                    entry.standing = Standing::Lent;
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
        if self.process {
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
        if self.process || !session.locked() {
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

    /// The session with its key, out of the book while a settlement (or a handler's transaction)
    /// uses it; `None` when the session is not in its slot, as once `shutdown` released it.
    pub(crate) fn lend(&self) -> Option<Locked<'static, DB>> {
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
        lent.map(|(session, key)| Locked {
            session: Some(session),
            key: Cow::Owned(key),
            book: self.book,
        })
    }

    /// The session back into its slot, with its key, after a handler's transaction used it. A slot
    /// that no longer waits for it ends it, the lock released first.
    #[cfg_attr(
        not(all(test, feature = "sqlite")),
        expect(
            dead_code,
            reason = "a handler's transaction in transactional mode returns its session this way"
        )
    )]
    pub(crate) fn give_back(&self, lent: Locked<'static, DB>) {
        let (session, key) = lent.into_parts();
        let mut slots = self.book.slots();
        let refused = match slots.entries.get_mut(self.slot) {
            Some(entry) if matches!(entry.standing, Standing::Lent) => {
                entry.key = key.into_owned();
                entry.standing = Standing::Held(session);
                None
            }
            _ => Some((session, key)),
        };
        drop(slots);
        self.book.returned.notify_waiters();
        // A slot that no longer waits for its session leaves the session to end here.
        if let Some((session, key)) = refused {
            self.book.end(session, key);
        }
    }

    /// Frees the slot after a settlement, keeping the key's buffer for the next delivery.
    pub(crate) fn leave(self, key: String) {
        let hold = ManuallyDrop::new(self);
        let mut slots = hold.book.slots();
        let Slots { entries, free, .. } = &mut *slots;
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
        let Slots { entries, free, .. } = &mut *slots;
        let Some(entry) = entries.get_mut(self.slot) else {
            return;
        };
        let (session, key) = match mem::replace(&mut entry.standing, Standing::Free) {
            // A hold frees its slot once.
            Standing::Free => return,
            // A settlement dropped midway: its session ends with it. A released one has ended.
            Standing::Lent | Standing::Released => (None, None),
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

/// Whether the process keeps a subscription's keys in work: its dialect built no lock statement,
/// and its row runs no lock of the service's own, which holds a key where the service says.
const fn in_process(prepared: &Prepared, shape: Shape) -> bool {
    prepared.lock.is_none() && !shape.custom_lock
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
    use std::pin::pin;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::future::BoxFuture;
    use futures::poll;
    use ruststream_sqlx_dialect::{Column, Form, KeyPart, TableSpec};
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Error, Sqlite, SqliteConnection, SqlitePool};
    use tokio::runtime::Handle;
    use tokio::sync::Notify;

    use super::{LockBook, ProcessLocks, Slots, Standing, Unlock};
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

    /// An unlock that finds the session not holding the key.
    fn not_held<'a>(
        _: &'a mut SqliteConnection,
        _: Settling,
        _: &'a str,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        Box::pin(async { Ok(false) })
    }

    /// A book whose keys the process keeps, or the database, released with `unlock`.
    fn book_with(process: bool, unlock: Unlock<Sqlite>) -> &'static LockBook<Sqlite> {
        Box::leak(Box::new(LockBook {
            slots: Mutex::new(Slots {
                entries: Vec::new(),
                free: Vec::new(),
                released: false,
            }),
            returned: Notify::new(),
            closing: Closing::leak(Handle::current()),
            queue: queue(),
            process,
            unlock,
            now: Now::default(),
        }))
    }

    /// A book whose keys the process keeps, or the database.
    fn book(process: bool) -> &'static LockBook<Sqlite> {
        book_with(process, unlocked)
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

    fn is_released(book: &LockBook<Sqlite>, index: usize) -> bool {
        matches!(book.slots().entries[index].standing, Standing::Released)
    }

    /// A session of `pool` that holds a lock in the database.
    async fn locked_session(
        pool: &SqlitePool,
        book: &'static LockBook<Sqlite>,
    ) -> Result<Session<Sqlite>, Error> {
        let mut session = Session::acquire(pool, book.closing).await?;
        session.set_locked(true);
        Ok(session)
    }

    #[tokio::test]
    async fn a_free_slot_keeps_its_key_buffer_for_the_next_delivery() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(false);
        let hold = book.enter("jobs-0001", Session::acquire(&pool, book.closing).await?);
        let storage = key_storage(book, 0);
        let (session, key) = hold
            .lend()
            .expect("the session waits in its slot")
            .into_parts();
        assert_eq!(key, "jobs-0001");
        assert!(hold.lend().is_none(), "a lent session is lent once");
        session.release();
        hold.leave(key.into_owned());
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
        let (session, key) = next
            .lend()
            .expect("the session waits in its slot")
            .into_parts();
        session.release();
        next.leave(key.into_owned());
        Ok(())
    }

    #[tokio::test]
    async fn a_lent_session_comes_back_to_its_slot() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(false);
        let hold = book.enter("jobs-7", Session::acquire(&pool, book.closing).await?);
        let lent = hold.lend().expect("the session waits in its slot");
        hold.give_back(lent);
        let (session, key) = hold
            .lend()
            .expect("the session is back in its slot")
            .into_parts();
        assert_eq!(key, "jobs-7");
        session.release();
        hold.leave(key.into_owned());
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_session_lent_and_dropped_closes_after_the_unlock_of_its_key() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(false);
        book.closing.begin_shutdown();
        let hold = book.enter("jobs-8", locked_session(&pool, book).await?);
        let lent = hold.lend().expect("the session waits in its slot");
        drop(lent);
        assert_eq!(pool.size(), 0, "the connection left its pool to close");
        book.closing.settled().await;
        assert_eq!(
            book.closing.forced(),
            1,
            "the session closed holding its lock"
        );
        drop(hold);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_release_unlocks_each_held_session_and_returns_it_to_the_pool() -> Result<(), Error>
    {
        let pool = pool().await?;
        let book = book(false);
        book.closing.begin_shutdown();
        let hold = book.enter("jobs-1", locked_session(&pool, book).await?);
        assert_eq!(pool.size(), 1);
        assert_eq!(book.release_all().await, 1);
        assert!(is_released(book, 0));
        assert!(hold.lend().is_none(), "the delivery settles no more");
        assert_eq!(pool.size(), 1, "the connection went back to the pool");
        drop(hold);
        assert!(is_free(book, 0), "the released delivery leaves its slot");
        book.closing.settled().await;
        assert_eq!(book.closing.forced(), 0, "no session closed");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_release_the_database_does_not_confirm_closes_the_session() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book_with(false, not_held);
        book.closing.begin_shutdown();
        let hold = book.enter("jobs-1", locked_session(&pool, book).await?);
        assert_eq!(book.release_all().await, 0);
        assert_eq!(pool.size(), 0, "the connection left its pool to close");
        book.closing.settled().await;
        assert_eq!(book.closing.forced(), 1, "the close is counted");
        drop(hold);
        Ok(())
    }

    #[tokio::test]
    async fn the_release_frees_each_key_the_process_keeps() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(true);
        let mut session = Session::acquire(&pool, book.closing).await?;
        assert!(session.take_in_process("unit-released-key"));
        let hold = book.enter("unit-released-key", session);
        assert_eq!(book.release_all().await, 1);
        assert!(
            ProcessLocks::try_take("unit-released-key").is_some(),
            "the release freed the key"
        );
        drop(hold);
        Ok(())
    }

    #[tokio::test]
    async fn the_release_waits_for_a_lent_session_and_releases_it_once_it_is_back()
    -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(false);
        let hold = book.enter("jobs-3", locked_session(&pool, book).await?);
        let lent = hold.lend().expect("the session waits in its slot");
        let mut releasing = pin!(book.release_all());
        assert!(
            poll!(releasing.as_mut()).is_pending(),
            "the release waits while a session is lent"
        );
        hold.give_back(lent);
        assert_eq!(releasing.await, 1, "the session given back is released");
        assert!(hold.lend().is_none());
        drop(hold);
        Ok(())
    }

    #[tokio::test]
    async fn the_release_waits_for_a_settlement_until_it_leaves() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(false);
        let hold = book.enter("jobs-4", Session::acquire(&pool, book.closing).await?);
        let (session, key) = hold
            .lend()
            .expect("the session waits in its slot")
            .into_parts();
        let mut releasing = pin!(book.release_all());
        assert!(poll!(releasing.as_mut()).is_pending());
        session.release();
        hold.leave(key.into_owned());
        assert_eq!(releasing.await, 0, "the settlement released its own lock");
        assert!(is_free(book, 0));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_that_enters_a_released_book_ends_its_session() -> Result<(), Error> {
        let pool = pool().await?;
        let book = book(false);
        book.closing.begin_shutdown();
        assert_eq!(book.release_all().await, 0);
        let hold = book.enter("jobs-5", locked_session(&pool, book).await?);
        assert!(is_released(book, 0));
        assert!(hold.lend().is_none(), "the late delivery settles no more");
        book.closing.settled().await;
        assert_eq!(
            book.closing.forced(),
            1,
            "its session closed after the unlock of its key"
        );
        drop(hold);
        assert!(is_free(book, 0));
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
