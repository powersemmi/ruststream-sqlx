//! The SQLite delivery transactions in work, by the task that runs their handler: what a tracked
//! publish of that task reads before it writes its record.
//!
//! SQLite takes one writer at a time, and a delivery's transaction holds the write lock from its
//! first write until the delivery settles. A tracked publish of the handler that holds it writes
//! its record on a connection of its own, which would wait for the handler, and the handler for
//! the publish. The book is kept only in a process that tracks publishes, and only for SQLite.

use std::any::Any as Dynamic;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

#[cfg(feature = "sqlite")]
use sqlx::SqliteConnection;
use sqlx::{AnyConnection, Database};
use tokio::task::{self, Id};

use super::book::TxSlot;
use crate::inbox::form::advisory::LockSlot;

/// Whether the process tracks publishes: set as an outbox is made, before any delivery.
static TRACKING: AtomicBool = AtomicBool::new(false);

/// The tasks that hold a SQLite delivery transaction, as many as hold one at a time.
static HOLDERS: Mutex<Vec<Holder>> = Mutex::new(Vec::new());

/// How many entries of [`HOLDERS`] name a task: a publish reads it without the lock, and finds
/// no holder of its own while it reads zero.
static HELD: AtomicUsize = AtomicUsize::new(0);

/// The book a delivery's transaction is lent from, asked whether the delivery is still in work.
pub(crate) trait InWork: Sync {
    /// Whether the delivery `slot` names is still in work.
    fn in_work(&self, slot: Slot) -> bool;
}

/// A delivery's place in the book its transaction is lent from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Slot {
    /// In a [`TxBook`](super::TxBook).
    Tx(TxSlot),
    /// In a [`LockBook`](crate::inbox::form::advisory::LockBook).
    Lock(LockSlot),
}

/// One entry of the book, kept once made: a task that holds a delivery transaction, or none.
struct Holder {
    task: Option<Id>,
    /// The book of the delivery, and its place there: an entry its settlement did not clear, as
    /// a settlement on another task does not, is stale once the delivery leaves the book.
    delivery: Option<(&'static dyn InWork, Slot)>,
    /// The database the transaction runs in, as the process tells databases apart.
    database: u64,
    /// Whether the transaction took the write lock as it began, in an immediate or exclusive mode.
    at_begin: bool,
    /// Whether the transaction may hold the write lock: from its begin in an immediate or
    /// exclusive mode, from the handler's first statement through `Tx` otherwise.
    locked: &'static AtomicBool,
}

/// What a tracked publish into a database learns of its own task's delivery transaction there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Holding {
    /// The task holds no SQLite delivery transaction in the database, or one that has run no
    /// statement: the record waits for the write lock as any writer does.
    Nothing,
    /// The task's delivery transaction may hold the write lock the record needs: it ran a
    /// statement, and SQLite tells whether that statement wrote only by the wait.
    MayHoldWriteLock,
    /// The task's delivery transaction holds the write lock the record needs: it took the lock as
    /// it began.
    HoldsWriteLock,
}

/// Records that the process tracks publishes, so its SQLite deliveries enter the book.
pub(crate) fn track() {
    TRACKING.store(true, Ordering::Relaxed);
}

/// Whether a delivery on `conn` writes into a database that takes one writer at a time, in a
/// process that tracks publishes.
pub(crate) fn watched<DB: Database>(conn: &DB::Connection) -> bool {
    if !TRACKING.load(Ordering::Relaxed) {
        return false;
    }
    let conn: &dyn Dynamic = conn;
    #[cfg(feature = "sqlite")]
    if conn.is::<SqliteConnection>() {
        return true;
    }
    conn.downcast_ref::<AnyConnection>()
        .is_some_and(|conn| conn.backend_name() == "SQLite")
}

/// Whether a transaction opened with `statement` takes the write lock as it begins.
pub(crate) fn locks_at_begin(statement: Option<&str>) -> bool {
    statement.is_some_and(|statement| {
        let statement = statement.trim_start();
        ["BEGIN IMMEDIATE", "BEGIN EXCLUSIVE"].iter().any(|mode| {
            statement
                .get(..mode.len())
                .is_some_and(|start| start.eq_ignore_ascii_case(mode))
        })
    })
}

/// Enters the current task as the holder of a SQLite delivery transaction in `database`,
/// `at_begin` when the transaction took the write lock as it began. Returns the flag the handler's
/// first statement sets; `None` outside a Tokio task.
pub(crate) fn enlist(
    at_begin: bool,
    database: u64,
    delivery: (&'static dyn InWork, Slot),
) -> Option<&'static AtomicBool> {
    let task = task::try_id()?;
    let mut holders = holders();
    let own = holders.iter().position(|holder| holder.task == Some(task));
    let index = own
        .or_else(|| holders.iter().position(|holder| holder.task.is_none()))
        .unwrap_or_else(|| {
            // One flag per entry, made once: the book holds as many entries as tasks hold
            // delivery transactions at one time.
            holders.push(Holder {
                task: None,
                delivery: None,
                database,
                at_begin,
                locked: Box::leak(Box::new(AtomicBool::new(false))),
            });
            holders.len() - 1
        });
    if own.is_none() {
        HELD.store(HELD.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    }
    let holder = &mut holders[index];
    holder.task = Some(task);
    holder.delivery = Some(delivery);
    holder.database = database;
    holder.at_begin = at_begin;
    holder.locked.store(at_begin, Ordering::Relaxed);
    let flag = holder.locked;
    drop(holders);
    Some(flag)
}

/// Takes the current task out of the book as its delivery settles.
pub(crate) fn release() {
    if HELD.load(Ordering::Relaxed) == 0 {
        return;
    }
    let Some(task) = task::try_id() else { return };
    let mut holders = holders();
    if let Some(holder) = holders.iter_mut().find(|holder| holder.task == Some(task)) {
        holder.task = None;
        holder.delivery = None;
        holder.locked.store(false, Ordering::Relaxed);
        HELD.store(HELD.load(Ordering::Relaxed) - 1, Ordering::Relaxed);
    }
}

/// Whether the current task holds a SQLite delivery transaction: what a tracked publish asks
/// before it learns which database its record goes to. One load while no task holds one.
pub(crate) fn any_held() -> bool {
    HELD.load(Ordering::Relaxed) != 0
}

/// What the current task's delivery transaction means for a record it writes into `database`.
pub(crate) fn holding(database: u64) -> Holding {
    let Some(task) = task::try_id() else {
        return Holding::Nothing;
    };
    let holders = holders();
    let found = holders
        .iter()
        .find(|holder| holder.task == Some(task) && holder.database == database)
        .map(|holder| {
            (
                holder.at_begin,
                holder.locked.load(Ordering::Relaxed),
                holder.delivery,
            )
        });
    drop(holders);
    match found {
        // A stale entry may only cost a bounded wait, never refuse a publish: the refusal asks the
        // delivery's book first, outside the lock of this one.
        Some((true, _, Some((book, slot)))) if book.in_work(slot) => Holding::HoldsWriteLock,
        Some((_, true, _)) => Holding::MayHoldWriteLock,
        _ => Holding::Nothing,
    }
}

fn holders() -> MutexGuard<'static, Vec<Holder>> {
    HOLDERS.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::locks_at_begin;

    #[test]
    fn an_immediate_or_exclusive_begin_takes_the_write_lock() {
        assert!(locks_at_begin(Some("BEGIN IMMEDIATE")));
        assert!(locks_at_begin(Some("begin exclusive transaction")));
        assert!(!locks_at_begin(Some("BEGIN DEFERRED")));
        assert!(!locks_at_begin(Some("BEGIN")));
        assert!(!locks_at_begin(None));
    }
}
