//! What a tracked publish learns of its own task's SQLite delivery transaction before it writes its
//! record.
//!
//! SQLite takes one writer at a time, and a delivery's transaction holds the write lock from its
//! first write until the delivery settles. A tracked publish of the handler that holds it writes
//! its record on a connection of its own, which would wait for the handler, and the handler for
//! the publish. Each subscription's book of transactions notes, beside a delivery's transaction and
//! under the lock it takes anyway to lend it, the task the handler runs on and whether the handler
//! ran a statement through `Tx`. A tracked publish asks the books of the SQLite subscriptions in
//! the process, which enter a list once, as their first delivery is lent.

use std::any::{Any as Dynamic, TypeId};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use sqlx::{Any, AnyConnection, Database};
#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, SqliteConnection};
use tokio::task::{self, Id};

/// Whether the process tracks publishes: set as an outbox is made, before any delivery.
static TRACKING: AtomicBool = AtomicBool::new(false);

/// The first of the books that lend SQLite delivery transactions; each holds the next.
static BOOKS: OnceLock<&'static Listed> = OnceLock::new();

/// A book in the list, with the one after it.
struct Listed {
    book: &'static dyn Lookup,
    next: OnceLock<&'static Self>,
}

/// What a subscription's transactions are, for a tracked publish: learnt at its first lend in a
/// process that tracks publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Watch {
    /// The database the transactions run in, as the process tells databases apart.
    pub(crate) database: u64,
    /// Whether each transaction takes the write lock as it begins, in an immediate or exclusive
    /// mode.
    pub(crate) at_begin: bool,
}

/// A book of a subscription's delivery transactions, asked by a tracked publish.
pub(crate) trait Lookup: Sync {
    /// What the transaction of the delivery whose handler runs on `task` means for a record
    /// written into `database`.
    fn holding(&self, task: Id, database: u64) -> Holding;
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

impl Holding {
    /// What a delivery in work whose handler ran on the asking task means, in a subscription that
    /// opens its transactions as `watch` says; `ran` tells whether the handler ran a statement.
    pub(crate) const fn of(watch: Watch, ran: bool) -> Self {
        if watch.at_begin {
            Self::HoldsWriteLock
        } else if ran {
            Self::MayHoldWriteLock
        } else {
            Self::Nothing
        }
    }
}

/// A slot's note of whether its delivery's handler ran a statement through `Tx`: made once per
/// slot, set by the handler's first statement, cleared under the book's lock as a delivery is lent.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Ran(&'static AtomicBool);

impl Ran {
    /// The note of a new slot.
    pub(crate) fn new() -> Self {
        Self(Box::leak(Box::new(AtomicBool::new(false))))
    }

    /// Clears the note for the slot's next delivery.
    pub(crate) fn clear(self) {
        self.0.store(false, Ordering::Relaxed);
    }

    /// Notes the handler's first statement.
    pub(crate) fn set(self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Whether the handler ran a statement.
    pub(crate) fn get(self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// Records that the process tracks publishes, so its SQLite deliveries note their handler's task.
pub(crate) fn track() {
    TRACKING.store(true, Ordering::Relaxed);
}

/// The task a delivery on `DB` notes as its handler's, in a process that tracks publishes on a
/// driver that may reach SQLite: one load and a type check the compiler folds otherwise.
pub(crate) fn noted_task<DB: Database>() -> Option<Id> {
    let may_be_sqlite = TypeId::of::<DB>() == TypeId::of::<Any>();
    #[cfg(feature = "sqlite")]
    let may_be_sqlite = may_be_sqlite || TypeId::of::<DB>() == TypeId::of::<Sqlite>();
    if may_be_sqlite && TRACKING.load(Ordering::Relaxed) {
        task::try_id()
    } else {
        None
    }
}

/// Whether `conn` runs on SQLite, through its own driver or `Any`.
pub(crate) fn on_sqlite<DB: Database>(conn: &DB::Connection) -> bool {
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

/// Enters `book` in the list a tracked publish asks: once per book, as it learns its [`Watch`].
pub(crate) fn list(book: &'static dyn Lookup) {
    let listed: &'static Listed = Box::leak(Box::new(Listed {
        book,
        next: OnceLock::new(),
    }));
    let mut tail = &BOOKS;
    while tail.set(listed).is_err() {
        // Another book took this place first: the next one is that book's.
        if let Some(taken) = tail.get() {
            tail = &taken.next;
        }
    }
}

/// Whether any book lends SQLite delivery transactions: one load before a publish asks them.
pub(crate) fn any_listed() -> bool {
    BOOKS.get().is_some()
}

/// What the current task's delivery transaction means for a record it writes into `database`.
pub(crate) fn holding(database: u64) -> Holding {
    let Some(task) = task::try_id() else {
        return Holding::Nothing;
    };
    let mut next = BOOKS.get();
    while let Some(listed) = next {
        let holding = listed.book.holding(task, database);
        if holding != Holding::Nothing {
            return holding;
        }
        next = listed.next.get();
    }
    Holding::Nothing
}

#[cfg(test)]
mod tests {
    use super::{Holding, Watch, locks_at_begin};

    #[test]
    fn an_immediate_or_exclusive_begin_takes_the_write_lock() {
        assert!(locks_at_begin(Some("BEGIN IMMEDIATE")));
        assert!(locks_at_begin(Some("begin exclusive transaction")));
        assert!(!locks_at_begin(Some("BEGIN DEFERRED")));
        assert!(!locks_at_begin(Some("BEGIN")));
        assert!(!locks_at_begin(None));
    }

    #[test]
    fn a_lock_taken_at_begin_is_held_and_a_statement_may_have_taken_it() {
        let deferred = Watch {
            database: 1,
            at_begin: false,
        };
        let immediate = Watch {
            at_begin: true,
            ..deferred
        };
        assert_eq!(Holding::of(immediate, false), Holding::HoldsWriteLock);
        assert_eq!(Holding::of(deferred, true), Holding::MayHoldWriteLock);
        assert_eq!(Holding::of(deferred, false), Holding::Nothing);
    }
}
