//! The statements a subscription prepared, interned for the life of the process.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, PoisonError};

use ruststream_sqlx_dialect::{Param, Statement};

/// One statement a subscription prepared: its text and the parameters it binds, interned for the
/// life of the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Stmt {
    /// The text.
    pub sql: &'static str,
    /// The parameters, in placeholder order.
    pub params: &'static [Param],
}

/// The statements of one subscription: one per event the crate runs by default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Prepared {
    /// Taking the subscription's group for a claim's transaction, in a table whose groups keep
    /// their order and on a dialect that takes one: the claim runs only once it answers nonzero.
    pub fifo_guard: Option<Stmt>,
    /// Claiming rows, or ids for a fetch of the service's own; in the advisory lock form, the
    /// candidates with their lock keys.
    pub claim: Option<Stmt>,
    /// Reading the rows of ids a claim of the service's own returned.
    pub fetch: Option<Stmt>,
    /// `ack`.
    pub ack: Option<Stmt>,
    /// `retry()`, where it writes anything.
    pub retry: Option<Stmt>,
    /// `retry_after(d)`, with a `retry_after` field.
    pub retry_after: Option<Stmt>,
    /// `drop`.
    pub discard: Option<Stmt>,
    /// The declared dead-letter move.
    pub dead_letter: Option<Stmt>,
    /// The second statement of a dead-letter move the dialect splits in two; the transaction of
    /// the first runs it.
    pub dead_letter_then: Option<Stmt>,
    /// Extending a delivery's lease, in the lease form.
    pub extend: Option<Stmt>,
    /// Leasing one claimed row, where the claim only selects.
    pub stamp: Option<Stmt>,
    /// Taking the advisory lock on a candidate's key for the delivery's session, in the advisory
    /// lock form, where the dialect's database keeps the locks.
    pub lock: Option<Stmt>,
    /// Releasing that lock.
    pub unlock: Option<Stmt>,
    /// Taking a candidate whose key the session holds, in the advisory lock form: it counts the
    /// attempt and reads the row while the row is still claimable.
    pub take: Option<Stmt>,
    /// The read of a take the dialect splits in two; it runs only once the first statement
    /// changed the row.
    pub take_then: Option<Stmt>,
    /// Whether the claim only selects its rows, so the claim's transaction stamps each one: a
    /// claim of the service's own, or a dialect whose lease claim writes no lease.
    pub stamps: bool,
    /// Whether the subscription runs in transactional mode: its handler writes through the
    /// delivery's transaction, which acknowledgement commits.
    pub transactional: bool,
    /// The text that opens a transaction at the table's opening in place of `BEGIN`, where the
    /// dialect names one: what transactional mode opens a delivery's transaction with where the
    /// claim leaves none open. Sent as text, never prepared.
    pub begin_work: Option<&'static str>,
    /// Where a handler's writes start in the claim's transaction: in the row lock form's
    /// transactional mode, set after each claim that took a row; `None` elsewhere.
    pub savepoint: Option<Savepoint>,
}

/// The savepoint a transactional delivery's handler writes after, in the claim's transaction.
/// Machinery.
///
/// Its two texts are sent as they are, never prepared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Savepoint {
    /// Sets the savepoint.
    pub set: &'static str,
    /// Rolls the transaction back to the savepoint, keeping it open.
    pub rollback_to: &'static str,
}

/// Statements and names for the life of the process, shared by every subscription that builds the
/// same one. Interned rather than reference-counted: a delivery reaches its statements with no
/// atomic per message, and a process builds few distinct statements, one set per table and
/// declaration.
static STATEMENTS: LazyLock<Mutex<HashMap<&'static str, &'static [Param]>>> =
    LazyLock::new(Mutex::default);
static NAMES: LazyLock<Mutex<HashSet<&'static str>>> = LazyLock::new(Mutex::default);

/// `statement` for the life of the process.
pub(crate) fn intern(statement: &Statement) -> Stmt {
    let mut interned = STATEMENTS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some((&sql, &params)) = interned.get_key_value(statement.sql()) {
        return Stmt { sql, params };
    }
    let sql: &'static str = Box::leak(statement.sql().into());
    let params: &'static [Param] = Box::leak(statement.params().into());
    interned.insert(sql, params);
    drop(interned);
    Stmt { sql, params }
}

/// `name` for the life of the process.
pub(crate) fn intern_name(name: &str) -> &'static str {
    let mut interned = NAMES.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(&name) = interned.get(name) {
        return name;
    }
    let name: &'static str = Box::leak(name.into());
    interned.insert(name);
    drop(interned);
    name
}

impl Prepared {
    /// Every statement it holds, for the startup check.
    pub(crate) fn statements(&self) -> impl Iterator<Item = Stmt> {
        [
            self.fifo_guard,
            self.claim,
            self.fetch,
            self.ack,
            self.retry,
            self.retry_after,
            self.discard,
            self.dead_letter,
            self.dead_letter_then,
            self.extend,
            self.stamp,
            self.lock,
            self.unlock,
            self.take,
            self.take_then,
        ]
        .into_iter()
        .flatten()
    }
}
