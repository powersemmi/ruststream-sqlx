//! The built-in SQLite dialect.

use std::num::NonZeroUsize;

use crate::advisory::Advisory;
use crate::dialect::Dialect;
use crate::form::KeyPart;
use crate::lease::Lease;
use crate::opening::{Mode, Opening, Opens, level};
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
use crate::table_name::TableName;
use crate::writer::{BuiltIn, Probe, SqlWriter};

/// How SQLite reads the current time: as text, in the layout sqlx writes `chrono` times in, so it
/// compares with the times a service binds.
const DATABASE_NOW: &str = "strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')";

/// How a claim of the service's own opens its transaction: with the database's write lock taken,
/// so no other writer comes between its select and its stamps.
const BEGIN_CLAIM: &str = "BEGIN IMMEDIATE";

/// SQLite: backtick-quoted names, `?` placeholders, rows claimed by lease.
///
/// It builds the statements of the lease form ([`Lease`]), of the advisory lock form
/// ([`Advisory`]), and the insert. A writer locks the whole database, not rows, so it builds no
/// claim that holds rows for a handler: it does not implement [`RowLock`](crate::RowLock), and a
/// SQLite table declares `locked_until` or `advisory_lock`. A lease claim is one
/// update that writes the lease, counts the attempt and returns the rows it took, as they were
/// before; one writer at a time keeps two claims apart. The rows of one claim come back in no
/// particular order. A dead letter into a table copies the row, then deletes it, in one
/// transaction. A claim of the service's own opens its transaction with `BEGIN IMMEDIATE`
/// ([`begin_lease_claim`](Lease::begin_lease_claim)), so it takes the write lock before it
/// selects. Every name is quoted, so a name keeps its case and may hold any character, and SQLite
/// keeps a name of any length. The quotes are backticks, which SQLite always reads as a name: a
/// statement that names a column the table lacks fails when it is prepared.
/// [`fetch`](Dialect::fetch) is refused, so a claim of the service's own brings a fetch of its
/// own.
///
/// SQLite keeps times as text, so the statements compare times as text: two times compare right
/// when their text sorts as the times do.
///
/// SQLite has no locks a session holds, so in the advisory lock form the broker keeps the keys in
/// work in the process: [`lock`](Advisory::lock) and [`unlock`](Advisory::unlock) are `None`, and
/// the claim selects up to [`Param::Limit`] claimable rows in claim order with their keys, the text
/// the key's parts render, a column without a value read as empty text. The select cannot leave
/// out the keys in work, so the broker binds a limit that reaches past them. The take counts the
/// attempt and returns the row in one statement.
///
/// A table with FIFO groups needs no guard ([`fifo_guard`](Dialect::fifo_guard) is `None`): one
/// writer at a time keeps two claims apart, and a lease claim takes nothing while a row of the
/// group holds a lease.
///
/// SQLite runs every transaction serializable, so a table names no isolation level here: it names
/// a mode, and its transactions open with `BEGIN DEFERRED`, `BEGIN IMMEDIATE` or
/// `BEGIN EXCLUSIVE` ([`begin`](Dialect::begin)), or with `BEGIN` where it names none.
///
/// # Examples
///
/// A subscription asks SQLite for the statements of its table, as `ruststream-sqlx` does when the
/// subscription starts:
///
/// ```
/// use ruststream_sqlx_dialect::{
///     Advisory, Column, Form, KeyPart, Sqlite, Statement, StatementError, TableSpec,
/// };
///
/// // The jobs table as `#[derive(Inbox)]` describes it, with `advisory_lock = "jobs-{job_id}"`.
/// const JOBS: TableSpec<'static> = TableSpec::new(
///     "jobs",
///     Column::new("job_id"),
///     Form::Advisory(&[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")]),
/// );
///
/// // What a subscription to an advisory lock table prepares: the claim of candidates, and the
/// // lock on a key where the database keeps locks; without one the broker keeps the keys in work
/// // in the process.
/// fn advisory_statements(
///     dialect: &impl Advisory,
///     spec: &TableSpec<'_>,
/// ) -> Result<(Statement, Option<Statement>), StatementError> {
///     Ok((dialect.advisory_claim(spec)?, dialect.lock()))
/// }
///
/// fn main() -> Result<(), StatementError> {
///     // SQLite keeps no locks a session holds, so the process keeps the keys in work.
///     let (_claim, lock) = advisory_statements(&Sqlite, &JOBS)?;
///     assert!(lock.is_none());
///     Ok(())
/// }
/// ```
///
/// A table in the row lock form finds no claim here, so a subscription to one does not compile:
///
/// ```compile_fail,E0277
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Column, Form, RowLock, Sqlite, Statement, StatementError, TableSpec,
/// };
///
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
///
/// // What a subscription to a row lock table asks of its dialect.
/// fn claims_its_rows(dialect: &impl RowLock) -> Result<Statement, StatementError> {
///     dialect.lock_claim(&JOBS, ClaimShape::Rows)
/// }
///
/// let claim = claims_its_rows(&Sqlite);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Sqlite;

impl BuiltIn for Sqlite {
    /// SQLite keeps a name of any length.
    const NAME_LIMIT: Option<NameLimit> = None;

    /// A writer locks the whole database, so no claim can hold rows for a handler.
    const ROW_LOCKS: bool = false;

    const DEFAULT_ROW: &'static str = " DEFAULT VALUES";

    const BACKSLASH_ESCAPES: bool = false;

    const UPDATE_RETURNS: bool = true;

    /// The process keeps the locks, so the database cannot tell a key in work.
    const PROBE: Probe = Probe::Blind;

    fn database_now(&self) -> &'static str {
        DATABASE_NOW
    }

    fn database_later(&self, sql: &mut SqlWriter<'_, Self>) {
        sql.push("strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', (")
            .param(Param::Delay)
            .push(" / 1000000.0) || ' seconds')");
    }

    // SQLite takes no locks: the broker keeps the keys in work in the process, so the key alone
    // is rendered.
    fn render_lock_key(&self, sql: &mut SqlWriter<'_, Self>, _: Option<&str>, key: &[KeyPart<'_>]) {
        // `||` with a column without a value gives no text at all, so each column reads as empty
        // text there; the cast makes a key of one number text.
        sql.push("CAST(")
            .key_parts(key, " || ", |sql, column| {
                sql.push("ifnull(").ident(column).push(", '')");
            })
            .push(" AS TEXT)");
    }
}

impl Dialect for Sqlite {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        // SQLite reads a double-quoted name that matches no column as a string literal, so a
        // misnamed column would prepare and read as text; a backtick-quoted name is always a name.
        out.push('`');
        for character in ident.chars() {
            if character == '`' {
                out.push('`');
            }
            out.push(character);
        }
        out.push('`');
    }

    fn placeholder_into(&self, _: NonZeroUsize, out: &mut String) {
        out.push('?');
    }

    fn fetch(&self, _: &TableSpec<'_>) -> Result<Statement, StatementError> {
        // A statement is prepared once with fixed text, and SQLite binds no list as one parameter.
        Err(StatementError::UnsupportedFetch {
            dialect: self.name(),
        })
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.finish_statement(spec)
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        self.retry_statement(spec)
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.retry_after_statement(spec)
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.finish_statement(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.dead_letter_group_statement(spec)
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        // SQLite feeds no delete's rows into an insert.
        self.copy_then_delete(spec, target)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.insert_statement(spec)
    }

    fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
        match opening {
            Opening::Default => Ok(None),
            Opening::Mode(Mode::Deferred) => Ok(Some("BEGIN DEFERRED")),
            Opening::Mode(Mode::Immediate) => Ok(Some("BEGIN IMMEDIATE")),
            Opening::Mode(Mode::Exclusive) => Ok(Some("BEGIN EXCLUSIVE")),
            Opening::Isolation(_) => Err(opening.refused(self.name())),
        }
    }
}

impl Opens<level::Deferred> for Sqlite {}

impl Opens<level::Immediate> for Sqlite {}

impl Opens<level::Exclusive> for Sqlite {}

impl Advisory for Sqlite {
    fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.advisory_claim_statement(spec)
    }

    fn lock(&self) -> Option<Statement> {
        None
    }

    fn unlock(&self) -> Option<Statement> {
        None
    }

    fn take(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError> {
        self.take_statements(spec, shape)
    }
}

impl Lease for Sqlite {
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        let expiry = self.leased(spec, "lease_claim")?;
        let mut sql = SqlWriter::new(self);
        sql.returning_claim(spec, shape, expiry.name());
        Ok(sql.finish())
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.extend_statement(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.stamp_statement(spec)
    }

    fn claim_counts_attempt(&self, spec: &TableSpec<'_>) -> bool {
        // `*` names no column, so the returned rows carry the attempt the claim wrote.
        spec.selects_all()
    }

    fn begin_lease_claim(&self) -> Option<&'static str> {
        Some(BEGIN_CLAIM)
    }
}
