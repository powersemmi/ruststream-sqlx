//! The built-in SQLite dialect.

use std::num::NonZeroUsize;

use crate::dialect::Dialect;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
use crate::table_name::TableName;
use crate::writer::{BuiltIn, SqlWriter};

/// How SQLite reads the current time: as text, in the layout sqlx writes `chrono` times in, so it
/// compares with the times a service binds.
const DATABASE_NOW: &str = "strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')";

/// How a claim of the service's own opens its transaction: with the database's write lock taken,
/// so no other writer comes between its select and its stamps.
const BEGIN_CLAIM: &str = "BEGIN IMMEDIATE";

/// SQLite: double-quoted names, `?` placeholders, rows claimed by lease.
///
/// It builds the statements of the lease form and the insert. A writer locks the whole database,
/// not rows, so the row lock form, whose claim would hold its rows for the handler, is refused
/// ([`StatementError::UnsupportedForm`]): a SQLite table declares `locked_until`. A lease claim is
/// one update that writes the lease, counts the attempt and returns the rows it took, as they were
/// before; one writer at a time keeps two claims apart. The rows of one claim come back in no
/// particular order. A dead letter into a table copies the row, then deletes it, in one
/// transaction. A claim of the service's own opens its transaction with `BEGIN IMMEDIATE`
/// ([`begin_claim`](Dialect::begin_claim)), so it takes the write lock before it selects. Every
/// name is quoted, so a name keeps its case and may hold any character, and SQLite keeps a name of
/// any length. Rows are not read by a list of ids, so a claim of the service's own brings its own
/// fetch.
///
/// SQLite keeps times as text, so the statements compare times as text: a time column holds a
/// layout that sorts as the times it holds.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, Param, Sqlite, TableSpec};
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
///         .attempt(Column::new("attempt"))
///         .payload(Column::new("payload"));
///
/// let claim = Sqlite.claim(&JOBS, ClaimShape::Rows)?;
/// assert_eq!(
///     claim.sql(),
///     r#"UPDATE "jobs" SET "locked_until" = ?, "attempt" = "attempt" + 1 WHERE "job_id" IN (SELECT "job_id" FROM "jobs" WHERE ("locked_until" IS NULL OR "locked_until" <= ?) ORDER BY "job_id" LIMIT ?) RETURNING "job_id", "attempt" - 1 AS "attempt", "locked_until", "payload""#,
/// );
/// assert_eq!(claim.params(), [Param::Lease, Param::LeaseNow, Param::Limit]);
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
///
/// A table in the row lock form is refused, and the refusal names the form:
///
/// ```
/// use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, Sqlite, TableSpec};
///
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
///
/// // What a subscription to the table reports when it starts.
/// let refused = Sqlite
///     .claim(&JOBS, ClaimShape::Rows)
///     .map_err(|refused| format!("subscription `jobs`: {refused}"));
/// assert_eq!(
///     refused,
///     Err("subscription `jobs`: the sqlite dialect has no statements for the row lock form"
///         .to_owned()),
/// );
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Sqlite;

impl BuiltIn for Sqlite {
    /// SQLite keeps a name of any length.
    const NAME_LIMIT: Option<NameLimit> = None;

    /// A writer locks the whole database, so no claim can hold rows for a handler.
    const ROW_LOCKS: bool = false;

    const DEFAULT_ROW: &'static str = " DEFAULT VALUES";

    fn database_now(&self) -> &'static str {
        DATABASE_NOW
    }

    fn database_later(&self, sql: &mut SqlWriter<'_, Self>) {
        sql.push("strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', (")
            .param(Param::Delay)
            .push(" / 1000000.0) || ' seconds')");
    }
}

impl Dialect for Sqlite {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        out.push('"');
        for character in ident.chars() {
            if character == '"' {
                out.push('"');
            }
            out.push(character);
        }
        out.push('"');
    }

    fn placeholder_into(&self, _: NonZeroUsize, out: &mut String) {
        out.push('?');
    }

    fn claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> {
        self.claim_form(spec)?;
        let expiry = self.lease(spec)?;
        let mut sql = SqlWriter::new(self);
        sql.returning_claim(spec, shape, expiry.name());
        Ok(sql.finish())
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

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.extend_statement(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.stamp_statement(spec)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.insert_statement(spec)
    }

    fn claim_counts_attempt(&self, spec: &TableSpec<'_>) -> bool {
        // `*` names no column, so the returned rows carry the attempt the claim wrote.
        spec.selects_all()
    }

    fn begin_claim(&self) -> Option<&'static str> {
        Some(BEGIN_CLAIM)
    }
}
