//! The built-in Postgres dialect.

use std::num::NonZeroUsize;

use crate::dialect::Dialect;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
use crate::table_name::TableName;
use crate::writer::{Built, BuiltIn, SqlWriter};

/// How Postgres reads the current time: the start of the statement, so a settlement made long
/// after the claim began records its own moment.
const DATABASE_NOW: &str = "statement_timestamp()";

/// How a claim locks the rows it takes: until its transaction ends, skipping the rows another
/// claim holds.
const LOCK: &str = " FOR UPDATE SKIP LOCKED";

/// Postgres: double-quoted names, `$1` placeholders, rows claimed with `FOR UPDATE SKIP LOCKED`.
///
/// It builds the statements of the row lock and lease forms and the insert. A lease claim is one
/// statement: it locks the claimable rows, writes their lease, and returns them as they were.
/// Every name is quoted, so a name keeps its case and may hold any character; a name over 63
/// bytes, which Postgres would cut short without a word, is refused. A table on the database's
/// clock reads `statement_timestamp()`.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, Postgres, TableSpec};
///
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
///     .within("app")
///     .priority(Column::new("priority"))
///     .payload(Column::new("payload"));
///
/// let claim = Postgres.claim(&JOBS, ClaimShape::Rows)?;
/// assert_eq!(
///     claim.sql(),
///     r#"SELECT "job_id", "priority", "payload" FROM "app"."jobs" ORDER BY "priority", "job_id" LIMIT $1 FOR UPDATE SKIP LOCKED"#,
/// );
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
///
/// In the lease form a settlement changes the row only while the row holds the delivery's lease:
///
/// ```
/// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
///         .payload(Column::new("payload"));
///
/// let ack = Postgres.ack(&JOBS)?;
/// assert_eq!(
///     ack.sql(),
///     r#"DELETE FROM "jobs" WHERE "job_id" = $1 AND "locked_until" = $2"#,
/// );
/// assert_eq!(ack.params(), [Param::Id, Param::Held]);
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Postgres;

impl BuiltIn for Postgres {
    /// `NAMEDATALEN` less its terminator. Postgres truncates a longer identifier without an
    /// error, so the statement would address another object.
    const NAME_LIMIT: NameLimit = NameLimit::Bytes(63);

    const DEFAULT_ROW: &'static str = " DEFAULT VALUES";

    fn database_now(&self) -> &'static str {
        DATABASE_NOW
    }

    fn database_later(&self, sql: &mut SqlWriter<'_, Self>) {
        sql.push(DATABASE_NOW)
            .push(" + ")
            .param(Param::Delay)
            .push(" * interval '1 microsecond'");
    }
}

impl Dialect for Postgres {
    fn name(&self) -> &'static str {
        "postgres"
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

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        out.push('$');
        out.push_str(&index.to_string());
    }

    fn claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> {
        let form = self.claim_form(spec)?;
        let mut sql = SqlWriter::new(self);
        match form {
            Built::RowLock => sql.claim(spec, shape, LOCK),
            Built::Lease(expiry) => sql.lease_claim(spec, shape, expiry.name(), LOCK),
        };
        Ok(sql.finish())
    }

    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.spec_fits(spec)?;
        let id = spec.id().name();
        let mut sql = SqlWriter::new(self);
        sql.push("SELECT ")
            .columns(spec)
            .push(" FROM ")
            .table(spec)
            .push(" WHERE ")
            .ident(id)
            .push(" = ANY(")
            .param(Param::Ids)
            .push(")");
        Ok(sql.finish())
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
        self.movable(spec, target)?;
        let mut sql = SqlWriter::new(self);
        sql.push("WITH moved AS (DELETE FROM ")
            .table(spec)
            .settled_row(spec)
            .push(" RETURNING ")
            .columns(spec)
            .push(") INSERT INTO ")
            .table_name(target);
        if !spec.selects_all() {
            sql.push(" (").columns(spec).push(")");
        }
        sql.push(" SELECT ").moved_columns(spec).push(" FROM moved");
        Ok(vec![sql.finish()])
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
}
