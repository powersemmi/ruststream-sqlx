//! The built-in Postgres dialect.

use std::num::NonZeroUsize;

use crate::advisory::Advisory;
use crate::dialect::Dialect;
use crate::form::KeyPart;
use crate::lease::Lease;
use crate::opening::{Isolation, Opening, Opens, level};
use crate::row_lock::RowLock;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
use crate::table_name::TableName;
use crate::writer::{BuiltIn, Probe, SqlWriter};

/// How Postgres reads the current time: the start of the statement, so a settlement made long
/// after the claim began records its own moment.
const DATABASE_NOW: &str = "statement_timestamp()";

/// How a claim locks the rows it takes: until its transaction ends, skipping the rows another
/// claim holds.
const LOCK: &str = " FOR UPDATE SKIP LOCKED";

/// Tries the session's lock on the 64-bit hash of a key, without waiting.
const TRY_LOCK: &str = "SELECT pg_try_advisory_lock(hashtextextended($1, 0))::int::bigint";

/// Releases the session's lock on the 64-bit hash of a key.
const UNLOCK: &str = "SELECT pg_advisory_unlock(hashtextextended($1, 0))::int::bigint";

/// Postgres: double-quoted names, `$1` placeholders, rows claimed with `FOR UPDATE SKIP LOCKED`.
///
/// It builds the statements of the row lock form ([`RowLock`]), of the lease form ([`Lease`]), of
/// the advisory lock form ([`Advisory`]), and the insert. A lease claim is one statement: it locks
/// the claimable rows, writes their lease, and returns them as they were. Every name is quoted, so
/// a name keeps its case and may hold any character; a name over 63 bytes, which Postgres would
/// cut short without a word, is refused. A table on the database's clock reads
/// `statement_timestamp()`.
///
/// In the advisory lock form a row's key is the text `concat` renders from the key's parts, a
/// column without a value read as empty text. A session lock on the key's 64-bit hash
/// (`hashtextextended`, Postgres 11 or later) holds the row, so two keys with one hash wait for
/// each other: a delay, never a double delivery. The claim leaves out the keys another session
/// holds: it probes each candidate's key with a shared lock that ends with the claim's own
/// transaction, in claim order and only until it has its rows. The take counts the attempt and
/// returns the row in one statement.
///
/// In a table with FIFO groups a claim's transaction takes its group
/// ([`fifo_guard`](Dialect::fifo_guard)) with a lock the transaction holds on the 64-bit hash of
/// the table's name and the group. Two groups with one hash wait for each other: a delay, never
/// two rows of one group in work.
///
/// A table's transactions open with `BEGIN`, or at the isolation level it names
/// ([`begin`](Dialect::begin)): READ COMMITTED, REPEATABLE READ or SERIALIZABLE. Postgres runs
/// READ UNCOMMITTED as READ COMMITTED, so a table that names it is refused: a level the database
/// does not keep is a level it lacks.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{ClaimShape, Column, Form, Postgres, RowLock, TableSpec};
///
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
///     .within("app")
///     .priority(Column::new("priority"))
///     .payload(Column::new("payload"));
///
/// let claim = Postgres.lock_claim(&JOBS, ClaimShape::Rows)?;
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
///
/// A table that names READ UNCOMMITTED is refused, and the refusal names the level:
///
/// ```
/// use ruststream_sqlx_dialect::{Dialect, Isolation, Opening, Postgres};
///
/// let refused = Postgres
///     .begin(Opening::Isolation(Isolation::ReadUncommitted))
///     .map_err(|refused| format!("subscription `emails`: {refused}"));
/// assert_eq!(
///     refused,
///     Err("subscription `emails`: the postgres dialect opens no transaction at isolation \
///          `read_uncommitted`"
///         .to_owned()),
/// );
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Postgres;

impl BuiltIn for Postgres {
    /// `NAMEDATALEN` less its terminator. Postgres truncates a longer identifier without an
    /// error, so the statement would address another object.
    const NAME_LIMIT: Option<NameLimit> = Some(NameLimit::Bytes(63));

    const ROW_LOCKS: bool = true;

    const DEFAULT_ROW: &'static str = " DEFAULT VALUES";

    /// `standard_conforming_strings`, on by default since Postgres 9.1, keeps a backslash as it is.
    const BACKSLASH_ESCAPES: bool = false;

    const UPDATE_RETURNS: bool = true;

    /// A shared lock conflicts only with the session lock a delivery holds, so concurrent claims
    /// probe one key without excluding each other.
    const PROBE: Probe = Probe::Lock(
        "pg_try_advisory_xact_lock_shared(hashtextextended(",
        ", 0))",
    );

    fn database_now(&self) -> &'static str {
        DATABASE_NOW
    }

    fn database_later(&self, sql: &mut SqlWriter<'_, Self>) {
        sql.push(DATABASE_NOW)
            .push(" + ")
            .param(Param::Delay)
            .push(" * interval '1 microsecond'");
    }

    fn render_lock_key(&self, sql: &mut SqlWriter<'_, Self>, key: &[KeyPart<'_>]) {
        sql.push("concat(")
            .key_parts(key, ", ", |sql, column| {
                sql.ident(column);
            })
            .push(")");
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

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.insert_statement(spec)
    }

    fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
        match opening {
            Opening::Default => Ok(None),
            Opening::Isolation(Isolation::ReadCommitted) => {
                Ok(Some("BEGIN ISOLATION LEVEL READ COMMITTED"))
            }
            Opening::Isolation(Isolation::RepeatableRead) => {
                Ok(Some("BEGIN ISOLATION LEVEL REPEATABLE READ"))
            }
            Opening::Isolation(Isolation::Serializable) => {
                Ok(Some("BEGIN ISOLATION LEVEL SERIALIZABLE"))
            }
            // Postgres runs READ UNCOMMITTED as READ COMMITTED: a declared level it does not keep
            // is a level it lacks.
            Opening::Isolation(Isolation::ReadUncommitted) | Opening::Mode(_) => {
                Err(opening.refused(self.name()))
            }
        }
    }

    fn fifo_guard(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        if !self.guards_group(spec)? {
            return Ok(None);
        }
        // The group's name: the table as the statements name it, unquoted, then `:` and the
        // group, so the groups of two tables are two names.
        let mut table = spec
            .schema()
            .map(|schema| format!("{schema}."))
            .unwrap_or_default();
        table.push_str(spec.table());
        table.push(':');
        let mut sql = SqlWriter::new(self);
        sql.push("SELECT pg_try_advisory_xact_lock(hashtextextended(")
            .literal(&table)
            .push(" || ")
            .param(Param::Group)
            .push(", 0))::int::bigint");
        Ok(Some(sql.finish()))
    }
}

impl Opens<level::ReadCommitted> for Postgres {}

impl Opens<level::RepeatableRead> for Postgres {}

impl Opens<level::Serializable> for Postgres {}

impl RowLock for Postgres {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        self.locked(spec, "lock_claim")?;
        let mut sql = SqlWriter::new(self);
        sql.claim(spec, shape, LOCK);
        Ok(sql.finish())
    }
}

impl Advisory for Postgres {
    fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.advisory_claim_statement(spec)
    }

    fn lock(&self) -> Option<Statement> {
        Some(Statement::new(TRY_LOCK, [Param::Key]))
    }

    fn unlock(&self) -> Option<Statement> {
        Some(Statement::new(UNLOCK, [Param::Key]))
    }

    fn take(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError> {
        self.take_statements(spec, shape)
    }
}

impl Lease for Postgres {
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        let expiry = self.leased(spec, "lease_claim")?;
        let mut sql = SqlWriter::new(self);
        sql.lease_claim(spec, shape, expiry.name(), LOCK);
        Ok(sql.finish())
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.extend_statement(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.stamp_statement(spec)
    }
}
