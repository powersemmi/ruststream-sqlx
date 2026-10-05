//! The built-in Postgres dialect.

use std::num::NonZeroUsize;

use crate::column::Column;
use crate::dialect::Dialect;
use crate::form::Form;
use crate::role::Role;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Param, Statement, StatementError};
use crate::table_name::TableName;
use crate::writer::SqlWriter;

/// The longest name Postgres keeps whole, in bytes: `NAMEDATALEN` less its terminator.
const NAME_LIMIT: usize = 63;

/// How Postgres reads the current time: the start of the statement, so a settlement made long
/// after the claim began records its own moment.
const DATABASE_NOW: &str = "statement_timestamp()";

/// How a claim locks the rows it takes: until its transaction ends, skipping the rows another
/// claim holds.
const LOCK: &str = " FOR UPDATE SKIP LOCKED";

/// A form this dialect builds statements for.
#[derive(Debug, Clone, Copy)]
enum Built<'a> {
    /// The claim's transaction holds the row.
    RowLock,
    /// The expiry in this column holds the row.
    Lease(Column<'a>),
}

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

impl Postgres {
    /// Refuses a name Postgres would cut short: it truncates an identifier over 63 bytes without
    /// an error, so the statement would address another object.
    fn names_fit<'a>(self, names: impl IntoIterator<Item = &'a str>) -> Result<(), StatementError> {
        names
            .into_iter()
            .find(|name| name.len() > NAME_LIMIT)
            .map_or(Ok(()), |name| {
                Err(StatementError::IdentifierTooLong {
                    dialect: self.name(),
                    identifier: name.to_owned(),
                    limit: NAME_LIMIT,
                })
            })
    }

    /// Every name a statement of `spec` writes.
    fn spec_fits(self, spec: &TableSpec<'_>) -> Result<(), StatementError> {
        self.names_fit(
            spec.schema()
                .into_iter()
                .chain([spec.table()])
                .chain(spec.columns().map(|column| column.name())),
        )
    }

    /// The table's form, when this dialect builds it: the row lock, or a lease on the crate's
    /// clock.
    fn form<'a>(self, spec: &TableSpec<'a>) -> Result<Built<'a>, StatementError> {
        self.spec_fits(spec)?;
        match spec.form() {
            Form::RowLock => Ok(Built::RowLock),
            // Settlement matches the expiry the claim wrote, and the claim knows it only when the
            // crate's clock computes it.
            Form::Lease(_) if spec.uses_database_clock() => {
                Err(StatementError::LeaseOnDatabaseClock {
                    dialect: self.name(),
                })
            }
            Form::Lease(expiry) => Ok(Built::Lease(expiry)),
            other => Err(StatementError::UnsupportedForm {
                dialect: self.name(),
                form: other.name(),
            }),
        }
    }

    /// The lease column of a lease table; every other form is refused.
    fn lease<'a>(self, spec: &TableSpec<'a>) -> Result<Column<'a>, StatementError> {
        match self.form(spec)? {
            Built::Lease(expiry) => Ok(expiry),
            Built::RowLock => Err(StatementError::UnsupportedForm {
                dialect: self.name(),
                form: spec.form().name(),
            }),
        }
    }

    /// Acknowledgement and drop: the row is deleted, or marked finished.
    fn finish_row(self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.form(spec)?;
        let id = spec.id().name();
        let mut sql = SqlWriter::new(&self);
        match spec.column(Role::ProcessedAt) {
            Some(processed_at) => sql
                .push("UPDATE ")
                .table(spec)
                .push(" SET ")
                .ident(processed_at.name())
                .push(" = ")
                .now(spec, DATABASE_NOW)
                .release(spec),
            None => sql.push("DELETE FROM ").table(spec),
        };
        sql.push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id)
            .held(spec);
        Ok(sql.finish())
    }
}

/// The name of the column that plays `role`, which `statement` cannot do without.
fn required<'a>(
    spec: &TableSpec<'a>,
    role: Role,
    statement: &'static str,
) -> Result<&'a str, StatementError> {
    spec.column(role)
        .map(|column| column.name())
        .ok_or(StatementError::MissingRole { statement, role })
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
        let form = self.form(spec)?;
        if spec.is_fifo() {
            return Err(StatementError::UnsupportedFifo {
                dialect: self.name(),
            });
        }
        let mut sql = SqlWriter::new(self);
        match form {
            Built::RowLock => sql.claim(spec, shape, DATABASE_NOW, LOCK),
            Built::Lease(expiry) => sql.lease_claim(spec, shape, expiry.name(), DATABASE_NOW, LOCK),
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
        self.finish_row(spec)
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        let form = self.form(spec)?;
        let id = spec.id().name();
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ").table(spec).push(" SET ");
        match form {
            // The claim counted the attempt; the release only frees the row.
            Built::Lease(expiry) => sql.ident(expiry.name()).push(" = NULL"),
            Built::RowLock => match spec.column(Role::Attempt) {
                Some(attempt) => sql.increment(attempt.name()),
                // The rollback releases the row, and there is no attempt to count.
                None => return Ok(None),
            },
        };
        sql.push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id)
            .held(spec);
        Ok(Some(sql.finish()))
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let form = self.form(spec)?;
        let id = spec.id().name();
        let retry_after = required(spec, Role::RetryAfter, "retry_after")?;
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(retry_after)
            .push(" = ")
            .later(spec, DATABASE_NOW);
        match form {
            Built::RowLock => {
                if let Some(attempt) = spec.column(Role::Attempt) {
                    sql.push(", ").increment(attempt.name());
                }
            }
            // The claim counted the attempt.
            Built::Lease(_) => {
                sql.release(spec);
            }
        }
        sql.push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id)
            .held(spec);
        Ok(sql.finish())
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.finish_row(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.form(spec)?;
        let id = spec.id().name();
        let group = required(spec, Role::Group, "dead_letter_group")?;
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(group)
            .push(" = ")
            .param(Param::Destination)
            .release(spec)
            .push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id)
            .held(spec);
        Ok(sql.finish())
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        if matches!(self.form(spec)?, Built::Lease(_)) && spec.selects_all() {
            // The moved row arrives without a lease, and `*` cannot put `NULL` in the lease
            // column's place.
            return Err(StatementError::Flattened {
                statement: "dead_letter_table",
            });
        }
        self.names_fit(target.schema().into_iter().chain([target.table()]))?;
        let id = spec.id().name();
        let mut sql = SqlWriter::new(self);
        sql.push("WITH moved AS (DELETE FROM ")
            .table(spec)
            .push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id)
            .held(spec)
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
        let expiry = self.lease(spec)?;
        let id = spec.id().name();
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(expiry.name())
            .push(" = ")
            .param(Param::Lease)
            .push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id)
            .held(spec);
        Ok(sql.finish())
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let expiry = self.lease(spec)?;
        let id = spec.id().name();
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(expiry.name())
            .push(" = ")
            .param(Param::Lease);
        if let Some(attempt) = spec.column(Role::Attempt) {
            sql.push(", ").increment(attempt.name());
        }
        sql.push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id)
            .push(" AND ")
            .lease_free(expiry.name());
        Ok(sql.finish())
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.spec_fits(spec)?;
        if spec.selects_all() {
            return Err(StatementError::Flattened {
                statement: "insert",
            });
        }
        let mut sql = SqlWriter::new(self);
        sql.push("INSERT INTO ").table(spec);
        let written: Vec<(usize, &str)> = spec
            .columns()
            .enumerate()
            .filter(|(_, column)| !column.is_generated())
            .map(|(position, column)| (position, column.name()))
            .collect();
        if written.is_empty() {
            sql.push(" DEFAULT VALUES");
            return Ok(sql.finish());
        }
        sql.push(" (");
        for (index, (_, name)) in written.iter().enumerate() {
            if index > 0 {
                sql.push(", ");
            }
            sql.ident(name);
        }
        sql.push(") VALUES (");
        for (index, (position, _)) in written.iter().enumerate() {
            if index > 0 {
                sql.push(", ");
            }
            sql.param(Param::Column(*position));
        }
        sql.push(")");
        Ok(sql.finish())
    }
}
