//! The built-in Postgres dialect.

use std::num::NonZeroUsize;

use crate::dialect::Dialect;
use crate::form::Form;
use crate::role::Role;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Param, Statement, StatementError};
use crate::table_name::TableName;
use crate::writer::SqlWriter;

/// Postgres: double-quoted names, `$1` placeholders, rows claimed with `FOR UPDATE SKIP LOCKED`.
///
/// It builds the statements of the row lock form. Every name is quoted, so a name keeps its case
/// and may hold any character.
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Postgres;

impl Postgres {
    /// Refuses a form other than the row lock, the one this dialect builds.
    fn row_lock(self, spec: &TableSpec<'_>) -> Result<(), StatementError> {
        match spec.form() {
            Form::RowLock => Ok(()),
            other => Err(StatementError::UnsupportedForm {
                dialect: self.name(),
                form: other.name(),
            }),
        }
    }

    /// Acknowledgement and drop: the row is deleted, or marked finished.
    fn finish_row(self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.row_lock(spec)?;
        let id = spec.id().name();
        let mut sql = SqlWriter::new(&self);
        match spec.column(Role::ProcessedAt) {
            Some(processed_at) => sql
                .push("UPDATE ")
                .table(spec)
                .push(" SET ")
                .ident(processed_at.name())
                .push(" = ")
                .param(Param::Now),
            None => sql.push("DELETE FROM ").table(spec),
        };
        sql.push(" WHERE ").ident(id).push(" = ").param(Param::Id);
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
        self.row_lock(spec)?;
        if spec.is_fifo() {
            return Err(StatementError::UnsupportedFifo {
                dialect: self.name(),
            });
        }
        let id = spec.id().name();
        let mut sql = SqlWriter::new(self);
        sql.push("SELECT ");
        match shape {
            ClaimShape::Rows => sql.columns(spec),
            ClaimShape::Ids => sql.ident(id),
        };
        sql.push(" FROM ")
            .table(spec)
            .claimable(spec)
            .claim_order(spec, id)
            .push(" LIMIT ")
            .param(Param::Limit)
            .push(" FOR UPDATE SKIP LOCKED");
        Ok(sql.finish())
    }

    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
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
        self.row_lock(spec)?;
        let id = spec.id().name();
        let Some(attempt) = spec.column(Role::Attempt) else {
            return Ok(None);
        };
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .increment(attempt.name())
            .push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id);
        Ok(Some(sql.finish()))
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.row_lock(spec)?;
        let id = spec.id().name();
        let retry_after = required(spec, Role::RetryAfter, "retry_after")?;
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(retry_after)
            .push(" = ")
            .param(Param::RetryAfter);
        if let Some(attempt) = spec.column(Role::Attempt) {
            sql.push(", ").increment(attempt.name());
        }
        sql.push(" WHERE ").ident(id).push(" = ").param(Param::Id);
        Ok(sql.finish())
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.finish_row(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.row_lock(spec)?;
        let id = spec.id().name();
        let group = required(spec, Role::Group, "dead_letter_group")?;
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(group)
            .push(" = ")
            .param(Param::Destination)
            .push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id);
        Ok(sql.finish())
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        self.row_lock(spec)?;
        let id = spec.id().name();
        let mut sql = SqlWriter::new(self);
        sql.push("WITH moved AS (DELETE FROM ")
            .table(spec)
            .push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id)
            .push(" RETURNING ")
            .columns(spec)
            .push(") INSERT INTO ")
            .table_name(target);
        if !spec.selects_all() {
            sql.push(" (").columns(spec).push(")");
        }
        sql.push(" SELECT ").columns(spec).push(" FROM moved");
        Ok(vec![sql.finish()])
    }
}
