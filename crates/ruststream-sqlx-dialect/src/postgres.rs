//! The built-in Postgres dialect.

use std::num::NonZeroUsize;

use crate::dialect::Dialect;
use crate::spec::{Form, TableSpec};
use crate::statement::{ClaimShape, Param, Statement, StatementError};
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
}
