//! The statements of an outbox table, which every built-in dialect writes the same way: a record
//! is taken and marked by its id, and recovered by its name.

use super::{BuiltIn, SqlWriter};
use crate::role::Role;
use crate::spec::TableSpec;
use crate::statement::{Param, Statement, StatementError};

impl<D> SqlWriter<'_, D>
where
    D: BuiltIn + ?Sized,
{
    /// `AND "processed_at" IS NULL` where the table marks its finished records; nothing where it
    /// deletes them.
    fn unprocessed(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if let Some(processed_at) = spec.column(Role::ProcessedAt) {
            self.push(" AND ")
                .ident(processed_at.name())
                .push(" IS NULL");
        }
        self
    }
}

/// The statements of an outbox table every built-in dialect writes the same way. The table's form
/// does not apply: a record is taken by its id, not claimed.
pub(crate) trait OutboxWriter: BuiltIn {
    /// The unprocessed record [`Param::Id`] names, with every column.
    fn outbox_fetch_statement(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.spec_fits(spec)?;
        let mut sql = SqlWriter::new(self);
        sql.push("SELECT ")
            .columns(spec)
            .push(" FROM ")
            .table(spec)
            .push(" WHERE ")
            .ident(spec.id().name())
            .push(" = ")
            .param(Param::Id)
            .unprocessed(spec);
        Ok(sql.finish())
    }

    /// The mark of a processed record: `processed_at` set to now, or the record deleted.
    fn outbox_mark_statement(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.spec_fits(spec)?;
        let mut sql = SqlWriter::new(self);
        match spec.column(Role::ProcessedAt) {
            Some(processed_at) => sql
                .push("UPDATE ")
                .table(spec)
                .push(" SET ")
                .ident(processed_at.name())
                .push(" = ")
                .now(spec),
            None => sql.push("DELETE FROM ").table(spec),
        };
        sql.push(" WHERE ")
            .ident(spec.id().name())
            .push(" = ")
            .param(Param::Id);
        Ok(sql.finish())
    }

    /// The unprocessed records of the name [`Param::Group`] binds, with every column.
    fn outbox_recover_statement(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.spec_fits(spec)?;
        let name = spec
            .column(Role::Group)
            .ok_or(StatementError::MissingRole {
                statement: "outbox_recover",
                role: Role::Group,
            })?;
        let mut sql = SqlWriter::new(self);
        sql.push("SELECT ")
            .columns(spec)
            .push(" FROM ")
            .table(spec)
            .push(" WHERE ")
            .ident(name.name())
            .push(" = ")
            .param(Param::Group)
            .unprocessed(spec);
        Ok(sql.finish())
    }
}

impl<D: BuiltIn> OutboxWriter for D {}
