//! The description of a queue table: its columns, the role each one plays, and how rows are
//! claimed.

mod builder;
mod reading;

use crate::column::Column;
use crate::form::Form;
use crate::opening::Opening;

// The `const` inserts walk the columns with the same walker as `TableSpec::columns`.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) use reading::Columns;

/// Whether the rows of a table split into groups, and whether each group keeps its order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Grouping<'a> {
    None,
    Groups(Column<'a>),
    Fifo(Column<'a>),
}

/// The description of a queue table: its name, the column that identifies a row, one slot per
/// role, the message's data columns, the form its rows are claimed in, and what its transactions
/// open at.
///
/// `#[derive(Inbox)]` builds one as a constant, and a dialect reads it to build statements. The id
/// column is part of the constructor and every role has one slot, which its setter fills once (a
/// second call panics), so a table without an id or with a role played twice cannot be described.
/// Column names are strings, so the types do not catch a name used twice or a lock key reading a
/// column the table lacks: `#[derive(Inbox)]` refuses both at compile time, and a description
/// written by hand meets the database's own checks when the statements are prepared.
///
/// # Examples
///
/// A dialect of the service's own reads the description to build a statement:
///
/// ```
/// # use std::num::NonZeroUsize;
/// use ruststream_sqlx_dialect::{
///     Dialect, Form, Param, Role, Statement, StatementError, TableSpec,
/// };
/// # use ruststream_sqlx_dialect::TableName;
///
/// /// SQL Server, a database without a built-in dialect, keeping the finished rows of a table with
/// /// groups in its `done` group.
/// #[derive(Debug)]
/// pub struct Mssql;
///
/// impl Dialect for Mssql {
///     fn name(&self) -> &'static str {
///         "mssql"
///     }
///
///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         // The service's tables take their rows by row lock.
///         if spec.form() != Form::RowLock {
///             return Err(StatementError::UnsupportedForm {
///                 dialect: self.name(),
///                 form: spec.form().name(),
///             });
///         }
///         // A table that names no schema lives in SQL Server's default one, `dbo`.
///         let mut table = String::new();
///         self.quote_into(spec.schema().unwrap_or("dbo"), &mut table);
///         table.push('.');
///         self.quote_into(spec.table(), &mut table);
///         let mut id = String::new();
///         self.quote_into(spec.id().name(), &mut id);
///         let Some(group) = spec.column(Role::Group) else {
///             return Ok(Statement::new(
///                 format!("DELETE FROM {table} WHERE {id} = @p1"),
///                 [Param::Id],
///             ));
///         };
///         let mut done = String::new();
///         self.quote_into(group.name(), &mut done);
///         Ok(Statement::new(
///             format!("UPDATE {table} SET {done} = 'done' WHERE {id} = @p1"),
///             [Param::Id],
///         ))
///     }
///
///     fn quote_into(&self, ident: &str, out: &mut String) {
///         out.push('[');
///         out.push_str(&ident.replace(']', "]]"));
///         out.push(']');
///     }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableSpec<'a> {
    schema: Option<&'a str>,
    table: &'a str,
    id: Column<'a>,
    grouping: Grouping<'a>,
    partition_key: Option<Column<'a>>,
    priority: Option<Column<'a>>,
    retry_after: Option<Column<'a>>,
    attempt: Option<Column<'a>>,
    processed_at: Option<Column<'a>>,
    headers: Option<Column<'a>>,
    payload: Option<Column<'a>>,
    data: &'a [Column<'a>],
    fetched: &'a [Column<'a>],
    form: Form<'a>,
    opening: Opening,
    select_all: bool,
    database_clock: bool,
}
