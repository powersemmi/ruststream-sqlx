//! How a subscription claims the rows of a table: the forms, and the pieces of an advisory
//! lock key.

use crate::column::Column;

/// One piece of an advisory lock key: literal text, or the value of a column of the row.
///
/// `#[inbox(advisory_lock = "jobs-{job_id}")]` becomes
/// `[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")]`. The database renders each row's key
/// from the parts, in the claim that selects the row: a literal as it is, a column's value as
/// text, a column without a value as empty text.
///
/// # Examples
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     Advisory, ClaimShape, Dialect, Form, KeyPart, Param, Statement, StatementError, TableSpec,
/// };
///
/// /// SQL Server, a database without a built-in dialect; the service's advisory tables keep no
/// /// groups, no claim order, no delays and no finished rows.
/// #[derive(Debug)]
/// pub struct Mssql;
///
/// impl Advisory for Mssql {
///     // The candidates of a table read in id order, with their keys.
///     fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         let Form::Advisory(key) = spec.form() else {
///             return Err(StatementError::FormMismatch {
///                 statement: "advisory_claim",
///                 form: spec.form().name(),
///             });
///         };
///         // `advisory_lock = "jobs-{job_id}"` arrives as `[Literal("jobs-"), Column("job_id")]`,
///         // and the database renders each row's key from it. `CONCAT` reads a column without a
///         // value as empty text, and the leading empty literal gives it the two arguments it
///         // takes at least.
///         let mut rendered = String::from("N''");
///         for part in key {
///             rendered.push_str(", ");
///             match part {
///                 KeyPart::Literal(text) => {
///                     rendered.push_str("N'");
///                     rendered.push_str(&text.replace('\'', "''"));
///                     rendered.push('\'');
///                 }
///                 KeyPart::Column(column) => self.quote_into(column, &mut rendered),
///             }
///         }
///         let mut table = String::new();
///         self.quote_into(spec.table(), &mut table);
///         let mut id = String::new();
///         self.quote_into(spec.id().name(), &mut id);
///         Ok(Statement::new(
///             format!(
///                 "SELECT TOP (@p1) {id}, CONCAT({rendered}) AS [__lock] \
///                  FROM {table} ORDER BY {id}"
///             ),
///             [Param::Limit],
///         ))
///     }
/// #     fn lock(&self) -> Option<Statement> { None }
/// #     fn unlock(&self) -> Option<Statement> { None }
/// #     fn take(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// }
/// # impl Dialect for Mssql {
/// #     fn name(&self) -> &'static str { "mssql" }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyPart<'a> {
    /// Text copied into the key as it is.
    Literal(&'a str),
    /// The value of the named column.
    Column(&'a str),
}

/// How a subscription claims the rows of a table; a table has exactly one form.
///
/// The lease form carries its `locked_until` column and the advisory form its key, so a table
/// cannot declare two forms. The row lock is the default: the form of a table that declares
/// neither.
///
/// # Examples
///
/// A settlement of a dialect of the service's own follows the form:
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{Dialect, Form, Param, Statement, StatementError, TableSpec};
///
/// /// SQL Server, a database without a built-in dialect.
/// #[derive(Debug)]
/// pub struct Mssql;
///
/// impl Dialect for Mssql {
///     fn name(&self) -> &'static str {
///         "mssql"
///     }
///
///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         let mut sql = String::from("DELETE FROM ");
///         self.quote_into(spec.table(), &mut sql);
///         sql.push_str(" WHERE ");
///         self.quote_into(spec.id().name(), &mut sql);
///         sql.push_str(" = @p1");
///         match spec.form() {
///             // In the lease form the row goes only while it holds the delivery's lease.
///             Form::Lease(expiry) => {
///                 sql.push_str(" AND ");
///                 self.quote_into(expiry.name(), &mut sql);
///                 sql.push_str(" = @p2");
///                 Ok(Statement::new(sql, [Param::Id, Param::Held]))
///             }
///             _ => Ok(Statement::new(sql, [Param::Id])),
///         }
///     }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Form<'a> {
    /// Rows stay locked in a transaction held for the whole handler; the default.
    #[default]
    RowLock,
    /// A claim sets this column, the lease's expiry, and commits; the value holds the row.
    Lease(Column<'a>),
    /// A session lock on the key these parts build from the row holds the row.
    Advisory(&'a [KeyPart<'a>]),
}

impl Form<'_> {
    /// The form's name in messages: `row lock`, `lease` or `advisory lock`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{Dialect, Form, Param, Statement, StatementError, TableSpec};
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Dialect for Mssql {
    ///     fn name(&self) -> &'static str {
    ///         "mssql"
    ///     }
    ///
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         // This dialect serves the row lock form alone, and its refusal names the table's
    ///         // form.
    ///         if spec.form() != Form::RowLock {
    ///             return Err(StatementError::UnsupportedForm {
    ///                 dialect: self.name(),
    ///                 form: spec.form().name(),
    ///             });
    ///         }
    ///         let mut sql = String::from("DELETE FROM ");
    ///         self.quote_into(spec.table(), &mut sql);
    ///         sql.push_str(" WHERE ");
    ///         self.quote_into(spec.id().name(), &mut sql);
    ///         sql.push_str(" = @p1");
    ///         Ok(Statement::new(sql, [Param::Id]))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
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
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::RowLock => "row lock",
            Self::Lease(_) => "lease",
            Self::Advisory(_) => "advisory lock",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Column, Form};

    #[test]
    fn form_names_read_in_messages() {
        assert_eq!(Form::RowLock.name(), "row lock");
        assert_eq!(Form::Lease(Column::new("locked_until")).name(), "lease");
        assert_eq!(Form::Advisory(&[]).name(), "advisory lock");
    }

    #[test]
    fn the_row_lock_is_the_default_form() {
        assert_eq!(Form::default(), Form::RowLock);
    }
}
