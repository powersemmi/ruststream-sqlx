//! A table named the way a service writes it: `table`, or `schema.table`.

use thiserror::Error;

/// A table outside the queue's own description, such as the table dead-lettered rows move to.
///
/// It is read from `table` or `schema.table`, and checked when it is read: no segment is empty,
/// and there is at most one schema.
///
/// # Examples
///
/// ```
/// # use std::num::NonZeroUsize;
/// use ruststream_sqlx_dialect::{
///     Dialect, Param, Statement, StatementError, TableName, TableSpec,
/// };
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
///     // A dead email of the service's one queue table moves with the time it died.
///     fn dead_letter_table(
///         &self,
///         _spec: &TableSpec<'_>,
///         target: TableName<'_>,
///     ) -> Result<Vec<Statement>, StatementError> {
///         let mut into = String::new();
///         self.quote_into(target.schema().unwrap_or("dbo"), &mut into);
///         into.push('.');
///         self.quote_into(target.table(), &mut into);
///         Ok(vec![
///             Statement::new(
///                 format!(
///                     "INSERT INTO {into} ([job_id], [payload], [died_at]) \
///                      SELECT [job_id], [payload], @p1 FROM [email_jobs] WHERE [job_id] = @p2"
///                 ),
///                 [Param::Now, Param::Id],
///             ),
///             Statement::new("DELETE FROM [email_jobs] WHERE [job_id] = @p1", [Param::Id]),
///         ])
///     }
///
///     fn quote_into(&self, ident: &str, out: &mut String) {
///         out.push('[');
///         out.push_str(&ident.replace(']', "]]"));
///         out.push(']');
///     }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableName<'a> {
    schema: Option<&'a str>,
    table: &'a str,
}

impl<'a> TableName<'a> {
    /// Reads `table` or `schema.table`.
    ///
    /// # Errors
    ///
    /// [`ParseTableNameError::EmptySegment`] when the name or one of its segments is empty;
    /// [`ParseTableNameError::TooManySegments`] when the name has more than one dot.
    pub fn parse(name: &'a str) -> Result<Self, ParseTableNameError> {
        let parsed = match name.split_once('.') {
            None => Self {
                schema: None,
                table: name,
            },
            Some((_, table)) if table.contains('.') => {
                return Err(ParseTableNameError::TooManySegments {
                    name: name.to_owned(),
                });
            }
            Some((schema, table)) => Self {
                schema: Some(schema),
                table,
            },
        };
        if parsed.table.is_empty() || parsed.schema.is_some_and(str::is_empty) {
            return Err(ParseTableNameError::EmptySegment {
                name: name.to_owned(),
            });
        }
        Ok(parsed)
    }

    /// The schema, or `None` for the connection's default.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Param, Statement, StatementError, TableName, TableSpec,
    /// };
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
    ///     fn dead_letter_table(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         target: TableName<'_>,
    ///     ) -> Result<Vec<Statement>, StatementError> {
    ///         // A struct that flattens another hides columns, so its row moves by position.
    ///         let mut columns = String::new();
    ///         if spec.selects_all() {
    ///             columns.push('*');
    ///         } else {
    ///             for column in spec.columns() {
    ///                 if !columns.is_empty() {
    ///                     columns.push_str(", ");
    ///                 }
    ///                 self.quote_into(column.name(), &mut columns);
    ///             }
    ///         }
    ///         // A target that names no schema lives in SQL Server's default one, `dbo`.
    ///         let mut into = String::new();
    ///         self.quote_into(target.schema().unwrap_or("dbo"), &mut into);
    ///         into.push('.');
    ///         self.quote_into(target.table(), &mut into);
    ///         let mut from = String::new();
    ///         self.quote_into(spec.table(), &mut from);
    ///         let mut id = String::new();
    ///         self.quote_into(spec.id().name(), &mut id);
    ///         let into_columns = if spec.selects_all() {
    ///             String::new()
    ///         } else {
    ///             format!(" ({columns})")
    ///         };
    ///         Ok(vec![
    ///             Statement::new(
    ///                 format!(
    ///                     "INSERT INTO {into}{into_columns} SELECT {columns} FROM {from} \
    ///                      WHERE {id} = @p1"
    ///                 ),
    ///                 [Param::Id],
    ///             ),
    ///             Statement::new(format!("DELETE FROM {from} WHERE {id} = @p1"), [Param::Id]),
    ///         ])
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    #[must_use]
    pub const fn schema(&self) -> Option<&'a str> {
        self.schema
    }

    /// The table's name, without its schema.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Param, Statement, StatementError, TableName, TableSpec,
    /// };
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
    ///     fn dead_letter_table(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         target: TableName<'_>,
    ///     ) -> Result<Vec<Statement>, StatementError> {
    ///         // A struct that flattens another hides columns, so its row moves by position.
    ///         let mut columns = String::new();
    ///         if spec.selects_all() {
    ///             columns.push('*');
    ///         } else {
    ///             for column in spec.columns() {
    ///                 if !columns.is_empty() {
    ///                     columns.push_str(", ");
    ///                 }
    ///                 self.quote_into(column.name(), &mut columns);
    ///             }
    ///         }
    ///         // A target that names no schema lives in SQL Server's default one, `dbo`.
    ///         let mut into = String::new();
    ///         self.quote_into(target.schema().unwrap_or("dbo"), &mut into);
    ///         into.push('.');
    ///         self.quote_into(target.table(), &mut into);
    ///         let mut from = String::new();
    ///         self.quote_into(spec.table(), &mut from);
    ///         let mut id = String::new();
    ///         self.quote_into(spec.id().name(), &mut id);
    ///         let into_columns = if spec.selects_all() {
    ///             String::new()
    ///         } else {
    ///             format!(" ({columns})")
    ///         };
    ///         Ok(vec![
    ///             Statement::new(
    ///                 format!(
    ///                     "INSERT INTO {into}{into_columns} SELECT {columns} FROM {from} \
    ///                      WHERE {id} = @p1"
    ///                 ),
    ///                 [Param::Id],
    ///             ),
    ///             Statement::new(format!("DELETE FROM {from} WHERE {id} = @p1"), [Param::Id]),
    ///         ])
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    #[must_use]
    pub const fn table(&self) -> &'a str {
        self.table
    }
}

/// Why a string does not name a table.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ParseTableNameError {
    /// The name, or one of its segments, is empty.
    #[error("table name `{name}` has an empty segment: write `table` or `schema.table`")]
    EmptySegment {
        /// The name as given.
        name: String,
    },
    /// The name has more than one dot.
    #[error("table name `{name}` has more than two segments: write `table` or `schema.table`")]
    TooManySegments {
        /// The name as given.
        name: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{ParseTableNameError, TableName};

    #[test]
    fn a_name_reads_with_or_without_a_schema() -> Result<(), ParseTableNameError> {
        let bare = TableName::parse("jobs_dead")?;
        assert_eq!((bare.schema(), bare.table()), (None, "jobs_dead"));
        let qualified = TableName::parse("Archive.Jobs Dead")?;
        assert_eq!(
            (qualified.schema(), qualified.table()),
            (Some("Archive"), "Jobs Dead")
        );
        Ok(())
    }

    #[test]
    fn an_empty_segment_is_refused() {
        for name in ["", ".jobs_dead", "archive.", "."] {
            assert_eq!(
                TableName::parse(name),
                Err(ParseTableNameError::EmptySegment {
                    name: name.to_owned()
                })
            );
        }
    }

    #[test]
    fn more_than_one_schema_is_refused() {
        for name in ["db.archive.jobs_dead", "archive..jobs_dead"] {
            assert_eq!(
                TableName::parse(name),
                Err(ParseTableNameError::TooManySegments {
                    name: name.to_owned()
                })
            );
        }
    }
}
