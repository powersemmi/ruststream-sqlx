//! The trait every dialect implements: names, placeholders, and the statements every form runs.

use std::fmt::Debug;
use std::num::NonZeroUsize;

use crate::opening::Opening;
use crate::spec::TableSpec;
use crate::statement::{Statement, StatementError};
use crate::table_name::TableName;

#[doc = include_str!("dialect/README.md")]
pub trait Dialect: Debug + Send + Sync {
    /// The dialect's name, for messages.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Form, Param, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Dialect for Mssql {
    ///     // The name a refusal of this dialect carries.
    ///     fn name(&self) -> &'static str {
    ///         "mssql"
    ///     }
    ///
    ///     // A table in another form stops with "the mssql dialect has no statements for the lease
    ///     // form".
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.form() != Form::RowLock {
    ///             return Err(StatementError::UnsupportedForm {
    ///                 dialect: self.name(),
    ///                 form: spec.form().name(),
    ///             });
    ///         }
    ///         Ok(Statement::new(
    ///             "DELETE FROM [email_jobs] WHERE [job_id] = @p1",
    ///             [Param::Id],
    ///         ))
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
    fn name(&self) -> &'static str;

    /// Appends `ident` to `out`, quoted as a name of this database.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     // A name in brackets keeps its case and its spaces; a closing bracket doubles.
    ///     fn quote_into(&self, ident: &str, out: &mut String) {
    ///         out.push('[');
    ///         out.push_str(&ident.replace(']', "]]"));
    ///         out.push(']');
    ///     }
    ///
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         let mut sql = String::from("DELETE FROM ");
    ///         self.quote_into(spec.table(), &mut sql);
    ///         sql.push_str(" WHERE ");
    ///         self.quote_into(spec.id().name(), &mut sql);
    ///         sql.push_str(" = @p1");
    ///         Ok(Statement::new(sql, [Param::Id]))
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
    fn quote_into(&self, ident: &str, out: &mut String);

    /// Appends the placeholder of parameter number `index` (counted from 1) to `out`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     // SQL Server numbers its parameters `@p1`, `@p2` and on.
    ///     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
    ///         out.push_str("@p");
    ///         out.push_str(&index.to_string());
    ///     }
    ///
    ///     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         let mut sql = String::from("UPDATE [email_jobs] SET [retry_after] = ");
    ///         self.placeholder_into(NonZeroUsize::MIN, &mut sql);
    ///         sql.push_str(" WHERE [job_id] = ");
    ///         self.placeholder_into(NonZeroUsize::MIN.saturating_add(1), &mut sql);
    ///         Ok(Statement::new(sql, [Param::RetryAfter, Param::Id]))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String);

    /// The statement that reads the rows of claimed ids, bound as one list.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedFetch`] when the dialect cannot read rows by a list of ids, so
    /// a claim of the service's own needs a fetch of its own too. Postgres builds it for every
    /// table; MySQL and SQLite refuse it.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     // SQL Server binds no list of ids: a table whose claim is the service's own lists
    ///     // `fetch`
    ///     // in `custom(..)` beside `claim`.
    ///     fn fetch(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         Err(StatementError::UnsupportedFetch { dialect: self.name() })
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that acknowledges a row: it deletes the row, or sets `processed_at` when the
    /// table has that column.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Form, Param, Role, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// SQL Server, a database without a built-in dialect; the service's tables take their rows
    /// /// by row lock.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Dialect for Mssql {
    ///     fn name(&self) -> &'static str {
    ///         "mssql"
    ///     }
    ///
    ///     // A finished row goes, or stays with the moment it finished where the table keeps one.
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.form() != Form::RowLock {
    ///             return Err(StatementError::UnsupportedForm {
    ///                 dialect: self.name(),
    ///                 form: spec.form().name(),
    ///             });
    ///         }
    ///         let mut table = String::new();
    ///         self.quote_into(spec.table(), &mut table);
    ///         let mut id = String::new();
    ///         self.quote_into(spec.id().name(), &mut id);
    ///         let Some(processed_at) = spec.column(Role::ProcessedAt) else {
    ///             return Ok(Statement::new(
    ///                 format!("DELETE FROM {table} WHERE {id} = @p1"),
    ///                 [Param::Id],
    ///             ));
    ///         };
    ///         let mut finished = String::new();
    ///         self.quote_into(processed_at.name(), &mut finished);
    ///         Ok(Statement::new(
    ///             format!("UPDATE {table} SET {finished} = @p1 WHERE {id} = @p2"),
    ///             [Param::Now, Param::Id],
    ///         ))
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
    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that releases a row for another attempt at once, or `None` when releasing
    /// the row needs no statement.
    ///
    /// In the lease form it clears the lease, and the attempt stays as the claim counted it. In the
    /// advisory lock form it is `None`: the take counted the attempt, and the unlock frees the row.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Role, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// SQL Server, a database without a built-in dialect; the service's tables take their rows
    /// /// by row lock.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Dialect for Mssql {
    ///     fn name(&self) -> &'static str {
    ///         "mssql"
    ///     }
    ///
    ///     // A retry counts the attempt where the table counts them. A table without the column
    ///     // needs no statement: the end of the claim's transaction frees the row.
    ///     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
    ///         let Some(attempt) = spec.column(Role::Attempt) else {
    ///             return Ok(None);
    ///         };
    ///         let mut table = String::new();
    ///         self.quote_into(spec.table(), &mut table);
    ///         let mut count = String::new();
    ///         self.quote_into(attempt.name(), &mut count);
    ///         let mut id = String::new();
    ///         self.quote_into(spec.id().name(), &mut id);
    ///         Ok(Some(Statement::new(
    ///             format!("UPDATE {table} SET {count} = {count} + 1 WHERE {id} = @p1"),
    ///             [Param::Id],
    ///         )))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError>;

    /// The statement that releases a row for another attempt after a delay, bound as
    /// [`Param::RetryAfter`](crate::Param::RetryAfter).
    ///
    /// # Errors
    ///
    /// [`StatementError::MissingRole`] when the table has no `retry_after` column;
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Role, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// SQL Server, a database without a built-in dialect; the service's tables take their rows
    /// /// by row lock.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Dialect for Mssql {
    ///     fn name(&self) -> &'static str {
    ///         "mssql"
    ///     }
    ///
    ///     // The row waits in its table until the delay ends.
    ///     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         let Some(retry_after) = spec.column(Role::RetryAfter) else {
    ///             return Err(StatementError::MissingRole {
    ///                 statement: "retry_after",
    ///                 role: Role::RetryAfter,
    ///             });
    ///         };
    ///         let mut table = String::new();
    ///         self.quote_into(spec.table(), &mut table);
    ///         let mut until = String::new();
    ///         self.quote_into(retry_after.name(), &mut until);
    ///         let mut id = String::new();
    ///         self.quote_into(spec.id().name(), &mut id);
    ///         Ok(Statement::new(
    ///             format!("UPDATE {table} SET {until} = @p1 WHERE {id} = @p2"),
    ///             [Param::RetryAfter, Param::Id],
    ///         ))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that drops a row: it deletes the row, or sets `processed_at` when the table
    /// has that column.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     // A dropped email stays in the service's one queue table, marked with the time it was
    ///     // dropped.
    ///     fn discard(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         Ok(Statement::new(
    ///             "UPDATE [email_jobs] SET [dropped_at] = @p1 WHERE [job_id] = @p2",
    ///             [Param::Now, Param::Id],
    ///         ))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that moves a row whose attempts are spent to another group, bound as
    /// [`Param::Destination`](crate::Param::Destination).
    ///
    /// # Errors
    ///
    /// [`StatementError::MissingRole`] when the table has no `group` column;
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Role, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// SQL Server, a database without a built-in dialect; the service's tables take their rows
    /// /// by row lock.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Dialect for Mssql {
    ///     fn name(&self) -> &'static str {
    ///         "mssql"
    ///     }
    ///
    ///     // A dead letter starts its new group with the attempt count of a new row.
    ///     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         let Some(group) = spec.column(Role::Group) else {
    ///             return Err(StatementError::MissingRole {
    ///                 statement: "dead_letter_group",
    ///                 role: Role::Group,
    ///             });
    ///         };
    ///         let mut sql = String::from("UPDATE ");
    ///         self.quote_into(spec.table(), &mut sql);
    ///         sql.push_str(" SET ");
    ///         self.quote_into(group.name(), &mut sql);
    ///         sql.push_str(" = @p1");
    ///         if let Some(attempt) = spec.column(Role::Attempt) {
    ///             sql.push_str(", ");
    ///             self.quote_into(attempt.name(), &mut sql);
    ///             sql.push_str(" = DEFAULT");
    ///         }
    ///         sql.push_str(" WHERE ");
    ///         self.quote_into(spec.id().name(), &mut sql);
    ///         sql.push_str(" = @p2");
    ///         Ok(Statement::new(sql, [Param::Destination, Param::Id]))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statements that move a row whose attempts are spent to `target`, a table with the same
    /// columns; they run in one transaction.
    ///
    /// A table read with `*` ([`TableSpec::selects_all`]) has columns the description does not
    /// name, so its row moves by position: `target` has the same columns in the same order. In
    /// the lease form the row arrives without a lease, so whatever reads `target` can claim it at
    /// once.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock;
    /// [`StatementError::Flattened`] for a lease table read with `*`, whose lease column the move
    /// cannot name.
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
    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError>;

    /// The statement that inserts a row: every column the database does not fill, in the order of
    /// [`TableSpec::columns`], each bound as [`Param::Column`](crate::Param::Column).
    ///
    /// # Errors
    ///
    /// [`StatementError::Flattened`] when the table is read with `*`
    /// ([`TableSpec::selects_all`]), whose columns the description cannot see.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     // Publishing a job twice keeps one row: the insert skips a row whose id is already
    ///     // there. The service's one queue table lists its id, then its payload, and the lock
    ///     // held to the end of the statement keeps two inserts of one id apart.
    ///     fn insert(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         Ok(Statement::new(
    ///             "INSERT INTO [email_jobs] ([job_id], [payload]) SELECT @p1, @p2 \
    ///              WHERE NOT EXISTS (SELECT 1 FROM [email_jobs] WITH (UPDLOCK, HOLDLOCK) \
    ///              WHERE [job_id] = @p1)",
    ///             [Param::Column(0), Param::Column(1)],
    ///         ))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The query that reads the server's version as one text column, or `None` when the
    /// dialect's statements run on every version of its server.
    ///
    /// The provided method returns `None`: the broker asks the server nothing, and every statement
    /// runs on whatever version the server is.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     // The server reports its major version as a number: 14 is SQL Server 2017.
    ///     fn server_version(&self) -> Option<&'static str> {
    ///         Some("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS nvarchar(128))")
    ///     }
    ///
    ///     fn check_server(
    ///         &self,
    ///         _spec: &TableSpec<'_>,
    ///         version: &str,
    ///     ) -> Result<(), StatementError> {
    ///         match version.trim().parse::<u32>() {
    ///             Ok(major) if major >= 14 => Ok(()),
    ///             _ => Err(StatementError::ServerTooOld {
    ///                 dialect: self.name(),
    ///                 server: version.to_owned(),
    ///                 required: "SQL Server 2017",
    ///             }),
    ///         }
    ///     }
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
    /// }
    /// ```
    fn server_version(&self) -> Option<&'static str> {
        None
    }

    /// Refuses a server older than the statements of `spec` need; `version` is what the
    /// [`server_version`](Self::server_version) query returned.
    ///
    /// The provided method accepts every version, as a dialect whose statements run on every
    /// version of its server does.
    ///
    /// # Errors
    ///
    /// [`StatementError::ServerTooOld`] when the server predates the dialect's statements for the
    /// table's form.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     fn server_version(&self) -> Option<&'static str> {
    ///         Some("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS nvarchar(128))")
    ///     }
    ///
    ///     // The service's statements need SQL Server 2017, so an older server stops the
    ///     // subscription.
    ///     fn check_server(
    ///         &self,
    ///         _spec: &TableSpec<'_>,
    ///         version: &str,
    ///     ) -> Result<(), StatementError> {
    ///         match version.trim().parse::<u32>() {
    ///             Ok(major) if major >= 14 => Ok(()),
    ///             _ => Err(StatementError::ServerTooOld {
    ///                 dialect: self.name(),
    ///                 server: version.to_owned(),
    ///                 required: "SQL Server 2017",
    ///             }),
    ///         }
    ///     }
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
    /// }
    /// ```
    fn check_server(&self, spec: &TableSpec<'_>, version: &str) -> Result<(), StatementError> {
        let _ = (spec, version);
        Ok(())
    }

    /// The statement that opens a transaction at `opening` in place of `BEGIN`, or `None` when
    /// `BEGIN` opens it.
    ///
    /// A broker opens the row lock claim's transaction with it, at the table's opening
    /// ([`TableSpec::opening`]). The statement leaves the connection inside a transaction, as
    /// `BEGIN` does. A dialect accepts the openings it implements [`Opens`](crate::Opens) for. The
    /// provided method opens [`Opening::Default`] with `BEGIN` and refuses every level and mode: on
    /// a dialect that keeps it, a table that declares `isolation` or `mode` stops when its
    /// subscription starts.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedOpening`] for an opening the dialect does not open: an
    /// isolation level on SQLite, a SQLite mode elsewhere, READ UNCOMMITTED on Postgres.
    ///
    /// # Examples
    ///
    /// A dialect of the service's own opens the levels its database keeps, and says so with
    /// `Opens`:
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Isolation, Opening, Opens, Statement, StatementError, TableSpec, level,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     // SQL Server starts a transaction with `BEGIN TRANSACTION` and keeps a level it set for
    ///     // the rest of the session, so every opening names its level.
    ///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
    ///         match opening {
    ///             Opening::Default | Opening::Isolation(Isolation::ReadCommitted) => Ok(Some(
    ///                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION",
    ///             )),
    ///             Opening::Isolation(Isolation::RepeatableRead) => Ok(Some(
    ///                 "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; BEGIN TRANSACTION",
    ///             )),
    ///             other => Err(StatementError::UnsupportedOpening {
    ///                 dialect: self.name(),
    ///                 opening: other.name(),
    ///             }),
    ///         }
    ///     }
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
    /// }
    ///
    /// // A table at one of these levels mounts on `Mssql`.
    /// impl Opens<level::ReadCommitted> for Mssql {}
    /// impl Opens<level::RepeatableRead> for Mssql {}
    /// ```
    fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
        match opening {
            Opening::Default => Ok(None),
            Opening::Isolation(_) | Opening::Mode(_) => Err(opening.refused(self.name())),
        }
    }

    /// The statement that sets the savepoint after a claim, inside the claim's transaction: what
    /// a handler writes after it can be discarded while the claim's own work stays.
    ///
    /// The provided method returns `SAVEPOINT ruststream_claim`, the SQL standard's form, which
    /// Postgres, MySQL and SQLite take; a database that sets a savepoint another way returns its
    /// own statement here and in [`rollback_to_savepoint`](Self::rollback_to_savepoint).
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::Dialect;
    /// # use ruststream_sqlx_dialect::{Statement, StatementError, TableName, TableSpec};
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
    ///     // SQL Server names a savepoint with `SAVE TRANSACTION`.
    ///     fn savepoint(&self) -> &'static str {
    ///         "SAVE TRANSACTION ruststream_claim"
    ///     }
    ///
    ///     fn rollback_to_savepoint(&self) -> &'static str {
    ///         "ROLLBACK TRANSACTION ruststream_claim"
    ///     }
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
    /// }
    /// ```
    fn savepoint(&self) -> &'static str {
        "SAVEPOINT ruststream_claim"
    }

    /// The statement that discards what the transaction did after [`savepoint`](Self::savepoint),
    /// and keeps the transaction open.
    ///
    /// The provided method returns `ROLLBACK TO SAVEPOINT ruststream_claim`, the SQL standard's
    /// form, which goes back to the savepoint the provided [`savepoint`](Self::savepoint) sets; a
    /// dialect that sets its savepoint another way returns the statement that goes back to it.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::Dialect;
    /// # use ruststream_sqlx_dialect::{Statement, StatementError, TableName, TableSpec};
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
    ///     fn savepoint(&self) -> &'static str {
    ///         "SAVE TRANSACTION ruststream_claim"
    ///     }
    ///
    ///     // SQL Server goes back to a savepoint with `ROLLBACK TRANSACTION`, and the
    ///     // transaction stays open.
    ///     fn rollback_to_savepoint(&self) -> &'static str {
    ///         "ROLLBACK TRANSACTION ruststream_claim"
    ///     }
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
    /// }
    /// ```
    fn rollback_to_savepoint(&self) -> &'static str {
        "ROLLBACK TO SAVEPOINT ruststream_claim"
    }

    /// The statement a claim of a table with FIFO groups runs first in its transaction, to take
    /// the subscription's group for it; `None` for a table without FIFO groups, or a dialect that
    /// takes no group.
    ///
    /// It binds [`Param::Group`](crate::Param::Group) and returns one row whose first column is a
    /// 64-bit integer: nonzero when the claim's transaction now holds the group, zero when another
    /// transaction holds it. It does not wait. A claim that reads zero ends empty, so a row that
    /// enters the group ahead of a head in work waits for that head to settle. The transaction
    /// holds the group until it ends: in the row lock form until the delivery settles, in the
    /// lease form until the claim commits, after which the lease claim takes nothing while a row
    /// of the group holds a lease.
    ///
    /// A dialect of the service's own returns such a statement where its database can hold the
    /// group for one transaction: a lock the transaction owns, or a locking read of the group's
    /// rows. `None`, which the provided method returns, leaves the order to the claim alone. That
    /// suffices where claims run one at a time and see every lease, as on SQLite. Where two claims
    /// run side by side, it gives the order up: a row that enters the group ahead of a head in
    /// work (a smaller `priority`, an earlier `retry_after`, a dead letter moved into the group)
    /// becomes a second head, and the other claim takes it while the first is still in work.
    ///
    /// # Errors
    ///
    /// For a table with FIFO groups, the refusals of its claim: [`StatementError::AdvisoryFifo`]
    /// in the advisory lock form, [`StatementError::LeaseOnDatabaseClock`] for a lease table on
    /// the database's clock, [`StatementError::IdentifierTooLong`] for a name the database does
    /// not keep. A guard may also refuse the isolation level its claim opens at: MySQL refuses a
    /// row lock table at SERIALIZABLE with [`StatementError::FifoAtSerializable`].
    ///
    /// # Examples
    ///
    /// A dialect of the service's own takes the group with a lock its transaction owns, so a FIFO
    /// group keeps one row in work:
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     // An application lock on the table's name and the group, owned by the claim's
    ///     // transaction. `@LockTimeout = 0` answers at once, and a negative result means another
    ///     // transaction holds the group.
    ///     fn fifo_guard(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///     ) -> Result<Option<Statement>, StatementError> {
    ///         if !spec.is_fifo() {
    ///             return Ok(None);
    ///         }
    ///         let table = spec.table().replace('\'', "''");
    ///         Ok(Some(Statement::new(
    ///             format!(
    ///                 "DECLARE @resource nvarchar(255) = CONCAT(N'{table}:', @p1), @result int; \
    ///                  EXEC @result = sp_getapplock @Resource = @resource, \
    ///                  @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 0; \
    ///                  SELECT CAST(CASE WHEN @result >= 0 THEN 1 ELSE 0 END AS bigint)"
    ///             ),
    ///             [Param::Group],
    ///         )))
    ///     }
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
    /// }
    /// ```
    fn fifo_guard(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        let _ = spec;
        Ok(None)
    }
}
