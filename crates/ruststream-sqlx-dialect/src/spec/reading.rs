//! What a description holds, read back: its names, its form, its switches, the column of each
//! role and every column in order.

use super::{Grouping, TableSpec};
use crate::column::Column;
use crate::form::Form;
use crate::opening::Opening;
use crate::role::Role;

impl<'a> TableSpec<'a> {
    /// The table's name, without its schema.
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
    ///     // The service keeps its finished emails for an audit, in the `sent` group; every other
    ///     // table deletes its finished rows.
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.table() == "email_jobs" {
    ///             return Ok(Statement::new(
    ///                 "UPDATE [email_jobs] SET [name] = 'sent' WHERE [job_id] = @p1",
    ///                 [Param::Id],
    ///             ));
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
    pub const fn table(&self) -> &'a str {
        self.table
    }

    /// The schema the table lives in, or `None` for the connection's default.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
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
    ///         // A table that names no schema lives in SQL Server's default one, `dbo`.
    ///         let mut sql = String::from("DELETE FROM ");
    ///         self.quote_into(spec.schema().unwrap_or("dbo"), &mut sql);
    ///         sql.push('.');
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
    pub const fn schema(&self) -> Option<&'a str> {
        self.schema
    }

    /// The column that identifies a row.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
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
    ///     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         // Every settlement names its row by the id column.
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
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    #[must_use]
    pub const fn id(&self) -> Column<'a> {
        self.id
    }

    /// The form the table's rows are claimed in.
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
    #[must_use]
    pub const fn form(&self) -> Form<'a> {
        self.form
    }

    /// What the table's transactions open at: [`Opening::Default`] unless the table names an
    /// isolation level or a mode.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Isolation, Opening, Param, RowLock, Statement, StatementError,
    ///     TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl RowLock for Mssql {
    ///     // The claim of the service's one queue table, which it reads whole.
    ///     fn lock_claim(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         _shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         // READPAST, which skips the rows another claim holds, refuses SERIALIZABLE.
    ///         let opening = spec.opening();
    ///         if opening == Opening::Isolation(Isolation::Serializable) {
    ///             return Err(StatementError::UnsupportedOpening {
    ///                 dialect: self.name(),
    ///                 opening: opening.name(),
    ///             });
    ///         }
    ///         Ok(Statement::new(
    ///             "SELECT TOP (@p1) [job_id], [payload] FROM [email_jobs] \
    ///              WITH (UPDLOCK, READPAST) ORDER BY [job_id]",
    ///             [Param::Limit],
    ///         ))
    ///     }
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
    #[must_use]
    pub const fn opening(&self) -> Opening {
        self.opening
    }

    /// Whether each group of the table keeps its order: at most one row of a group is in work.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Param, RowLock, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl RowLock for Mssql {
    ///     // The claim of the service's one queue table, which it reads whole.
    ///     fn lock_claim(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         _shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         // This claim keeps no group in order.
    ///         if spec.is_fifo() {
    ///             return Err(StatementError::UnsupportedFifo { dialect: self.name() });
    ///         }
    ///         Ok(Statement::new(
    ///             "SELECT TOP (@p1) [job_id], [payload] FROM [email_jobs] \
    ///              WITH (UPDLOCK, READPAST) ORDER BY [job_id]",
    ///             [Param::Limit],
    ///         ))
    ///     }
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
    #[must_use]
    pub const fn is_fifo(&self) -> bool {
        matches!(self.grouping, Grouping::Fifo(_))
    }

    /// Whether statements select `*` instead of listing the columns.
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
    pub const fn selects_all(&self) -> bool {
        self.select_all
    }

    /// Whether the statements read the database's own clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
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
    ///         // A table on the database's clock reads the server's time; otherwise the service
    ///         // binds it.
    ///         if spec.uses_database_clock() {
    ///             return Ok(Statement::new(
    ///                 "UPDATE [email_jobs] SET [processed_at] = SYSUTCDATETIME() \
    ///                  WHERE [job_id] = @p1",
    ///                 [Param::Id],
    ///             ));
    ///         }
    ///         Ok(Statement::new(
    ///             "UPDATE [email_jobs] SET [processed_at] = @p1 WHERE [job_id] = @p2",
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
    #[must_use]
    pub const fn uses_database_clock(&self) -> bool {
        self.database_clock
    }

    /// The message's data columns, in the order the description lists them.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const EMAILS: TableSpec<'static> =
    ///     TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
    ///         .data(&[Column::new("recipient"), Column::new("subject")]);
    ///
    /// let names: Vec<&str> = EMAILS.data_columns().iter().map(|column| column.name()).collect();
    /// assert_eq!(names, ["recipient", "subject"]);
    /// ```
    #[must_use]
    pub const fn data_columns(&self) -> &'a [Column<'a>] {
        self.data
    }

    /// The columns only a message assembled from the table reads, in the order the description
    /// lists them ([`fetching`](Self::fetching)).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const ORDERS: TableSpec<'static> =
    ///     TableSpec::new("order_jobs", Column::new("job_id"), Form::RowLock)
    ///         .data(&[Column::new("tenant")])
    ///         .fetching(&[Column::new("note")]);
    ///
    /// let names: Vec<&str> = ORDERS.fetched_columns().iter().map(|column| column.name()).collect();
    /// assert_eq!(names, ["note"]);
    /// ```
    #[must_use]
    pub const fn fetched_columns(&self) -> &'a [Column<'a>] {
        self.fetched
    }

    /// The column that plays `role`, or `None` when the table has none.
    ///
    /// The `locked_until` column is the one the lease form carries.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Role, Statement, StatementError, TableSpec};
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
    ///         let mut table = String::new();
    ///         self.quote_into(spec.table(), &mut table);
    ///         let mut id = String::new();
    ///         self.quote_into(spec.id().name(), &mut id);
    ///         // A table with a `processed_at` column keeps its finished rows and marks them.
    ///         let Some(done) = spec.column(Role::ProcessedAt) else {
    ///             return Ok(Statement::new(
    ///                 format!("DELETE FROM {table} WHERE {id} = @p1"),
    ///                 [Param::Id],
    ///             ));
    ///         };
    ///         let mut done_at = String::new();
    ///         self.quote_into(done.name(), &mut done_at);
    ///         Ok(Statement::new(
    ///             format!("UPDATE {table} SET {done_at} = @p1 WHERE {id} = @p2"),
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
    #[must_use]
    pub const fn column(&self, role: Role) -> Option<Column<'a>> {
        match role {
            Role::Id => Some(self.id),
            Role::Group => match self.grouping {
                Grouping::None => None,
                Grouping::Groups(column) | Grouping::Fifo(column) => Some(column),
            },
            Role::PartitionKey => self.partition_key,
            Role::Priority => self.priority,
            Role::RetryAfter => self.retry_after,
            Role::Attempt => self.attempt,
            Role::LockedUntil => match self.form {
                Form::Lease(column) => Some(column),
                Form::RowLock | Form::Advisory(_) => None,
            },
            Role::ProcessedAt => self.processed_at,
            Role::Headers => self.headers,
            Role::Payload => self.payload,
        }
    }

    /// Every column: the one of each role in [`Role::ALL`] order, then the message's data, then
    /// the columns a message assembled from the table reads ([`fetching`](Self::fetching)).
    ///
    /// # Examples
    ///
    /// ```
    /// # use ruststream_sqlx_dialect::TableName;
    /// use std::num::NonZeroUsize;
    ///
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
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
    ///     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.selects_all() {
    ///             return Err(StatementError::Flattened { statement: "insert" });
    ///         }
    ///         let mut names = String::new();
    ///         let mut values = String::new();
    ///         let mut params = Vec::new();
    ///         // Every column the database does not fill, bound by its position in the
    ///         // description.
    ///         for (index, column) in spec.columns().enumerate() {
    ///             if column.is_generated() {
    ///                 continue;
    ///             }
    ///             if !params.is_empty() {
    ///                 names.push_str(", ");
    ///                 values.push_str(", ");
    ///             }
    ///             self.quote_into(column.name(), &mut names);
    ///             let position = NonZeroUsize::MIN.saturating_add(params.len());
    ///             self.placeholder_into(position, &mut values);
    ///             params.push(Param::Column(index));
    ///         }
    ///         let mut table = String::new();
    ///         self.quote_into(spec.table(), &mut table);
    ///         let sql = format!("INSERT INTO {table} ({names}) VALUES ({values})");
    ///         Ok(Statement::new(sql, params))
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
    pub fn columns(&self) -> impl Iterator<Item = Column<'a>> {
        Columns::new(self)
    }
}

/// Every column of a description in [`TableSpec::columns`] order, walked by a `const fn` as well
/// as by an iterator, so a `const` insert and a statement built at run time list the same columns.
#[derive(Debug, Clone)]
pub(crate) struct Columns<'s, 'a> {
    spec: &'s TableSpec<'a>,
    role: usize,
    data: usize,
    fetched: usize,
}

impl<'s, 'a> Columns<'s, 'a> {
    pub(crate) const fn new(spec: &'s TableSpec<'a>) -> Self {
        Self {
            spec,
            role: 0,
            data: 0,
            fetched: 0,
        }
    }

    /// The next column, or `None` past the last.
    pub(crate) const fn next_column(&mut self) -> Option<Column<'a>> {
        while self.role < Role::ALL.len() {
            let role = Role::ALL[self.role];
            self.role += 1;
            if let Some(column) = self.spec.column(role) {
                return Some(column);
            }
        }
        if self.data < self.spec.data.len() {
            self.data += 1;
            return Some(self.spec.data[self.data - 1]);
        }
        if self.fetched < self.spec.fetched.len() {
            self.fetched += 1;
            return Some(self.spec.fetched[self.fetched - 1]);
        }
        None
    }
}

impl<'a> Iterator for Columns<'_, 'a> {
    type Item = Column<'a>;

    fn next(&mut self) -> Option<Column<'a>> {
        self.next_column()
    }
}
