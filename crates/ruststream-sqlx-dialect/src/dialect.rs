//! The trait every dialect implements: names, placeholders, and the statements every form runs.

use std::fmt::Debug;
use std::num::NonZeroUsize;

use crate::opening::Opening;
use crate::spec::TableSpec;
use crate::statement::{Statement, StatementError};
use crate::table_name::TableName;

/// A database's SQL: how it quotes names and numbers placeholders, and the statements every form
/// of claiming runs to settle a row.
///
/// A dialect reads a [`TableSpec`] and answers with [`Statement`]s whose
/// [`Param`](crate::Param)s are bound in order. Statements are built while a subscription starts,
/// never per message. A dialect refuses with a [`StatementError`] what it does not build, and
/// never hands out a statement with other semantics instead.
///
/// The forms of claiming are traits of their own, each over this one: [`RowLock`](crate::RowLock)
/// builds the claim that locks rows for its transaction, [`Lease`](crate::Lease) the claim that
/// writes a lease and the lease's extension, [`Advisory`](crate::Advisory) the claim of candidates
/// with their lock keys, the lock, the unlock and the take. A dialect implements the traits of the
/// forms its database serves, and a table in another form does not compile against it. A dialect
/// of the service's own does the same: it implements this trait, then the trait of each form it
/// builds, with every statement its own or delegated to a built-in dialect it wraps.
///
/// The settlements here serve every form the dialect builds. In the lease form each of them names
/// the row and the delivery's ownership token ([`Param::Held`](crate::Param::Held)), so a delivery
/// whose lease ran out, and whose row another claim took, changes nothing. In the advisory lock
/// form they name the row alone: the lock on its key keeps every other claim off it.
///
/// A dialect also opens transactions: [`begin`](Self::begin) gives the statement that opens one
/// at a table's isolation level or SQLite mode, and [`savepoint`](Self::savepoint) and
/// [`rollback_to_savepoint`](Self::rollback_to_savepoint) mark where a handler's writes start
/// and discard them. In a table with FIFO groups, a claim's transaction first takes the
/// subscription's group with the statement [`fifo_guard`](Self::fifo_guard) gives.
///
/// # Examples
///
/// A dialect for a database without a built-in one implements the trait itself:
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
///     fn quote_into(&self, ident: &str, out: &mut String) {
///         out.push('[');
///         out.push_str(&ident.replace(']', "]]"));
///         out.push(']');
///     }
///
///     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
///         out.push_str("@p");
///         out.push_str(&index.to_string());
///     }
///
///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         let mut sql = String::from("DELETE FROM ");
///         self.quote_into(spec.table(), &mut sql);
///         sql.push_str(" WHERE ");
///         self.quote_into(spec.id().name(), &mut sql);
///         sql.push_str(" = ");
///         self.placeholder_into(NonZeroUsize::MIN, &mut sql);
///         Ok(Statement::new(sql, [Param::Id]))
///     }
///
///     // The statements this service never runs refuse the table.
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// }
/// ```
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
    /// A finished email stays in its table, in the `sent` group, for an audit:
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Audited;
    ///
    /// impl Dialect for Audited {
    ///     fn name(&self) -> &'static str {
    ///         "audited"
    ///     }
    ///
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.table() == "email_jobs" {
    ///             return Ok(Statement::new(
    ///                 r#"UPDATE "email_jobs" SET "name" = 'sent' WHERE "job_id" = $1"#,
    ///                 [Param::Id],
    ///             ));
    ///         }
    ///         Postgres.ack(spec)
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    /// # }
    /// # fn main() {}
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Audited;
    ///
    /// impl Dialect for Audited {
    ///     fn name(&self) -> &'static str {
    ///         "audited"
    ///     }
    ///
    ///     // A retry of an email also records when it happened.
    ///     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
    ///         if spec.table() == "email_jobs" {
    ///             return Ok(Some(Statement::new(
    ///                 r#"UPDATE "email_jobs" SET "attempt" = "attempt" + 1, "retried_at" = $1 WHERE "job_id" = $2"#,
    ///                 [Param::Now, Param::Id],
    ///             )));
    ///         }
    ///         Postgres.retry(spec)
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    /// # }
    /// # fn main() {}
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Audited;
    ///
    /// impl Dialect for Audited {
    ///     fn name(&self) -> &'static str {
    ///         "audited"
    ///     }
    ///
    ///     // A delayed email also counts its delays.
    ///     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.table() == "email_jobs" {
    ///             return Ok(Statement::new(
    ///                 r#"UPDATE "email_jobs" SET "retry_after" = $1, "delays" = "delays" + 1 WHERE "job_id" = $2"#,
    ///                 [Param::RetryAfter, Param::Id],
    ///             ));
    ///         }
    ///         Postgres.retry_after(spec)
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    /// # }
    /// # fn main() {}
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Audited;
    ///
    /// impl Dialect for Audited {
    ///     fn name(&self) -> &'static str {
    ///         "audited"
    ///     }
    ///
    ///     // A dropped email stays in its table, marked with the time it was dropped.
    ///     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.table() == "email_jobs" {
    ///             return Ok(Statement::new(
    ///                 r#"UPDATE "email_jobs" SET "dropped_at" = $1 WHERE "job_id" = $2"#,
    ///                 [Param::Now, Param::Id],
    ///             ));
    ///         }
    ///         Postgres.discard(spec)
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    /// # }
    /// # fn main() {}
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Audited;
    ///
    /// impl Dialect for Audited {
    ///     fn name(&self) -> &'static str {
    ///         "audited"
    ///     }
    ///
    ///     // A dead letter starts its new group with the attempt count of a new row.
    ///     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.table() == "email_jobs" {
    ///             return Ok(Statement::new(
    ///                 r#"UPDATE "email_jobs" SET "name" = $1, "attempt" = DEFAULT WHERE "job_id" = $2"#,
    ///                 [Param::Destination, Param::Id],
    ///             ));
    ///         }
    ///         Postgres.dead_letter_group(spec)
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    /// # }
    /// # fn main() {}
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Param, Postgres, Statement, StatementError, TableName, TableSpec,
    /// };
    ///
    /// #[derive(Debug)]
    /// pub struct Dated;
    ///
    /// impl Dialect for Dated {
    ///     fn name(&self) -> &'static str {
    ///         "dated"
    ///     }
    ///
    ///     // A dead email moves with the time it died.
    ///     fn dead_letter_table(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         target: TableName<'_>,
    ///     ) -> Result<Vec<Statement>, StatementError> {
    ///         if spec.table() != "email_jobs" {
    ///             return Postgres.dead_letter_table(spec, target);
    ///         }
    ///         let mut into = String::new();
    ///         if let Some(schema) = target.schema() {
    ///             self.quote_into(schema, &mut into);
    ///             into.push('.');
    ///         }
    ///         self.quote_into(target.table(), &mut into);
    ///         Ok(vec![
    ///             Statement::new(
    ///                 format!(
    ///                     r#"INSERT INTO {into} ("job_id", "payload", "died_at") SELECT "job_id", "payload", $1 FROM "email_jobs" WHERE "job_id" = $2"#
    ///                 ),
    ///                 [Param::Now, Param::Id],
    ///             ),
    ///             Statement::new(r#"DELETE FROM "email_jobs" WHERE "job_id" = $1"#, [Param::Id]),
    ///         ])
    ///     }
    ///
    ///     fn quote_into(&self, ident: &str, out: &mut String) {
    ///         Postgres.quote_into(ident, out);
    ///     }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    /// # }
    /// # fn main() {}
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Postgres, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Idempotent;
    ///
    /// impl Dialect for Idempotent {
    ///     fn name(&self) -> &'static str {
    ///         "idempotent"
    ///     }
    ///
    ///     // Publishing a job twice keeps one row: the insert skips a row whose id is already
    ///     // there.
    ///     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         let insert = Postgres.insert(spec)?;
    ///         Ok(Statement::new(
    ///             format!("{} ON CONFLICT DO NOTHING", insert.sql()),
    ///             insert.params().iter().copied(),
    ///         ))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The query that reads the server's version as one text column, or `None` when the
    /// dialect's statements run on every version of its server.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Postgres, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Modern;
    ///
    /// impl Dialect for Modern {
    ///     fn name(&self) -> &'static str {
    ///         "modern"
    ///     }
    ///
    ///     // The server reports its version as a number: 150000 and above is Postgres 15.
    ///     fn server_version(&self) -> Option<&'static str> {
    ///         Some("SHOW server_version_num")
    ///     }
    ///
    ///     fn check_server(
    ///         &self,
    ///         _spec: &TableSpec<'_>,
    ///         version: &str,
    ///     ) -> Result<(), StatementError> {
    ///         match version.trim().parse::<u32>() {
    ///             Ok(number) if number >= 150_000 => Ok(()),
    ///             _ => Err(StatementError::ServerTooOld {
    ///                 dialect: self.name(),
    ///                 server: version.to_owned(),
    ///                 required: "Postgres 15",
    ///             }),
    ///         }
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    fn server_version(&self) -> Option<&'static str> {
        None
    }

    /// Refuses a server older than the statements of `spec` need; `version` is what the
    /// [`server_version`](Self::server_version) query returned.
    ///
    /// # Errors
    ///
    /// [`StatementError::ServerTooOld`] when the server predates the dialect's statements for the
    /// table's form.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Postgres, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Modern;
    ///
    /// impl Dialect for Modern {
    ///     fn name(&self) -> &'static str {
    ///         "modern"
    ///     }
    ///
    ///     fn server_version(&self) -> Option<&'static str> {
    ///         Some("SHOW server_version_num")
    ///     }
    ///
    ///     // The service's statements need Postgres 15, so an older server stops the subscription.
    ///     fn check_server(
    ///         &self,
    ///         _spec: &TableSpec<'_>,
    ///         version: &str,
    ///     ) -> Result<(), StatementError> {
    ///         match version.trim().parse::<u32>() {
    ///             Ok(number) if number >= 150_000 => Ok(()),
    ///             _ => Err(StatementError::ServerTooOld {
    ///                 dialect: self.name(),
    ///                 server: version.to_owned(),
    ///                 required: "Postgres 15",
    ///             }),
    ///         }
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    /// # }
    /// # fn main() {}
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
    /// `BEGIN` does. A dialect accepts the openings it implements [`Opens`](crate::Opens) for;
    /// the provided method accepts only [`Opening::Default`], with `BEGIN`.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedOpening`] for an opening the dialect does not open: an
    /// isolation level on SQLite, a SQLite mode elsewhere, READ UNCOMMITTED on Postgres.
    ///
    /// # Examples
    ///
    /// A dialect that wraps Postgres opens what Postgres opens, and says so:
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Opening, Opens, Postgres, Statement, StatementError, TableSpec, level,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Audited;
    ///
    /// impl Dialect for Audited {
    ///     fn name(&self) -> &'static str {
    ///         "audited"
    ///     }
    ///
    ///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
    ///         Postgres.begin(opening)
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    ///
    /// // A table at one of these levels mounts on `Audited`.
    /// impl Opens<level::ReadCommitted> for Audited {}
    /// impl Opens<level::RepeatableRead> for Audited {}
    /// impl Opens<level::Serializable> for Audited {}
    /// # }
    /// # fn main() {}
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
    /// A dialect that wraps Postgres's claims wraps its guard too, so a FIFO group keeps one row
    /// in work:
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Postgres, RowLock, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Audited;
    ///
    /// impl Dialect for Audited {
    ///     fn name(&self) -> &'static str {
    ///         "audited"
    ///     }
    ///
    ///     fn fifo_guard(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///     ) -> Result<Option<Statement>, StatementError> {
    ///         Postgres.fifo_guard(spec)
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    ///
    /// impl RowLock for Audited {
    ///     fn lock_claim(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         Postgres.lock_claim(spec, shape)
    ///     }
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    fn fifo_guard(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        let _ = spec;
        Ok(None)
    }
}
