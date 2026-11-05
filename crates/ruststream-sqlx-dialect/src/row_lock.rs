//! The row lock form: a claim that locks its rows for the transaction its handler settles in.

use crate::dialect::Dialect;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Statement, StatementError};

/// The row lock form of a dialect: the claim that locks its rows until its transaction ends.
///
/// A table that declares neither `#[field(locked_until)]` nor `advisory_lock` takes its rows
/// this way. The claim selects the claimable rows under a lock, the handler runs while the
/// transaction holds them, and the settlement ends the transaction; after a crash the database
/// rolls back and the rows return at once. The transaction opens at the table's isolation level,
/// with the statement [`Dialect::begin`] gives. A dialect implements this trait where its
/// database locks rows for a transaction, and a table in the row lock form does not compile
/// against a dialect that does not. [`Postgres`](crate::Postgres) and [`MySql`](crate::MySql)
/// implement it; [`Sqlite`](crate::Sqlite) does not, as its writer locks the whole database.
///
/// # Examples
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Dialect, Param, RowLock, Statement, StatementError, TableSpec,
/// };
///
/// /// SQL Server, a database without a built-in dialect.
/// #[derive(Debug)]
/// pub struct Mssql;
///
/// // A table without `locked_until` or `advisory_lock` mounts on `Mssql`. The claim holds an
/// // update lock on each row it takes until its transaction ends, and skips the rows another claim
/// // holds.
/// impl RowLock for Mssql {
///     fn lock_claim(
///         &self,
///         _spec: &TableSpec<'_>,
///         shape: ClaimShape,
///     ) -> Result<Statement, StatementError> {
///         let selected = match shape {
///             ClaimShape::Rows => "[job_id], [attempt], [payload]",
///             ClaimShape::Ids => "[job_id]",
///             ClaimShape::Roles => {
///                 "[job_id] AS [id], [attempt] AS [attempt], [payload] AS [payload]"
///             }
///         };
///         Ok(Statement::new(
///             format!(
///                 "SELECT TOP (@p1) {selected} FROM [email_jobs] \
///                  WITH (UPDLOCK, READPAST, ROWLOCK) ORDER BY [job_id]"
///             ),
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
#[diagnostic::on_unimplemented(
    message = "the `{Self}` dialect builds no row lock claim, so a table on it cannot take its \
               rows by row lock",
    label = "a table without `locked_until` or `advisory_lock` takes its rows by row lock",
    note = "declare `#[field(locked_until)]` (the lease form) or `#[inbox(advisory_lock = \"..\")]` \
            (the advisory lock form) on the struct",
    note = "a dialect of the service's own serves the row lock form by implementing `RowLock`, \
            where its database locks rows"
)]
pub trait RowLock: Dialect {
    /// The statement that claims up to [`Param::Limit`](crate::Param::Limit) rows of the
    /// subscription's group, in claim order, and locks them until the claim's transaction ends.
    ///
    /// A table with FIFO groups keeps one row of a group in work. Its claim takes the group's
    /// head, the first unfinished row of the group in claim order, and takes nothing while the
    /// head is in work (another claim's transaction holds it) or not yet due. It binds no
    /// [`Param::Limit`](crate::Param::Limit). The claim's transaction first takes the group with
    /// the statement [`fifo_guard`](Dialect::fifo_guard) gives and holds it until the delivery
    /// settles, so a row that enters the group ahead of the head in work waits too.
    ///
    /// # Errors
    ///
    /// [`StatementError::FormMismatch`] for a table in another form. A dialect of the service's
    /// own that builds no claim for FIFO groups returns [`StatementError::UnsupportedFifo`] for a
    /// table with them.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Param, RowLock, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl RowLock for Mssql {
    ///     // The due rows of the subscription's group in `email_jobs`, in claim order. `UPDLOCK`
    ///     // keeps two claims apart until the transaction ends, `READPAST` skips the rows another
    ///     // claim holds, and `ROWLOCK` keeps the lock to the rows themselves.
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
    ///             "SELECT TOP (@p3) [job_id], [attempt], [payload] FROM [email_jobs] \
    ///              WITH (UPDLOCK, READPAST, ROWLOCK) \
    ///              WHERE [name] = @p1 AND [retry_after] <= @p2 \
    ///              ORDER BY [priority], [retry_after], [job_id]",
    ///             [Param::Group, Param::Now, Param::Limit],
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
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError>;
}
