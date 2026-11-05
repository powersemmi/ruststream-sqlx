//! The advisory lock form: a lock on each row's key, held by the delivery's session, holds the row.

use crate::dialect::Dialect;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Statement, StatementError};

/// The advisory lock form of a dialect: the claim of candidates, the lock and the unlock of a key in
/// the session, and the take of a candidate once its lock is held.
///
/// A table with `#[inbox(advisory_lock = "..")]` takes its rows this way. The key template names
/// columns of the row ([`Form::Advisory`](crate::Form::Advisory)), and the database renders each
/// row's key from them as text. A claim selects candidates with their keys
/// ([`advisory_claim`](Self::advisory_claim)), locks each key on the delivery's own connection
/// ([`lock`](Self::lock), bound as [`Param::Key`](crate::Param::Key)), and takes the row while it
/// is still claimable ([`take`](Self::take)), counting its attempt. No transaction stays open
/// while the handler runs. The settlements are the statements every form runs, naming the row
/// alone, and the unlock ([`unlock`](Self::unlock)) follows them; a crash ends the session and
/// releases its locks at once. A dialect implements this trait where it builds these statements,
/// and a table with `advisory_lock` does not compile against a dialect that does not. Every
/// built-in dialect implements it.
///
/// The lock and the unlock return one 64-bit integer, nonzero when the lock was taken or released,
/// so one decode serves every database. A dialect whose database keeps no locks returns neither
/// statement, and the broker keeps the keys in work in the process: [`Sqlite`](crate::Sqlite) does.
///
/// # Examples
///
/// A dialect for a database without a built-in one builds the advisory lock form itself. On SQL
/// Server an application lock the delivery's session owns holds the row:
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     Advisory, ClaimShape, Dialect, Param, Statement, StatementError, TableSpec,
/// };
///
/// /// SQL Server, a database without a built-in dialect.
/// #[derive(Debug)]
/// pub struct Mssql;
///
/// // A table with `advisory_lock` mounts on `Mssql`.
/// impl Advisory for Mssql {
///     // The candidates of `email_jobs` (`advisory_lock = "email-{job_id}"`) in id order, each
///     // with its key. `APPLOCK_TEST` leaves out the keys another session holds.
///     fn advisory_claim(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         Ok(Statement::new(
///             "SELECT TOP (@p1) [job_id], CONCAT(N'email-', [job_id]) AS [__lock] \
///              FROM [email_jobs] \
///              WHERE APPLOCK_TEST('public', CONCAT(N'email-', [job_id]), \
///              'Exclusive', 'Session') = 1 \
///              ORDER BY [job_id]",
///             [Param::Limit],
///         ))
///     }
///
///     // An application lock the session owns, tried without waiting: `sp_getapplock` answers
///     // zero or more when it granted the lock.
///     fn lock(&self) -> Option<Statement> {
///         Some(Statement::new(
///             "DECLARE @result int; \
///              EXEC @result = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', \
///              @LockOwner = 'Session', @LockTimeout = 0; \
///              SELECT CAST(CASE WHEN @result >= 0 THEN 1 ELSE 0 END AS bigint)",
///             [Param::Key],
///         ))
///     }
///
///     // The release of the session's lock. A key the session does not hold answers zero, as
///     // `sp_releaseapplock` would raise an error for it.
///     fn unlock(&self) -> Option<Statement> {
///         Some(Statement::new(
///             "IF APPLOCK_MODE('public', @p1, 'Session') = 'NoLock' SELECT CAST(0 AS bigint) \
///              ELSE BEGIN DECLARE @result int; \
///              EXEC @result = sp_releaseapplock @Resource = @p1, @LockOwner = 'Session'; \
///              SELECT CAST(CASE WHEN @result = 0 THEN 1 ELSE 0 END AS bigint) END",
///             [Param::Key],
///         ))
///     }
///
///     fn take(
///         &self,
///         _spec: &TableSpec<'_>,
///         _shape: ClaimShape,
///     ) -> Result<Vec<Statement>, StatementError> {
///         Ok(vec![Statement::new(
///             "UPDATE [email_jobs] SET [attempt] = [attempt] + 1 \
///              OUTPUT deleted.[job_id], deleted.[attempt], deleted.[payload] \
///              WHERE [job_id] = @p1",
///             [Param::Id],
///         )])
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
    message = "the `{Self}` dialect takes no advisory locks, so a table on it cannot take its rows \
               by advisory lock",
    label = "a table with `advisory_lock` takes its rows by advisory lock",
    note = "declare `#[field(locked_until)]` (the lease form), or drop `advisory_lock` (the row lock \
            form)",
    note = "a dialect of the service's own takes the advisory lock form by implementing \
            `Advisory`: the claim of candidates, the lock, the unlock and the take"
)]
pub trait Advisory: Dialect {
    /// The candidates: up to [`Param::Limit`](crate::Param::Limit) claimable rows of the
    /// subscription's group in claim order, two columns each, the id and the lock key as text
    /// (named `__lock`). It takes no lock and writes nothing; where the database can tell, it
    /// leaves out the rows whose key another session holds.
    ///
    /// # Errors
    ///
    /// [`StatementError::FormMismatch`] for a table in another form;
    /// [`StatementError::AdvisoryFifo`] for a table with FIFO groups, as the lock key keeps a
    /// group in order in this form.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     Advisory, ClaimShape, Dialect, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Advisory for Mssql {
    ///     // The candidates of `email_jobs` (`advisory_lock = "email-{job_id}"`) in id order, each
    ///     // with its key. `APPLOCK_TEST` leaves out the keys another session holds.
    ///     fn advisory_claim(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         Ok(Statement::new(
    ///             "SELECT TOP (@p1) [job_id], CONCAT(N'email-', [job_id]) AS [__lock] \
    ///              FROM [email_jobs] \
    ///              WHERE APPLOCK_TEST('public', CONCAT(N'email-', [job_id]), \
    ///              'Exclusive', 'Session') = 1 \
    ///              ORDER BY [job_id]",
    ///             [Param::Limit],
    ///         ))
    ///     }
    /// #     fn lock(&self) -> Option<Statement> { Some(Statement::new("DECLARE @result int; EXEC @result = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0; SELECT CAST(CASE WHEN @result >= 0 THEN 1 ELSE 0 END AS bigint)", [Param::Key])) }
    /// #     fn unlock(&self) -> Option<Statement> { Some(Statement::new("IF APPLOCK_MODE('public', @p1, 'Session') = 'NoLock' SELECT CAST(0 AS bigint) ELSE BEGIN DECLARE @result int; EXEC @result = sp_releaseapplock @Resource = @p1, @LockOwner = 'Session'; SELECT CAST(CASE WHEN @result = 0 THEN 1 ELSE 0 END AS bigint) END", [Param::Key])) }
    /// #     fn take(&self, _spec: &TableSpec<'_>, _shape: ClaimShape) -> Result<Vec<Statement>, StatementError> { Ok(vec![Statement::new("UPDATE [email_jobs] SET [attempt] = [attempt] + 1 OUTPUT deleted.[job_id], deleted.[attempt], deleted.[payload] WHERE [job_id] = @p1", [Param::Id])]) }
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
    fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// Tries the lock on [`Param::Key`](crate::Param::Key) for the session, without waiting: one
    /// row whose first column is a 64-bit integer, nonzero when the lock was taken. `None` when the
    /// process keeps the locks.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     Advisory, ClaimShape, Dialect, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Advisory for Mssql {
    ///     // An application lock the session owns, tried without waiting: `sp_getapplock` answers
    ///     // zero or more when it granted the lock.
    ///     fn lock(&self) -> Option<Statement> {
    ///         Some(Statement::new(
    ///             "DECLARE @result int; \
    ///              EXEC @result = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', \
    ///              @LockOwner = 'Session', @LockTimeout = 0; \
    ///              SELECT CAST(CASE WHEN @result >= 0 THEN 1 ELSE 0 END AS bigint)",
    ///             [Param::Key],
    ///         ))
    ///     }
    /// #     fn advisory_claim(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Ok(Statement::new("SELECT TOP (@p1) [job_id], CONCAT(N'email-', [job_id]) AS [__lock] FROM [email_jobs] WHERE APPLOCK_TEST('public', CONCAT(N'email-', [job_id]), 'Exclusive', 'Session') = 1 ORDER BY [job_id]", [Param::Limit])) }
    /// #     fn unlock(&self) -> Option<Statement> { Some(Statement::new("IF APPLOCK_MODE('public', @p1, 'Session') = 'NoLock' SELECT CAST(0 AS bigint) ELSE BEGIN DECLARE @result int; EXEC @result = sp_releaseapplock @Resource = @p1, @LockOwner = 'Session'; SELECT CAST(CASE WHEN @result = 0 THEN 1 ELSE 0 END AS bigint) END", [Param::Key])) }
    /// #     fn take(&self, _spec: &TableSpec<'_>, _shape: ClaimShape) -> Result<Vec<Statement>, StatementError> { Ok(vec![Statement::new("UPDATE [email_jobs] SET [attempt] = [attempt] + 1 OUTPUT deleted.[job_id], deleted.[attempt], deleted.[payload] WHERE [job_id] = @p1", [Param::Id])]) }
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
    fn lock(&self) -> Option<Statement>;

    /// Releases the lock on [`Param::Key`](crate::Param::Key): one row whose first column is a
    /// 64-bit integer, nonzero when the session held it. `None` when the process keeps the locks.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     Advisory, ClaimShape, Dialect, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Advisory for Mssql {
    ///     // The release of the session's lock. A key the session does not hold answers zero, as
    ///     // `sp_releaseapplock` would raise an error for it.
    ///     fn unlock(&self) -> Option<Statement> {
    ///         Some(Statement::new(
    ///             "IF APPLOCK_MODE('public', @p1, 'Session') = 'NoLock' SELECT CAST(0 AS bigint) \
    ///              ELSE BEGIN DECLARE @result int; \
    ///              EXEC @result = sp_releaseapplock @Resource = @p1, @LockOwner = 'Session'; \
    ///              SELECT CAST(CASE WHEN @result = 0 THEN 1 ELSE 0 END AS bigint) END",
    ///             [Param::Key],
    ///         ))
    ///     }
    /// #     fn advisory_claim(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Ok(Statement::new("SELECT TOP (@p1) [job_id], CONCAT(N'email-', [job_id]) AS [__lock] FROM [email_jobs] WHERE APPLOCK_TEST('public', CONCAT(N'email-', [job_id]), 'Exclusive', 'Session') = 1 ORDER BY [job_id]", [Param::Limit])) }
    /// #     fn lock(&self) -> Option<Statement> { Some(Statement::new("DECLARE @result int; EXEC @result = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0; SELECT CAST(CASE WHEN @result >= 0 THEN 1 ELSE 0 END AS bigint)", [Param::Key])) }
    /// #     fn take(&self, _spec: &TableSpec<'_>, _shape: ClaimShape) -> Result<Vec<Statement>, StatementError> { Ok(vec![Statement::new("UPDATE [email_jobs] SET [attempt] = [attempt] + 1 OUTPUT deleted.[job_id], deleted.[attempt], deleted.[payload] WHERE [job_id] = @p1", [Param::Id])]) }
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
    fn unlock(&self) -> Option<Statement>;

    /// The statements that take a candidate whose lock the session holds: they count its attempt
    /// where the table has one and read the row again in `shape`, as it was before the count, only
    /// while it is still claimable. The last returns the row, or none when the row is gone or no
    /// longer claimable; a first statement, where there are two, counts the attempt and changes no row
    /// in that case.
    ///
    /// A table read with `*` ([`TableSpec::selects_all`]) names no column, so its row returns
    /// with the attempt counted.
    ///
    /// # Errors
    ///
    /// [`StatementError::FormMismatch`] for a table in another form;
    /// [`StatementError::AdvisoryFifo`] for a table with FIFO groups.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     Advisory, ClaimShape, Dialect, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Advisory for Mssql {
    ///     // Taking an email also records when. `email_jobs` keeps no finished rows and no delays,
    ///     // so a row that is still there is claimable; `deleted` returns it as it was before the
    ///     // count.
    ///     fn take(
    ///         &self,
    ///         _spec: &TableSpec<'_>,
    ///         shape: ClaimShape,
    ///     ) -> Result<Vec<Statement>, StatementError> {
    ///         let returned = match shape {
    ///             ClaimShape::Rows => "deleted.[job_id], deleted.[attempt], deleted.[payload]",
    ///             ClaimShape::Ids => "deleted.[job_id]",
    ///             ClaimShape::Roles => {
    ///                 "deleted.[job_id] AS [id], deleted.[attempt] AS [attempt], \
    ///                  deleted.[payload] AS [payload]"
    ///             }
    ///         };
    ///         Ok(vec![Statement::new(
    ///             format!(
    ///                 "UPDATE [email_jobs] SET [attempt] = [attempt] + 1, [taken_at] = @p1 \
    ///                  OUTPUT {returned} WHERE [job_id] = @p2"
    ///             ),
    ///             [Param::Now, Param::Id],
    ///         )])
    ///     }
    /// #     fn advisory_claim(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Ok(Statement::new("SELECT TOP (@p1) [job_id], CONCAT(N'email-', [job_id]) AS [__lock] FROM [email_jobs] WHERE APPLOCK_TEST('public', CONCAT(N'email-', [job_id]), 'Exclusive', 'Session') = 1 ORDER BY [job_id]", [Param::Limit])) }
    /// #     fn lock(&self) -> Option<Statement> { Some(Statement::new("DECLARE @result int; EXEC @result = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0; SELECT CAST(CASE WHEN @result >= 0 THEN 1 ELSE 0 END AS bigint)", [Param::Key])) }
    /// #     fn unlock(&self) -> Option<Statement> { Some(Statement::new("IF APPLOCK_MODE('public', @p1, 'Session') = 'NoLock' SELECT CAST(0 AS bigint) ELSE BEGIN DECLARE @result int; EXEC @result = sp_releaseapplock @Resource = @p1, @LockOwner = 'Session'; SELECT CAST(CASE WHEN @result = 0 THEN 1 ELSE 0 END AS bigint) END", [Param::Key])) }
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
    fn take(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError>;
}
