//! The lease form: a claim that writes a lease into each row it takes and commits at once.

use crate::dialect::Dialect;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Statement, StatementError};

/// The lease form of a dialect: the claim that writes a lease into each row it takes, the
/// extension of a lease in work, and the stamp that leases a row a claim only selected.
///
/// A table with a `#[field(locked_until)]` field takes its rows this way. A claim writes the
/// lease's expiry ([`Param::Lease`](crate::Param::Lease)) into each row it takes, counts the
/// attempt and commits, so the handler runs outside any transaction. It skips every row whose
/// lease has not ended by [`Param::LeaseNow`](crate::Param::LeaseNow). The expiry the claim wrote
/// is the delivery's ownership token ([`Param::Held`](crate::Param::Held)): the extension and
/// every settlement name the row and that token. A dialect implements this trait where it builds
/// these statements, and a lease table does not compile against a dialect that does not. Every
/// built-in dialect implements it.
///
/// Beside its statements the trait tells whether the claim writes the lease itself
/// ([`claim_writes_lease`](Self::claim_writes_lease)), whether the rows it returns carry the
/// claim's count of the attempt ([`claim_counts_attempt`](Self::claim_counts_attempt)), and how
/// the transaction of a claim that only selects its rows opens
/// ([`begin_lease_claim`](Self::begin_lease_claim)).
///
/// # Examples
///
/// A dialect for a database without a built-in one builds the lease form itself. SQL Server takes
/// the claimable rows, writes their lease and counts the attempt in one update, and returns the
/// rows as they were before:
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Dialect, Lease, Param, Statement, StatementError, TableSpec,
/// };
///
/// /// SQL Server, a database without a built-in dialect.
/// #[derive(Debug)]
/// pub struct Mssql;
///
/// // The service's one lease table, `email_jobs`, mounts on `Mssql`.
/// impl Lease for Mssql {
///     fn lease_claim(
///         &self,
///         _spec: &TableSpec<'_>,
///         shape: ClaimShape,
///     ) -> Result<Statement, StatementError> {
///         let returned = match shape {
///             ClaimShape::Rows => {
///                 "deleted.[job_id], deleted.[attempt], deleted.[locked_until], \\
///                  deleted.[payload]"
///             }
///             ClaimShape::Ids => "deleted.[job_id]",
///             ClaimShape::Roles => {
///                 "deleted.[job_id] AS [id], deleted.[attempt] AS [attempt], \
///                  deleted.[payload] AS [payload]"
///             }
///         };
///         Ok(Statement::new(
///             format!(
///                 "WITH [claimed] AS (SELECT TOP (@p3) * FROM [email_jobs] \
///                  WITH (UPDLOCK, READPAST, ROWLOCK) \
///                  WHERE [locked_until] IS NULL OR [locked_until] <= @p2 ORDER BY [job_id]) \
///                  UPDATE [claimed] SET [locked_until] = @p1, [attempt] = [attempt] + 1 \
///                  OUTPUT {returned}"
///             ),
///             [Param::Lease, Param::LeaseNow, Param::Limit],
///         ))
///     }
///
///     fn extend(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         Ok(Statement::new(
///             "UPDATE [email_jobs] SET [locked_until] = @p1 \
///              WHERE [job_id] = @p2 AND [locked_until] = @p3",
///             [Param::Lease, Param::Id, Param::Held],
///         ))
///     }
///
///     fn stamp(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         Ok(Statement::new(
///             "UPDATE [email_jobs] SET [locked_until] = @p1, [attempt] = [attempt] + 1 \
///              WHERE [job_id] = @p2 AND ([locked_until] IS NULL OR [locked_until] <= @p3)",
///             [Param::Lease, Param::Id, Param::LeaseNow],
///         ))
///     }
///
///     // A claim of the service's own (`custom(claim)`) selects its rows, then stamps them, in
///     // the transaction this opens. SQL Server starts one with `BEGIN TRANSACTION`.
///     fn begin_lease_claim(&self) -> Option<&'static str> {
///         Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION")
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
    message = "the `{Self}` dialect builds no lease claim, so a table on it cannot take its rows \
               by lease",
    label = "a table with `locked_until` takes its rows by lease",
    note = "a dialect of the service's own takes the lease form by implementing `Lease`: the \
            claim, the extension and the stamp"
)]
pub trait Lease: Dialect {
    /// The statement that claims up to [`Param::Limit`](crate::Param::Limit) rows of the
    /// subscription's group, in claim order, among the rows whose lease ended by
    /// [`Param::LeaseNow`](crate::Param::LeaseNow).
    ///
    /// Where [`claim_writes_lease`](Self::claim_writes_lease) answers `true`, it also writes the
    /// new lease ([`Param::Lease`](crate::Param::Lease)), counts the attempt, and returns the rows
    /// as they were before.
    ///
    /// A table with FIFO groups keeps one row of a group in work. Its claim takes the group's
    /// head, the first unfinished row of the group in claim order, and takes nothing while a row
    /// of the group holds a lease or the head is not yet due, so a row that enters the group ahead
    /// of the head in work waits too. It binds no [`Param::Limit`](crate::Param::Limit). It runs
    /// in a transaction that first takes the group with the statement
    /// [`fifo_guard`](Dialect::fifo_guard) gives, which keeps two claims apart until one of them
    /// commits its lease.
    ///
    /// # Errors
    ///
    /// [`StatementError::FormMismatch`] for a table in another form;
    /// [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock. A
    /// dialect of the service's own that builds no claim for FIFO groups returns
    /// [`StatementError::UnsupportedFifo`] for a table with them.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Lease for Mssql {
    ///     // One update takes the rows of the service's one lease table, `email_jobs`, in id
    ///     // order, skipping the rows another claim holds. It writes their lease, counts the
    ///     // attempt and returns them as they were before (`deleted`).
    ///     fn lease_claim(
    ///         &self,
    ///         _spec: &TableSpec<'_>,
    ///         shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         let returned = match shape {
    ///             ClaimShape::Rows => {
    ///                 "deleted.[job_id], deleted.[attempt], deleted.[locked_until], \\
    ///                  deleted.[payload]"
    ///             }
    ///             ClaimShape::Ids => "deleted.[job_id]",
    ///             ClaimShape::Roles => {
    ///                 "deleted.[job_id] AS [id], deleted.[attempt] AS [attempt], \
    ///                  deleted.[payload] AS [payload]"
    ///             }
    ///         };
    ///         Ok(Statement::new(
    ///             format!(
    ///                 "WITH [claimed] AS (SELECT TOP (@p3) * FROM [email_jobs] \
    ///                  WITH (UPDLOCK, READPAST, ROWLOCK) \
    ///                  WHERE [locked_until] IS NULL OR [locked_until] <= @p2 ORDER BY [job_id]) \
    ///                  UPDATE [claimed] SET [locked_until] = @p1, [attempt] = [attempt] + 1 \
    ///                  OUTPUT {returned}"
    ///             ),
    ///             [Param::Lease, Param::LeaseNow, Param::Limit],
    ///         ))
    ///     }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
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
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError>;

    /// The statement that extends a delivery's lease: it writes the new expiry
    /// ([`Param::Lease`](crate::Param::Lease)) while the row still holds the delivery's token
    /// ([`Param::Held`](crate::Param::Held)), so a handler that runs longer than one lease keeps
    /// its row.
    ///
    /// # Errors
    ///
    /// [`StatementError::FormMismatch`] for a table in another form;
    /// [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Lease for Mssql {
    ///     // An extension of an email's lease also records when its handler was last seen. It
    ///     // changes the row only while the row holds the delivery's lease.
    ///     fn extend(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         Ok(Statement::new(
    ///             "UPDATE [email_jobs] SET [locked_until] = @p1, [seen_at] = @p2 \
    ///              WHERE [job_id] = @p3 AND [locked_until] = @p4",
    ///             [Param::Lease, Param::Now, Param::Id, Param::Held],
    ///         ))
    ///     }
    /// #     fn lease_claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
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
    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that leases one claimed row: it writes the expiry
    /// ([`Param::Lease`](crate::Param::Lease)) and counts the attempt, while no lease holds the
    /// row at [`Param::LeaseNow`](crate::Param::LeaseNow).
    ///
    /// A claim that only selects its rows runs it for each of them inside its transaction: the
    /// claim of a dialect whose [`claim_writes_lease`](Self::claim_writes_lease) answers `false`,
    /// or a claim the service writes itself.
    ///
    /// # Errors
    ///
    /// [`StatementError::FormMismatch`] for a table in another form;
    /// [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Lease for Mssql {
    ///     // The claim only selects, so the broker stamps each row it took.
    ///     fn claim_writes_lease(&self) -> bool {
    ///         false
    ///     }
    ///
    ///     fn begin_lease_claim(&self) -> Option<&'static str> {
    ///         Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION")
    ///     }
    ///
    ///     // The stamp of an email also records when it was claimed, on the server's clock.
    ///     fn stamp(&self, _spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         Ok(Statement::new(
    ///             "UPDATE [email_jobs] SET [locked_until] = @p1, [attempt] = [attempt] + 1, \
    ///              [claimed_at] = SYSUTCDATETIME() WHERE [job_id] = @p2 \
    ///              AND ([locked_until] IS NULL OR [locked_until] <= @p3)",
    ///             [Param::Lease, Param::Id, Param::LeaseNow],
    ///         ))
    ///     }
    /// #     fn lease_claim(&self, _spec: &TableSpec<'_>, _shape: ClaimShape) -> Result<Statement, StatementError> { Ok(Statement::new("SELECT TOP (@p2) [job_id], [attempt], [payload] FROM [email_jobs] WITH (UPDLOCK, READPAST, ROWLOCK) WHERE [locked_until] IS NULL OR [locked_until] <= @p1 ORDER BY [job_id]", [Param::LeaseNow, Param::Limit])) }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
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
    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// Whether the lease claim writes the lease itself. A dialect whose lease claim only selects
    /// the rows answers `false`, and the claim's transaction then runs [`stamp`](Self::stamp) for
    /// each claimed row before it commits.
    ///
    /// The provided method answers `true`, which suits a claim that writes the lease in its own
    /// statement. A dialect whose claim only selects answers `false`, or the rows it selects reach
    /// their handlers without a lease.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Lease for Mssql {
    ///     // The claim reads the claimable rows under an update lock and writes nothing.
    ///     fn lease_claim(
    ///         &self,
    ///         _spec: &TableSpec<'_>,
    ///         _shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         Ok(Statement::new(
    ///             "SELECT TOP (@p2) [job_id], [attempt], [payload] FROM [email_jobs] \
    ///              WITH (UPDLOCK, READPAST, ROWLOCK) \
    ///              WHERE [locked_until] IS NULL OR [locked_until] <= @p1 ORDER BY [job_id]",
    ///             [Param::LeaseNow, Param::Limit],
    ///         ))
    ///     }
    ///
    ///     // So the broker stamps each row the claim took before the claim's transaction commits.
    ///     fn claim_writes_lease(&self) -> bool {
    ///         false
    ///     }
    ///
    ///     fn begin_lease_claim(&self) -> Option<&'static str> {
    ///         Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION")
    ///     }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
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
    fn claim_writes_lease(&self) -> bool {
        true
    }

    /// Whether the rows the lease claim of `spec` returns carry the attempt the claim counted. A
    /// dialect whose claim returns the rows as they were before answers `false`; where it answers
    /// `true`, a delivery reports one less than its row carries.
    ///
    /// The provided method answers `false`, which suits a claim that returns its rows as they were
    /// before the count. A dialect whose claim returns them as it left them answers `true`, or each
    /// delivery reports one attempt more than it is.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Lease for Mssql {
    ///     // One update takes the rows of `email_jobs`, writes their lease, counts the attempt and
    ///     // returns them as it left them (`inserted`).
    ///     fn lease_claim(
    ///         &self,
    ///         _spec: &TableSpec<'_>,
    ///         shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         let returned = match shape {
    ///             ClaimShape::Rows => {
    ///                 "inserted.[job_id], inserted.[attempt], inserted.[locked_until], \
    ///                  inserted.[payload]"
    ///             }
    ///             ClaimShape::Ids => "inserted.[job_id]",
    ///             ClaimShape::Roles => {
    ///                 "inserted.[job_id] AS [id], inserted.[attempt] AS [attempt], \
    ///                  inserted.[payload] AS [payload]"
    ///             }
    ///         };
    ///         Ok(Statement::new(
    ///             format!(
    ///                 "WITH [claimed] AS (SELECT TOP (@p3) * FROM [email_jobs] \
    ///                  WITH (UPDLOCK, READPAST, ROWLOCK) \
    ///                  WHERE [locked_until] IS NULL OR [locked_until] <= @p2 ORDER BY [job_id]) \
    ///                  UPDATE [claimed] SET [locked_until] = @p1, [attempt] = [attempt] + 1 \
    ///                  OUTPUT {returned}"
    ///             ),
    ///             [Param::Lease, Param::LeaseNow, Param::Limit],
    ///         ))
    ///     }
    ///
    ///     // So each delivery reports the attempt before the count.
    ///     fn claim_counts_attempt(&self, _spec: &TableSpec<'_>) -> bool {
    ///         true
    ///     }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
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
    fn claim_counts_attempt(&self, spec: &TableSpec<'_>) -> bool {
        let _ = spec;
        false
    }

    /// The statement that opens the transaction of a claim that only selects its rows, in place
    /// of `BEGIN`, or `None` when `BEGIN` opens it.
    ///
    /// The transaction holds the claim's select and the stamp of each row it took, and commits
    /// before the handlers run. MySQL opens it at READ COMMITTED, as its row lock claim; SQLite
    /// opens it with the write lock taken, so no other writer comes between the select and the
    /// stamps. The provided method returns `None`: the transaction opens with a plain `BEGIN`, at
    /// the connection's level.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Lease for Mssql {
    ///     // A table with a claim of its own (`custom(claim)`) selects its rows, then stamps them,
    ///     // in the transaction this opens. SQL Server starts one with `BEGIN TRANSACTION`, at the
    ///     // level the statement names.
    ///     fn begin_lease_claim(&self) -> Option<&'static str> {
    ///         Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION")
    ///     }
    /// #     fn lease_claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
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
    fn begin_lease_claim(&self) -> Option<&'static str> {
        None
    }
}
