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
/// # #[cfg(feature = "mysql")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Dialect, MySql, RowLock, Statement, StatementError, TableSpec,
/// };
///
/// /// MySQL, with statements of the service's own in its `Dialect` impl.
/// #[derive(Debug)]
/// pub struct Audited;
///
/// // A table without `locked_until` or `advisory_lock` mounts on `Audited`.
/// impl RowLock for Audited {
///     fn lock_claim(
///         &self,
///         spec: &TableSpec<'_>,
///         shape: ClaimShape,
///     ) -> Result<Statement, StatementError> {
///         MySql.lock_claim(spec, shape)
///     }
/// }
/// # impl Dialect for Audited {
/// #     fn name(&self) -> &'static str { "audited" }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { MySql.quote_into(ident, out); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { MySql.placeholder_into(index, out); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.fetch(spec) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.ack(spec) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { MySql.retry(spec) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.retry_after(spec) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.discard(spec) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.dead_letter_group(spec) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { MySql.dead_letter_table(spec, target) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.insert(spec) }
/// # }
/// # }
/// # fn main() {}
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Postgres, RowLock, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// Postgres, with claims that let other rows reference a row in work by foreign key.
    /// #[derive(Debug)]
    /// pub struct NoKeyUpdate;
    ///
    /// impl RowLock for NoKeyUpdate {
    ///     fn lock_claim(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         // `FOR NO KEY UPDATE` still keeps two claims apart, and lets an insert that
    ///         // references
    ///         // a claimed row go on.
    ///         let claim = Postgres.lock_claim(spec, shape)?;
    ///         let sql = claim
    ///             .sql()
    ///             .replace(" FOR UPDATE SKIP LOCKED", " FOR NO KEY UPDATE SKIP LOCKED");
    ///         Ok(Statement::new(sql, claim.params().iter().copied()))
    ///     }
    /// }
    /// # impl Dialect for NoKeyUpdate {
    /// #     fn name(&self) -> &'static str { "no_key_update" }
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
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError>;
}
