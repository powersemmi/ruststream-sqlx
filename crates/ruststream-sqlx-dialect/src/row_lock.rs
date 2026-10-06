//! The row lock form: a claim that locks its rows for the transaction its handler settles in.

use crate::dialect::Dialect;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Statement, StatementError};

/// The row lock form of a dialect: the claim that locks its rows until its transaction ends, and
/// the statement that opens that transaction.
///
/// A table that declares neither `#[field(locked_until)]` nor `advisory_lock` takes its rows
/// this way. The claim selects the claimable rows under a lock, the handler runs while the
/// transaction holds them, and the settlement ends the transaction; after a crash the database
/// rolls back and the rows return at once. A dialect implements this trait where its database
/// locks rows for a transaction, and a table in the row lock form does not compile against a
/// dialect that does not. [`Postgres`](crate::Postgres) and [`MySql`](crate::MySql) implement it;
/// [`Sqlite`](crate::Sqlite) does not, as its writer locks the whole database.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Column, Form, Postgres, RowLock, Statement, StatementError, TableSpec,
/// };
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
///
/// // What a subscription to a row lock table prepares when it starts: the claim, and the
/// // statement its transaction opens with.
/// fn claiming(
///     dialect: &dyn RowLock,
///     spec: &TableSpec<'_>,
/// ) -> Result<(Statement, &'static str), StatementError> {
///     let opening = dialect.begin_lock_claim().unwrap_or("BEGIN");
///     Ok((dialect.lock_claim(spec, ClaimShape::Rows)?, opening))
/// }
///
/// let (claim, opening) = claiming(&Postgres, &JOBS)?;
/// assert_eq!(opening, "BEGIN");
/// assert!(claim.sql().ends_with("FOR UPDATE SKIP LOCKED"));
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
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
    /// # Errors
    ///
    /// [`StatementError::FormMismatch`] for a table in another form;
    /// [`StatementError::UnsupportedFifo`] when the table has FIFO groups and the dialect has no
    /// claim that keeps them in order.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{ClaimShape, Column, Form, Param, Postgres, RowLock, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .group(Column::new("name"))
    ///     .retry_after(Column::new("retry_after"))
    ///     .payload(Column::new("payload"));
    ///
    /// let claim = Postgres.lock_claim(&JOBS, ClaimShape::Ids)?;
    /// assert_eq!(
    ///     claim.sql(),
    ///     r#"SELECT "job_id" FROM "jobs" WHERE "name" = $1 AND "retry_after" <= $2 ORDER BY "retry_after", "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED"#,
    /// );
    /// assert_eq!(claim.params(), [Param::Group, Param::Now, Param::Limit]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError>;

    /// The statement that opens a claim's transaction in place of `BEGIN`, or `None` when `BEGIN`
    /// opens it.
    ///
    /// The statement leaves the connection inside a transaction, as `BEGIN` does. MySQL opens it
    /// at READ COMMITTED, so a claim held for a handler locks no gaps between rows and holds back
    /// no insert into its table.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "mysql"))] {
    /// use ruststream_sqlx_dialect::{MySql, Postgres, RowLock};
    ///
    /// // What a broker sends to open a row lock claim's transaction.
    /// fn opening(dialect: &dyn RowLock) -> &'static str {
    ///     dialect.begin_lock_claim().unwrap_or("BEGIN")
    /// }
    ///
    /// assert_eq!(opening(&Postgres), "BEGIN");
    /// assert_eq!(
    ///     opening(&MySql),
    ///     "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION",
    /// );
    /// # }
    /// ```
    fn begin_lock_claim(&self) -> Option<&'static str> {
        None
    }
}
