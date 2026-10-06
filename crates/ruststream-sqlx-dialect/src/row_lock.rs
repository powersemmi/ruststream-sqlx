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
///     let opening = dialect.begin(spec.opening())?.unwrap_or("BEGIN");
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
    /// A table with FIFO groups keeps one row of a group in work. Its claim takes the group's
    /// head, the first unfinished row of the group in claim order, and takes nothing while the
    /// head is in work (another claim's transaction holds it) or not yet due. It binds no
    /// [`Param::Limit`](crate::Param::Limit).
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
    ///
    /// The claim of a FIFO group locks the group's head alone:
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{ClaimShape, Column, Form, Param, Postgres, RowLock, TableSpec};
    ///
    /// const LEDGER: TableSpec<'static> = TableSpec::new("ledger", Column::new("id"), Form::RowLock)
    ///     .fifo_group(Column::new("account"))
    ///     .payload(Column::new("payload"));
    ///
    /// let claim = Postgres.lock_claim(&LEDGER, ClaimShape::Ids)?;
    /// assert_eq!(
    ///     claim.sql(),
    ///     r#"SELECT "id" FROM "ledger" WHERE "id" = (SELECT "id" FROM "ledger" WHERE "account" = $1 ORDER BY "id" LIMIT 1) FOR UPDATE SKIP LOCKED"#,
    /// );
    /// assert_eq!(claim.params(), [Param::Group]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError>;
}
