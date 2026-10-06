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
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Column, Form, Lease, Postgres, Statement, StatementError, TableSpec,
/// };
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")));
///
/// // What a subscription to a lease table prepares when it starts.
/// fn leasing(dialect: &dyn Lease, spec: &TableSpec<'_>) -> Result<Vec<Statement>, StatementError> {
///     let mut statements = vec![dialect.lease_claim(spec, ClaimShape::Rows)?, dialect.extend(spec)?];
///     if !dialect.claim_writes_lease() {
///         // The claim only selects: its transaction stamps each claimed row before the commit.
///         statements.push(dialect.stamp(spec)?);
///     }
///     Ok(statements)
/// }
///
/// // Postgres locks, stamps and returns the rows in one statement.
/// assert_eq!(leasing(&Postgres, &JOBS)?.len(), 2);
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
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
    /// # Errors
    ///
    /// [`StatementError::FormMismatch`] for a table in another form;
    /// [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock;
    /// [`StatementError::UnsupportedFifo`] when the table has FIFO groups and the dialect has no
    /// claim that keeps them in order.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{ClaimShape, Column, Form, Lease, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
    ///         .payload(Column::new("payload"));
    ///
    /// let claim = Postgres.lease_claim(&JOBS, ClaimShape::Rows)?;
    /// assert!(claim.sql().starts_with("WITH __claimed AS (SELECT"));
    /// // The time a lease must have ended by, the most rows to take, the new lease.
    /// assert_eq!(claim.params(), [Param::LeaseNow, Param::Limit, Param::Lease]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
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
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Form, Lease, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")));
    ///
    /// let extend = Postgres.extend(&JOBS)?;
    /// assert_eq!(
    ///     extend.sql(),
    ///     r#"UPDATE "jobs" SET "locked_until" = $1 WHERE "job_id" = $2 AND "locked_until" = $3"#,
    /// );
    /// // The new expiry, the row, and the expiry the delivery holds until this statement runs.
    /// assert_eq!(extend.params(), [Param::Lease, Param::Id, Param::Held]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
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
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Form, Lease, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
    ///         .attempt(Column::new("attempt"));
    ///
    /// let stamp = Postgres.stamp(&JOBS)?;
    /// assert_eq!(
    ///     stamp.sql(),
    ///     r#"UPDATE "jobs" SET "locked_until" = $1, "attempt" = "attempt" + 1 WHERE "job_id" = $2 AND ("locked_until" IS NULL OR "locked_until" <= $3)"#,
    /// );
    /// assert_eq!(stamp.params(), [Param::Lease, Param::Id, Param::LeaseNow]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// Whether the lease claim writes the lease itself. A dialect whose lease claim only selects
    /// the rows answers `false`, and the claim's transaction then runs [`stamp`](Self::stamp) for
    /// each claimed row before it commits.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "mysql"))] {
    /// use ruststream_sqlx_dialect::{Lease, MySql, Postgres};
    ///
    /// // How many statements a claim of `rows` leased rows runs.
    /// fn statements(dialect: &dyn Lease, rows: usize) -> usize {
    ///     if dialect.claim_writes_lease() {
    ///         1
    ///     } else {
    ///         1 + rows
    ///     }
    /// }
    ///
    /// // Postgres locks, stamps and returns the rows in one statement; MySQL selects, then stamps.
    /// assert_eq!(statements(&Postgres, 10), 1);
    /// assert_eq!(statements(&MySql, 10), 11);
    /// # }
    /// ```
    fn claim_writes_lease(&self) -> bool {
        true
    }

    /// Whether the rows the lease claim of `spec` returns carry the attempt the claim counted. A
    /// dialect whose claim returns the rows as they were before answers `false`; where it answers
    /// `true`, a delivery reports one less than its row carries.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Form, Lease, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
    ///         .attempt(Column::new("attempt"));
    ///
    /// // The attempt a delivery reports, from the one its claimed row carries.
    /// fn reported(dialect: &dyn Lease, spec: &TableSpec<'_>, carried: u64) -> u64 {
    ///     if dialect.claim_counts_attempt(spec) {
    ///         carried.saturating_sub(1)
    ///     } else {
    ///         carried
    ///     }
    /// }
    ///
    /// // Postgres returns each row as it was before the claim counted the attempt.
    /// assert_eq!(reported(&Postgres, &JOBS, 1), 1);
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
    /// stamps.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "sqlite"))] {
    /// use ruststream_sqlx_dialect::{Lease, Postgres, Sqlite};
    ///
    /// // What a broker sends to open the transaction a claim stamps its rows in.
    /// fn opening(dialect: &dyn Lease) -> &'static str {
    ///     dialect.begin_lease_claim().unwrap_or("BEGIN")
    /// }
    ///
    /// assert_eq!(opening(&Postgres), "BEGIN");
    /// assert_eq!(opening(&Sqlite), "BEGIN IMMEDIATE");
    /// # }
    /// ```
    fn begin_lease_claim(&self) -> Option<&'static str> {
        None
    }
}
