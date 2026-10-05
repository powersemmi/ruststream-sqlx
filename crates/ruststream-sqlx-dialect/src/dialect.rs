//! The trait a database's SQL implements.

use std::fmt::Debug;
use std::num::NonZeroUsize;

use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Statement, StatementError};

/// A database's SQL: how it quotes names and numbers placeholders, and the statement each queue
/// event runs.
///
/// A dialect reads a [`TableSpec`] and answers with [`Statement`]s whose
/// [`Param`](crate::Param)s are bound in order. Statements are built while a subscription starts,
/// never per message, so a dialect of the service's own travels as `&dyn Dialect`. A dialect
/// refuses with a [`StatementError`] what it does not build, and never hands out a statement with
/// other semantics instead.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Column, Dialect, Form, Postgres, Statement, StatementError, TableSpec,
/// };
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
///
/// // What a subscription prepares when it starts, whatever the database.
/// fn prepare(dialect: &dyn Dialect, spec: &TableSpec<'_>) -> Result<Vec<Statement>, StatementError> {
///     Ok(vec![dialect.claim(spec, ClaimShape::Rows)?])
/// }
///
/// let statements = prepare(&Postgres, &JOBS)?;
/// assert_eq!(
///     statements[0].sql(),
///     r#"SELECT "job_id", "payload" FROM "jobs" ORDER BY "job_id" LIMIT $1 FOR UPDATE SKIP LOCKED"#,
/// );
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
pub trait Dialect: Debug + Send + Sync {
    /// The dialect's name, for messages.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Dialect, Postgres};
    ///
    /// let dialect: &dyn Dialect = &Postgres;
    /// let context = format!("building the claim with the {} dialect", dialect.name());
    /// assert_eq!(context, "building the claim with the postgres dialect");
    /// # }
    /// ```
    fn name(&self) -> &'static str;

    /// Appends `ident` to `out`, quoted as a name of this database.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Dialect, Postgres};
    ///
    /// // A name keeps its case and its spaces; an embedded quote doubles.
    /// let mut sql = String::from("SELECT * FROM ");
    /// Postgres.quote_into(r#"Email "Jobs""#, &mut sql);
    /// assert_eq!(sql, r#"SELECT * FROM "Email ""Jobs""""#);
    /// # }
    /// ```
    fn quote_into(&self, ident: &str, out: &mut String);

    /// Appends the placeholder of parameter number `index` (counted from 1) to `out`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use std::num::NonZeroUsize;
    ///
    /// use ruststream_sqlx_dialect::{Dialect, Postgres};
    ///
    /// let mut sql = String::from(r#"DELETE FROM "jobs" WHERE "job_id" = "#);
    /// Postgres.placeholder_into(NonZeroUsize::MIN, &mut sql);
    /// assert_eq!(sql, r#"DELETE FROM "jobs" WHERE "job_id" = $1"#);
    /// # }
    /// ```
    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String);

    /// The statement that claims up to [`Param::Limit`](crate::Param::Limit) rows of the
    /// subscription's group, in claim order.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::UnsupportedFifo`] when the table has FIFO groups and the dialect
    /// has no claim that keeps them in order.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .group(Column::new("name"))
    ///     .retry_after(Column::new("retry_after"))
    ///     .payload(Column::new("payload"));
    ///
    /// let claim = Postgres.claim(&JOBS, ClaimShape::Ids)?;
    /// assert_eq!(
    ///     claim.sql(),
    ///     r#"SELECT "job_id" FROM "jobs" WHERE "name" = $1 AND "retry_after" <= $2 ORDER BY "retry_after", "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED"#,
    /// );
    /// assert_eq!(claim.params(), [Param::Group, Param::Now, Param::Limit]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError>;
}
