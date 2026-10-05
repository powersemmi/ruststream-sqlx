//! The trait a database's SQL implements.

use std::fmt::Debug;
use std::num::NonZeroUsize;

use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Statement, StatementError};
use crate::table_name::TableName;

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
///     Ok(vec![dialect.claim(spec, ClaimShape::Rows)?, dialect.ack(spec)?])
/// }
///
/// let statements = prepare(&Postgres, &JOBS)?;
/// assert_eq!(statements[1].sql(), r#"DELETE FROM "jobs" WHERE "job_id" = $1"#);
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

    /// The statement that reads the rows of claimed ids, bound as one list.
    ///
    /// # Errors
    ///
    /// A [`StatementError`] when the dialect cannot read rows by a list of ids; the built-in
    /// dialects build it for every table.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
    ///
    /// let fetch = Postgres.fetch(&JOBS)?;
    /// assert_eq!(
    ///     fetch.sql(),
    ///     r#"SELECT "job_id", "payload" FROM "jobs" WHERE "job_id" = ANY($1)"#,
    /// );
    /// assert_eq!(fetch.params(), [Param::Ids]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that acknowledges a row: it deletes the row, or sets `processed_at` when the
    /// table has that column.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .processed_at(Column::new("processed_at"));
    ///
    /// let ack = Postgres.ack(&JOBS)?;
    /// assert_eq!(ack.sql(), r#"UPDATE "jobs" SET "processed_at" = $1 WHERE "job_id" = $2"#);
    /// assert_eq!(ack.params(), [Param::Now, Param::Id]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that releases a row for another attempt at once, or `None` when releasing
    /// the row needs no statement.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).attempt(Column::new("attempt"));
    ///
    /// // In the row lock form a retry counts the attempt; the rollback releases the row.
    /// let retry = Postgres.retry(&JOBS)?.map(|statement| statement.sql().to_owned());
    /// assert_eq!(
    ///     retry.as_deref(),
    ///     Some(r#"UPDATE "jobs" SET "attempt" = "attempt" + 1 WHERE "job_id" = $1"#),
    /// );
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError>;

    /// The statement that releases a row for another attempt after a delay, bound as
    /// [`Param::RetryAfter`](crate::Param::RetryAfter).
    ///
    /// # Errors
    ///
    /// [`StatementError::MissingRole`] when the table has no `retry_after` column;
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .retry_after(Column::new("retry_after"));
    ///
    /// let retry_after = Postgres.retry_after(&JOBS)?;
    /// assert_eq!(
    ///     retry_after.sql(),
    ///     r#"UPDATE "jobs" SET "retry_after" = $1 WHERE "job_id" = $2"#,
    /// );
    /// assert_eq!(retry_after.params(), [Param::RetryAfter, Param::Id]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that drops a row: it deletes the row, or sets `processed_at` when the table
    /// has that column.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
    ///
    /// let discard = Postgres.discard(&JOBS)?;
    /// assert_eq!(discard.sql(), r#"DELETE FROM "jobs" WHERE "job_id" = $1"#);
    /// assert_eq!(discard.params(), [Param::Id]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that moves a row whose attempts are spent to another group, bound as
    /// [`Param::Destination`](crate::Param::Destination).
    ///
    /// # Errors
    ///
    /// [`StatementError::MissingRole`] when the table has no `group` column;
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).group(Column::new("name"));
    ///
    /// let dead_letter = Postgres.dead_letter_group(&JOBS)?;
    /// assert_eq!(dead_letter.sql(), r#"UPDATE "jobs" SET "name" = $1 WHERE "job_id" = $2"#);
    /// assert_eq!(dead_letter.params(), [Param::Destination, Param::Id]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statements that move a row whose attempts are spent to `target`, a table with the same
    /// columns; they run in one transaction.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::error::Error;
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Postgres, TableName, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
    ///
    /// let target = TableName::parse("archive.jobs_dead")?;
    /// let moves = Postgres.dead_letter_table(&JOBS, target)?;
    /// assert_eq!(
    ///     moves[0].sql(),
    ///     r#"WITH moved AS (DELETE FROM "jobs" WHERE "job_id" = $1 RETURNING "job_id", "payload") INSERT INTO "archive"."jobs_dead" ("job_id", "payload") SELECT "job_id", "payload" FROM moved"#,
    /// );
    /// # }
    /// # Ok::<(), Box<dyn Error>>(())
    /// ```
    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError>;
}
