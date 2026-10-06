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
/// writes a lease and the lease's extension. A dialect implements the traits of the forms its
/// database serves, and a table in another form does not compile against it. A dialect of the
/// service's own does the same: it implements this trait, then the trait of each form it builds,
/// with every statement its own or delegated to a built-in dialect it wraps.
///
/// The settlements here serve every form the dialect builds. In the lease form each of them names
/// the row and the delivery's ownership token ([`Param::Held`](crate::Param::Held)), so a delivery
/// whose lease ran out, and whose row another claim took, changes nothing.
///
/// A dialect also opens transactions: [`begin`](Self::begin) gives the statement that opens one
/// at a table's isolation level or SQLite mode, and [`savepoint`](Self::savepoint) and
/// [`rollback_to_savepoint`](Self::rollback_to_savepoint) mark where a handler's writes start
/// and discard them.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{
///     Column, Dialect, Form, Postgres, Statement, StatementError, TableSpec,
/// };
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
///
/// // The settlements a subscription prepares when it starts, whatever its form.
/// fn settlements(
///     dialect: &dyn Dialect,
///     spec: &TableSpec<'_>,
/// ) -> Result<Vec<Statement>, StatementError> {
///     Ok(vec![dialect.ack(spec)?, dialect.discard(spec)?])
/// }
///
/// let statements = settlements(&Postgres, &JOBS)?;
/// assert_eq!(statements[0].sql(), r#"DELETE FROM "jobs" WHERE "job_id" = $1"#);
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
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
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
    /// In the lease form it clears the lease, and the attempt stays as the claim counted it.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
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
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
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
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
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
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
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
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id").generated(), Form::RowLock)
    ///         .payload(Column::new("payload"));
    ///
    /// // The database fills the id; the service writes the payload.
    /// let insert = Postgres.insert(&JOBS)?;
    /// assert_eq!(insert.sql(), r#"INSERT INTO "jobs" ("payload") VALUES ($1)"#);
    /// assert_eq!(insert.params(), [Param::Column(1)]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The query that reads the server's version as one text column, or `None` when the
    /// dialect's statements run on every version of its server.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Postgres, StatementError, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // What a broker runs once per subscription, before it prepares the statements.
    /// fn check(
    ///     dialect: &dyn Dialect,
    ///     spec: &TableSpec<'_>,
    ///     mut query: impl FnMut(&str) -> String,
    /// ) -> Result<(), StatementError> {
    ///     match dialect.server_version() {
    ///         Some(sql) => dialect.check_server(spec, &query(sql)),
    ///         None => Ok(()),
    ///     }
    /// }
    ///
    /// // Postgres asks its server nothing.
    /// let mut asked = Vec::new();
    /// check(&Postgres, &JOBS, |sql| {
    ///     asked.push(sql.to_owned());
    ///     "17.2".to_owned()
    /// })?;
    /// assert!(asked.is_empty());
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
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
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // A startup check of the version the server reported; a refusal stops the subscription
    /// // and names it.
    /// let checked = Postgres
    ///     .check_server(&JOBS, "17.2")
    ///     .map_err(|refused| format!("subscription `emails`: {refused}"));
    /// assert_eq!(checked, Ok(()));
    /// # }
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
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "mysql"))] {
    /// use ruststream_sqlx_dialect::{
    ///     Column, Dialect, Form, Isolation, MySql, Postgres, StatementError, TableSpec,
    /// };
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .isolation(Isolation::Serializable);
    ///
    /// // What a broker sends to open a claim's transaction.
    /// fn opening(
    ///     dialect: &dyn Dialect,
    ///     spec: &TableSpec<'_>,
    /// ) -> Result<&'static str, StatementError> {
    ///     Ok(dialect.begin(spec.opening())?.unwrap_or("BEGIN"))
    /// }
    ///
    /// assert_eq!(opening(&Postgres, &JOBS)?, "BEGIN ISOLATION LEVEL SERIALIZABLE");
    /// assert_eq!(
    ///     opening(&MySql, &JOBS)?,
    ///     "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION",
    /// );
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
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
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Dialect, Postgres};
    ///
    /// // A delivery whose handler writes in the claim's transaction, then retries: the writes
    /// // go, the claim and the count of the attempt stay.
    /// let retried = [
    ///     Postgres.savepoint(),
    ///     r#"INSERT INTO "audit" ("note") VALUES ('sent')"#,
    ///     Postgres.rollback_to_savepoint(),
    ///     r#"UPDATE "jobs" SET "attempt" = "attempt" + 1 WHERE "job_id" = $1"#,
    ///     "COMMIT",
    /// ];
    /// assert_eq!(retried[0], "SAVEPOINT ruststream_claim");
    /// # }
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
    /// # #[cfg(feature = "mysql")] {
    /// use ruststream_sqlx_dialect::{Dialect, MySql};
    ///
    /// // A delivery whose handler wrote in the claim's transaction and was dropped: its writes go,
    /// // and the row is finished.
    /// let dropped = [
    ///     MySql.rollback_to_savepoint(),
    ///     "DELETE FROM `jobs` WHERE `job_id` = ?",
    ///     "COMMIT",
    /// ];
    /// assert_eq!(dropped[0], "ROLLBACK TO SAVEPOINT ruststream_claim");
    /// # }
    /// ```
    fn rollback_to_savepoint(&self) -> &'static str {
        "ROLLBACK TO SAVEPOINT ruststream_claim"
    }
}
