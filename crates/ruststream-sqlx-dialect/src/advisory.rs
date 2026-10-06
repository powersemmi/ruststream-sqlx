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
/// ```
/// # #[cfg(all(feature = "postgres", feature = "sqlite"))] {
/// use ruststream_sqlx_dialect::{
///     Advisory, ClaimShape, Column, Form, KeyPart, Postgres, Sqlite, Statement, StatementError,
///     TableSpec,
/// };
///
/// const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")];
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(KEY))
///     .attempt(Column::new("attempt"))
///     .payload(Column::new("payload"));
///
/// // What a subscription to an advisory table prepares when it starts.
/// fn advising(
///     dialect: &dyn Advisory,
///     spec: &TableSpec<'_>,
/// ) -> Result<Vec<Statement>, StatementError> {
///     let mut statements = vec![dialect.advisory_claim(spec)?];
///     statements.extend(dialect.lock());
///     statements.extend(dialect.unlock());
///     statements.extend(dialect.take(spec, ClaimShape::Rows)?);
///     Ok(statements)
/// }
///
/// // Postgres: the candidates, the lock, the unlock and a take of one statement. SQLite keeps
/// // its locks in the process: the candidates and the take.
/// assert_eq!(advising(&Postgres, &JOBS)?.len(), 4);
/// assert_eq!(advising(&Sqlite, &JOBS)?.len(), 2);
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
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
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Advisory, Column, Form, KeyPart, Param, Postgres, TableSpec};
    ///
    /// const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")];
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(KEY))
    ///         .group(Column::new("name"))
    ///         .payload(Column::new("payload"));
    ///
    /// // Postgres probes each key in claim order, and only as far as the limit reaches.
    /// let claim = Postgres.advisory_claim(&JOBS)?;
    /// assert_eq!(
    ///     claim.sql(),
    ///     r#"SELECT "job_id", "__lock" FROM (SELECT "job_id", concat('jobs-', "job_id") AS "__lock" FROM "jobs" WHERE "name" = $1 ORDER BY "job_id" OFFSET 0) AS __candidates WHERE pg_try_advisory_xact_lock_shared(hashtextextended("__lock", 0)) LIMIT $2"#,
    /// );
    /// assert_eq!(claim.params(), [Param::Group, Param::Limit]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// Tries the lock on [`Param::Key`](crate::Param::Key) for the session, without waiting: one
    /// row whose first column is a 64-bit integer, nonzero when the lock was taken. `None` when the
    /// process keeps the locks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "mysql", feature = "sqlite"))] {
    /// use ruststream_sqlx_dialect::{Advisory, MySql, Param, Sqlite, Statement};
    ///
    /// // Where a broker holds the keys in work: in the database's locks, or in the process.
    /// fn held_by_the_database(dialect: &dyn Advisory) -> bool {
    ///     dialect.lock().is_some()
    /// }
    ///
    /// let lock = MySql.lock();
    /// assert_eq!(
    ///     lock.as_ref().map(Statement::sql),
    ///     Some("SELECT CAST(COALESCE(GET_LOCK(?, 0), 0) AS SIGNED)"),
    /// );
    /// assert_eq!(lock.as_ref().map(Statement::params), Some([Param::Key].as_slice()));
    /// assert!(!held_by_the_database(&Sqlite));
    /// # }
    /// ```
    fn lock(&self) -> Option<Statement>;

    /// Releases the lock on [`Param::Key`](crate::Param::Key): one row whose first column is a
    /// 64-bit integer, nonzero when the session held it. `None` when the process keeps the locks.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::error::Error;
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Advisory, Param, Postgres};
    ///
    /// // A settlement runs its step, then this, on the session that holds the key.
    /// let unlock = Postgres.unlock().ok_or("postgres keeps its locks in the database")?;
    /// assert_eq!(
    ///     unlock.sql(),
    ///     "SELECT pg_advisory_unlock(hashtextextended($1, 0))::int::bigint",
    /// );
    /// assert_eq!(unlock.params(), [Param::Key]);
    /// # }
    /// # Ok::<(), Box<dyn Error>>(())
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
    /// # #[cfg(feature = "mysql")] {
    /// use ruststream_sqlx_dialect::{
    ///     Advisory, ClaimShape, Column, Form, KeyPart, MySql, Param, Statement, TableSpec,
    /// };
    ///
    /// const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")];
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(KEY))
    ///         .attempt(Column::new("attempt"))
    ///         .payload(Column::new("payload"));
    ///
    /// // An update returns no rows on MySQL, so the take counts the attempt, then reads the row.
    /// let take = MySql.take(&JOBS, ClaimShape::Rows)?;
    /// assert_eq!(
    ///     take.iter().map(Statement::sql).collect::<Vec<_>>(),
    ///     [
    ///         "UPDATE `jobs` SET `attempt` = `attempt` + 1 WHERE `job_id` = ?",
    ///         "SELECT `job_id`, `attempt` - 1 AS `attempt`, `payload` FROM `jobs` WHERE `job_id` = ?",
    ///     ],
    /// );
    /// assert_eq!(take[1].params(), [Param::Id]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn take(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError>;
}
