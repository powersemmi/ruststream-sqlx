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
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     Advisory, ClaimShape, Dialect, Postgres, Statement, StatementError, TableSpec,
/// };
///
/// /// Postgres, with statements of the service's own in its `Dialect` impl.
/// #[derive(Debug)]
/// pub struct Audited;
///
/// // A table with `advisory_lock` mounts on `Audited`.
/// impl Advisory for Audited {
///     fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         Postgres.advisory_claim(spec)
///     }
///
///     fn lock(&self) -> Option<Statement> {
///         Postgres.lock()
///     }
///
///     fn unlock(&self) -> Option<Statement> {
///         Postgres.unlock()
///     }
///
///     fn take(
///         &self,
///         spec: &TableSpec<'_>,
///         shape: ClaimShape,
///     ) -> Result<Vec<Statement>, StatementError> {
///         Postgres.take(spec, shape)
///     }
/// }
/// # impl Dialect for Audited {
/// #     fn name(&self) -> &'static str { "audited" }
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     Advisory, ClaimShape, Dialect, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// Postgres, with the service's advisory locks in a key space of their own, apart from the
    /// /// locks other programs on the database take.
    /// #[derive(Debug)]
    /// pub struct Namespaced;
    ///
    /// impl Advisory for Namespaced {
    ///     // The candidates of `email_jobs` (`advisory_lock = "email-{job_id}"`), probed in the
    ///     // key
    ///     // space the lock takes.
    ///     fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.table() != "email_jobs" {
    ///             return Postgres.advisory_claim(spec);
    ///         }
    ///         Ok(Statement::new(
    ///             r#"SELECT "job_id", "__lock" FROM (SELECT "job_id", concat('email-', "job_id") AS "__lock" FROM "email_jobs" ORDER BY "job_id" OFFSET 0) AS __candidates WHERE pg_try_advisory_xact_lock_shared(7, hashtext("__lock")) LIMIT $1"#,
    ///             [Param::Limit],
    ///         ))
    ///     }
    /// #     fn lock(&self) -> Option<Statement> { Postgres.lock() }
    /// #     fn unlock(&self) -> Option<Statement> { Postgres.unlock() }
    /// #     fn take(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Vec<Statement>, StatementError> { Postgres.take(spec, shape) }
    /// }
    /// # impl Dialect for Namespaced {
    /// #     fn name(&self) -> &'static str { "namespaced" }
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
    fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// Tries the lock on [`Param::Key`](crate::Param::Key) for the session, without waiting: one
    /// row whose first column is a 64-bit integer, nonzero when the lock was taken. `None` when the
    /// process keeps the locks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     Advisory, ClaimShape, Dialect, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// Postgres, with the service's advisory locks in a key space of their own, apart from the
    /// /// locks other programs on the database take.
    /// #[derive(Debug)]
    /// pub struct Namespaced;
    ///
    /// impl Advisory for Namespaced {
    ///     // The session's lock on a key, in key space 7.
    ///     fn lock(&self) -> Option<Statement> {
    ///         Some(Statement::new(
    ///             "SELECT pg_try_advisory_lock(7, hashtext($1))::int::bigint",
    ///             [Param::Key],
    ///         ))
    ///     }
    /// #     fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.advisory_claim(spec) }
    /// #     fn unlock(&self) -> Option<Statement> { Postgres.unlock() }
    /// #     fn take(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Vec<Statement>, StatementError> { Postgres.take(spec, shape) }
    /// }
    /// # impl Dialect for Namespaced {
    /// #     fn name(&self) -> &'static str { "namespaced" }
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
    fn lock(&self) -> Option<Statement>;

    /// Releases the lock on [`Param::Key`](crate::Param::Key): one row whose first column is a
    /// 64-bit integer, nonzero when the session held it. `None` when the process keeps the locks.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     Advisory, ClaimShape, Dialect, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// Postgres, with the service's advisory locks in a key space of their own, apart from the
    /// /// locks other programs on the database take.
    /// #[derive(Debug)]
    /// pub struct Namespaced;
    ///
    /// impl Advisory for Namespaced {
    ///     // The release of the session's lock on a key, in key space 7.
    ///     fn unlock(&self) -> Option<Statement> {
    ///         Some(Statement::new(
    ///             "SELECT pg_advisory_unlock(7, hashtext($1))::int::bigint",
    ///             [Param::Key],
    ///         ))
    ///     }
    /// #     fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.advisory_claim(spec) }
    /// #     fn lock(&self) -> Option<Statement> { Postgres.lock() }
    /// #     fn take(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Vec<Statement>, StatementError> { Postgres.take(spec, shape) }
    /// }
    /// # impl Dialect for Namespaced {
    /// #     fn name(&self) -> &'static str { "namespaced" }
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     Advisory, ClaimShape, Dialect, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    ///
    /// #[derive(Debug)]
    /// pub struct Taken;
    ///
    /// impl Advisory for Taken {
    ///     // Taking an email also records when. `email_jobs` keeps no finished rows and no delays,
    ///     // so
    ///     // a row that is still there is claimable; it returns as it was before the count.
    ///     fn take(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         shape: ClaimShape,
    ///     ) -> Result<Vec<Statement>, StatementError> {
    ///         if spec.table() != "email_jobs" {
    ///             return Postgres.take(spec, shape);
    ///         }
    ///         let returned = match shape {
    ///             ClaimShape::Rows => r#""job_id", "attempt" - 1 AS "attempt", "payload""#,
    ///             ClaimShape::Ids => r#""job_id""#,
    ///             ClaimShape::Roles => r#""job_id" AS "id", "attempt" - 1 AS "attempt", "payload""#,
    ///         };
    ///         Ok(vec![Statement::new(
    ///             format!(
    ///                 r#"UPDATE "email_jobs" SET "attempt" = "attempt" + 1, "taken_at" = $1 WHERE "job_id" = $2 RETURNING {returned}"#
    ///             ),
    ///             [Param::Now, Param::Id],
    ///         )])
    ///     }
    /// #     fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.advisory_claim(spec) }
    /// #     fn lock(&self) -> Option<Statement> { Postgres.lock() }
    /// #     fn unlock(&self) -> Option<Statement> { Postgres.unlock() }
    /// }
    /// # impl Dialect for Taken {
    /// #     fn name(&self) -> &'static str { "taken" }
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
    fn take(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError>;
}
