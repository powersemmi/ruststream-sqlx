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
/// A dialect of the service's own that wraps SQLite keeps its lease form whole:
///
/// ```
/// # #[cfg(feature = "sqlite")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Dialect, Lease, Sqlite, Statement, StatementError, TableSpec,
/// };
///
/// /// SQLite, with statements of the service's own in its `Dialect` impl.
/// #[derive(Debug)]
/// pub struct Audited;
///
/// impl Lease for Audited {
///     fn lease_claim(
///         &self,
///         spec: &TableSpec<'_>,
///         shape: ClaimShape,
///     ) -> Result<Statement, StatementError> {
///         Sqlite.lease_claim(spec, shape)
///     }
///
///     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         Sqlite.extend(spec)
///     }
///
///     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         Sqlite.stamp(spec)
///     }
///
///     // The answers come from the dialect the statements come from.
///     fn claim_writes_lease(&self) -> bool {
///         Sqlite.claim_writes_lease()
///     }
///
///     fn claim_counts_attempt(&self, spec: &TableSpec<'_>) -> bool {
///         Sqlite.claim_counts_attempt(spec)
///     }
///
///     fn begin_lease_claim(&self) -> Option<&'static str> {
///         Sqlite.begin_lease_claim()
///     }
/// }
/// # impl Dialect for Audited {
/// #     fn name(&self) -> &'static str { "audited" }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { Sqlite.quote_into(ident, out); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Sqlite.placeholder_into(index, out); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.fetch(spec) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.ack(spec) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Sqlite.retry(spec) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.retry_after(spec) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.discard(spec) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.dead_letter_group(spec) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Sqlite.dead_letter_table(spec, target) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.insert(spec) }
/// # }
/// # }
/// # fn main() {}
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    ///
    /// #[derive(Debug)]
    /// pub struct Returning;
    ///
    /// impl Lease for Returning {
    ///     // One update takes the rows of `email_jobs`, writes their lease, counts the attempt and
    ///     // returns them as it left them.
    ///     fn lease_claim(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         if spec.table() != "email_jobs" {
    ///             return Postgres.lease_claim(spec, shape);
    ///         }
    ///         let returned = match shape {
    ///             ClaimShape::Rows => r#""job_id", "attempt", "locked_until", "payload""#,
    ///             ClaimShape::Ids => r#""job_id""#,
    ///             ClaimShape::Roles => r#""job_id" AS "id", "attempt", "payload""#,
    ///         };
    ///         Ok(Statement::new(
    ///             format!(
    ///                 r#"UPDATE "email_jobs" SET "locked_until" = $1, "attempt" = "attempt" + 1 WHERE "job_id" IN (SELECT "job_id" FROM "email_jobs" WHERE "locked_until" IS NULL OR "locked_until" <= $2 ORDER BY "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED) RETURNING {returned}"#
    ///             ),
    ///             [Param::Lease, Param::LeaseNow, Param::Limit],
    ///         ))
    ///     }
    ///
    ///     // So the rows of `email_jobs` carry the attempt the claim counted.
    ///     fn claim_counts_attempt(&self, spec: &TableSpec<'_>) -> bool {
    ///         spec.table() == "email_jobs"
    ///     }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.extend(spec) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.stamp(spec) }
    /// }
    /// # impl Dialect for Returning {
    /// #     fn name(&self) -> &'static str { "returning" }
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    ///
    /// #[derive(Debug)]
    /// pub struct Heartbeat;
    ///
    /// impl Lease for Heartbeat {
    ///     // An extension of an email's lease also records when its handler was last seen. It
    ///     // changes
    ///     // the row only while the row holds the delivery's lease.
    ///     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.table() == "email_jobs" {
    ///             return Ok(Statement::new(
    ///                 r#"UPDATE "email_jobs" SET "locked_until" = $1, "seen_at" = $2 WHERE "job_id" = $3 AND "locked_until" = $4"#,
    ///                 [Param::Lease, Param::Now, Param::Id, Param::Held],
    ///             ));
    ///         }
    ///         Postgres.extend(spec)
    ///     }
    /// #     fn lease_claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> { Postgres.lease_claim(spec, shape) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.stamp(spec) }
    /// }
    /// # impl Dialect for Heartbeat {
    /// #     fn name(&self) -> &'static str { "heartbeat" }
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
    /// # #[cfg(feature = "mysql")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, MySql, Param, Statement, StatementError, TableSpec,
    /// };
    ///
    /// #[derive(Debug)]
    /// pub struct Stamped;
    ///
    /// impl Lease for Stamped {
    ///     // MySQL's claim only selects, so the broker stamps each row it took.
    ///     fn claim_writes_lease(&self) -> bool {
    ///         MySql.claim_writes_lease()
    ///     }
    ///
    ///     fn begin_lease_claim(&self) -> Option<&'static str> {
    ///         MySql.begin_lease_claim()
    ///     }
    ///
    ///     // The stamp of an email also records when it was claimed.
    ///     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.table() == "email_jobs" {
    ///             return Ok(Statement::new(
    ///                 "UPDATE `email_jobs` SET `locked_until` = ?, `attempt` = `attempt` + 1, \
    ///                  `claimed_at` = UTC_TIMESTAMP(6) WHERE `job_id` = ? \
    ///                  AND (`locked_until` IS NULL OR `locked_until` <= ?)",
    ///                 [Param::Lease, Param::Id, Param::LeaseNow],
    ///             ));
    ///         }
    ///         MySql.stamp(spec)
    ///     }
    /// #     fn lease_claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> { MySql.lease_claim(spec, shape) }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.extend(spec) }
    /// }
    /// # impl Dialect for Stamped {
    /// #     fn name(&self) -> &'static str { "stamped" }
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
    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// Whether the lease claim writes the lease itself. A dialect whose lease claim only selects
    /// the rows answers `false`, and the claim's transaction then runs [`stamp`](Self::stamp) for
    /// each claimed row before it commits.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "mysql")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, MySql, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// MySQL, with statements of the service's own in its `Dialect` impl.
    /// #[derive(Debug)]
    /// pub struct Audited;
    ///
    /// impl Lease for Audited {
    ///     // MySQL's claim only selects. A dialect that keeps the claim answers as MySQL does, so
    ///     // the
    ///     // broker stamps each row the claim took before the claim's transaction commits.
    ///     fn claim_writes_lease(&self) -> bool {
    ///         MySql.claim_writes_lease()
    ///     }
    /// #     fn lease_claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> { MySql.lease_claim(spec, shape) }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.extend(spec) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.stamp(spec) }
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Param, Postgres, Statement, StatementError, TableSpec,
    /// };
    ///
    /// #[derive(Debug)]
    /// pub struct Returning;
    ///
    /// impl Lease for Returning {
    ///     // One update takes the rows of `email_jobs`, writes their lease, counts the attempt and
    ///     // returns them as it left them.
    ///     fn lease_claim(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         if spec.table() != "email_jobs" {
    ///             return Postgres.lease_claim(spec, shape);
    ///         }
    ///         let returned = match shape {
    ///             ClaimShape::Rows => r#""job_id", "attempt", "locked_until", "payload""#,
    ///             ClaimShape::Ids => r#""job_id""#,
    ///             ClaimShape::Roles => r#""job_id" AS "id", "attempt", "payload""#,
    ///         };
    ///         Ok(Statement::new(
    ///             format!(
    ///                 r#"UPDATE "email_jobs" SET "locked_until" = $1, "attempt" = "attempt" + 1 WHERE "job_id" IN (SELECT "job_id" FROM "email_jobs" WHERE "locked_until" IS NULL OR "locked_until" <= $2 ORDER BY "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED) RETURNING {returned}"#
    ///             ),
    ///             [Param::Lease, Param::LeaseNow, Param::Limit],
    ///         ))
    ///     }
    ///
    ///     // So the rows of `email_jobs` carry the attempt the claim counted.
    ///     fn claim_counts_attempt(&self, spec: &TableSpec<'_>) -> bool {
    ///         spec.table() == "email_jobs"
    ///     }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.extend(spec) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.stamp(spec) }
    /// }
    /// # impl Dialect for Returning {
    /// #     fn name(&self) -> &'static str { "returning" }
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
    /// # #[cfg(feature = "sqlite")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Lease, Sqlite, Statement, StatementError, TableSpec,
    /// };
    ///
    /// /// SQLite, with statements of the service's own in its `Dialect` impl.
    /// #[derive(Debug)]
    /// pub struct Audited;
    ///
    /// impl Lease for Audited {
    ///     // A table with a claim of its own (`custom(claim)`) selects its rows, then stamps them,
    ///     // in
    ///     // the transaction this opens: `BEGIN IMMEDIATE` takes SQLite's write lock before the
    ///     // select.
    ///     fn begin_lease_claim(&self) -> Option<&'static str> {
    ///         Sqlite.begin_lease_claim()
    ///     }
    /// #     fn lease_claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> { Sqlite.lease_claim(spec, shape) }
    /// #     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.extend(spec) }
    /// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.stamp(spec) }
    /// }
    /// # impl Dialect for Audited {
    /// #     fn name(&self) -> &'static str { "audited" }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Sqlite.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Sqlite.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Sqlite.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Sqlite.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Sqlite.insert(spec) }
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    fn begin_lease_claim(&self) -> Option<&'static str> {
        None
    }
}
