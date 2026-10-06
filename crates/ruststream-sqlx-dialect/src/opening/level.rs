/// [`Isolation::ReadUncommitted`](crate::Isolation::ReadUncommitted) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "mysql")] {
/// use ruststream_sqlx_dialect::{Isolation, MySql, Opening, Opens, StatementError, level};
///
/// // A report that tolerates reading what other transactions have not committed yet.
/// fn dirty<D: Opens<level::ReadUncommitted>>(
///     dialect: &D,
/// ) -> Result<Option<&'static str>, StatementError> {
///     dialect.begin(Opening::Isolation(Isolation::ReadUncommitted))
/// }
///
/// assert!(dirty(&MySql)?.is_some_and(|begin| begin.contains("READ UNCOMMITTED")));
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ReadUncommitted;

/// [`Isolation::ReadCommitted`](crate::Isolation::ReadCommitted) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{Isolation, Opening, Opens, Postgres, StatementError, level};
///
/// // What a subscription to a table at READ COMMITTED opens its claims with.
/// fn read_committed<D: Opens<level::ReadCommitted>>(
///     dialect: &D,
/// ) -> Result<Option<&'static str>, StatementError> {
///     dialect.begin(Opening::Isolation(Isolation::ReadCommitted))
/// }
///
/// assert_eq!(
///     read_committed(&Postgres)?,
///     Some("BEGIN ISOLATION LEVEL READ COMMITTED"),
/// );
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ReadCommitted;

/// [`Isolation::RepeatableRead`](crate::Isolation::RepeatableRead) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "mysql")] {
/// use ruststream_sqlx_dialect::{Isolation, MySql, Opening, Opens, StatementError, level};
///
/// // What a subscription to a table at REPEATABLE READ opens its claims with.
/// fn repeatable<D: Opens<level::RepeatableRead>>(
///     dialect: &D,
/// ) -> Result<Option<&'static str>, StatementError> {
///     dialect.begin(Opening::Isolation(Isolation::RepeatableRead))
/// }
///
/// assert_eq!(
///     repeatable(&MySql)?,
///     Some("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION"),
/// );
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RepeatableRead;

/// [`Isolation::Serializable`](crate::Isolation::Serializable) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{Isolation, Opening, Opens, Postgres, StatementError, level};
///
/// // What a subscription to a table at SERIALIZABLE opens its claims with.
/// fn serializable<D: Opens<level::Serializable>>(
///     dialect: &D,
/// ) -> Result<Option<&'static str>, StatementError> {
///     dialect.begin(Opening::Isolation(Isolation::Serializable))
/// }
///
/// assert_eq!(
///     serializable(&Postgres)?,
///     Some("BEGIN ISOLATION LEVEL SERIALIZABLE"),
/// );
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Serializable;

/// [`Mode::Deferred`](crate::Mode::Deferred) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "sqlite")] {
/// use ruststream_sqlx_dialect::{Mode, Opening, Opens, Sqlite, StatementError, level};
///
/// // What a subscription to a table in deferred mode opens its transactions with.
/// fn deferred<D: Opens<level::Deferred>>(
///     dialect: &D,
/// ) -> Result<Option<&'static str>, StatementError> {
///     dialect.begin(Opening::Mode(Mode::Deferred))
/// }
///
/// assert_eq!(deferred(&Sqlite)?, Some("BEGIN DEFERRED"));
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Deferred;

/// [`Mode::Immediate`](crate::Mode::Immediate) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "sqlite")] {
/// use ruststream_sqlx_dialect::{Mode, Opening, Opens, Sqlite, StatementError, level};
///
/// // What a subscription to a table in immediate mode opens its transactions with.
/// fn immediate<D: Opens<level::Immediate>>(
///     dialect: &D,
/// ) -> Result<Option<&'static str>, StatementError> {
///     dialect.begin(Opening::Mode(Mode::Immediate))
/// }
///
/// assert_eq!(immediate(&Sqlite)?, Some("BEGIN IMMEDIATE"));
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Immediate;

/// [`Mode::Exclusive`](crate::Mode::Exclusive) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "sqlite")] {
/// use ruststream_sqlx_dialect::{Mode, Opening, Opens, Sqlite, StatementError, level};
///
/// // What a subscription to a table in exclusive mode opens its transactions with.
/// fn exclusive<D: Opens<level::Exclusive>>(
///     dialect: &D,
/// ) -> Result<Option<&'static str>, StatementError> {
///     dialect.begin(Opening::Mode(Mode::Exclusive))
/// }
///
/// assert_eq!(exclusive(&Sqlite)?, Some("BEGIN EXCLUSIVE"));
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Exclusive;
