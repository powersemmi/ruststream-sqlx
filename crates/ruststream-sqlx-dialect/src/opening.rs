//! What a table's transactions open at: an isolation level, a SQLite mode, or the database's
//! default; and the levels a dialect opens, as types.

use crate::dialect::Dialect;
use crate::statement::StatementError;

/// The isolation level a transaction opens at, on Postgres, MySQL and MariaDB.
///
/// A table names one ([`TableSpec::isolation`](crate::TableSpec::isolation)), and the dialect's
/// [`begin`](Dialect::begin) opens the table's transactions at it. A database that runs a level
/// as a stronger one does not keep it, so its dialect refuses it: Postgres refuses
/// [`ReadUncommitted`](Self::ReadUncommitted).
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{Column, Dialect, Form, Isolation, Postgres, TableSpec};
///
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
///     .isolation(Isolation::RepeatableRead);
///
/// // The statement a claim of `jobs` opens its transaction with.
/// let begin = Postgres.begin(JOBS.opening())?;
/// assert_eq!(begin, Some("BEGIN ISOLATION LEVEL REPEATABLE READ"));
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Isolation {
    /// A read may see what other transactions wrote and have not committed.
    ReadUncommitted,
    /// Each statement sees what was committed before it began.
    ReadCommitted,
    /// Every statement sees what was committed before the transaction's first one.
    RepeatableRead,
    /// Transactions take effect as if they ran one after another.
    Serializable,
}

impl Isolation {
    /// The level as SQL names it: `READ COMMITTED`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::Isolation;
    ///
    /// // The line a service logs when a subscription opens its claims at a level.
    /// let level = Isolation::RepeatableRead;
    /// let line = format!("claims of `jobs` open at {}", level.sql());
    /// assert_eq!(line, "claims of `jobs` open at REPEATABLE READ");
    /// ```
    #[must_use]
    pub const fn sql(self) -> &'static str {
        match self {
            Self::ReadUncommitted => "READ UNCOMMITTED",
            Self::ReadCommitted => "READ COMMITTED",
            Self::RepeatableRead => "REPEATABLE READ",
            Self::Serializable => "SERIALIZABLE",
        }
    }

    /// The level as a table names it: `read_committed`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::Isolation;
    ///
    /// // A message that points at what the table declares.
    /// let level = Isolation::RepeatableRead;
    /// let hint = format!("the table declares `isolation = {}`", level.attribute());
    /// assert_eq!(hint, "the table declares `isolation = repeatable_read`");
    /// ```
    #[must_use]
    pub const fn attribute(self) -> &'static str {
        match self {
            Self::ReadUncommitted => "read_uncommitted",
            Self::ReadCommitted => "read_committed",
            Self::RepeatableRead => "repeatable_read",
            Self::Serializable => "serializable",
        }
    }
}

/// How a SQLite transaction takes the database's write lock.
///
/// SQLite runs every transaction serializable, with one writer at a time, so a table on it names
/// the moment its transactions take the write lock ([`TableSpec::mode`](crate::TableSpec::mode))
/// instead of a level.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "sqlite")] {
/// use ruststream_sqlx_dialect::{Column, Dialect, Form, Mode, Sqlite, TableSpec};
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
///         .mode(Mode::Immediate);
///
/// // The statement a transaction of `jobs` opens with: it takes the write lock at once.
/// let begin = Sqlite.begin(JOBS.opening())?;
/// assert_eq!(begin, Some("BEGIN IMMEDIATE"));
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Mode {
    /// The transaction takes no lock until it first reads or writes, as a plain `BEGIN` does.
    Deferred,
    /// The transaction takes the write lock when it begins, so no other writer comes between its
    /// reads and its writes.
    Immediate,
    /// The transaction takes the write lock when it begins and, outside WAL mode, keeps every
    /// other connection from reading until it ends.
    Exclusive,
}

impl Mode {
    /// The mode as SQL names it: `IMMEDIATE`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::Mode;
    ///
    /// // A transaction a service opens on its own SQLite connection, in the table's mode.
    /// let begin = format!("BEGIN {}", Mode::Exclusive.sql());
    /// assert_eq!(begin, "BEGIN EXCLUSIVE");
    /// ```
    #[must_use]
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Deferred => "DEFERRED",
            Self::Immediate => "IMMEDIATE",
            Self::Exclusive => "EXCLUSIVE",
        }
    }

    /// The mode as a table names it: `immediate`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::Mode;
    ///
    /// // A message that points at what the table declares.
    /// let hint = format!("the table declares `mode = {}`", Mode::Immediate.attribute());
    /// assert_eq!(hint, "the table declares `mode = immediate`");
    /// ```
    #[must_use]
    pub const fn attribute(self) -> &'static str {
        match self {
            Self::Deferred => "deferred",
            Self::Immediate => "immediate",
            Self::Exclusive => "exclusive",
        }
    }
}

/// What a table's transactions open at: the dialect's default, an isolation level, or a SQLite
/// mode.
///
/// A table description carries one ([`TableSpec::opening`](crate::TableSpec::opening)), and the
/// dialect's [`begin`](Dialect::begin) gives the statement that opens a transaction at it, or
/// refuses an opening its database lacks.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{Column, Form, Isolation, Opening, StatementError, TableSpec};
///
/// // The `begin` of a SQL Server dialect of the service's own: SERIALIZABLE beside the default.
/// fn begin(opening: Opening) -> Result<Option<&'static str>, StatementError> {
///     match opening {
///         Opening::Default => Ok(None),
///         Opening::Isolation(Isolation::Serializable) => Ok(Some(
///             "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION",
///         )),
///         other => Err(StatementError::UnsupportedOpening {
///             dialect: "mssql",
///             opening: other.name(),
///         }),
///     }
/// }
///
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
///     .isolation(Isolation::Serializable);
///
/// let opened = begin(JOBS.opening())?;
/// assert_eq!(
///     opened,
///     Some("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION"),
/// );
/// # Ok::<(), StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Opening {
    /// What the dialect opens a table's transactions at when the table names neither a level nor
    /// a mode: the database's own level on Postgres and SQLite, READ COMMITTED on MySQL and
    /// MariaDB.
    #[default]
    Default,
    /// An isolation level.
    Isolation(Isolation),
    /// A SQLite mode.
    Mode(Mode),
}

impl Opening {
    /// The opening as a message names it: `the database's default`, ``isolation `serializable` ``
    /// or ``mode `immediate` ``.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Mode, Opening, StatementError};
    ///
    /// // A SQL Server dialect of the service's own refuses a SQLite mode, naming it.
    /// let opening = Opening::Mode(Mode::Immediate);
    /// let refused = StatementError::UnsupportedOpening {
    ///     dialect: "mssql",
    ///     opening: opening.name(),
    /// };
    /// assert_eq!(
    ///     refused.to_string(),
    ///     "the mssql dialect opens no transaction at mode `immediate`",
    /// );
    /// ```
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Default => "the database's default",
            Self::Isolation(Isolation::ReadUncommitted) => "isolation `read_uncommitted`",
            Self::Isolation(Isolation::ReadCommitted) => "isolation `read_committed`",
            Self::Isolation(Isolation::RepeatableRead) => "isolation `repeatable_read`",
            Self::Isolation(Isolation::Serializable) => "isolation `serializable`",
            Self::Mode(Mode::Deferred) => "mode `deferred`",
            Self::Mode(Mode::Immediate) => "mode `immediate`",
            Self::Mode(Mode::Exclusive) => "mode `exclusive`",
        }
    }

    /// The refusal of the dialect named `dialect`, which opens no transaction at `self`.
    pub(crate) const fn refused(self, dialect: &'static str) -> StatementError {
        StatementError::UnsupportedOpening {
            dialect,
            opening: self.name(),
        }
    }
}

/// The isolation levels and SQLite modes as types: a table names one, and its dialect opens it
/// where it implements [`Opens`] for it.
///
/// A table that names a level carries it twice: as one of these types, which the bound on its
/// dialect checks when the service compiles, and as the [`Opening`] in its description, which
/// [`Dialect::begin`] turns into a statement.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "mysql")] {
/// use ruststream_sqlx_dialect::{Isolation, MySql, Opening, Opens, StatementError, level};
///
/// // What a subscription to a table at `Level` asks of its dialect.
/// fn begin_at<Level, D: Opens<Level>>(
///     dialect: &D,
///     opening: Opening,
/// ) -> Result<Option<&'static str>, StatementError> {
///     dialect.begin(opening)
/// }
///
/// let begin = begin_at::<level::ReadUncommitted, _>(
///     &MySql,
///     Opening::Isolation(Isolation::ReadUncommitted),
/// )?;
/// assert_eq!(
///     begin,
///     Some("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; START TRANSACTION"),
/// );
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
pub mod level {
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
}

/// A dialect that opens transactions at `Level`: an isolation level or a SQLite mode as one of
/// the [`level`] types, or `()`, which a table that names neither carries.
///
/// A subscription requires `Opens<Level>` of its table's dialect, so a table that names a level
/// its dialect lacks does not compile. Every dialect opens `()`. A dialect implements
/// `Opens<Level>` for each level whose [`Opening`] its [`begin`](Dialect::begin) opens:
/// [`Postgres`](crate::Postgres) READ COMMITTED, REPEATABLE READ and SERIALIZABLE;
/// [`MySql`](crate::MySql) those and READ UNCOMMITTED; [`Sqlite`](crate::Sqlite) its three
/// modes.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{Isolation, Opening, Opens, Postgres, StatementError, level};
///
/// // What a subscription to a table with `isolation = serializable` asks of its dialect.
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
///
/// SQLite opens no transaction at an isolation level, so the same subscription on SQLite does
/// not compile:
///
/// ```compile_fail
/// # #[cfg(not(feature = "sqlite"))]
/// # compile_error!("the example needs the sqlite feature");
/// # #[cfg(feature = "sqlite")]
/// # mod demo {
/// use ruststream_sqlx_dialect::{Isolation, Opening, Opens, Sqlite, StatementError, level};
///
/// fn serializable<D: Opens<level::Serializable>>(
///     dialect: &D,
/// ) -> Result<Option<&'static str>, StatementError> {
///     dialect.begin(Opening::Isolation(Isolation::Serializable))
/// }
///
/// fn subscribe() -> Result<Option<&'static str>, StatementError> {
///     serializable(&Sqlite)
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "the `{Self}` dialect opens no transaction at `{Level}`",
    label = "the table's `isolation` or `mode` names it",
    note = "Postgres opens `read_committed`, `repeatable_read` and `serializable`; MySQL and \
            MariaDB those and `read_uncommitted`; SQLite takes the modes `deferred`, \
            `immediate` and `exclusive`",
    note = "a dialect of the service's own opens a level by implementing `Opens<Level>` and \
            `Dialect::begin`"
)]
pub trait Opens<Level>: Dialect {}

impl<D: Dialect> Opens<()> for D {}

#[cfg(test)]
mod tests {
    use super::{Isolation, Mode, Opening};
    use crate::statement::StatementError;

    #[test]
    fn a_level_names_itself_in_sql_and_as_a_table_does() {
        let levels = [
            Isolation::ReadUncommitted,
            Isolation::ReadCommitted,
            Isolation::RepeatableRead,
            Isolation::Serializable,
        ];
        assert_eq!(
            levels.map(Isolation::sql),
            [
                "READ UNCOMMITTED",
                "READ COMMITTED",
                "REPEATABLE READ",
                "SERIALIZABLE"
            ]
        );
        assert_eq!(
            levels.map(Isolation::attribute),
            [
                "read_uncommitted",
                "read_committed",
                "repeatable_read",
                "serializable"
            ]
        );
    }

    #[test]
    fn a_mode_names_itself_in_sql_and_as_a_table_does() {
        let modes = [Mode::Deferred, Mode::Immediate, Mode::Exclusive];
        assert_eq!(modes.map(Mode::sql), ["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]);
        assert_eq!(
            modes.map(Mode::attribute),
            ["deferred", "immediate", "exclusive"]
        );
    }

    #[test]
    fn an_opening_names_itself_for_messages() {
        let openings = [
            Opening::default(),
            Opening::Isolation(Isolation::ReadUncommitted),
            Opening::Isolation(Isolation::ReadCommitted),
            Opening::Isolation(Isolation::RepeatableRead),
            Opening::Isolation(Isolation::Serializable),
            Opening::Mode(Mode::Deferred),
            Opening::Mode(Mode::Immediate),
            Opening::Mode(Mode::Exclusive),
        ];
        assert_eq!(
            openings.map(Opening::name),
            [
                "the database's default",
                "isolation `read_uncommitted`",
                "isolation `read_committed`",
                "isolation `repeatable_read`",
                "isolation `serializable`",
                "mode `deferred`",
                "mode `immediate`",
                "mode `exclusive`",
            ]
        );
    }

    #[test]
    fn a_refusal_names_the_dialect_and_the_opening() {
        assert_eq!(
            Opening::Mode(Mode::Exclusive).refused("postgres"),
            StatementError::UnsupportedOpening {
                dialect: "postgres",
                opening: "mode `exclusive`",
            }
        );
    }
}
