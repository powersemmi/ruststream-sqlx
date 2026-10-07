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
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     Dialect, Isolation, Opening, Opens, Statement, StatementError, TableSpec, level,
/// };
///
/// /// SQL Server, a database without a built-in dialect.
/// #[derive(Debug)]
/// pub struct Mssql;
///
/// impl Dialect for Mssql {
///     fn name(&self) -> &'static str {
///         "mssql"
///     }
///
///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
///         match opening {
///             Opening::Default => Ok(None),
///             Opening::Isolation(Isolation::ReadCommitted) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION",
///             )),
///             Opening::Isolation(Isolation::RepeatableRead) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; BEGIN TRANSACTION",
///             )),
///             // A refusal names the opening the table declares.
///             other => Err(StatementError::UnsupportedOpening {
///                 dialect: self.name(),
///                 opening: other.name(),
///             }),
///         }
///     }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// }
///
/// // A table at one of these levels mounts on `Mssql`.
/// impl Opens<level::ReadCommitted> for Mssql {}
/// impl Opens<level::RepeatableRead> for Mssql {}
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
/// A dialect that wraps SQLite opens its transactions in the modes SQLite opens:
///
/// ```
/// # #[cfg(feature = "sqlite")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     Dialect, Opening, Opens, Sqlite, Statement, StatementError, TableSpec, level,
/// };
///
/// #[derive(Debug)]
/// pub struct Audited;
///
/// impl Dialect for Audited {
///     fn name(&self) -> &'static str {
///         "audited"
///     }
///
///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
///         Sqlite.begin(opening)
///     }
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
/// }
///
/// // A table with `mode = deferred`, `immediate` or `exclusive` mounts on `Audited`.
/// impl Opens<level::Deferred> for Audited {}
/// impl Opens<level::Immediate> for Audited {}
/// impl Opens<level::Exclusive> for Audited {}
/// # }
/// # fn main() {}
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
    #[must_use]
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Deferred => "DEFERRED",
            Self::Immediate => "IMMEDIATE",
            Self::Exclusive => "EXCLUSIVE",
        }
    }

    /// The mode as a table names it: `immediate`.
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
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     Dialect, Opening, Opens, Postgres, Statement, StatementError, TableSpec, level,
/// };
///
/// /// Postgres, with every claim of a table that names no level reading one snapshot.
/// #[derive(Debug)]
/// pub struct Snapshot;
///
/// impl Dialect for Snapshot {
///     fn name(&self) -> &'static str {
///         "snapshot"
///     }
///
///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
///         match opening {
///             Opening::Default => Ok(Some("BEGIN ISOLATION LEVEL REPEATABLE READ")),
///             other => Postgres.begin(other),
///         }
///     }
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
/// }
///
/// impl Opens<level::ReadCommitted> for Snapshot {}
/// impl Opens<level::RepeatableRead> for Snapshot {}
/// impl Opens<level::Serializable> for Snapshot {}
/// # }
/// # fn main() {}
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
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Isolation, Opening, Opens, Statement, StatementError, TableSpec, level,
    /// };
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Dialect for Mssql {
    ///     fn name(&self) -> &'static str {
    ///         "mssql"
    ///     }
    ///
    ///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
    ///         match opening {
    ///             Opening::Default => Ok(None),
    ///             Opening::Isolation(Isolation::ReadCommitted) => Ok(Some(
    ///                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION",
    ///             )),
    ///             Opening::Isolation(Isolation::RepeatableRead) => Ok(Some(
    ///                 "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; BEGIN TRANSACTION",
    ///             )),
    ///             // A refusal names the opening the table declares.
    ///             other => Err(StatementError::UnsupportedOpening {
    ///                 dialect: self.name(),
    ///                 opening: other.name(),
    ///             }),
    ///         }
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    ///
    /// // A table at one of these levels mounts on `Mssql`.
    /// impl Opens<level::ReadCommitted> for Mssql {}
    /// impl Opens<level::RepeatableRead> for Mssql {}
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
/// A dialect implements `Opens` for each level its `begin` opens:
///
/// ```
/// # #[cfg(feature = "mysql")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     Dialect, Opening, Opens, MySql, Statement, StatementError, TableSpec, level,
/// };
///
/// #[derive(Debug)]
/// pub struct Audited;
///
/// impl Dialect for Audited {
///     fn name(&self) -> &'static str {
///         "audited"
///     }
///
///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
///         MySql.begin(opening)
///     }
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
/// }
///
/// // A table at any of the four levels mounts on `Audited`, as on MySQL.
/// impl Opens<level::ReadUncommitted> for Audited {}
/// impl Opens<level::ReadCommitted> for Audited {}
/// impl Opens<level::RepeatableRead> for Audited {}
/// impl Opens<level::Serializable> for Audited {}
/// # }
/// # fn main() {}
/// ```
pub mod level;

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
/// A dialect that wraps Postgres opens the levels Postgres opens:
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     Dialect, Opening, Opens, Postgres, Statement, StatementError, TableSpec, level,
/// };
///
/// #[derive(Debug)]
/// pub struct Audited;
///
/// impl Dialect for Audited {
///     fn name(&self) -> &'static str {
///         "audited"
///     }
///
///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
///         Postgres.begin(opening)
///     }
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
/// }
///
/// // A table at one of these levels mounts on `Audited`; one at `read_uncommitted` does not.
/// impl Opens<level::ReadCommitted> for Audited {}
/// impl Opens<level::RepeatableRead> for Audited {}
/// impl Opens<level::Serializable> for Audited {}
/// # }
/// # fn main() {}
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
