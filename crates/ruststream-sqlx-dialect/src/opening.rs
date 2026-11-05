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
///             Opening::Default | Opening::Isolation(Isolation::ReadCommitted) => Ok(Some(
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # fn main() -> Result<(), ruststream_sqlx_dialect::StatementError> {
    /// use ruststream_sqlx_dialect::{Dialect, Isolation, Opening, Postgres};
    ///
    /// // A dialect's test: each level it serves opens a transaction at that level.
    /// for level in [Isolation::ReadCommitted, Isolation::RepeatableRead, Isolation::Serializable] {
    ///     let begin = Postgres.begin(Opening::Isolation(level))?;
    ///     assert_eq!(begin, Some(format!("BEGIN ISOLATION LEVEL {}", level.sql())).as_deref());
    /// }
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "postgres"))]
    /// # fn main() {}
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
    // The derives and the crate read it to parse and check a table; a service never names it.
    #[doc(hidden)]
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
/// A dialect for a database that takes SQLite's modes opens its transactions in them:
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     Dialect, Mode, Opening, Opens, Statement, StatementError, TableSpec, level,
/// };
///
/// /// libSQL, a fork of SQLite served over the network, a database without a built-in dialect.
/// #[derive(Debug)]
/// pub struct Libsql;
///
/// impl Dialect for Libsql {
///     fn name(&self) -> &'static str {
///         "libsql"
///     }
///
///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
///         match opening {
///             Opening::Default => Ok(None),
///             Opening::Mode(Mode::Deferred) => Ok(Some("BEGIN DEFERRED")),
///             Opening::Mode(Mode::Immediate) => Ok(Some("BEGIN IMMEDIATE")),
///             Opening::Mode(Mode::Exclusive) => Ok(Some("BEGIN EXCLUSIVE")),
///             // libSQL runs every transaction serializable, as SQLite does: it names no level.
///             other => Err(StatementError::UnsupportedOpening {
///                 dialect: self.name(),
///                 opening: other.name(),
///             }),
///         }
///     }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('"'); out.push_str(&ident.replace('"', "\"\"")); out.push('"'); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push('?'); out.push_str(&index.to_string()); }
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
/// // A table with `mode = deferred`, `immediate` or `exclusive` mounts on `Libsql`.
/// impl Opens<level::Deferred> for Libsql {}
/// impl Opens<level::Immediate> for Libsql {}
/// impl Opens<level::Exclusive> for Libsql {}
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
    /// # #[cfg(feature = "sqlite")]
    /// # fn main() -> Result<(), ruststream_sqlx_dialect::StatementError> {
    /// use ruststream_sqlx_dialect::{Dialect, Mode, Opening, Sqlite};
    ///
    /// // A dialect's test: each mode opens a transaction in that mode.
    /// for mode in [Mode::Deferred, Mode::Immediate, Mode::Exclusive] {
    ///     let begin = Sqlite.begin(Opening::Mode(mode))?;
    ///     assert_eq!(begin, Some(format!("BEGIN {}", mode.sql())).as_deref());
    /// }
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "sqlite"))]
    /// # fn main() {}
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
    // The derives and the crate read it to parse and check a table; a service never names it.
    #[doc(hidden)]
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
///     // SQL Server keeps a level set with `SET TRANSACTION ISOLATION LEVEL` for the rest of the
///     // session, so a table that names no level opens at READ COMMITTED by name rather than at
///     // the level the connection last ran.
///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
///         match opening {
///             Opening::Default | Opening::Isolation(Isolation::ReadCommitted) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION",
///             )),
///             Opening::Isolation(Isolation::RepeatableRead) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; BEGIN TRANSACTION",
///             )),
///             Opening::Isolation(Isolation::Serializable) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION",
///             )),
///             // A SQLite mode, and the levels the service's claims do not run at.
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
/// impl Opens<level::ReadCommitted> for Mssql {}
/// impl Opens<level::RepeatableRead> for Mssql {}
/// impl Opens<level::Serializable> for Mssql {}
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
    ///             Opening::Default | Opening::Isolation(Isolation::ReadCommitted) => Ok(Some(
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
///             Opening::Default | Opening::Isolation(Isolation::ReadCommitted) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION",
///             )),
///             Opening::Isolation(Isolation::RepeatableRead) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; BEGIN TRANSACTION",
///             )),
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
/// // A table at `read_committed` or `repeatable_read` mounts on `Mssql`.
/// impl Opens<level::ReadCommitted> for Mssql {}
/// impl Opens<level::RepeatableRead> for Mssql {}
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
/// A dialect of the service's own opens the levels its `begin` opens:
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
///             Opening::Default | Opening::Isolation(Isolation::ReadCommitted) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION",
///             )),
///             Opening::Isolation(Isolation::RepeatableRead) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; BEGIN TRANSACTION",
///             )),
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
/// // A table at one of these levels mounts on `Mssql`; one at any other level does not compile.
/// impl Opens<level::ReadCommitted> for Mssql {}
/// impl Opens<level::RepeatableRead> for Mssql {}
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
