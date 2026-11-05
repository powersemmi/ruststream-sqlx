/// [`Isolation::ReadUncommitted`](crate::Isolation::ReadUncommitted) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
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
///             Opening::Default => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION",
///             )),
///             Opening::Isolation(Isolation::ReadUncommitted) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; BEGIN TRANSACTION",
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
/// // A table with `isolation = read_uncommitted` mounts on `Mssql`.
/// impl Opens<level::ReadUncommitted> for Mssql {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ReadUncommitted;

/// [`Isolation::ReadCommitted`](crate::Isolation::ReadCommitted) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
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
/// // A table with `isolation = read_committed` mounts on `Mssql`.
/// impl Opens<level::ReadCommitted> for Mssql {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ReadCommitted;

/// [`Isolation::RepeatableRead`](crate::Isolation::RepeatableRead) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
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
///             Opening::Default => Ok(Some(
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
/// // A table with `isolation = repeatable_read` mounts on `Mssql`.
/// impl Opens<level::RepeatableRead> for Mssql {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RepeatableRead;

/// [`Isolation::Serializable`](crate::Isolation::Serializable) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
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
///             Opening::Default => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; BEGIN TRANSACTION",
///             )),
///             Opening::Isolation(Isolation::Serializable) => Ok(Some(
///                 "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION",
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
/// // A table with `isolation = serializable` mounts on `Mssql`.
/// impl Opens<level::Serializable> for Mssql {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Serializable;

/// [`Mode::Deferred`](crate::Mode::Deferred) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
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
/// // A table with `mode = deferred` mounts on `Libsql`.
/// impl Opens<level::Deferred> for Libsql {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Deferred;

/// [`Mode::Immediate`](crate::Mode::Immediate) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
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
///             Opening::Mode(Mode::Immediate) => Ok(Some("BEGIN IMMEDIATE")),
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
/// // A table with `mode = immediate` mounts on `Libsql`.
/// impl Opens<level::Immediate> for Libsql {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Immediate;

/// [`Mode::Exclusive`](crate::Mode::Exclusive) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
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
///             Opening::Mode(Mode::Exclusive) => Ok(Some("BEGIN EXCLUSIVE")),
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
/// // A table with `mode = exclusive` mounts on `Libsql`.
/// impl Opens<level::Exclusive> for Libsql {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Exclusive;
