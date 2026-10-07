/// [`Isolation::ReadUncommitted`](crate::Isolation::ReadUncommitted) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
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
/// // A table with `isolation = read_uncommitted` mounts on `Audited`.
/// impl Opens<level::ReadUncommitted> for Audited {}
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ReadUncommitted;

/// [`Isolation::ReadCommitted`](crate::Isolation::ReadCommitted) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
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
/// // A table with `isolation = read_committed` mounts on `Audited`.
/// impl Opens<level::ReadCommitted> for Audited {}
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ReadCommitted;

/// [`Isolation::RepeatableRead`](crate::Isolation::RepeatableRead) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
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
/// // A table with `isolation = repeatable_read` mounts on `Audited`.
/// impl Opens<level::RepeatableRead> for Audited {}
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RepeatableRead;

/// [`Isolation::Serializable`](crate::Isolation::Serializable) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
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
/// // A table with `isolation = serializable` mounts on `Audited`.
/// impl Opens<level::Serializable> for Audited {}
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Serializable;

/// [`Mode::Deferred`](crate::Mode::Deferred) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
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
/// // A table with `mode = deferred` mounts on `Audited`.
/// impl Opens<level::Deferred> for Audited {}
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Deferred;

/// [`Mode::Immediate`](crate::Mode::Immediate) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
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
/// // A table with `mode = immediate` mounts on `Audited`.
/// impl Opens<level::Immediate> for Audited {}
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Immediate;

/// [`Mode::Exclusive`](crate::Mode::Exclusive) as a type, for the bound
/// [`Opens`](crate::Opens) sets.
///
/// # Examples
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
/// // A table with `mode = exclusive` mounts on `Audited`.
/// impl Opens<level::Exclusive> for Audited {}
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Exclusive;
