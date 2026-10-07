//! The dialect built into the crate for each database: the broker's dialect unless the service
//! passes its own.

use std::fmt;
use std::num::NonZeroUsize;

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use ruststream_sqlx_dialect as dialect;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "any"))]
use ruststream_sqlx_dialect::RowLock;
use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Dialect, Lease, Opening, Statement, StatementError, TableName, TableSpec,
};
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite",
    feature = "any"
))]
use ruststream_sqlx_dialect::{Opens, level};
#[cfg(feature = "any")]
use sqlx::{Any, AnyConnection};
#[cfg(feature = "mysql")]
use sqlx::{MySql, MySqlConnection};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, Postgres};
#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, SqliteConnection};

use super::QueueDatabase;
#[cfg(feature = "any")]
use super::any::AnyDialect;

/// The dialect built into the crate for the database `DB`: what [`SqlxBroker::new`] builds its
/// statements with.
///
/// It builds them with the dialect of the crate's [`dialect`](crate::dialect) module that serves
/// the database: [`Postgres`](crate::dialect::Postgres) on Postgres,
/// [`MySql`](crate::dialect::MySql) on MySQL and MariaDB, [`Sqlite`](crate::dialect::Sqlite) on
/// SQLite. On an `AnyPool` it is the dialect of the database the pool reaches, among those whose
/// features are on, picked when the broker connects.
///
/// It serves the forms its database serves, through the traits of the dialect module: every
/// `BuiltIn` implements [`Lease`] and [`Advisory`], and `BuiltIn<Postgres>`, `BuiltIn<MySql>` and
/// `BuiltIn<Any>` implement [`RowLock`]. A SQLite table in the row lock form therefore does not
/// compile. An `AnyPool` names its database only when the broker connects, so on a SQLite backend
/// the subscription to a row lock table stops when it starts, with
/// [`StatementError::UnsupportedForm`].
///
/// It opens transactions at the isolation levels and SQLite modes its database keeps, one
/// [`Opens`](crate::dialect::Opens) per level: `BuiltIn<Postgres>` READ COMMITTED, REPEATABLE
/// READ and SERIALIZABLE, `BuiltIn<MySql>` those and READ UNCOMMITTED, `BuiltIn<Sqlite>` the three
/// modes. A table that names another does not compile. `BuiltIn<Any>` takes all seven, and the
/// picked backend's dialect refuses the ones its database lacks when the subscription starts,
/// with [`StatementError::UnsupportedOpening`].
///
/// [`SqlxBroker::new`]: crate::SqlxBroker::new
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::BuiltIn;
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::{PgPool, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
/// async fn send(email: &Email) -> HandlerOutcome {
///     tracing::info!(to = %email.to, "sending");
///     HandlerOutcome::ack()
/// }
///
/// // `SqlxBroker<Postgres>` names the same type: the built-in dialect is the default.
/// pub fn broker(pool: PgPool) -> SqlxBroker<Postgres, BuiltIn<Postgres>> {
///     SqlxBroker::new(pool)
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(broker(pool), |b| {
///         b.include(send);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct BuiltIn<DB: BuiltInDialect>(DB::Picked);

impl<DB: BuiltInDialect> BuiltIn<DB> {
    /// The built-in dialect that `picked` builds the statements of.
    #[cfg(any(
        feature = "postgres",
        feature = "mysql",
        feature = "sqlite",
        feature = "any"
    ))]
    pub(crate) const fn new(picked: DB::Picked) -> Self {
        Self(picked)
    }
}

impl<DB: BuiltInDialect> Clone for BuiltIn<DB> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<DB: BuiltInDialect> Copy for BuiltIn<DB> {}

impl<DB: BuiltInDialect> fmt::Debug for BuiltIn<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BuiltIn").field(&self.0.name()).finish()
    }
}

impl<DB: BuiltInDialect> Dialect for BuiltIn<DB> {
    fn name(&self) -> &'static str {
        self.0.name()
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        self.0.quote_into(ident, out);
    }

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        self.0.placeholder_into(index, out);
    }

    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.0.fetch(spec)
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.0.ack(spec)
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        self.0.retry(spec)
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.0.retry_after(spec)
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.0.discard(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.0.dead_letter_group(spec)
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        self.0.dead_letter_table(spec, target)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.0.insert(spec)
    }

    fn server_version(&self) -> Option<&'static str> {
        self.0.server_version()
    }

    fn check_server(&self, spec: &TableSpec<'_>, version: &str) -> Result<(), StatementError> {
        self.0.check_server(spec, version)
    }

    fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
        self.0.begin(opening)
    }

    fn savepoint(&self) -> &'static str {
        self.0.savepoint()
    }

    fn rollback_to_savepoint(&self) -> &'static str {
        self.0.rollback_to_savepoint()
    }

    fn fifo_guard(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        self.0.fifo_guard(spec)
    }
}

impl<DB: BuiltInDialect> Lease for BuiltIn<DB> {
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        self.0.lease_claim(spec, shape)
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.0.extend(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.0.stamp(spec)
    }

    fn claim_writes_lease(&self) -> bool {
        self.0.claim_writes_lease()
    }

    fn claim_counts_attempt(&self, spec: &TableSpec<'_>) -> bool {
        self.0.claim_counts_attempt(spec)
    }

    fn begin_lease_claim(&self) -> Option<&'static str> {
        self.0.begin_lease_claim()
    }
}

impl<DB: BuiltInDialect> Advisory for BuiltIn<DB> {
    fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.0.advisory_claim(spec)
    }

    fn lock(&self) -> Option<Statement> {
        self.0.lock()
    }

    fn unlock(&self) -> Option<Statement> {
        self.0.unlock()
    }

    fn take(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError> {
        self.0.take(spec, shape)
    }
}

#[cfg(feature = "postgres")]
impl RowLock for BuiltIn<Postgres> {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        self.0.lock_claim(spec, shape)
    }
}

#[cfg(feature = "mysql")]
impl RowLock for BuiltIn<MySql> {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        self.0.lock_claim(spec, shape)
    }
}

#[cfg(feature = "any")]
impl RowLock for BuiltIn<Any> {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        self.0.lock_claim(spec, shape)
    }
}

#[cfg(feature = "postgres")]
impl Opens<level::ReadCommitted> for BuiltIn<Postgres> {}

#[cfg(feature = "postgres")]
impl Opens<level::RepeatableRead> for BuiltIn<Postgres> {}

#[cfg(feature = "postgres")]
impl Opens<level::Serializable> for BuiltIn<Postgres> {}

#[cfg(feature = "mysql")]
impl Opens<level::ReadUncommitted> for BuiltIn<MySql> {}

#[cfg(feature = "mysql")]
impl Opens<level::ReadCommitted> for BuiltIn<MySql> {}

#[cfg(feature = "mysql")]
impl Opens<level::RepeatableRead> for BuiltIn<MySql> {}

#[cfg(feature = "mysql")]
impl Opens<level::Serializable> for BuiltIn<MySql> {}

#[cfg(feature = "sqlite")]
impl Opens<level::Deferred> for BuiltIn<Sqlite> {}

#[cfg(feature = "sqlite")]
impl Opens<level::Immediate> for BuiltIn<Sqlite> {}

#[cfg(feature = "sqlite")]
impl Opens<level::Exclusive> for BuiltIn<Sqlite> {}

// Why every level and mode compiles on `Any`: an `AnyPool` names its database only when the
// broker connects, so the type cannot say which of them the database opens. The picked backend's
// dialect refuses the ones it lacks when the subscription to such a table starts.
#[cfg(feature = "any")]
impl Opens<level::ReadUncommitted> for BuiltIn<Any> {}

#[cfg(feature = "any")]
impl Opens<level::ReadCommitted> for BuiltIn<Any> {}

#[cfg(feature = "any")]
impl Opens<level::RepeatableRead> for BuiltIn<Any> {}

#[cfg(feature = "any")]
impl Opens<level::Serializable> for BuiltIn<Any> {}

#[cfg(feature = "any")]
impl Opens<level::Deferred> for BuiltIn<Any> {}

#[cfg(feature = "any")]
impl Opens<level::Immediate> for BuiltIn<Any> {}

#[cfg(feature = "any")]
impl Opens<level::Exclusive> for BuiltIn<Any> {}

/// A database whose dialect is built into this crate, so `SqlxBroker::new` needs no dialect of
/// the service's own.
///
/// Postgres (feature `postgres`), MySQL with MariaDB (feature `mysql`) and SQLite (feature
/// `sqlite`) have one, [`BuiltIn<DB>`](BuiltIn). `Any` (feature `any`) takes the dialect of the
/// database its pool reaches, among those whose features are on, and the broker picks it when it
/// connects. A row on an `AnyPool` holds only the types `sqlx::Any` carries, and no time is among
/// them, so a table with `locked_until`, `retry_after` or `processed_at` is out of its reach.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "any", feature = "postgres", feature = "mysql"))]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::AnyPool;
///
/// // The columns `sqlx::Any` carries: integers, text and bytes, no time.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(attempt, generated)]
///     attempt: i32,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
/// async fn send(email: &Email) -> HandlerOutcome {
///     tracing::info!(to = %email.to, "sending");
///     HandlerOutcome::ack()
/// }
///
/// // One build for every deployment: `Any` is a `BuiltInDialect`, and the broker takes the
/// // dialect of the database the URL names, Postgres or MySQL, when it connects. `main` installs
/// // sqlx's drivers (`sqlx::any::install_default_drivers`) before it builds the pool.
/// pub fn app(pool: AnyPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(send);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no dialect built into the crate",
    label = "no built-in dialect for this database",
    note = "build the broker with a dialect of the service's own: \
            `SqlxBroker::with_dialect(pool, dialect)`, a `SqlxBroker<{Self}, YourDialect>`"
)]
pub trait BuiltInDialect: QueueDatabase {
    /// The dialect of the crate's `dialect` module that builds the database's statements.
    /// Machinery; [`BuiltIn`] holds it.
    #[doc(hidden)]
    type Picked: Lease + Advisory + Copy + 'static;

    /// The dialect that builds the statements of the database `conn` reaches, or `None` when no
    /// built-in dialect serves it: an `AnyPool`'s backend whose feature is off.
    ///
    /// Postgres, MySQL and SQLite answer without reading `conn`; the broker asks with the
    /// connection it checks when it connects.
    fn dialect(conn: &Self::Connection) -> Option<BuiltIn<Self>>;

    /// The name of the database `conn` reaches, which an error names when no built-in dialect
    /// serves it. Machinery; the broker calls it.
    #[doc(hidden)]
    fn backend(conn: &Self::Connection) -> &str {
        let _ = conn;
        Self::NAME
    }
}

#[cfg(feature = "postgres")]
impl BuiltInDialect for Postgres {
    type Picked = dialect::Postgres;

    fn dialect(_: &PgConnection) -> Option<BuiltIn<Self>> {
        Some(BuiltIn::new(dialect::Postgres))
    }
}

#[cfg(feature = "mysql")]
impl BuiltInDialect for MySql {
    type Picked = dialect::MySql;

    fn dialect(_: &MySqlConnection) -> Option<BuiltIn<Self>> {
        Some(BuiltIn::new(dialect::MySql))
    }
}

#[cfg(feature = "sqlite")]
impl BuiltInDialect for Sqlite {
    type Picked = dialect::Sqlite;

    fn dialect(_: &SqliteConnection) -> Option<BuiltIn<Self>> {
        Some(BuiltIn::new(dialect::Sqlite))
    }
}

#[cfg(feature = "any")]
impl BuiltInDialect for Any {
    type Picked = AnyDialect;

    fn dialect(conn: &AnyConnection) -> Option<BuiltIn<Self>> {
        AnyDialect::of(conn.backend_name()).map(BuiltIn::new)
    }

    fn backend(conn: &AnyConnection) -> &str {
        conn.backend_name()
    }
}

/// The name an `AnyConnection` reports for a Postgres backend: its sqlx driver's
/// `Database::NAME`.
#[cfg(feature = "any")]
pub(crate) const POSTGRES_BACKEND: &str = "PostgreSQL";

/// The name an `AnyConnection` reports for a MySQL or MariaDB backend.
#[cfg(feature = "any")]
pub(crate) const MYSQL_BACKEND: &str = "MySQL";

/// The name an `AnyConnection` reports for a SQLite backend.
#[cfg(feature = "any")]
pub(crate) const SQLITE_BACKEND: &str = "SQLite";

#[cfg(all(test, feature = "any"))]
mod tests {
    use sqlx::Database;

    use super::{MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};

    #[test]
    fn the_backend_names_are_those_sqlx_reports() {
        assert_eq!(POSTGRES_BACKEND, <sqlx::Postgres as Database>::NAME);
        assert_eq!(MYSQL_BACKEND, <sqlx::MySql as Database>::NAME);
        assert_eq!(SQLITE_BACKEND, <sqlx::Sqlite as Database>::NAME);
    }
}
