//! The dialect built into the crate for each database: the broker's dialect unless the service
//! passes its own.

use std::fmt;
use std::num::NonZeroUsize;

#[cfg(all(
    feature = "any",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
use ruststream_sqlx_dialect as dialect;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "any"))]
use ruststream_sqlx_dialect::RowLock;
use ruststream_sqlx_dialect::{
    ClaimShape, Dialect, Lease, Opening, Statement, StatementError, TableName, TableSpec,
};
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite",
    feature = "any"
))]
use ruststream_sqlx_dialect::{Opens, level};
#[cfg(feature = "any")]
use sqlx::Any;
#[cfg(feature = "mysql")]
use sqlx::MySql;
#[cfg(feature = "postgres")]
use sqlx::Postgres;
#[cfg(feature = "sqlite")]
use sqlx::Sqlite;

use super::database::BuiltInDialect;
#[cfg(all(feature = "any", feature = "mysql"))]
use super::database::MYSQL_BACKEND;
#[cfg(all(feature = "any", feature = "postgres"))]
use super::database::POSTGRES_BACKEND;
#[cfg(all(feature = "any", feature = "sqlite"))]
use super::database::SQLITE_BACKEND;

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
/// `BuiltIn` implements [`Lease`], and `BuiltIn<Postgres>`, `BuiltIn<MySql>` and `BuiltIn<Any>`
/// implement [`RowLock`]. A SQLite table in the row lock form therefore does not compile. An
/// `AnyPool` names its database only when the broker connects, so on a SQLite backend the
/// subscription to a row lock table stops when it starts, with
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
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # async fn run(pool: sqlx::PgPool) -> Result<(), ruststream_sqlx::SqlxBrokerError> {
/// use ruststream::Broker;
/// use ruststream_sqlx::{BuiltIn, ConnectedSqlxBroker, SqlxBroker};
/// use sqlx::Postgres;
///
/// // `SqlxBroker<Postgres>` names the same type: the built-in dialect is the default.
/// let broker: SqlxBroker<Postgres, BuiltIn<Postgres>> = SqlxBroker::new(pool);
/// let connected: ConnectedSqlxBroker<Postgres> = broker.connect().await?;
/// tracing::info!(?connected, "the inbox broker connected");
/// # Ok(())
/// # }
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

/// The built-in dialect an `AnyPool`'s backend takes, picked when the broker connects.
/// Machinery: `BuiltIn<Any>` builds its statements with it.
#[cfg(feature = "any")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct AnyDialect {
    /// The picked dialect: its lease form and, through it, its base trait.
    lease: &'static dyn Lease,
    /// Its row lock form, where its database locks rows.
    row_lock: Option<&'static dyn RowLock>,
}

#[cfg(feature = "any")]
impl AnyDialect {
    /// The built-in dialect of the `AnyPool` backend named `backend`, where its feature is on.
    pub(crate) fn of(backend: &str) -> Option<Self> {
        let dialects: &[(&str, Self)] = &[
            #[cfg(feature = "postgres")]
            (
                POSTGRES_BACKEND,
                Self {
                    lease: &dialect::Postgres,
                    row_lock: Some(&dialect::Postgres),
                },
            ),
            #[cfg(feature = "mysql")]
            (
                MYSQL_BACKEND,
                Self {
                    lease: &dialect::MySql,
                    row_lock: Some(&dialect::MySql),
                },
            ),
            #[cfg(feature = "sqlite")]
            (
                SQLITE_BACKEND,
                Self {
                    lease: &dialect::Sqlite,
                    row_lock: None,
                },
            ),
        ];
        dialects
            .iter()
            .find(|(name, _)| *name == backend)
            .map(|&(_, dialect)| dialect)
    }

    /// The picked dialect, through its base trait.
    fn base(&self) -> &'static dyn Dialect {
        self.lease
    }
}

#[cfg(feature = "any")]
impl Dialect for AnyDialect {
    fn name(&self) -> &'static str {
        self.base().name()
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        self.base().quote_into(ident, out);
    }

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        self.base().placeholder_into(index, out);
    }

    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.base().fetch(spec)
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.base().ack(spec)
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        self.base().retry(spec)
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.base().retry_after(spec)
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.base().discard(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.base().dead_letter_group(spec)
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        self.base().dead_letter_table(spec, target)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.base().insert(spec)
    }

    fn server_version(&self) -> Option<&'static str> {
        self.base().server_version()
    }

    fn check_server(&self, spec: &TableSpec<'_>, version: &str) -> Result<(), StatementError> {
        self.base().check_server(spec, version)
    }

    fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
        self.base().begin(opening)
    }

    fn savepoint(&self) -> &'static str {
        self.base().savepoint()
    }

    fn rollback_to_savepoint(&self) -> &'static str {
        self.base().rollback_to_savepoint()
    }

    fn fifo_guard(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        self.base().fifo_guard(spec)
    }
}

#[cfg(feature = "any")]
impl Lease for AnyDialect {
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        self.lease.lease_claim(spec, shape)
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.lease.extend(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.lease.stamp(spec)
    }

    fn claim_writes_lease(&self) -> bool {
        self.lease.claim_writes_lease()
    }

    fn claim_counts_attempt(&self, spec: &TableSpec<'_>) -> bool {
        self.lease.claim_counts_attempt(spec)
    }

    fn begin_lease_claim(&self) -> Option<&'static str> {
        self.lease.begin_lease_claim()
    }
}

// Why a startup refusal: an `AnyPool` names its database only when the broker connects, so the
// type cannot say whether the database locks rows. A SQLite backend refuses the row lock claim
// when the subscription to such a table starts.
#[cfg(feature = "any")]
impl RowLock for AnyDialect {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        self.row_lock.map_or_else(
            || {
                Err(StatementError::UnsupportedForm {
                    dialect: self.name(),
                    form: spec.form().name(),
                })
            },
            |dialect| dialect.lock_claim(spec, shape),
        )
    }
}

#[cfg(all(test, feature = "any"))]
mod tests {
    use ruststream_sqlx_dialect::Dialect;

    use super::AnyDialect;

    #[test]
    fn an_any_backend_takes_the_dialect_of_its_database() {
        let picked = |backend| AnyDialect::of(backend).map(|dialect| dialect.name());
        #[cfg(feature = "postgres")]
        assert_eq!(picked(super::POSTGRES_BACKEND), Some("postgres"));
        #[cfg(feature = "mysql")]
        assert_eq!(picked(super::MYSQL_BACKEND), Some("mysql"));
        #[cfg(feature = "sqlite")]
        assert_eq!(picked(super::SQLITE_BACKEND), Some("sqlite"));
        assert_eq!(
            picked("MSSQL"),
            None,
            "a backend without a built-in dialect"
        );
    }
}
