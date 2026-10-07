//! The dialect an `AnyPool` takes: the built-in dialect of the database its pool reaches, picked
//! when the broker connects.

use std::num::NonZeroUsize;

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use ruststream_sqlx_dialect as dialect;
use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Dialect, Lease, Opening, RowLock, Statement, StatementError, TableName,
    TableSpec,
};

#[cfg(feature = "mysql")]
use super::built_in::MYSQL_BACKEND;
#[cfg(feature = "postgres")]
use super::built_in::POSTGRES_BACKEND;
#[cfg(feature = "sqlite")]
use super::built_in::SQLITE_BACKEND;

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
    /// Its advisory lock form.
    advisory: &'static dyn Advisory,
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
                    advisory: &dialect::Postgres,
                },
            ),
            #[cfg(feature = "mysql")]
            (
                MYSQL_BACKEND,
                Self {
                    lease: &dialect::MySql,
                    row_lock: Some(&dialect::MySql),
                    advisory: &dialect::MySql,
                },
            ),
            #[cfg(feature = "sqlite")]
            (
                SQLITE_BACKEND,
                Self {
                    lease: &dialect::Sqlite,
                    row_lock: None,
                    advisory: &dialect::Sqlite,
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

#[cfg(feature = "any")]
impl Advisory for AnyDialect {
    fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.advisory.advisory_claim(spec)
    }

    fn lock(&self) -> Option<Statement> {
        self.advisory.lock()
    }

    fn unlock(&self) -> Option<Statement> {
        self.advisory.unlock()
    }

    fn take(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError> {
        self.advisory.take(spec, shape)
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
