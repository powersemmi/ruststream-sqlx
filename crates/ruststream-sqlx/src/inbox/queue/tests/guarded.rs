//! What a subscription to a table with FIFO groups prepares through the dialect built into the
//! crate for its database: the guard its claims take the group with, as the dialect the broker
//! picked builds it.

use std::sync::Arc;

use ruststream::RetryDeclaration;
#[cfg(feature = "mysql")]
use ruststream_sqlx_dialect::MySql;
#[cfg(feature = "postgres")]
use ruststream_sqlx_dialect::Postgres;
use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, Param, TableSpec};

use super::described;
#[cfg(all(feature = "any", feature = "postgres", feature = "mysql"))]
use crate::inbox::AnyDialect;
#[cfg(all(feature = "any", feature = "postgres", feature = "mysql"))]
use crate::inbox::database::built_in::{MYSQL_BACKEND, POSTGRES_BACKEND};
use crate::inbox::engine::Shape;
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::form::{FormOn, LeaseForm, RowLockForm};
use crate::inbox::queue::open::build;
use crate::inbox::{BuiltIn, FormDialect};

/// A ledger whose accounts keep their order, in the row lock form.
const LEDGER: TableSpec<'static> = TableSpec::new("ledger", Column::new("id"), Form::RowLock)
    .fifo_group(Column::new("account"))
    .payload(Column::new("payload"));

/// The same ledger in the lease form.
const LEASED_LEDGER: TableSpec<'static> = TableSpec::new(
    "ledger",
    Column::new("id"),
    Form::Lease(Column::new("locked_until")),
)
.fifo_group(Column::new("account"))
.payload(Column::new("payload"));

/// A guard's text and parameters.
type Guard = Option<(String, Vec<Param>)>;

/// The guard a subscription to `spec` prepares, its statements built by the dialect `form`
/// shows.
fn prepared(form: &FormDialect, spec: &TableSpec<'static>) -> Guard {
    let fail = |reason: String| SqlxBrokerError::Declaration {
        subscription: "acct-a".to_owned(),
        table: "ledger".to_owned(),
        row: "Entry",
        reason,
    };
    let described = described(spec, Shape::default(), ClaimShape::Rows);
    build(form, &RetryDeclaration::new(), &described, &fail)
        .expect("the ledger's statements build")
        .fifo_guard
        .map(|guard| (guard.sql.to_owned(), guard.params.to_vec()))
}

/// The guard `dialect` itself builds for `spec`.
fn picked(dialect: &dyn Dialect, spec: &TableSpec<'_>) -> Guard {
    dialect
        .fifo_guard(spec)
        .expect("the dialect guards the ledger")
        .map(|guard| (guard.sql().to_owned(), guard.params().to_vec()))
}

#[cfg(feature = "postgres")]
#[test]
fn the_built_in_postgres_dialect_guards_a_fifo_table_in_either_form() {
    let built_in = Arc::new(BuiltIn::<sqlx::Postgres>::new(Postgres));
    let locked = <RowLockForm as FormOn<BuiltIn<sqlx::Postgres>>>::erase(&built_in);
    let leased = <LeaseForm as FormOn<BuiltIn<sqlx::Postgres>>>::erase(&built_in);
    let guard = picked(&Postgres, &LEDGER);
    assert!(
        guard.is_some(),
        "Postgres takes a group with a lock of the claim's transaction"
    );
    assert_eq!(prepared(&locked, &LEDGER), guard);
    assert_eq!(
        prepared(&leased, &LEASED_LEDGER),
        picked(&Postgres, &LEASED_LEDGER)
    );
    // A table whose groups keep no order claims without one.
    assert_eq!(prepared(&locked, &super::JOBS), None);
}

#[cfg(feature = "mysql")]
#[test]
fn the_built_in_mysql_dialect_guards_a_fifo_table_in_either_form() {
    let built_in = Arc::new(BuiltIn::<sqlx::MySql>::new(MySql));
    let locked = <RowLockForm as FormOn<BuiltIn<sqlx::MySql>>>::erase(&built_in);
    let leased = <LeaseForm as FormOn<BuiltIn<sqlx::MySql>>>::erase(&built_in);
    let guard = picked(&MySql, &LEDGER);
    assert!(
        guard.is_some(),
        "MySQL takes a group with a locking read of its rows"
    );
    assert_eq!(prepared(&locked, &LEDGER), guard);
    assert_eq!(
        prepared(&leased, &LEASED_LEDGER),
        picked(&MySql, &LEASED_LEDGER)
    );
}

#[cfg(all(feature = "any", feature = "postgres", feature = "mysql"))]
#[test]
fn an_any_backend_guards_a_fifo_table_as_its_database_does() {
    let locked = |backend| {
        let picked = AnyDialect::of(backend).expect("the backend's feature is on");
        <RowLockForm as FormOn<BuiltIn<sqlx::Any>>>::erase(&Arc::new(BuiltIn::new(picked)))
    };
    let postgres = picked(&Postgres, &LEDGER);
    assert!(postgres.is_some());
    assert_eq!(prepared(&locked(POSTGRES_BACKEND), &LEDGER), postgres);
    let mysql = picked(&MySql, &LEDGER);
    assert!(mysql.is_some());
    assert_eq!(prepared(&locked(MYSQL_BACKEND), &LEDGER), mysql);
}
