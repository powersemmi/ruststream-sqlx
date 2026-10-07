//! What a subscription to a table opens its claims with on MySQL and MariaDB, through the built-in
//! dialect the broker builds its statements with. The servers do not tell a transaction its own
//! level reliably, so the begin statement is what pins it.

use std::sync::Arc;

use ruststream_sqlx_dialect::{Column, Form, Isolation, Mode, MySql, StatementError, TableSpec};

use crate::inbox::BuiltIn;
use crate::inbox::form::{FormOn, LeaseForm, RowLockForm};

const LEASED: TableSpec<'static> = TableSpec::new(
    "jobs",
    Column::new("job_id"),
    Form::Lease(Column::new("locked_until")),
)
.payload(Column::new("body"));

fn built_in() -> Arc<BuiltIn<sqlx::MySql>> {
    Arc::new(BuiltIn::new(MySql))
}

#[test]
fn a_row_lock_claim_opens_at_the_tables_isolation() -> Result<(), StatementError> {
    let form = <RowLockForm as FormOn<BuiltIn<sqlx::MySql>>>::erase(&built_in());
    let cases = [
        (
            super::JOBS,
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION",
        ),
        (
            super::JOBS.isolation(Isolation::RepeatableRead),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION",
        ),
        (
            super::JOBS.isolation(Isolation::Serializable),
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION",
        ),
    ];
    for (spec, begin) in cases {
        assert_eq!(
            form.begin_claim(&spec)?,
            Some(begin),
            "{:?}",
            spec.opening()
        );
    }
    Ok(())
}

#[test]
fn a_lease_claim_keeps_read_committed_and_still_refuses_a_mode() -> Result<(), StatementError> {
    let form = <LeaseForm as FormOn<BuiltIn<sqlx::MySql>>>::erase(&built_in());
    let serializable = LEASED.isolation(Isolation::Serializable);
    assert_eq!(
        form.begin_claim(&serializable)?,
        Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION")
    );
    let immediate = LEASED.mode(Mode::Immediate);
    assert_eq!(
        form.begin_claim(&immediate),
        Err(StatementError::UnsupportedOpening {
            dialect: "mysql",
            opening: "mode `immediate`",
        })
    );
    Ok(())
}
