//! What an `AnyPool` refuses when a subscription starts: its database is known only then, so a
//! table may name a level the picked backend lacks.

use std::sync::Arc;

use ruststream_sqlx_dialect::{Column, Form, Isolation, Mode, StatementError, TableSpec};
use sqlx::Any;

use crate::inbox::database::built_in::SQLITE_BACKEND;
use crate::inbox::form::{FormOn, LeaseForm};
use crate::inbox::{AnyDialect, BuiltIn};

const LEASED: TableSpec<'static> = TableSpec::new(
    "jobs",
    Column::new("job_id"),
    Form::Lease(Column::new("locked_until")),
)
.payload(Column::new("body"));

#[test]
fn a_sqlite_backend_refuses_an_isolation_level_whatever_the_form() -> Result<(), StatementError> {
    let picked = AnyDialect::of(SQLITE_BACKEND).expect("the sqlite feature is on");
    let form = <LeaseForm as FormOn<BuiltIn<Any>>>::erase(&Arc::new(BuiltIn::new(picked)));
    let serializable = LEASED.isolation(Isolation::Serializable);
    assert_eq!(
        form.begin_claim(&serializable),
        Err(StatementError::UnsupportedOpening {
            dialect: "sqlite",
            opening: "isolation `serializable`",
        })
    );
    // A mode it opens passes, and the lease claim opens as the lease form opens it.
    assert_eq!(
        form.begin_claim(&LEASED.mode(Mode::Exclusive))?,
        Some("BEGIN IMMEDIATE")
    );
    Ok(())
}
