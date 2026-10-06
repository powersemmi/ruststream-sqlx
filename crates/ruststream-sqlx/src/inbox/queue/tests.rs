//! What opening a subscription builds: its description, its lease and its statements.

use std::time::Duration;

use ruststream_sqlx_dialect::{ClaimShape, Column, Form, TableSpec};

use super::Description;
use super::description::whole_seconds;
use crate::inbox::engine::{IdAt, Shape};

const JOBS: TableSpec<'static> =
    TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("body"));

fn described(spec: &TableSpec<'static>, shape: Shape, claim: ClaimShape) -> Description {
    Description {
        spec: *spec,
        shape,
        claim,
        row: "Job",
        kinds: None,
    }
}

#[test]
fn a_delayed_retry_is_native_with_its_column_or_the_services_event() {
    let bare = described(&JOBS, Shape::default(), ClaimShape::Rows);
    assert!(!bare.native_retry_after());
    let column = described(
        &JOBS.retry_after(Column::new("retry_after")),
        Shape::default(),
        ClaimShape::Rows,
    );
    assert!(column.native_retry_after());
    let custom = Shape {
        custom_retry_after: true,
        ..Shape::default()
    };
    assert!(described(&JOBS, custom, ClaimShape::Rows).native_retry_after());
}

#[test]
fn a_lease_is_whole_seconds_and_at_least_one() {
    assert_eq!(
        whole_seconds(Duration::from_secs(30)),
        Duration::from_secs(30)
    );
    assert_eq!(
        whole_seconds(Duration::from_millis(1500)),
        Duration::from_secs(2)
    );
    assert_eq!(whole_seconds(Duration::ZERO), Duration::from_secs(1));
    assert_eq!(
        whole_seconds(Duration::MAX),
        Duration::from_secs(u64::MAX),
        "the longest lease saturates instead of wrapping"
    );
}

#[test]
fn a_claim_by_role_carries_the_id_first_whatever_the_struct_flattens() {
    let flat = JOBS.selecting_all();
    assert_eq!(
        described(&flat, Shape::default(), ClaimShape::Rows).id_at(),
        IdAt::Named("job_id")
    );
    assert_eq!(
        described(&flat, Shape::default(), ClaimShape::Roles).id_at(),
        IdAt::First
    );
    assert_eq!(
        described(&JOBS, Shape::default(), ClaimShape::Rows).id_at(),
        IdAt::First
    );
}

#[cfg(feature = "postgres")]
mod built;

#[cfg(feature = "mysql")]
mod on_mysql;

#[cfg(all(feature = "any", feature = "sqlite"))]
mod on_any;

#[cfg(any(feature = "postgres", feature = "mysql"))]
mod guarded;

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
mod advised;
