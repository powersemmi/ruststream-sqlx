//! `build` against a dialect of a service's own, which may split a dead-letter move or a take,
//! or claim leased rows by selecting them alone.

use std::num::NonZeroUsize;
use std::sync::Arc;

use ruststream::RetryDeclaration;
use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Column, Dialect, Form, Isolation, KeyPart, Lease, Opening, Param,
    Postgres, RowLock, Statement, StatementError, TableName, TableSpec,
};

use super::described;
use crate::inbox::FormDialect;
use crate::inbox::engine::{Prepared, Shape};
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::queue::open::{build, counted_attempt};

/// The Postgres dialect, its dead-letter move and its take cut into `parts` statements, its
/// lease claim writing the lease or only selecting the rows, and each form's claim and
/// transaction opening marked with the trait that built it. The row lock claim opens with the
/// dialect's own `begin`, which opens SERIALIZABLE beside the default and refuses every other
/// level.
#[derive(Debug)]
struct Reshaped {
    parts: usize,
    writes_lease: bool,
}

/// The claim `trait_name` built for `spec`: Postgres's text, marked with the trait's name.
fn marked(
    trait_name: &str,
    claim: Result<Statement, StatementError>,
) -> Result<Statement, StatementError> {
    let claim = claim?;
    Ok(Statement::new(
        format!("/* {trait_name} */ {}", claim.sql()),
        claim.params().iter().copied(),
    ))
}

impl Dialect for Reshaped {
    fn name(&self) -> &'static str {
        "reshaped"
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        Postgres.quote_into(ident, out);
    }

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        Postgres.placeholder_into(index, out);
    }

    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Postgres.fetch(spec)
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Postgres.ack(spec)
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        Postgres.retry(spec)
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Postgres.retry_after(spec)
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Postgres.discard(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Postgres.dead_letter_group(spec)
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        let mut moves = Postgres.dead_letter_table(spec, target)?;
        moves.resize(self.parts, moves[0].clone());
        Ok(moves)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Postgres.insert(spec)
    }

    fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
        match opening {
            Opening::Default => Ok(Some("BEGIN /* Dialect */")),
            Opening::Isolation(Isolation::Serializable) => {
                Ok(Some("BEGIN ISOLATION LEVEL SERIALIZABLE /* Dialect */"))
            }
            _ => Err(StatementError::UnsupportedOpening {
                dialect: self.name(),
                opening: opening.name(),
            }),
        }
    }
}

impl RowLock for Reshaped {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        marked("RowLock", Postgres.lock_claim(spec, shape))
    }
}

impl Lease for Reshaped {
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        marked("Lease", Postgres.lease_claim(spec, shape))
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Postgres.extend(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Postgres.stamp(spec)
    }

    fn claim_writes_lease(&self) -> bool {
        self.writes_lease
    }

    fn begin_lease_claim(&self) -> Option<&'static str> {
        Some("BEGIN /* Lease */")
    }
}

impl Advisory for Reshaped {
    fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        marked("Advisory", Postgres.advisory_claim(spec))
    }

    fn lock(&self) -> Option<Statement> {
        Postgres.lock()
    }

    fn unlock(&self) -> Option<Statement> {
        Postgres.unlock()
    }

    fn take(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError> {
        let mut takes = Postgres.take(spec, shape)?;
        takes.resize(self.parts, takes[0].clone());
        Ok(takes)
    }
}

const LEASED: TableSpec<'static> = TableSpec::new(
    "jobs",
    Column::new("job_id"),
    Form::Lease(Column::new("locked_until")),
)
.payload(Column::new("body"));

/// The lock key of every job: its id.
const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")];

/// The jobs in the advisory lock form, with an attempt the take counts.
const ADVISED: TableSpec<'static> =
    TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(KEY))
        .attempt(Column::new("attempt"))
        .payload(Column::new("body"));

const RESHAPED: Reshaped = Reshaped {
    parts: 1,
    writes_lease: true,
};

/// `dialect` seen through the trait of `spec`'s form, as a subscription to it sees it.
fn form_of(dialect: Reshaped, spec: &TableSpec<'_>) -> FormDialect {
    match spec.form() {
        Form::RowLock => FormDialect::RowLock(Arc::new(dialect)),
        Form::Lease(_) => FormDialect::Lease(Arc::new(dialect)),
        _ => FormDialect::Advisory(Arc::new(dialect)),
    }
}

fn prepared(
    dialect: Reshaped,
    spec: &TableSpec<'static>,
    shape: Shape,
    declaration: &RetryDeclaration,
) -> Result<Prepared, SqlxBrokerError> {
    let fail = |reason: String| SqlxBrokerError::Declaration {
        subscription: "jobs".to_owned(),
        table: "jobs".to_owned(),
        row: "Job",
        reason,
    };
    build(
        &form_of(dialect, spec),
        declaration,
        &described(spec, shape, ClaimShape::Rows),
        &fail,
    )
}

#[test]
fn each_form_claims_with_the_statement_of_its_trait() -> Result<(), SqlxBrokerError> {
    let none = RetryDeclaration::new();
    let locked = prepared(RESHAPED, &super::JOBS, Shape::default(), &none)?;
    assert_eq!(
        locked.claim.map(|claim| claim.sql),
        Some(
            r#"/* RowLock */ SELECT "job_id", "body" FROM "jobs" ORDER BY "job_id" LIMIT $1 FOR UPDATE SKIP LOCKED"#
        )
    );
    let leased = prepared(RESHAPED, &LEASED, Shape::default(), &none)?;
    assert!(
        leased
            .claim
            .is_some_and(|claim| claim.sql.starts_with("/* Lease */ WITH __claimed AS")),
        "{:?}",
        leased.claim
    );
    assert_eq!(
        leased.claim.map(|claim| claim.params),
        Some([Param::LeaseNow, Param::Limit, Param::Lease].as_slice())
    );
    // The advisory lock form claims its candidates with their keys.
    let advised = prepared(RESHAPED, &ADVISED, Shape::default(), &none)?;
    assert_eq!(
        advised.claim.map(|claim| claim.sql),
        Some(
            r#"/* Advisory */ SELECT "job_id", "__lock" FROM (SELECT "job_id", concat('jobs-', "job_id") AS "__lock" FROM "jobs" ORDER BY "job_id" OFFSET 0) AS __candidates WHERE pg_try_advisory_xact_lock_shared(hashtextextended("__lock", 0)) LIMIT $1"#
        )
    );
    Ok(())
}

#[test]
fn each_form_opens_its_claim_with_the_statement_of_its_trait() -> Result<(), StatementError> {
    // The row lock claim's transaction opens at the table's opening, as the dialect opens it.
    assert_eq!(
        form_of(RESHAPED, &super::JOBS).begin_claim(&super::JOBS)?,
        Some("BEGIN /* Dialect */")
    );
    let serializable = super::JOBS.isolation(Isolation::Serializable);
    assert_eq!(
        form_of(RESHAPED, &serializable).begin_claim(&serializable)?,
        Some("BEGIN ISOLATION LEVEL SERIALIZABLE /* Dialect */")
    );
    // The lease claim's own short transaction opens as the lease form opens it, whatever the
    // table's opening.
    let leased = LEASED.isolation(Isolation::Serializable);
    assert_eq!(
        form_of(RESHAPED, &leased).begin_claim(&leased)?,
        Some("BEGIN /* Lease */")
    );
    // The candidates of an advisory claim are selected outside any transaction of the crate's.
    let advised = ADVISED.isolation(Isolation::Serializable);
    assert_eq!(form_of(RESHAPED, &advised).begin_claim(&advised)?, None);
    Ok(())
}

#[test]
fn a_table_at_an_opening_its_dialect_lacks_is_refused_whatever_its_form() {
    // The lease claim does not open at the table's opening, and still the subscription stops
    // when it starts: a level the dialect lacks is no level its transactions run at.
    for spec in [super::JOBS, LEASED, ADVISED] {
        let uncommitted = spec.isolation(Isolation::ReadUncommitted);
        assert_eq!(
            form_of(RESHAPED, &uncommitted).begin_claim(&uncommitted),
            Err(StatementError::UnsupportedOpening {
                dialect: "reshaped",
                opening: "isolation `read_uncommitted`",
            }),
            "{}",
            spec.form().name()
        );
    }
}

#[test]
fn an_advisory_table_takes_in_one_statement_or_two() -> Result<(), SqlxBrokerError> {
    let none = RetryDeclaration::new();
    let one = prepared(RESHAPED, &ADVISED, Shape::default(), &none)?;
    let taken = Postgres
        .take(&ADVISED, ClaimShape::Rows)
        .expect("Postgres takes the jobs");
    assert_eq!(one.take.map(|take| take.sql), Some(taken[0].sql()));
    assert_eq!(one.take_then, None);
    assert_eq!(
        one.lock.map(|lock| lock.params),
        Some([Param::Key].as_slice())
    );
    assert_eq!(
        one.unlock.map(|unlock| unlock.params),
        Some([Param::Key].as_slice())
    );
    // The take counts the attempt, so a retry writes nothing.
    assert_eq!(one.retry, None);
    let two = Reshaped {
        parts: 2,
        ..RESHAPED
    };
    let split = prepared(two, &ADVISED, Shape::default(), &none)?;
    assert!(split.take.is_some() && split.take_then.is_some());
    let three = Reshaped {
        parts: 3,
        ..RESHAPED
    };
    let refused = prepared(three, &ADVISED, Shape::default(), &none)
        .map_or_else(|error| error.to_string(), |_| String::new());
    assert!(
        refused.contains(
            "the reshaped dialect takes a candidate in 3 statements, and the inbox runs one \
             or two"
        ),
        "{refused}"
    );
    // A table in another form prepares no statement of the advisory lock form.
    let locked = prepared(RESHAPED, &super::JOBS, Shape::default(), &none)?;
    assert!(
        locked.lock.is_none()
            && locked.unlock.is_none()
            && locked.take.is_none()
            && locked.take_then.is_none()
    );
    Ok(())
}

#[test]
fn an_advisory_delivery_carries_the_counted_attempt_where_its_take_cannot_name_it() {
    let form = form_of(RESHAPED, &ADVISED);
    let counted = |spec: &TableSpec<'static>, claim| {
        counted_attempt(
            &form,
            &described(spec, Shape::default(), claim),
            &Prepared::default(),
        )
    };
    // The take reads named columns as they were before its count.
    assert!(!counted(&ADVISED, ClaimShape::Rows));
    assert!(!counted(&ADVISED, ClaimShape::Roles));
    // `*` names no column, and the service's fetch reads the row after the count.
    assert!(counted(&ADVISED.selecting_all(), ClaimShape::Rows));
    assert!(counted(&ADVISED, ClaimShape::Ids));
    // Without an attempt the take counts nothing.
    let uncounted = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(KEY))
        .payload(Column::new("body"));
    assert!(!counted(&uncounted, ClaimShape::Ids));
    assert!(!counted(&uncounted.selecting_all(), ClaimShape::Rows));
}

#[test]
fn a_lease_table_seen_through_another_form_stops_the_subscription() {
    // A description written by hand that pairs a lease table with the row lock form: the
    // service claims itself, so only the lease's own statements can refuse it.
    let own = Shape {
        custom_claim: true,
        custom_fetch: true,
        ..Shape::default()
    };
    let fail = |reason: String| SqlxBrokerError::Declaration {
        subscription: "jobs".to_owned(),
        table: "jobs".to_owned(),
        row: "Job",
        reason,
    };
    let refused = build(
        &FormDialect::RowLock(Arc::new(RESHAPED)),
        &RetryDeclaration::new(),
        &described(&LEASED, own, ClaimShape::Ids),
        &fail,
    );
    assert!(
        matches!(
            refused,
            Err(SqlxBrokerError::Dialect {
                source: StatementError::UnsupportedForm {
                    dialect: "reshaped",
                    form: "lease",
                },
                ..
            })
        ),
        "{refused:?}"
    );
}

#[test]
fn a_dead_letter_moves_in_one_statement_or_two() -> Result<(), SqlxBrokerError> {
    let dead = RetryDeclaration::new().with_dead_letter("jobs_dead");
    let moved = prepared(RESHAPED, &LEASED, Shape::default(), &dead)?;
    assert!(moved.dead_letter.is_some() && moved.dead_letter_then.is_none());
    let two = Reshaped {
        parts: 2,
        ..RESHAPED
    };
    let split = prepared(two, &LEASED, Shape::default(), &dead)?;
    assert!(split.dead_letter.is_some() && split.dead_letter_then.is_some());
    let three = Reshaped {
        parts: 3,
        ..RESHAPED
    };
    let refused = prepared(three, &LEASED, Shape::default(), &dead)
        .map_or_else(|error| error.to_string(), |_| String::new());
    assert!(
        refused.contains(
            "the reshaped dialect moves a dead letter in 3 statements, and the inbox runs \
             one or two"
        ),
        "{refused}"
    );
    Ok(())
}

#[test]
fn a_lease_table_stamps_where_its_claim_only_selects() -> Result<(), SqlxBrokerError> {
    let none = RetryDeclaration::new();
    let leased = prepared(RESHAPED, &LEASED, Shape::default(), &none)?;
    assert!(
        leased.extend.is_some(),
        "a lease table can extend its leases"
    );
    assert!(!leased.stamps && leased.stamp.is_none());
    // A claim of the service's own leases nothing: the crate stamps what it takes.
    let own = Shape {
        custom_claim: true,
        ..Shape::default()
    };
    let claimed = prepared(RESHAPED, &LEASED, own, &none)?;
    assert!(claimed.stamps && claimed.stamp.is_some());
    let selecting = Reshaped {
        writes_lease: false,
        ..RESHAPED
    };
    let stamped = prepared(selecting, &LEASED, Shape::default(), &none)?;
    assert!(stamped.stamps && stamped.stamp.is_some());
    // The service's own extension needs no statement of the crate's.
    let extending = Shape {
        custom_extend: true,
        ..Shape::default()
    };
    assert!(
        prepared(RESHAPED, &LEASED, extending, &none)?
            .extend
            .is_none()
    );
    // A row lock table neither extends nor stamps.
    let selecting = Reshaped {
        writes_lease: false,
        ..RESHAPED
    };
    let locked = prepared(selecting, &super::JOBS, Shape::default(), &none)?;
    assert!(locked.extend.is_none() && !locked.stamps && locked.stamp.is_none());
    Ok(())
}
