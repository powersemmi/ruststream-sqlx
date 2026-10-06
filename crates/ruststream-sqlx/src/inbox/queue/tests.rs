//! What opening a subscription builds: its description, its lease and its statements.

use std::time::Duration;

use ruststream_sqlx_dialect::{ClaimShape, Column, Form, TableSpec};

use super::{Description, whole_seconds};
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

/// `build` against a dialect of a service's own, which may split a dead-letter move or claim
/// leased rows by selecting them alone.
#[cfg(feature = "postgres")]
mod built {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use ruststream::RetryDeclaration;
    use ruststream_sqlx_dialect::{
        ClaimShape, Column, Dialect, Form, KeyPart, Lease, Param, Postgres, RowLock, Statement,
        StatementError, TableName, TableSpec,
    };

    use super::described;
    use crate::inbox::FormDialect;
    use crate::inbox::engine::{Prepared, Shape};
    use crate::inbox::error::SqlxBrokerError;
    use crate::inbox::queue::build;

    /// The Postgres dialect, its dead-letter move cut into `parts` statements, its lease claim
    /// writing the lease or only selecting the rows, and each form's claim and transaction
    /// opening marked with the trait that built it.
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
    }

    impl RowLock for Reshaped {
        fn lock_claim(
            &self,
            spec: &TableSpec<'_>,
            shape: ClaimShape,
        ) -> Result<Statement, StatementError> {
            marked("RowLock", Postgres.lock_claim(spec, shape))
        }

        fn begin_lock_claim(&self) -> Option<&'static str> {
            Some("BEGIN /* RowLock */")
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

    const LEASED: TableSpec<'static> = TableSpec::new(
        "jobs",
        Column::new("job_id"),
        Form::Lease(Column::new("locked_until")),
    )
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
            _ => FormDialect::Unbuilt(Arc::new(dialect)),
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
        Ok(())
    }

    #[test]
    fn each_form_opens_its_claim_with_the_statement_of_its_trait() {
        assert_eq!(
            form_of(RESHAPED, &super::JOBS).begin_claim(),
            Some("BEGIN /* RowLock */")
        );
        assert_eq!(
            form_of(RESHAPED, &LEASED).begin_claim(),
            Some("BEGIN /* Lease */")
        );
    }

    #[test]
    fn a_form_no_trait_builds_stops_the_subscription() {
        const KEY: &[KeyPart<'static>] = &[KeyPart::Column("job_id")];
        let advisory = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(KEY));
        let refused = prepared(
            RESHAPED,
            &advisory,
            Shape::default(),
            &RetryDeclaration::new(),
        );
        assert!(
            matches!(
                refused,
                Err(SqlxBrokerError::Dialect {
                    source: StatementError::UnsupportedForm {
                        dialect: "reshaped",
                        form: "advisory lock",
                    },
                    ..
                })
            ),
            "{refused:?}"
        );
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
}
