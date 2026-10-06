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

/// `build` against a dialect of a service's own, which may split a dead-letter move or a take,
/// or claim leased rows by selecting them alone.
#[cfg(feature = "postgres")]
mod built {
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
    use crate::inbox::queue::{build, counted_attempt};

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
}

/// What a subscription to a table opens its claims with on MySQL and MariaDB, through the built-in
/// dialect the broker builds its statements with. The servers do not tell a transaction its own
/// level reliably, so the begin statement is what pins it.
#[cfg(feature = "mysql")]
mod on_mysql {
    use std::sync::Arc;

    use ruststream_sqlx_dialect::{
        Column, Form, Isolation, Mode, MySql, StatementError, TableSpec,
    };

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
}

/// What an `AnyPool` refuses when a subscription starts: its database is known only then, so a
/// table may name a level the picked backend lacks.
#[cfg(all(feature = "any", feature = "sqlite"))]
mod on_any {
    use std::sync::Arc;

    use ruststream_sqlx_dialect::{Column, Form, Isolation, Mode, StatementError, TableSpec};
    use sqlx::Any;

    use crate::inbox::database::SQLITE_BACKEND;
    use crate::inbox::form::{FormOn, LeaseForm};
    use crate::inbox::{AnyDialect, BuiltIn};

    const LEASED: TableSpec<'static> = TableSpec::new(
        "jobs",
        Column::new("job_id"),
        Form::Lease(Column::new("locked_until")),
    )
    .payload(Column::new("body"));

    #[test]
    fn a_sqlite_backend_refuses_an_isolation_level_whatever_the_form() -> Result<(), StatementError>
    {
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
}

/// What a subscription to a table with FIFO groups prepares through the dialect built into the
/// crate for its database: the guard its claims take the group with, as the dialect the broker
/// picked builds it.
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod guarded {
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
    use crate::inbox::database::{MYSQL_BACKEND, POSTGRES_BACKEND};
    use crate::inbox::engine::Shape;
    use crate::inbox::error::SqlxBrokerError;
    use crate::inbox::form::{FormOn, LeaseForm, RowLockForm};
    use crate::inbox::queue::build;
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
}

/// What a subscription to a table in the advisory lock form prepares through the dialect built into
/// the crate for its database: the candidates, the lock and the unlock where the database keeps the
/// locks, and the take, as the dialect the broker picked builds them.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
mod advised {
    use std::sync::Arc;

    use ruststream::RetryDeclaration;
    #[cfg(feature = "mysql")]
    use ruststream_sqlx_dialect::MySql;
    #[cfg(feature = "postgres")]
    use ruststream_sqlx_dialect::Postgres;
    #[cfg(feature = "sqlite")]
    use ruststream_sqlx_dialect::Sqlite;
    use ruststream_sqlx_dialect::{
        Advisory, ClaimShape, Column, Form, KeyPart, Param, Statement, TableSpec,
    };

    use super::described;
    #[cfg(all(
        feature = "any",
        feature = "postgres",
        feature = "mysql",
        feature = "sqlite"
    ))]
    use crate::inbox::AnyDialect;
    #[cfg(all(
        feature = "any",
        feature = "postgres",
        feature = "mysql",
        feature = "sqlite"
    ))]
    use crate::inbox::database::{MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};
    use crate::inbox::engine::{Prepared, Shape, Stmt};
    use crate::inbox::error::SqlxBrokerError;
    use crate::inbox::form::{AdvisoryForm, FormOn};
    use crate::inbox::queue::build;
    use crate::inbox::{BuiltIn, BuiltInDialect, FormDialect};

    /// The lock key of every email: its id.
    const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("emails-"), KeyPart::Column("job_id")];

    /// The emails in the advisory lock form: a group per name, a delayed retry and an attempt
    /// the take counts.
    const EMAILS: TableSpec<'static> =
        TableSpec::new("email_jobs", Column::new("job_id"), Form::Advisory(KEY))
            .group(Column::new("name"))
            .retry_after(Column::new("retry_after"))
            .attempt(Column::new("attempt"))
            .payload(Column::new("payload"));

    /// A statement's text and parameters.
    type Text = (String, Vec<Param>);

    fn prepared_text(statement: Stmt) -> Text {
        (statement.sql.to_owned(), statement.params.to_vec())
    }

    fn built_text(statement: &Statement) -> Text {
        (statement.sql().to_owned(), statement.params().to_vec())
    }

    /// What a subscription to the emails prepares, its statements built by the dialect `form`
    /// shows.
    fn prepared(form: &FormDialect) -> Prepared {
        let fail = |reason: String| SqlxBrokerError::Declaration {
            subscription: "emails".to_owned(),
            table: "email_jobs".to_owned(),
            row: "SendEmail",
            reason,
        };
        let described = described(&EMAILS, Shape::default(), ClaimShape::Rows);
        build(form, &RetryDeclaration::new(), &described, &fail)
            .expect("the emails' statements build")
    }

    /// The emails' statements as the built-in dialect of `DB` builds them for a subscription.
    fn built_in<DB: BuiltInDialect>(picked: DB::Picked) -> Prepared {
        let form = <AdvisoryForm as FormOn<BuiltIn<DB>>>::erase(&Arc::new(BuiltIn::new(picked)));
        prepared(&form)
    }

    /// Checks that `prepared` holds what `dialect` itself builds for the emails: the candidates
    /// as the claim, the lock and the unlock, the take, and no retry statement.
    fn holds_what(dialect: &dyn Advisory, prepared: &Prepared) {
        let claim = dialect
            .advisory_claim(&EMAILS)
            .expect("the dialect claims the emails");
        assert_eq!(prepared.claim.map(prepared_text), Some(built_text(&claim)));
        assert_eq!(
            prepared.lock.map(prepared_text),
            dialect.lock().as_ref().map(built_text)
        );
        assert_eq!(
            prepared.unlock.map(prepared_text),
            dialect.unlock().as_ref().map(built_text)
        );
        let take = dialect
            .take(&EMAILS, ClaimShape::Rows)
            .expect("the dialect takes the emails");
        assert_eq!(
            prepared.take.map(prepared_text),
            take.first().map(built_text)
        );
        assert_eq!(
            prepared.take_then.map(prepared_text),
            take.get(1).map(built_text)
        );
        // The take counted the attempt: a retry has nothing left to write.
        assert_eq!(prepared.retry, None);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn the_built_in_postgres_dialect_builds_the_advisory_form() {
        let prepared = built_in::<sqlx::Postgres>(Postgres);
        holds_what(&Postgres, &prepared);
        assert!(prepared.lock.is_some() && prepared.unlock.is_some());
        // An update returns the row it counted: the take is one statement.
        assert!(prepared.take.is_some() && prepared.take_then.is_none());
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn the_built_in_mysql_dialect_builds_the_advisory_form() {
        let prepared = built_in::<sqlx::MySql>(MySql);
        holds_what(&MySql, &prepared);
        assert!(prepared.lock.is_some() && prepared.unlock.is_some());
        // An update returns no rows: the take counts the attempt, then reads the row.
        assert!(prepared.take.is_some() && prepared.take_then.is_some());
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn the_built_in_sqlite_dialect_builds_the_advisory_form() {
        let prepared = built_in::<sqlx::Sqlite>(Sqlite);
        holds_what(&Sqlite, &prepared);
        // The process keeps the locks: no statement takes or releases one.
        assert!(prepared.lock.is_none() && prepared.unlock.is_none());
        assert!(prepared.take.is_some() && prepared.take_then.is_none());
    }

    #[cfg(all(
        feature = "any",
        feature = "postgres",
        feature = "mysql",
        feature = "sqlite"
    ))]
    #[test]
    fn an_any_backend_builds_the_advisory_form_as_its_database_does() {
        let any = |backend| {
            let picked = AnyDialect::of(backend).expect("the backend's feature is on");
            built_in::<sqlx::Any>(picked)
        };
        assert_eq!(any(POSTGRES_BACKEND), built_in::<sqlx::Postgres>(Postgres));
        assert_eq!(any(MYSQL_BACKEND), built_in::<sqlx::MySql>(MySql));
        assert_eq!(any(SQLITE_BACKEND), built_in::<sqlx::Sqlite>(Sqlite));
    }
}
