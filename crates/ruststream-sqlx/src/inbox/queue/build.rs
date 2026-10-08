//! The statements a subscription to a table builds with the dialect its form shows: the claim,
//! the settlements, the guard of a FIFO group and each form's own, and whether its rows carry a
//! counted attempt.

use ruststream::RetryDeclaration;
use ruststream_sqlx_dialect::{ClaimShape, Dialect, Role, Statement, StatementError, TableName};

use super::description::Description;
use crate::inbox::FormDialect;
use crate::inbox::engine::{Prepared, intern};
use crate::inbox::error::SqlxBrokerError;

/// The statements a subscription to the table `description` reads runs, built by the dialect
/// `form` shows: the claim and the statements of the lease and advisory lock forms by the trait of
/// the table's form, the guard of a FIFO group and the settlements by the dialect itself.
pub(super) fn build(
    form: &FormDialect,
    declaration: &RetryDeclaration,
    description: &Description,
    fail: &impl Fn(String) -> SqlxBrokerError,
) -> Result<Prepared, SqlxBrokerError> {
    let dialect = form.dialect();
    let spec = description.spec;
    let shape = description.shape;
    let refused = |source| SqlxBrokerError::Dialect {
        subscription: String::new(),
        table: String::new(),
        row: "",
        source,
    };
    let claim = (!shape.custom_claim)
        .then(|| form.claim(&spec, description.claim))
        .transpose()
        .map_err(refused)?;
    // Whoever writes the claim: a claim of the service's own takes its rows in the transaction the
    // guard took the group in, as the crate's does.
    let fifo_guard = dialect.fifo_guard(&spec).map_err(refused)?;
    let fetch = (shape.custom_claim && !shape.custom_fetch)
        .then(|| dialect.fetch(&spec))
        .transpose()
        .map_err(refused)?;
    let ack = (!shape.custom_ack)
        .then(|| dialect.ack(&spec))
        .transpose()
        .map_err(refused)?;
    let retry = if shape.custom_retry {
        None
    } else {
        dialect.retry(&spec).map_err(refused)?
    };
    let retry_after = (!shape.custom_retry_after && spec.column(Role::RetryAfter).is_some())
        .then(|| dialect.retry_after(&spec))
        .transpose()
        .map_err(refused)?;
    let discard = (!shape.custom_discard)
        .then(|| dialect.discard(&spec))
        .transpose()
        .map_err(refused)?;
    let (dead_letter, dead_letter_then) = match declaration.dead_letter() {
        Some(_) if shape.custom_dead_letter => (None, None),
        Some(_) if spec.column(Role::Group).is_some() => (
            Some(dialect.dead_letter_group(&spec).map_err(refused)?),
            None,
        ),
        Some(target) => {
            let target = TableName::parse(target)
                .map_err(|err| fail(format!("the dead-letter table {err}")))?;
            let moves = dialect.dead_letter_table(&spec, target).map_err(refused)?;
            let (first, then) = one_or_two(dialect, "moves a dead letter", moves, fail)?;
            (Some(first), then)
        }
        None => (None, None),
    };
    // The advisory lock form's own statements: the lock and the unlock where the database keeps
    // the locks and the service runs none of its own, and the take of a candidate whose lock the
    // delivery's session holds.
    let (lock, unlock, take, take_then) = match form.advisory() {
        Some(advisory) => {
            let takes = advisory.take(&spec, description.claim).map_err(refused)?;
            let (take, then) = one_or_two(dialect, "takes a candidate", takes, fail)?;
            let lock = advisory.lock().filter(|_| !shape.custom_lock);
            let unlock = advisory.unlock().filter(|_| !shape.custom_unlock);
            (lock, unlock, Some(take), then)
        }
        None => (None, None, None, None),
    };
    // Why a startup refusal: the derive gives a table that declares a lease the lease form's
    // type, so only a description written by hand pairs one with another form's dialect.
    let lease = match (description.leased(), form.lease()) {
        (false, _) => None,
        (true, Some(lease)) => Some(lease),
        (true, None) => {
            return Err(refused(StatementError::UnsupportedForm {
                dialect: dialect.name(),
                form: spec.form().name(),
            }));
        }
    };
    // A claim of the service's own, or one the dialect only selects with, leaves each row to a
    // stamp of the crate's inside the claim's transaction.
    let stamps = lease.is_some_and(|lease| shape.custom_claim || !lease.claim_writes_lease());
    let extend = lease
        .filter(|_| !shape.custom_extend)
        .map(|lease| lease.extend(&spec))
        .transpose()
        .map_err(refused)?;
    let stamp = lease
        .filter(|_| stamps)
        .map(|lease| lease.stamp(&spec))
        .transpose()
        .map_err(refused)?;
    Ok(Prepared {
        fifo_guard: fifo_guard.as_ref().map(intern),
        claim: claim.as_ref().map(intern),
        fetch: fetch.as_ref().map(intern),
        ack: ack.as_ref().map(intern),
        retry: retry.as_ref().map(intern),
        retry_after: retry_after.as_ref().map(intern),
        discard: discard.as_ref().map(intern),
        dead_letter: dead_letter.as_ref().map(intern),
        dead_letter_then: dead_letter_then.as_ref().map(intern),
        extend: extend.as_ref().map(intern),
        stamp: stamp.as_ref().map(intern),
        lock: lock.as_ref().map(intern),
        unlock: unlock.as_ref().map(intern),
        take: take.as_ref().map(intern),
        take_then: take_then.as_ref().map(intern),
        stamps,
        // The mode's own texts, which `open` sets for the subscription's mode.
        ..Prepared::default()
    })
}

/// The one or two statements `dialect` builds to do `what`, the second run after the first.
///
/// # Errors
///
/// `fail`'s error where the dialect builds none or more than two.
fn one_or_two(
    dialect: &dyn Dialect,
    what: &str,
    statements: Vec<Statement>,
    fail: &impl Fn(String) -> SqlxBrokerError,
) -> Result<(Statement, Option<Statement>), SqlxBrokerError> {
    let count = statements.len();
    let mut statements = statements.into_iter();
    match (statements.next(), statements.next(), statements.next()) {
        (Some(first), then, None) => Ok((first, then)),
        _ => Err(fail(format!(
            "the {} dialect {what} in {count} statements, and the inbox runs one or two",
            dialect.name(),
        ))),
    }
}

/// Whether the rows a subscription to `description` hands out carry the attempt its claim or its
/// take counted, so that a delivery reports one less.
///
/// A lease claim that stamps its rows reads them before the stamps count them. A lease claim that
/// writes the lease itself counts and commits first: the service's own fetch after the crate's
/// claim of ids then reads counted rows, and whole rows come back counted where the dialect says
/// so. A claim by role reads the attempt as it was before the count. The take of the advisory lock
/// form reads the columns it names as they were before its count; `*` names none, and the
/// service's own fetch reads the row after the take committed its count.
pub(super) fn counted_attempt(
    form: &FormDialect,
    description: &Description,
    prepared: &Prepared,
) -> bool {
    if description.advisory() {
        let counted = match description.claim {
            ClaimShape::Rows => description.spec.selects_all(),
            ClaimShape::Ids => true,
            ClaimShape::Roles => false,
        };
        return counted && description.spec.column(Role::Attempt).is_some();
    }
    let Some(lease) = form.lease().filter(|_| description.leased()) else {
        return false;
    };
    if prepared.stamps {
        return false;
    }
    match description.claim {
        ClaimShape::Rows => lease.claim_counts_attempt(&description.spec),
        ClaimShape::Ids => true,
        ClaimShape::Roles => false,
    }
}

#[cfg(test)]
mod tests {
    //! What a subscription's statements are, as the dialect its form shows builds them.

    #[cfg(feature = "postgres")]
    mod reshaped {
        //! `build` against a dialect of a service's own, which may split a dead-letter move or a
        //! take, or claim leased rows by selecting them alone.

        use std::sync::Arc;

        use ruststream::RetryDeclaration;
        use ruststream_sqlx_dialect::{
            Advisory, ClaimShape, Column, Form, Param, Postgres, StatementError, TableSpec,
        };

        use crate::inbox::FormDialect;
        use crate::inbox::engine::{Prepared, Shape};
        use crate::inbox::error::SqlxBrokerError;
        use crate::inbox::form::tests::JOBS;
        use crate::inbox::form::tests::reshaped::{
            ADVISED, KEY, LEASED, RESHAPED, Reshaped, form_of,
        };
        use crate::inbox::queue::build::{build, counted_attempt};
        use crate::inbox::queue::description::tests::described;

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
            let locked = prepared(RESHAPED, &JOBS, Shape::default(), &none)?;
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
            let locked = prepared(RESHAPED, &JOBS, Shape::default(), &none)?;
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
            let locked = prepared(selecting, &JOBS, Shape::default(), &none)?;
            assert!(locked.extend.is_none() && !locked.stamps && locked.stamp.is_none());
            Ok(())
        }
    }

    #[cfg(any(feature = "postgres", feature = "mysql"))]
    mod guarded {
        //! What a subscription to a table with FIFO groups prepares through the dialect built into
        //! the crate for its database: the guard its claims take the group with, as the dialect the
        //! broker picked builds it.

        use std::sync::Arc;

        use ruststream::RetryDeclaration;
        #[cfg(feature = "mysql")]
        use ruststream_sqlx_dialect::MySql;
        #[cfg(feature = "postgres")]
        use ruststream_sqlx_dialect::Postgres;
        use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, Param, TableSpec};

        #[cfg(all(feature = "any", feature = "postgres", feature = "mysql"))]
        use crate::inbox::AnyDialect;
        #[cfg(all(feature = "any", feature = "postgres", feature = "mysql"))]
        use crate::inbox::database::built_in::{MYSQL_BACKEND, POSTGRES_BACKEND};
        use crate::inbox::engine::Shape;
        use crate::inbox::error::SqlxBrokerError;
        #[cfg(feature = "postgres")]
        use crate::inbox::form::tests::JOBS;
        use crate::inbox::form::{FormOn, LeaseForm, RowLockForm};
        use crate::inbox::queue::build::build;
        use crate::inbox::queue::description::tests::described;
        use crate::inbox::{BuiltIn, FormDialect};

        /// A ledger whose accounts keep their order, in the row lock form.
        const LEDGER: TableSpec<'static> =
            TableSpec::new("ledger", Column::new("id"), Form::RowLock)
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
            assert_eq!(prepared(&locked, &JOBS), None);
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

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    mod advised {
        //! What a subscription to a table in the advisory lock form prepares through the dialect
        //! built into the crate for its database: the candidates, the lock and the unlock where the
        //! database keeps the locks, and the take, as the dialect the broker picked builds them.

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
        use crate::inbox::database::built_in::{MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};
        use crate::inbox::engine::{Prepared, Shape, Stmt};
        use crate::inbox::error::SqlxBrokerError;
        use crate::inbox::form::{AdvisoryForm, FormOn};
        use crate::inbox::queue::build::build;
        use crate::inbox::queue::description::tests::described;
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
            let form =
                <AdvisoryForm as FormOn<BuiltIn<DB>>>::erase(&Arc::new(BuiltIn::new(picked)));
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
}
