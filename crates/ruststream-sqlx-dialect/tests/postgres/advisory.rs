//! The statements the Postgres dialect builds for the advisory lock form.

use std::error::Error;

use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Column, Dialect, Form, KeyPart, Lease, NameLimit, Param, Postgres,
    Statement, StatementError, TableName, TableSpec,
};

/// `#[inbox(advisory_lock = "jobs-{job_id}")]`.
const JOB_KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")];

/// Every role the advisory form reads; a lock on `jobs-{job_id}` holds a row.
const EMAILS: TableSpec<'static> =
    TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(JOB_KEY))
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

const EMAIL_SELECT: &str =
    r#""job_id", "name", "retry_after", "attempt", "processed_at", "payload""#;

/// The same queue without an attempt to count.
const UNCOUNTED: TableSpec<'static> =
    TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(JOB_KEY))
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after"))
        .payload(Column::new("payload"));

/// Only an id and a payload: rows are deleted when finished.
const BARE: TableSpec<'static> =
    TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(JOB_KEY))
        .payload(Column::new("payload"));

/// A table in another form, for each statement of this one.
fn in_form(form: Form<'static>) -> TableSpec<'static> {
    TableSpec::new("jobs", Column::new("job_id"), form).payload(Column::new("payload"))
}

/// The claim of `BARE` with the key `key`.
fn claim_keyed(key: &'static [KeyPart<'static>]) -> Result<Statement, StatementError> {
    Postgres.advisory_claim(&TableSpec::new(
        "jobs",
        Column::new("job_id"),
        Form::Advisory(key),
    ))
}

#[test]
fn the_candidates_carry_their_keys_and_skip_the_keys_held_elsewhere() -> Result<(), StatementError>
{
    let claim = Postgres.advisory_claim(&EMAILS)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "job_id", "__lock" FROM (SELECT "job_id", concat('jobs-', "job_id") AS "__lock" FROM "jobs" WHERE "name" = $1 AND "retry_after" <= $2 AND "processed_at" IS NULL ORDER BY "retry_after", "job_id" OFFSET 0) AS __candidates WHERE pg_try_advisory_xact_lock_shared(hashtextextended("__lock", 0)) LIMIT $3"#,
    );
    assert_eq!(claim.params(), [Param::Group, Param::Now, Param::Limit]);
    Ok(())
}

#[test]
fn a_claim_without_conditions_orders_the_candidates_by_id() -> Result<(), StatementError> {
    let claim = Postgres.advisory_claim(&BARE.within("app"))?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "job_id", "__lock" FROM (SELECT "job_id", concat('jobs-', "job_id") AS "__lock" FROM "app"."jobs" ORDER BY "job_id" OFFSET 0) AS __candidates WHERE pg_try_advisory_xact_lock_shared(hashtextextended("__lock", 0)) LIMIT $1"#,
    );
    assert_eq!(claim.params(), [Param::Limit]);
    Ok(())
}

#[test]
fn the_database_renders_the_key_from_its_parts() -> Result<(), StatementError> {
    // A key of one column renders without a literal.
    let column = claim_keyed(&[KeyPart::Column("job_id")])?;
    assert!(
        column
            .sql()
            .contains(r#"(SELECT "job_id", concat("job_id") AS "__lock" FROM"#),
        "{}",
        column.sql()
    );
    // A quote in a literal doubles; a backslash stays as it is.
    let quoted = claim_keyed(&[KeyPart::Literal(r"it's-a\b-"), KeyPart::Column("Job Id")])?;
    assert!(
        quoted
            .sql()
            .contains(r#"concat('it''s-a\b-', "Job Id") AS "__lock""#),
        "{}",
        quoted.sql()
    );
    // A key of no parts is the empty text: every row waits for one lock.
    let empty = claim_keyed(&[])?;
    assert!(
        empty.sql().contains(r#"concat('') AS "__lock""#),
        "{}",
        empty.sql()
    );
    Ok(())
}

#[test]
fn the_lock_and_the_unlock_hash_the_key_and_return_one_integer() {
    let lock = Postgres.lock();
    assert_eq!(
        lock.as_ref().map(Statement::sql),
        Some("SELECT pg_try_advisory_lock(hashtextextended($1, 0))::int::bigint")
    );
    assert_eq!(
        lock.as_ref().map(Statement::params),
        Some([Param::Key].as_slice())
    );
    let unlock = Postgres.unlock();
    assert_eq!(
        unlock.as_ref().map(Statement::sql),
        Some("SELECT pg_advisory_unlock(hashtextextended($1, 0))::int::bigint")
    );
    assert_eq!(
        unlock.as_ref().map(Statement::params),
        Some([Param::Key].as_slice())
    );
}

#[test]
fn the_take_counts_the_attempt_and_returns_the_row_as_it_was() -> Result<(), StatementError> {
    let rows = Postgres.take(&EMAILS, ClaimShape::Rows)?;
    assert_eq!(
        rows.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            r#"UPDATE "jobs" SET "attempt" = "attempt" + 1 WHERE "job_id" = $1 AND "name" = $2 AND "retry_after" <= $3 AND "processed_at" IS NULL RETURNING "job_id", "name", "retry_after", "attempt" - 1::smallint AS "attempt", "processed_at", "payload""#,
        ]
    );
    assert_eq!(rows[0].params(), [Param::Id, Param::Group, Param::Now]);
    let ids = Postgres.take(&EMAILS, ClaimShape::Ids)?;
    assert_eq!(
        ids.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            r#"UPDATE "jobs" SET "attempt" = "attempt" + 1 WHERE "job_id" = $1 AND "name" = $2 AND "retry_after" <= $3 AND "processed_at" IS NULL RETURNING "job_id""#,
        ]
    );
    let roles = Postgres.take(&EMAILS, ClaimShape::Roles)?;
    assert_eq!(
        roles.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            r#"UPDATE "jobs" SET "attempt" = "attempt" + 1 WHERE "job_id" = $1 AND "name" = $2 AND "retry_after" <= $3 AND "processed_at" IS NULL RETURNING "job_id" AS "id", "attempt" - 1::smallint AS "attempt", "payload" AS "payload""#,
        ]
    );
    // `*` names no column, so the row returns with the attempt counted.
    let flat = Postgres.take(&EMAILS.selecting_all(), ClaimShape::Rows)?;
    assert_eq!(
        flat.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            r#"UPDATE "jobs" SET "attempt" = "attempt" + 1 WHERE "job_id" = $1 AND "name" = $2 AND "retry_after" <= $3 AND "processed_at" IS NULL RETURNING *"#,
        ]
    );
    Ok(())
}

#[test]
fn a_table_without_an_attempt_takes_by_reading_the_row() -> Result<(), StatementError> {
    let rows = Postgres.take(&UNCOUNTED, ClaimShape::Rows)?;
    assert_eq!(
        rows.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            r#"SELECT "job_id", "name", "retry_after", "payload" FROM "jobs" WHERE "job_id" = $1 AND "name" = $2 AND "retry_after" <= $3"#,
        ]
    );
    assert_eq!(rows[0].params(), [Param::Id, Param::Group, Param::Now]);
    let roles = Postgres.take(&UNCOUNTED, ClaimShape::Roles)?;
    assert_eq!(
        roles[0].sql(),
        r#"SELECT "job_id" AS "id", "payload" AS "payload" FROM "jobs" WHERE "job_id" = $1 AND "name" = $2 AND "retry_after" <= $3"#,
    );
    let ids = Postgres.take(&BARE, ClaimShape::Ids)?;
    assert_eq!(
        ids.iter().map(Statement::sql).collect::<Vec<_>>(),
        [r#"SELECT "job_id" FROM "jobs" WHERE "job_id" = $1"#]
    );
    let flat = Postgres.take(&UNCOUNTED.selecting_all(), ClaimShape::Rows)?;
    assert_eq!(
        flat[0].sql(),
        r#"SELECT * FROM "jobs" WHERE "job_id" = $1 AND "name" = $2 AND "retry_after" <= $3"#,
    );
    Ok(())
}

#[test]
fn the_database_clock_reads_the_statement_timestamp() -> Result<(), StatementError> {
    let spec = EMAILS.database_clock();
    let claim = Postgres.advisory_claim(&spec)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "job_id", "__lock" FROM (SELECT "job_id", concat('jobs-', "job_id") AS "__lock" FROM "jobs" WHERE "name" = $1 AND "retry_after" <= statement_timestamp() AND "processed_at" IS NULL ORDER BY "retry_after", "job_id" OFFSET 0) AS __candidates WHERE pg_try_advisory_xact_lock_shared(hashtextextended("__lock", 0)) LIMIT $2"#,
    );
    assert_eq!(claim.params(), [Param::Group, Param::Limit]);
    let take = Postgres.take(&spec, ClaimShape::Ids)?;
    assert_eq!(
        take[0].sql(),
        r#"UPDATE "jobs" SET "attempt" = "attempt" + 1 WHERE "job_id" = $1 AND "name" = $2 AND "retry_after" <= statement_timestamp() AND "processed_at" IS NULL RETURNING "job_id""#,
    );
    assert_eq!(take[0].params(), [Param::Id, Param::Group]);
    let retry_after = Postgres.retry_after(&spec)?;
    assert_eq!(
        retry_after.sql(),
        r#"UPDATE "jobs" SET "retry_after" = statement_timestamp() + $1::bigint * interval '1 microsecond' WHERE "job_id" = $2"#,
    );
    assert_eq!(retry_after.params(), [Param::Delay, Param::Id]);
    Ok(())
}

#[test]
fn settlement_names_the_row_alone() -> Result<(), Box<dyn Error>> {
    for statement in [Postgres.ack(&EMAILS)?, Postgres.discard(&EMAILS)?] {
        assert_eq!(
            statement.sql(),
            r#"UPDATE "jobs" SET "processed_at" = $1 WHERE "job_id" = $2"#
        );
        assert_eq!(statement.params(), [Param::Now, Param::Id]);
    }
    for statement in [Postgres.ack(&BARE)?, Postgres.discard(&BARE)?] {
        assert_eq!(statement.sql(), r#"DELETE FROM "jobs" WHERE "job_id" = $1"#);
        assert_eq!(statement.params(), [Param::Id]);
    }
    let group = Postgres.dead_letter_group(&EMAILS)?;
    assert_eq!(
        group.sql(),
        r#"UPDATE "jobs" SET "name" = $1 WHERE "job_id" = $2"#
    );
    assert_eq!(group.params(), [Param::Destination, Param::Id]);
    let moves = Postgres.dead_letter_table(&EMAILS, TableName::parse("jobs_dead")?)?;
    assert_eq!(
        moves.iter().map(Statement::sql).collect::<Vec<_>>(),
        [format!(
            r#"WITH moved AS (DELETE FROM "jobs" WHERE "job_id" = $1 RETURNING {EMAIL_SELECT}) INSERT INTO "jobs_dead" ({EMAIL_SELECT}) SELECT {EMAIL_SELECT} FROM moved"#
        )]
    );
    // No lease column to clear, so a row read with `*` moves too.
    let flat = Postgres.dead_letter_table(&BARE.selecting_all(), TableName::parse("jobs_dead")?)?;
    assert_eq!(
        flat[0].sql(),
        r#"WITH moved AS (DELETE FROM "jobs" WHERE "job_id" = $1 RETURNING *) INSERT INTO "jobs_dead" SELECT * FROM moved"#
    );
    Ok(())
}

#[test]
fn a_retry_needs_no_statement_and_a_delayed_one_only_sets_the_time() -> Result<(), StatementError> {
    // The claim counted the attempt, and the unlock frees the row.
    assert_eq!(Postgres.retry(&EMAILS)?, None);
    assert_eq!(Postgres.retry(&BARE)?, None);
    let retry_after = Postgres.retry_after(&EMAILS)?;
    assert_eq!(
        retry_after.sql(),
        r#"UPDATE "jobs" SET "retry_after" = $1 WHERE "job_id" = $2"#
    );
    assert_eq!(retry_after.params(), [Param::RetryAfter, Param::Id]);
    Ok(())
}

#[test]
fn the_advisory_statements_refuse_tables_of_other_forms() {
    let mismatch = |statement, form| StatementError::FormMismatch { statement, form };
    let locked = in_form(Form::RowLock);
    let leased = in_form(Form::Lease(Column::new("locked_until")));
    assert_eq!(
        Postgres.advisory_claim(&locked),
        Err(mismatch("advisory_claim", "row lock"))
    );
    assert_eq!(
        Postgres.advisory_claim(&leased),
        Err(mismatch("advisory_claim", "lease"))
    );
    assert_eq!(
        Postgres.take(&locked, ClaimShape::Rows),
        Err(mismatch("take", "row lock"))
    );
    assert_eq!(
        Postgres.take(&leased, ClaimShape::Ids),
        Err(mismatch("take", "lease"))
    );
    // The lease's statements serve the lease form alone.
    assert_eq!(
        Postgres.extend(&BARE),
        Err(mismatch("extend", "advisory lock"))
    );
    assert_eq!(
        Postgres.stamp(&BARE),
        Err(mismatch("stamp", "advisory lock"))
    );
    assert!(
        Postgres.fetch(&BARE).is_ok(),
        "a fetch reads rows in every form"
    );
}

#[test]
fn a_fifo_group_keeps_its_order_through_the_key_instead() {
    let fifo = EMAILS.fifo_group(Column::new("name"));
    let refused = StatementError::AdvisoryFifo {
        dialect: "postgres",
    };
    assert_eq!(Postgres.advisory_claim(&fifo), Err(refused.clone()));
    assert_eq!(Postgres.take(&fifo, ClaimShape::Rows), Err(refused.clone()));
    assert_eq!(Postgres.fifo_guard(&fifo), Err(refused.clone()));
    assert_eq!(
        refused.to_string(),
        "FIFO groups do not combine with the advisory lock form in the postgres dialect: drop \
         `fifo` and put the group's column into the lock key"
    );
    // A table whose groups keep no order has no guard, in this form as in every other.
    assert_eq!(Postgres.fifo_guard(&EMAILS), Ok(None));
}

#[test]
fn a_key_column_longer_than_63_bytes_is_refused() {
    const LONG: &str = "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn";
    assert_eq!(LONG.len(), 64);
    let refused = StatementError::IdentifierTooLong {
        dialect: "postgres",
        identifier: LONG.to_owned(),
        limit: NameLimit::Bytes(63),
    };
    assert_eq!(claim_keyed(&[KeyPart::Column(LONG)]), Err(refused.clone()));
    let spec = TableSpec::new(
        "jobs",
        Column::new("job_id"),
        Form::Advisory(&[KeyPart::Column(LONG)]),
    );
    assert_eq!(Postgres.take(&spec, ClaimShape::Rows), Err(refused));
}
