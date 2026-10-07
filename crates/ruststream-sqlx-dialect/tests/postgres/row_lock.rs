//! The statements the Postgres dialect builds for the row lock form.

use std::error::Error;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, KeyPart, Param, Postgres, Role, RowLock, Statement,
    StatementError, TableName, TableSpec,
};

use crate::BARE;

/// Every role the row lock form reads, in a table inside a schema.
const EMAILS: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
        .within("app")
        .group(Column::new("name"))
        .priority(Column::new("priority"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

const EMAIL_SELECT: &str =
    r#""job_id", "name", "priority", "retry_after", "attempt", "processed_at", "payload""#;

const JOB_KEY: &[KeyPart<'static>] = &[KeyPart::Column("job_id")];

#[test]
fn the_claim_reads_every_role_it_has() -> Result<(), StatementError> {
    let claim = Postgres.lock_claim(&EMAILS, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        format!(
            r#"SELECT {EMAIL_SELECT} FROM "app"."email_jobs" WHERE "name" = $1 AND "retry_after" <= $2 AND "processed_at" IS NULL ORDER BY "priority", "retry_after", "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED"#
        )
    );
    assert_eq!(claim.params(), [Param::Group, Param::Now, Param::Limit]);
    Ok(())
}

#[test]
fn a_claim_for_a_custom_fetch_selects_only_ids() -> Result<(), StatementError> {
    let claim = Postgres.lock_claim(&EMAILS, ClaimShape::Ids)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "job_id" FROM "app"."email_jobs" WHERE "name" = $1 AND "retry_after" <= $2 AND "processed_at" IS NULL ORDER BY "priority", "retry_after", "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED"#
    );
    Ok(())
}

#[test]
fn a_claim_without_conditions_orders_by_id() -> Result<(), StatementError> {
    let claim = Postgres.lock_claim(&BARE, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "job_id", "payload" FROM "jobs" ORDER BY "job_id" LIMIT $1 FOR UPDATE SKIP LOCKED"#
    );
    assert_eq!(claim.params(), [Param::Limit]);
    Ok(())
}

#[test]
fn a_claim_of_a_flattening_struct_selects_everything() -> Result<(), StatementError> {
    let spec = BARE.selecting_all();
    let claim = Postgres.lock_claim(&spec, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT * FROM "jobs" ORDER BY "job_id" LIMIT $1 FOR UPDATE SKIP LOCKED"#
    );
    Ok(())
}

#[test]
fn a_claim_by_role_names_each_column_by_its_role() -> Result<(), StatementError> {
    const KEYED: TableSpec<'static> =
        TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
            .group(Column::new("name"))
            .partition_key(Column::new("customer"))
            .retry_after(Column::new("retry_after"))
            .attempt(Column::new("attempt"))
            .headers(Column::new("meta"))
            .payload(Column::new("payload"))
            .data(&[Column::new("subject")]);
    let claim = Postgres.lock_claim(&KEYED, ClaimShape::Roles)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "job_id" AS "id", "customer" AS "partition_key", "attempt" AS "attempt", "meta" AS "headers", "payload" AS "payload" FROM "email_jobs" WHERE "name" = $1 AND "retry_after" <= $2 ORDER BY "retry_after", "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED"#,
    );
    assert_eq!(claim.params(), [Param::Group, Param::Now, Param::Limit]);
    // A struct that flattens still names its role columns one by one.
    let flat = Postgres.lock_claim(&KEYED.selecting_all(), ClaimShape::Roles)?;
    assert!(
        flat.sql().starts_with(r#"SELECT "job_id" AS "id", "#),
        "{}",
        flat.sql()
    );
    Ok(())
}

#[test]
fn names_that_need_quoting_survive_into_statements() -> Result<(), StatementError> {
    let spec = TableSpec::new("Email Jobs", Column::new("Job Id"), Form::RowLock)
        .within("Mail")
        .payload(Column::new(r#"pay"load"#));
    let claim = Postgres.lock_claim(&spec, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "Job Id", "pay""load" FROM "Mail"."Email Jobs" ORDER BY "Job Id" LIMIT $1 FOR UPDATE SKIP LOCKED"#
    );
    Ok(())
}

#[test]
fn the_row_lock_claim_refuses_tables_of_other_forms() {
    let advisory = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(JOB_KEY));
    assert_eq!(
        Postgres.lock_claim(&advisory, ClaimShape::Rows),
        Err(StatementError::FormMismatch {
            statement: "lock_claim",
            form: "advisory lock",
        })
    );
    let leased = TableSpec::new(
        "jobs",
        Column::new("job_id"),
        Form::Lease(Column::new("locked_until")),
    );
    assert_eq!(
        Postgres.lock_claim(&leased, ClaimShape::Rows),
        Err(StatementError::FormMismatch {
            statement: "lock_claim",
            form: "lease",
        })
    );
}

#[test]
fn the_fetch_reads_claimed_ids_as_one_list() -> Result<(), StatementError> {
    let fetch = Postgres.fetch(&EMAILS)?;
    assert_eq!(
        fetch.sql(),
        format!(r#"SELECT {EMAIL_SELECT} FROM "app"."email_jobs" WHERE "job_id" = ANY($1)"#)
    );
    assert_eq!(fetch.params(), [Param::Ids]);
    Ok(())
}

#[test]
fn ack_and_discard_mark_a_row_when_the_table_keeps_finished_rows() -> Result<(), StatementError> {
    for statement in [Postgres.ack(&EMAILS)?, Postgres.discard(&EMAILS)?] {
        assert_eq!(
            statement.sql(),
            r#"UPDATE "app"."email_jobs" SET "processed_at" = $1 WHERE "job_id" = $2"#
        );
        assert_eq!(statement.params(), [Param::Now, Param::Id]);
    }
    Ok(())
}

#[test]
fn ack_and_discard_delete_a_row_otherwise() -> Result<(), StatementError> {
    for statement in [Postgres.ack(&BARE)?, Postgres.discard(&BARE)?] {
        assert_eq!(statement.sql(), r#"DELETE FROM "jobs" WHERE "job_id" = $1"#);
        assert_eq!(statement.params(), [Param::Id]);
    }
    Ok(())
}

#[test]
fn a_retry_counts_the_attempt_when_the_table_has_one() -> Result<(), StatementError> {
    let retry = Postgres.retry(&EMAILS)?;
    assert_eq!(
        retry.as_ref().map(Statement::sql),
        Some(r#"UPDATE "app"."email_jobs" SET "attempt" = "attempt" + 1 WHERE "job_id" = $1"#)
    );
    assert_eq!(
        retry.as_ref().map(Statement::params),
        Some([Param::Id].as_slice())
    );
    assert_eq!(Postgres.retry(&BARE)?, None);
    Ok(())
}

#[test]
fn a_delayed_retry_sets_the_time_and_counts_the_attempt() -> Result<(), StatementError> {
    let retry_after = Postgres.retry_after(&EMAILS)?;
    assert_eq!(
        retry_after.sql(),
        r#"UPDATE "app"."email_jobs" SET "retry_after" = $1, "attempt" = "attempt" + 1 WHERE "job_id" = $2"#
    );
    assert_eq!(retry_after.params(), [Param::RetryAfter, Param::Id]);
    assert_eq!(
        Postgres.retry_after(&BARE),
        Err(StatementError::MissingRole {
            statement: "retry_after",
            role: Role::RetryAfter,
        })
    );
    Ok(())
}

#[test]
fn dead_letters_move_to_another_group() -> Result<(), StatementError> {
    let dead_letter = Postgres.dead_letter_group(&EMAILS)?;
    assert_eq!(
        dead_letter.sql(),
        r#"UPDATE "app"."email_jobs" SET "name" = $1 WHERE "job_id" = $2"#
    );
    assert_eq!(dead_letter.params(), [Param::Destination, Param::Id]);
    assert_eq!(
        Postgres.dead_letter_group(&BARE),
        Err(StatementError::MissingRole {
            statement: "dead_letter_group",
            role: Role::Group,
        })
    );
    Ok(())
}

#[test]
fn dead_letters_move_to_a_table_in_another_schema() -> Result<(), Box<dyn Error>> {
    let moves = Postgres.dead_letter_table(&EMAILS, TableName::parse("app.jobs_dead")?)?;
    assert_eq!(moves.len(), 1);
    assert_eq!(
        moves[0].sql(),
        format!(
            r#"WITH moved AS (DELETE FROM "app"."email_jobs" WHERE "job_id" = $1 RETURNING {EMAIL_SELECT}) INSERT INTO "app"."jobs_dead" ({EMAIL_SELECT}) SELECT {EMAIL_SELECT} FROM moved"#
        )
    );
    assert_eq!(moves[0].params(), [Param::Id]);
    Ok(())
}

#[test]
fn a_dead_letter_table_without_a_schema_is_one_name() -> Result<(), Box<dyn Error>> {
    let moves =
        Postgres.dead_letter_table(&BARE.selecting_all(), TableName::parse("Jobs Dead")?)?;
    assert_eq!(
        moves[0].sql(),
        r#"WITH moved AS (DELETE FROM "jobs" WHERE "job_id" = $1 RETURNING *) INSERT INTO "Jobs Dead" SELECT * FROM moved"#
    );
    Ok(())
}

#[test]
fn the_database_clock_reads_the_statement_timestamp() -> Result<(), StatementError> {
    let spec = EMAILS.database_clock();
    let claim = Postgres.lock_claim(&spec, ClaimShape::Ids)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "job_id" FROM "app"."email_jobs" WHERE "name" = $1 AND "retry_after" <= statement_timestamp() AND "processed_at" IS NULL ORDER BY "priority", "retry_after", "job_id" LIMIT $2 FOR UPDATE SKIP LOCKED"#
    );
    assert_eq!(claim.params(), [Param::Group, Param::Limit]);

    let ack = Postgres.ack(&spec)?;
    assert_eq!(
        ack.sql(),
        r#"UPDATE "app"."email_jobs" SET "processed_at" = statement_timestamp() WHERE "job_id" = $1"#
    );
    assert_eq!(ack.params(), [Param::Id]);

    let retry_after = Postgres.retry_after(&spec)?;
    assert_eq!(
        retry_after.sql(),
        r#"UPDATE "app"."email_jobs" SET "retry_after" = statement_timestamp() + $1::bigint * interval '1 microsecond', "attempt" = "attempt" + 1 WHERE "job_id" = $2"#
    );
    assert_eq!(retry_after.params(), [Param::Delay, Param::Id]);
    Ok(())
}

/// A ledger whose accounts keep their order: one row of an account in work, taken in claim order.
const LEDGER: TableSpec<'static> = TableSpec::new("ledger", Column::new("id"), Form::RowLock)
    .fifo_group(Column::new("account"))
    .retry_after(Column::new("retry_after"))
    .attempt(Column::new("attempt"))
    .processed_at(Column::new("processed_at"))
    .payload(Column::new("payload"));

#[test]
fn a_fifo_claim_takes_the_head_of_its_group_or_nothing() -> Result<(), Box<dyn Error>> {
    let claim = Postgres.lock_claim(&LEDGER, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "id", "account", "retry_after", "attempt", "processed_at", "payload" FROM "ledger" WHERE "id" = (SELECT "id" FROM "ledger" WHERE "account" = $1 AND "processed_at" IS NULL ORDER BY "retry_after", "id" LIMIT 1) AND "retry_after" <= $2 FOR UPDATE SKIP LOCKED"#,
    );
    assert_eq!(claim.params(), [Param::Group, Param::Now]);
    let ids = Postgres.lock_claim(&LEDGER, ClaimShape::Ids)?;
    assert_eq!(
        ids.sql(),
        r#"SELECT "id" FROM "ledger" WHERE "id" = (SELECT "id" FROM "ledger" WHERE "account" = $1 AND "processed_at" IS NULL ORDER BY "retry_after", "id" LIMIT 1) AND "retry_after" <= $2 FOR UPDATE SKIP LOCKED"#,
    );
    Ok(())
}

/// The ledger of an odd name inside a schema: the guard's key names it as the claim does.
const ODD_LEDGER: TableSpec<'static> = TableSpec::new("it's", Column::new("id"), Form::RowLock)
    .within("app")
    .fifo_group(Column::new("account"));

#[test]
fn a_fifo_claim_takes_its_group_first() -> Result<(), Box<dyn Error>> {
    let guard = Postgres
        .fifo_guard(&LEDGER)?
        .ok_or("a table with FIFO groups has a guard")?;
    assert_eq!(
        guard.sql(),
        "SELECT pg_try_advisory_xact_lock(hashtextextended('ledger:' || $1, 0))::int::bigint",
    );
    assert_eq!(guard.params(), [Param::Group]);
    // The key names the table with its schema, unquoted, and a quote in it doubles.
    let odd = Postgres
        .fifo_guard(&ODD_LEDGER)?
        .ok_or("a table with FIFO groups has a guard")?;
    assert_eq!(
        odd.sql(),
        "SELECT pg_try_advisory_xact_lock(hashtextextended('app.it''s:' || $1, 0))::int::bigint",
    );
    // A table whose groups keep no order needs no guard.
    assert_eq!(Postgres.fifo_guard(&EMAILS)?, None);
    assert_eq!(Postgres.fifo_guard(&BARE)?, None);
    Ok(())
}
