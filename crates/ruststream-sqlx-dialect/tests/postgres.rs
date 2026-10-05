//! The statements the Postgres dialect builds for the row lock form.

#![cfg(feature = "postgres")]

use std::error::Error;
use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, KeyPart, Param, Postgres, Role, Statement, StatementError,
    TableName, TableSpec,
};

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

/// Only an id and a payload, in the default schema.
const BARE: TableSpec<'static> =
    TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));

const JOB_KEY: &[KeyPart<'static>] = &[KeyPart::Column("job_id")];

fn quoted(ident: &str) -> String {
    let mut out = String::new();
    Postgres.quote_into(ident, &mut out);
    out
}

#[test]
fn names_are_quoted_and_keep_their_case() {
    assert_eq!(quoted("email_jobs"), r#""email_jobs""#);
    assert_eq!(quoted("EmailJobs"), r#""EmailJobs""#);
    assert_eq!(quoted("email jobs"), r#""email jobs""#);
    assert_eq!(quoted(r#"odd"name"#), r#""odd""name""#);
    assert_eq!(quoted(r#""""#), r#""""""""#);
}

#[test]
fn placeholders_count_from_one() {
    let mut out = String::new();
    Postgres.placeholder_into(NonZeroUsize::MIN, &mut out);
    out.push(' ');
    Postgres.placeholder_into(NonZeroUsize::MIN.saturating_add(11), &mut out);
    assert_eq!(out, "$1 $12");
    assert_eq!(Postgres.name(), "postgres");
}

#[test]
fn the_claim_reads_every_role_it_has() -> Result<(), StatementError> {
    let claim = Postgres.claim(&EMAILS, ClaimShape::Rows)?;
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
    let claim = Postgres.claim(&EMAILS, ClaimShape::Ids)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "job_id" FROM "app"."email_jobs" WHERE "name" = $1 AND "retry_after" <= $2 AND "processed_at" IS NULL ORDER BY "priority", "retry_after", "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED"#
    );
    Ok(())
}

#[test]
fn a_claim_without_conditions_orders_by_id() -> Result<(), StatementError> {
    let claim = Postgres.claim(&BARE, ClaimShape::Rows)?;
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
    let claim = Postgres.claim(&spec, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT * FROM "jobs" ORDER BY "job_id" LIMIT $1 FOR UPDATE SKIP LOCKED"#
    );
    Ok(())
}

#[test]
fn names_that_need_quoting_survive_into_statements() -> Result<(), StatementError> {
    let spec = TableSpec::new("Email Jobs", Column::new("Job Id"), Form::RowLock)
        .within("Mail")
        .payload(Column::new(r#"pay"load"#));
    let claim = Postgres.claim(&spec, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "Job Id", "pay""load" FROM "Mail"."Email Jobs" ORDER BY "Job Id" LIMIT $1 FOR UPDATE SKIP LOCKED"#
    );
    Ok(())
}

#[test]
fn the_claim_refuses_what_the_row_lock_form_cannot_do() {
    let lease = TableSpec::new(
        "jobs",
        Column::new("job_id"),
        Form::Lease(Column::new("until")),
    );
    assert_eq!(
        Postgres.claim(&lease, ClaimShape::Rows),
        Err(StatementError::UnsupportedForm {
            dialect: "postgres",
            form: "lease",
        })
    );
    let advisory = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(JOB_KEY));
    assert_eq!(
        Postgres.claim(&advisory, ClaimShape::Rows),
        Err(StatementError::UnsupportedForm {
            dialect: "postgres",
            form: "advisory lock",
        })
    );
    assert_eq!(
        Postgres.claim(&EMAILS.fifo_group(Column::new("name")), ClaimShape::Rows),
        Err(StatementError::UnsupportedFifo {
            dialect: "postgres"
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
fn settlement_refuses_forms_this_dialect_does_not_build() -> Result<(), Box<dyn Error>> {
    let lease = TableSpec::new(
        "jobs",
        Column::new("job_id"),
        Form::Lease(Column::new("until")),
    );
    let unsupported = StatementError::UnsupportedForm {
        dialect: "postgres",
        form: "lease",
    };
    assert_eq!(Postgres.ack(&lease), Err(unsupported.clone()));
    assert_eq!(Postgres.retry(&lease), Err(unsupported.clone()));
    assert_eq!(Postgres.retry_after(&lease), Err(unsupported.clone()));
    assert_eq!(Postgres.discard(&lease), Err(unsupported.clone()));
    assert_eq!(Postgres.dead_letter_group(&lease), Err(unsupported.clone()));
    assert_eq!(
        Postgres.dead_letter_table(&lease, TableName::parse("jobs_dead")?),
        Err(unsupported)
    );
    assert!(
        Postgres.fetch(&lease).is_ok(),
        "a fetch reads rows in every form"
    );
    Ok(())
}
