//! The statements the Postgres dialect builds for the row lock form.

#![cfg(feature = "postgres")]

use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, KeyPart, Param, Postgres, StatementError, TableSpec,
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
