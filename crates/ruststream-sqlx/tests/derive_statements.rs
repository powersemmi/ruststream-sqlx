//! The statements Postgres builds from what `#[derive(Inbox)]` reads.

#![cfg(all(feature = "inbox", feature = "postgres"))]

use std::time::SystemTime;

use ruststream_sqlx::Inbox;
use ruststream_sqlx::InboxRow;
use ruststream_sqlx::dialect::{ClaimShape, Dialect, Postgres, StatementError};

#[derive(Inbox)]
#[inbox(table = "email_jobs", schema = "app")]
struct SendEmail {
    #[field(id)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(priority)]
    priority: i16,
    #[field(retry_after)]
    retry_after: SystemTime,
    #[field(attempt)]
    attempt: i16,
    #[field(processed_at)]
    processed_at: Option<SystemTime>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[test]
fn the_derived_table_claims_in_role_order() -> Result<(), StatementError> {
    let claim = Postgres.claim(&SendEmail::SPEC, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"SELECT "job_id", "name", "priority", "retry_after", "attempt", "processed_at", "payload" FROM "app"."email_jobs" WHERE "name" = $1 AND "retry_after" <= $2 AND "processed_at" IS NULL ORDER BY "priority", "retry_after", "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED"#
    );
    Ok(())
}

/// Names a database folds or splits unless they are quoted: case, spaces, an embedded quote.
#[derive(Inbox)]
#[inbox(table = "Email \"Jobs\"", schema = "Mail Box")]
#[sqlx(rename_all = "PascalCase")]
struct Quoted {
    #[field(id)]
    job_id: i64,
    #[sqlx(rename = "pay load")]
    #[field(payload)]
    payload: Vec<u8>,
}

#[test]
fn names_that_need_quoting_reach_the_statement_intact() -> Result<(), StatementError> {
    let ack = Postgres.ack(&Quoted::SPEC)?;
    assert_eq!(
        ack.sql(),
        r#"DELETE FROM "Mail Box"."Email ""Jobs""" WHERE "JobId" = $1"#
    );
    let fetch = Postgres.fetch(&Quoted::SPEC)?;
    assert_eq!(
        fetch.sql(),
        r#"SELECT "JobId", "pay load" FROM "Mail Box"."Email ""Jobs""" WHERE "JobId" = ANY($1)"#
    );
    Ok(())
}
