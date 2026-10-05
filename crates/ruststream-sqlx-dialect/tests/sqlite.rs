//! The statements the SQLite dialect builds: the lease form, and the forms it refuses.

#![cfg(feature = "sqlite")]

use std::error::Error;
use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, KeyPart, Param, Role, Sqlite, Statement, StatementError,
    TableName, TableSpec,
};

const EXPIRY: Column<'static> = Column::new("locked_until");

/// A lease table with a group, a delayed retry, an attempt and a finish mark.
const LEASED: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::Lease(EXPIRY))
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

/// Only an id, a lease and a payload: rows are deleted when finished.
const BARE: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::Lease(EXPIRY))
    .payload(Column::new("payload"));

/// Every role a claim by role reads, beside a group and a column of data.
const KEYED: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::Lease(EXPIRY))
        .group(Column::new("name"))
        .partition_key(Column::new("customer"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("tries"))
        .headers(Column::new("meta"))
        .payload(Column::new("payload"))
        .data(&[Column::new("subject")]);

/// The same queue in the row lock form, which SQLite has no locks for.
const LOCKED: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .payload(Column::new("payload"));

const JOB_KEY: &[KeyPart<'static>] = &[KeyPart::Column("job_id")];

fn quoted(ident: &str) -> String {
    let mut out = String::new();
    Sqlite.quote_into(ident, &mut out);
    out
}

#[test]
fn names_are_double_quoted_and_keep_their_case() {
    assert_eq!(quoted("email_jobs"), r#""email_jobs""#);
    assert_eq!(quoted("EmailJobs"), r#""EmailJobs""#);
    assert_eq!(quoted("email jobs"), r#""email jobs""#);
    assert_eq!(quoted(r#"a"b"#), r#""a""b""#);
    assert_eq!(quoted("a`b"), r#""a`b""#);
}

#[test]
fn every_placeholder_is_a_question_mark() {
    let mut out = String::new();
    Sqlite.placeholder_into(NonZeroUsize::MIN, &mut out);
    out.push(' ');
    Sqlite.placeholder_into(NonZeroUsize::MIN.saturating_add(11), &mut out);
    assert_eq!(out, "? ?");
    assert_eq!(Sqlite.name(), "sqlite");
}

#[test]
fn the_lease_claim_is_one_update_that_returns_the_rows() -> Result<(), StatementError> {
    assert_eq!(
        Sqlite.claim(&LEASED, ClaimShape::Rows)?.sql(),
        r#"UPDATE "email_jobs" SET "locked_until" = ?, "attempt" = "attempt" + 1 WHERE "job_id" IN (SELECT "job_id" FROM "email_jobs" WHERE "name" = ? AND "retry_after" <= ? AND "processed_at" IS NULL AND ("locked_until" IS NULL OR "locked_until" <= ?) ORDER BY "retry_after", "job_id" LIMIT ?) RETURNING "job_id", "name", "retry_after", "attempt" - 1 AS "attempt", "locked_until", "processed_at", "payload""#,
    );
    assert_eq!(
        Sqlite.claim(&LEASED, ClaimShape::Rows)?.params(),
        [
            Param::Lease,
            Param::Group,
            Param::Now,
            Param::LeaseNow,
            Param::Limit
        ]
    );
    assert!(Sqlite.claim_writes_lease());
    // The returned rows carry the attempt as it was before the claim counted it.
    assert!(!Sqlite.claim_counts_attempt(&LEASED));

    let bare = Sqlite.claim(&BARE, ClaimShape::Rows)?;
    assert_eq!(
        bare.sql(),
        r#"UPDATE "jobs" SET "locked_until" = ? WHERE "job_id" IN (SELECT "job_id" FROM "jobs" WHERE ("locked_until" IS NULL OR "locked_until" <= ?) ORDER BY "job_id" LIMIT ?) RETURNING "job_id", "locked_until", "payload""#,
    );
    assert_eq!(bare.params(), [Param::Lease, Param::LeaseNow, Param::Limit]);
    Ok(())
}

#[test]
fn the_claim_orders_by_priority_and_names_the_schema() -> Result<(), StatementError> {
    let spec = LEASED.within("app").priority(Column::new("priority"));
    let claim = Sqlite.claim(&spec, ClaimShape::Ids)?;
    assert_eq!(
        claim.sql(),
        r#"UPDATE "app"."email_jobs" SET "locked_until" = ?, "attempt" = "attempt" + 1 WHERE "job_id" IN (SELECT "job_id" FROM "app"."email_jobs" WHERE "name" = ? AND "retry_after" <= ? AND "processed_at" IS NULL AND ("locked_until" IS NULL OR "locked_until" <= ?) ORDER BY "priority", "retry_after", "job_id" LIMIT ?) RETURNING "job_id""#,
    );
    assert_eq!(
        claim.params(),
        [
            Param::Lease,
            Param::Group,
            Param::Now,
            Param::LeaseNow,
            Param::Limit
        ]
    );
    Ok(())
}

#[test]
fn a_claim_by_role_returns_the_attempt_before_the_claim() -> Result<(), StatementError> {
    let roles = Sqlite.claim(&KEYED, ClaimShape::Roles)?;
    assert_eq!(
        roles.sql(),
        r#"UPDATE "email_jobs" SET "locked_until" = ?, "tries" = "tries" + 1 WHERE "job_id" IN (SELECT "job_id" FROM "email_jobs" WHERE "name" = ? AND "retry_after" <= ? AND ("locked_until" IS NULL OR "locked_until" <= ?) ORDER BY "retry_after", "job_id" LIMIT ?) RETURNING "job_id" AS "id", "customer" AS "partition_key", "tries" - 1 AS "attempt", "meta" AS "headers", "payload" AS "payload""#,
    );
    assert_eq!(
        roles.params(),
        [
            Param::Lease,
            Param::Group,
            Param::Now,
            Param::LeaseNow,
            Param::Limit
        ]
    );
    // Whole rows name the attempt column under its own name.
    let rows = Sqlite.claim(&KEYED, ClaimShape::Rows)?;
    assert!(
        rows.sql().ends_with(r#"RETURNING "job_id", "name", "customer", "retry_after", "tries" - 1 AS "tries", "locked_until", "meta", "payload", "subject""#),
        "{}",
        rows.sql()
    );
    assert!(!Sqlite.claim_counts_attempt(&KEYED));
    Ok(())
}

#[test]
fn a_struct_that_flattens_reads_every_column_and_the_counted_attempt() -> Result<(), StatementError>
{
    let flat = LEASED.selecting_all();
    let claim = Sqlite.claim(&flat, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"UPDATE "email_jobs" SET "locked_until" = ?, "attempt" = "attempt" + 1 WHERE "job_id" IN (SELECT "job_id" FROM "email_jobs" WHERE "name" = ? AND "retry_after" <= ? AND "processed_at" IS NULL AND ("locked_until" IS NULL OR "locked_until" <= ?) ORDER BY "retry_after", "job_id" LIMIT ?) RETURNING *"#,
    );
    // `*` returns the attempt the claim wrote, so a delivery reports one less.
    assert!(Sqlite.claim_counts_attempt(&flat));
    Ok(())
}

#[test]
fn settlement_names_the_row_and_its_token() -> Result<(), StatementError> {
    for statement in [Sqlite.ack(&LEASED)?, Sqlite.discard(&LEASED)?] {
        assert_eq!(
            statement.sql(),
            r#"UPDATE "email_jobs" SET "processed_at" = ?, "locked_until" = NULL WHERE "job_id" = ? AND "locked_until" = ?"#,
        );
        assert_eq!(statement.params(), [Param::Now, Param::Id, Param::Held]);
    }
    for statement in [Sqlite.ack(&BARE)?, Sqlite.discard(&BARE)?] {
        assert_eq!(
            statement.sql(),
            r#"DELETE FROM "jobs" WHERE "job_id" = ? AND "locked_until" = ?"#,
        );
        assert_eq!(statement.params(), [Param::Id, Param::Held]);
    }
    Ok(())
}

#[test]
fn a_retry_releases_the_lease_and_counts_nothing_more() -> Result<(), StatementError> {
    let retry = Sqlite.retry(&LEASED)?;
    assert_eq!(
        retry.as_ref().map(Statement::sql),
        Some(
            r#"UPDATE "email_jobs" SET "locked_until" = NULL WHERE "job_id" = ? AND "locked_until" = ?"#
        ),
    );
    assert_eq!(
        retry.as_ref().map(Statement::params),
        Some([Param::Id, Param::Held].as_slice())
    );
    let later = Sqlite.retry_after(&LEASED)?;
    assert_eq!(
        later.sql(),
        r#"UPDATE "email_jobs" SET "retry_after" = ?, "locked_until" = NULL WHERE "job_id" = ? AND "locked_until" = ?"#,
    );
    assert_eq!(later.params(), [Param::RetryAfter, Param::Id, Param::Held]);
    assert_eq!(
        Sqlite.retry_after(&BARE),
        Err(StatementError::MissingRole {
            statement: "retry_after",
            role: Role::RetryAfter,
        })
    );
    Ok(())
}

#[test]
fn a_dead_letter_into_a_group_moves_only_a_row_still_held() -> Result<(), StatementError> {
    let group = Sqlite.dead_letter_group(&LEASED)?;
    assert_eq!(
        group.sql(),
        r#"UPDATE "email_jobs" SET "name" = ?, "locked_until" = NULL WHERE "job_id" = ? AND "locked_until" = ?"#,
    );
    assert_eq!(group.params(), [Param::Destination, Param::Id, Param::Held]);
    assert_eq!(
        Sqlite.dead_letter_group(&BARE),
        Err(StatementError::MissingRole {
            statement: "dead_letter_group",
            role: Role::Group,
        })
    );
    Ok(())
}

#[test]
fn a_dead_letter_into_a_table_is_a_copy_then_a_delete() -> Result<(), Box<dyn Error>> {
    let moves = Sqlite.dead_letter_table(&LEASED, TableName::parse("archive.dead_jobs")?)?;
    assert_eq!(
        moves.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            r#"INSERT INTO "archive"."dead_jobs" ("job_id", "name", "retry_after", "attempt", "locked_until", "processed_at", "payload") SELECT "job_id", "name", "retry_after", "attempt", NULL, "processed_at", "payload" FROM "email_jobs" WHERE "job_id" = ? AND "locked_until" = ?"#,
            r#"DELETE FROM "email_jobs" WHERE "job_id" = ? AND "locked_until" = ?"#,
        ]
    );
    assert_eq!(moves[0].params(), [Param::Id, Param::Held]);
    assert_eq!(moves[1].params(), [Param::Id, Param::Held]);
    // The moved row arrives without a lease, and `*` cannot name the lease column.
    assert_eq!(
        Sqlite.dead_letter_table(&BARE.selecting_all(), TableName::parse("dead_jobs")?),
        Err(StatementError::Flattened {
            statement: "dead_letter_table"
        })
    );
    Ok(())
}

#[test]
fn an_extension_and_a_stamp_write_the_lease() -> Result<(), StatementError> {
    let extend = Sqlite.extend(&LEASED)?;
    assert_eq!(
        extend.sql(),
        r#"UPDATE "email_jobs" SET "locked_until" = ? WHERE "job_id" = ? AND "locked_until" = ?"#,
    );
    assert_eq!(extend.params(), [Param::Lease, Param::Id, Param::Held]);
    // A claim of the service's own leaves each row to this stamp, inside its transaction.
    let stamp = Sqlite.stamp(&LEASED)?;
    assert_eq!(
        stamp.sql(),
        r#"UPDATE "email_jobs" SET "locked_until" = ?, "attempt" = "attempt" + 1 WHERE "job_id" = ? AND ("locked_until" IS NULL OR "locked_until" <= ?)"#,
    );
    assert_eq!(stamp.params(), [Param::Lease, Param::Id, Param::LeaseNow]);
    Ok(())
}

#[test]
fn a_claim_of_the_services_own_opens_its_transaction_for_writing() {
    // The write lock is taken before the claim's select, so two claims never read one row.
    assert_eq!(Sqlite.begin_claim(), Some("BEGIN IMMEDIATE"));
}

#[test]
fn the_insert_writes_every_column_the_database_does_not_fill() -> Result<(), StatementError> {
    let data = [
        Column::new("subject"),
        Column::new("created_at").generated(),
    ];
    // An insert serves a table in any form: publishing into a row lock table needs no lock.
    let spec = TableSpec::new(
        "email_jobs",
        Column::new("job_id").generated(),
        Form::RowLock,
    )
    .group(Column::new("name"))
    .retry_after(Column::new("retry_after"))
    .attempt(Column::new("attempt").generated())
    .payload(Column::new("payload"))
    .data(&data);
    let insert = Sqlite.insert(&spec)?;
    assert_eq!(
        insert.sql(),
        r#"INSERT INTO "email_jobs" ("name", "retry_after", "payload", "subject") VALUES (?, ?, ?, ?)"#,
    );
    assert_eq!(
        insert.params(),
        [
            Param::Column(1),
            Param::Column(2),
            Param::Column(4),
            Param::Column(5)
        ]
    );
    Ok(())
}

#[test]
fn an_insert_of_only_generated_columns_writes_the_defaults() -> Result<(), StatementError> {
    let spec = TableSpec::new(
        "jobs",
        Column::new("id").generated(),
        Form::Lease(EXPIRY.generated()),
    );
    let insert = Sqlite.insert(&spec)?;
    assert_eq!(insert.sql(), r#"INSERT INTO "jobs" DEFAULT VALUES"#);
    assert_eq!(insert.params(), []);
    assert_eq!(
        Sqlite.insert(&BARE.selecting_all()),
        Err(StatementError::Flattened {
            statement: "insert"
        })
    );
    Ok(())
}

#[test]
fn a_name_of_any_length_is_kept() -> Result<(), Box<dyn Error>> {
    let long = "n".repeat(1000);
    let spec =
        TableSpec::new(&long, Column::new(&long), Form::Lease(Column::new(&long))).within(&long);
    let claim = Sqlite.claim(&spec, ClaimShape::Ids)?;
    assert!(claim.sql().contains(&quoted(&long)), "{}", claim.sql());
    let target = format!("{long}.{long}");
    assert_eq!(
        Sqlite
            .dead_letter_table(&spec, TableName::parse(&target)?)?
            .len(),
        2
    );
    Ok(())
}

#[test]
fn the_row_lock_form_is_refused_for_every_statement() -> Result<(), Box<dyn Error>> {
    let refused = StatementError::UnsupportedForm {
        dialect: "sqlite",
        form: "row lock",
    };
    assert_eq!(
        Sqlite.claim(&LOCKED, ClaimShape::Rows),
        Err(refused.clone())
    );
    assert_eq!(
        Sqlite.claim(&LOCKED, ClaimShape::Roles),
        Err(refused.clone())
    );
    assert_eq!(Sqlite.ack(&LOCKED), Err(refused.clone()));
    assert_eq!(Sqlite.retry(&LOCKED), Err(refused.clone()));
    assert_eq!(Sqlite.retry_after(&LOCKED), Err(refused.clone()));
    assert_eq!(Sqlite.discard(&LOCKED), Err(refused.clone()));
    assert_eq!(Sqlite.dead_letter_group(&LOCKED), Err(refused.clone()));
    assert_eq!(
        Sqlite.dead_letter_table(&LOCKED, TableName::parse("jobs_dead")?),
        Err(refused.clone())
    );
    assert_eq!(Sqlite.extend(&LOCKED), Err(refused.clone()));
    assert_eq!(Sqlite.stamp(&LOCKED), Err(refused.clone()));
    // A table on the database's clock is refused for its form all the same.
    assert_eq!(Sqlite.ack(&LOCKED.database_clock()), Err(refused));
    Ok(())
}

#[test]
fn the_advisory_form_and_fifo_groups_are_refused() -> Result<(), Box<dyn Error>> {
    let advisory = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(JOB_KEY));
    let unsupported = StatementError::UnsupportedForm {
        dialect: "sqlite",
        form: "advisory lock",
    };
    assert_eq!(
        Sqlite.claim(&advisory, ClaimShape::Rows),
        Err(unsupported.clone())
    );
    assert_eq!(Sqlite.ack(&advisory), Err(unsupported.clone()));
    assert_eq!(
        Sqlite.dead_letter_table(&advisory, TableName::parse("jobs_dead")?),
        Err(unsupported)
    );
    assert_eq!(
        Sqlite.claim(&LEASED.fifo_group(Column::new("name")), ClaimShape::Rows),
        Err(StatementError::UnsupportedFifo { dialect: "sqlite" })
    );
    Ok(())
}

#[test]
fn a_fetch_by_a_list_of_ids_is_refused() {
    assert_eq!(
        Sqlite.fetch(&LEASED),
        Err(StatementError::UnsupportedFetch { dialect: "sqlite" })
    );
}

#[test]
fn the_lease_cannot_read_the_database_clock() {
    let clocked = LEASED.database_clock();
    let refused = StatementError::LeaseOnDatabaseClock { dialect: "sqlite" };
    assert_eq!(
        Sqlite.claim(&clocked, ClaimShape::Rows),
        Err(refused.clone())
    );
    assert_eq!(Sqlite.ack(&clocked), Err(refused.clone()));
    assert_eq!(Sqlite.retry_after(&clocked), Err(refused.clone()));
    assert_eq!(Sqlite.extend(&clocked), Err(refused.clone()));
    assert_eq!(Sqlite.stamp(&clocked), Err(refused));
}

#[test]
fn sqlite_asks_nothing_of_its_server() -> Result<(), StatementError> {
    assert_eq!(Sqlite.server_version(), None);
    Sqlite.check_server(&LEASED, "3.50.4")?;
    Ok(())
}
