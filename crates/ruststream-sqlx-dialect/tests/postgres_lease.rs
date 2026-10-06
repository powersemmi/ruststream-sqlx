//! The statements the Postgres dialect builds for the lease form.

#![cfg(feature = "postgres")]

use std::error::Error;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, Lease, Param, Postgres, StatementError, TableName, TableSpec,
};

const EXPIRY: Column<'static> = Column::new("locked_until");

/// Every role the lease form reads, in a table inside a schema.
const LEASED: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::Lease(EXPIRY))
        .within("app")
        .group(Column::new("name"))
        .priority(Column::new("priority"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

/// Only an id, a lease and a payload: rows are deleted when finished.
const BARE: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::Lease(EXPIRY))
    .payload(Column::new("payload"));

#[test]
fn a_lease_claim_locks_stamps_and_returns_the_rows_as_they_were() -> Result<(), StatementError> {
    let claim = Postgres.lease_claim(&LEASED, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"WITH __claimed AS (SELECT "job_id", "name", "priority", "retry_after", "attempt", "locked_until", "processed_at", "payload" FROM "app"."email_jobs" WHERE "name" = $1 AND "retry_after" <= $2 AND "processed_at" IS NULL AND ("locked_until" IS NULL OR "locked_until" <= $3) ORDER BY "priority", "retry_after", "job_id" LIMIT $4 FOR UPDATE SKIP LOCKED), __stamped AS (UPDATE "app"."email_jobs" AS __row SET "locked_until" = $5, "attempt" = __row."attempt" + 1 FROM __claimed WHERE __row."job_id" = __claimed."job_id") SELECT * FROM __claimed ORDER BY "priority", "retry_after", "job_id""#,
    );
    assert_eq!(
        claim.params(),
        [
            Param::Group,
            Param::Now,
            Param::LeaseNow,
            Param::Limit,
            Param::Lease
        ]
    );
    let bare = Postgres.lease_claim(&BARE, ClaimShape::Rows)?;
    assert_eq!(
        bare.sql(),
        r#"WITH __claimed AS (SELECT "job_id", "locked_until", "payload" FROM "jobs" WHERE ("locked_until" IS NULL OR "locked_until" <= $1) ORDER BY "job_id" LIMIT $2 FOR UPDATE SKIP LOCKED), __stamped AS (UPDATE "jobs" AS __row SET "locked_until" = $3 FROM __claimed WHERE __row."job_id" = __claimed."job_id") SELECT * FROM __claimed ORDER BY "job_id""#,
    );
    assert_eq!(bare.params(), [Param::LeaseNow, Param::Limit, Param::Lease]);
    assert!(Postgres.claim_writes_lease());
    Ok(())
}

#[test]
fn a_lease_claim_of_ids_or_roles_keeps_its_order_columns() -> Result<(), StatementError> {
    let ids = Postgres.lease_claim(&LEASED, ClaimShape::Ids)?;
    assert!(
        ids.sql().ends_with(
            r#"SELECT "job_id" FROM __claimed ORDER BY "priority", "retry_after", "job_id""#
        ),
        "{}",
        ids.sql()
    );
    let roles = Postgres.lease_claim(&LEASED, ClaimShape::Roles)?;
    assert!(
        roles.sql().ends_with(r#"SELECT "id", "attempt", "payload" FROM __claimed ORDER BY "priority", "retry_after", "id""#),
        "{}",
        roles.sql()
    );
    assert!(
        roles
            .sql()
            .contains(r#"WHERE __row."job_id" = __claimed."id""#),
        "{}",
        roles.sql()
    );
    Ok(())
}

#[test]
fn settlement_names_the_row_and_its_token() -> Result<(), StatementError> {
    let ack = Postgres.ack(&LEASED)?;
    assert_eq!(
        ack.sql(),
        r#"UPDATE "app"."email_jobs" SET "processed_at" = $1, "locked_until" = NULL WHERE "job_id" = $2 AND "locked_until" = $3"#,
    );
    assert_eq!(ack.params(), [Param::Now, Param::Id, Param::Held]);
    assert_eq!(Postgres.discard(&LEASED)?, ack);
    let deleted = Postgres.ack(&BARE)?;
    assert_eq!(
        deleted.sql(),
        r#"DELETE FROM "jobs" WHERE "job_id" = $1 AND "locked_until" = $2"#,
    );
    assert_eq!(deleted.params(), [Param::Id, Param::Held]);
    Ok(())
}

#[test]
fn a_retry_releases_the_lease_and_counts_nothing_more() -> Result<(), StatementError> {
    let retry = Postgres
        .retry(&LEASED)?
        .expect("the lease form always releases with a statement");
    assert_eq!(
        retry.sql(),
        r#"UPDATE "app"."email_jobs" SET "locked_until" = NULL WHERE "job_id" = $1 AND "locked_until" = $2"#,
    );
    assert_eq!(retry.params(), [Param::Id, Param::Held]);
    let later = Postgres.retry_after(&LEASED)?;
    assert_eq!(
        later.sql(),
        r#"UPDATE "app"."email_jobs" SET "retry_after" = $1, "locked_until" = NULL WHERE "job_id" = $2 AND "locked_until" = $3"#,
    );
    assert_eq!(later.params(), [Param::RetryAfter, Param::Id, Param::Held]);
    Ok(())
}

#[test]
fn a_dead_letter_moves_only_a_row_still_held() -> Result<(), Box<dyn Error>> {
    let group = Postgres.dead_letter_group(&LEASED)?;
    assert_eq!(
        group.sql(),
        r#"UPDATE "app"."email_jobs" SET "name" = $1, "locked_until" = NULL WHERE "job_id" = $2 AND "locked_until" = $3"#,
    );
    assert_eq!(group.params(), [Param::Destination, Param::Id, Param::Held]);
    let moves = Postgres.dead_letter_table(&BARE, TableName::parse("dead_jobs")?)?;
    assert_eq!(moves.len(), 1);
    assert_eq!(
        moves[0].sql(),
        r#"WITH moved AS (DELETE FROM "jobs" WHERE "job_id" = $1 AND "locked_until" = $2 RETURNING "job_id", "locked_until", "payload") INSERT INTO "dead_jobs" ("job_id", "locked_until", "payload") SELECT "job_id", NULL, "payload" FROM moved"#,
    );
    assert_eq!(moves[0].params(), [Param::Id, Param::Held]);
    Ok(())
}

#[test]
fn a_dead_letter_table_needs_every_column_of_a_lease_table() -> Result<(), Box<dyn Error>> {
    // The moved row arrives without a lease, and `*` cannot name the lease column.
    assert_eq!(
        Postgres.dead_letter_table(&BARE.selecting_all(), TableName::parse("dead_jobs")?),
        Err(StatementError::Flattened {
            statement: "dead_letter_table"
        })
    );
    Ok(())
}

#[test]
fn an_extension_and_a_stamp_write_the_lease() -> Result<(), StatementError> {
    let extend = Postgres.extend(&LEASED)?;
    assert_eq!(
        extend.sql(),
        r#"UPDATE "app"."email_jobs" SET "locked_until" = $1 WHERE "job_id" = $2 AND "locked_until" = $3"#,
    );
    assert_eq!(extend.params(), [Param::Lease, Param::Id, Param::Held]);
    let stamp = Postgres.stamp(&LEASED)?;
    assert_eq!(
        stamp.sql(),
        r#"UPDATE "app"."email_jobs" SET "locked_until" = $1, "attempt" = "attempt" + 1 WHERE "job_id" = $2 AND ("locked_until" IS NULL OR "locked_until" <= $3)"#,
    );
    assert_eq!(stamp.params(), [Param::Lease, Param::Id, Param::LeaseNow]);
    assert_eq!(
        Postgres.stamp(&BARE)?.sql(),
        r#"UPDATE "jobs" SET "locked_until" = $1 WHERE "job_id" = $2 AND ("locked_until" IS NULL OR "locked_until" <= $3)"#,
    );
    Ok(())
}

#[test]
fn the_lease_statements_refuse_other_forms_and_the_database_clock() {
    const LOCKED: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
        .payload(Column::new("payload"));
    let other = |statement| StatementError::FormMismatch {
        statement,
        form: "row lock",
    };
    assert_eq!(
        Postgres.lease_claim(&LOCKED, ClaimShape::Rows),
        Err(other("lease_claim"))
    );
    assert_eq!(Postgres.extend(&LOCKED), Err(other("extend")));
    assert_eq!(Postgres.stamp(&LOCKED), Err(other("stamp")));
    let clocked = BARE.database_clock();
    let refused = StatementError::LeaseOnDatabaseClock {
        dialect: "postgres",
    };
    assert_eq!(
        Postgres.lease_claim(&clocked, ClaimShape::Rows),
        Err(refused.clone())
    );
    assert_eq!(Postgres.ack(&clocked), Err(refused.clone()));
    assert_eq!(Postgres.extend(&clocked), Err(refused));
}

#[test]
fn postgres_asks_nothing_of_its_server() -> Result<(), StatementError> {
    assert_eq!(Postgres.server_version(), None);
    Postgres.check_server(&LEASED, "17.2")?;
    Ok(())
}

#[test]
fn the_new_errors_name_what_to_do() {
    assert_eq!(
        StatementError::ServerTooOld {
            dialect: "mysql",
            server: "5.7.44".to_owned(),
            required: "MySQL 8.0.1"
        }
        .to_string(),
        "the mysql dialect needs MySQL 8.0.1 or later for this form; the server reports `5.7.44`"
    );
    assert_eq!(
        StatementError::UnsupportedFetch { dialect: "sqlite" }.to_string(),
        "the sqlite dialect cannot read rows by a list of ids: list `fetch` in `custom(..)` beside `claim`"
    );
    assert_eq!(
        StatementError::LeaseOnDatabaseClock {
            dialect: "postgres"
        }
        .to_string(),
        "the lease form computes its expiry from the crate's clock: a table on `DatabaseClock` cannot \
         declare `locked_until`"
    );
}

/// A ledger whose accounts keep their order, in the lease form.
const LEASED_LEDGER: TableSpec<'static> = TableSpec::new(
    "ledger",
    Column::new("id"),
    Form::Lease(Column::new("locked_until")),
)
.fifo_group(Column::new("account"))
.retry_after(Column::new("retry_after"))
.attempt(Column::new("attempt"))
.processed_at(Column::new("processed_at"))
.payload(Column::new("payload"));

#[test]
fn a_fifo_lease_claim_stamps_the_head_alone() -> Result<(), Box<dyn Error>> {
    // Nothing while a row of the group holds a lease: a row that entered ahead of the head in work
    // waits for it.
    let claim = Postgres.lease_claim(&LEASED_LEDGER, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        r#"WITH __claimed AS (SELECT "id", "account", "retry_after", "attempt", "locked_until", "processed_at", "payload" FROM "ledger" WHERE "id" = (SELECT "id" FROM "ledger" WHERE "account" = $1 AND "processed_at" IS NULL ORDER BY "retry_after", "id" LIMIT 1) AND "retry_after" <= $2 AND ("locked_until" IS NULL OR "locked_until" <= $3) AND NOT EXISTS (SELECT 1 FROM "ledger" AS __work WHERE __work."account" = $4 AND __work."locked_until" > $5) FOR UPDATE SKIP LOCKED), __stamped AS (UPDATE "ledger" AS __row SET "locked_until" = $6, "attempt" = __row."attempt" + 1 FROM __claimed WHERE __row."id" = __claimed."id") SELECT * FROM __claimed ORDER BY "retry_after", "id""#,
    );
    assert_eq!(
        claim.params(),
        [
            Param::Group,
            Param::Now,
            Param::LeaseNow,
            Param::Group,
            Param::LeaseNow,
            Param::Lease
        ]
    );
    Ok(())
}

#[test]
fn a_fifo_lease_claim_takes_its_group_first() -> Result<(), Box<dyn Error>> {
    // The guard holds the group for the claim's transaction; the lease holds it after the commit.
    let guard = Postgres
        .fifo_guard(&LEASED_LEDGER)?
        .ok_or("a table with FIFO groups has a guard")?;
    assert_eq!(
        guard.sql(),
        "SELECT pg_try_advisory_xact_lock(hashtextextended('ledger:' || $1, 0))::int::bigint",
    );
    assert_eq!(guard.params(), [Param::Group]);
    assert_eq!(Postgres.fifo_guard(&LEASED)?, None);
    assert_eq!(
        Postgres.fifo_guard(&LEASED_LEDGER.database_clock()),
        Err(StatementError::LeaseOnDatabaseClock {
            dialect: "postgres"
        })
    );
    Ok(())
}
