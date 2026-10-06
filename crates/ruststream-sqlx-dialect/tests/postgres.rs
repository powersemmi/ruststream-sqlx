//! The statements the Postgres dialect builds for the row lock form.

#![cfg(feature = "postgres")]

use std::error::Error;
use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, Isolation, KeyPart, Lease, Mode, NameLimit, Opening, Opens,
    Param, Postgres, Role, RowLock, Statement, StatementError, TableName, TableSpec, level,
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
fn a_claim_transaction_opens_with_a_plain_begin() -> Result<(), StatementError> {
    // A table that names no isolation level opens its row lock claim with `BEGIN`.
    assert_eq!(Postgres.begin(BARE.opening())?, None);
    assert_eq!(Postgres.begin_lease_claim(), None);
    Ok(())
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

/// A name of `len` bytes.
fn name_of(len: usize) -> String {
    "n".repeat(len)
}

#[test]
fn a_name_longer_than_63_bytes_is_refused() {
    let long = name_of(64);
    let fits = name_of(63);
    let refused = |identifier: &str| StatementError::IdentifierTooLong {
        dialect: "postgres",
        identifier: identifier.to_owned(),
        limit: NameLimit::Bytes(63),
    };

    let table = TableSpec::new(&long, Column::new("job_id"), Form::RowLock);
    assert_eq!(
        Postgres.lock_claim(&table, ClaimShape::Rows),
        Err(refused(&long))
    );
    assert_eq!(Postgres.ack(&table), Err(refused(&long)));
    assert_eq!(Postgres.insert(&table), Err(refused(&long)));

    let schema = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).within(&long);
    assert_eq!(Postgres.fetch(&schema), Err(refused(&long)));

    let column =
        TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new(&long));
    assert_eq!(Postgres.discard(&column), Err(refused(&long)));
    assert_eq!(Postgres.retry(&column), Err(refused(&long)));
    let group =
        TableSpec::new("ledger", Column::new("id"), Form::RowLock).fifo_group(Column::new(&long));
    assert_eq!(Postgres.fifo_guard(&group), Err(refused(&long)));

    let target = format!("archive.{long}");
    let target = TableName::parse(&target).map_err(|err| err.to_string());
    assert_eq!(
        target.map(|target| Postgres.dead_letter_table(&BARE, target)),
        Ok(Err(refused(&long)))
    );

    // A multi-byte name is measured in bytes, as Postgres measures it: 32 two-byte letters.
    let wide = "\u{e9}".repeat(32);
    let accented = TableSpec::new(&wide, Column::new("job_id"), Form::RowLock);
    assert_eq!(Postgres.ack(&accented), Err(refused(&wide)));

    let edge = TableSpec::new(&fits, Column::new(&fits), Form::RowLock).within(&fits);
    assert!(Postgres.lock_claim(&edge, ClaimShape::Rows).is_ok());
}

#[test]
fn the_insert_writes_every_column_the_database_does_not_fill() -> Result<(), StatementError> {
    let data = [
        Column::new("subject"),
        Column::new("created_at").generated(),
    ];
    let spec = TableSpec::new(
        "email_jobs",
        Column::new("job_id").generated(),
        Form::RowLock,
    )
    .within("app")
    .group(Column::new("name"))
    .retry_after(Column::new("retry_after"))
    .attempt(Column::new("attempt").generated())
    .payload(Column::new("payload"))
    .data(&data);
    let insert = Postgres.insert(&spec)?;
    assert_eq!(
        insert.sql(),
        r#"INSERT INTO "app"."email_jobs" ("name", "retry_after", "payload", "subject") VALUES ($1, $2, $3, $4)"#
    );
    // The positions count every column, the generated ones included.
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
fn an_insert_of_only_generated_columns_writes_default_values() -> Result<(), StatementError> {
    let spec = TableSpec::new("ticks", Column::new("id").generated(), Form::RowLock);
    let insert = Postgres.insert(&spec)?;
    assert_eq!(insert.sql(), r#"INSERT INTO "ticks" DEFAULT VALUES"#);
    assert_eq!(insert.params(), []);
    Ok(())
}

#[test]
fn a_flattening_struct_has_no_insert() {
    assert_eq!(
        Postgres.insert(&BARE.selecting_all()),
        Err(StatementError::Flattened {
            statement: "insert"
        })
    );
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

#[test]
fn transactions_open_at_the_declared_isolation() -> Result<(), Box<dyn Error>> {
    assert_eq!(Postgres.begin(Opening::Default)?, None);
    assert_eq!(
        Postgres.begin(Opening::Isolation(Isolation::Serializable))?,
        Some("BEGIN ISOLATION LEVEL SERIALIZABLE")
    );
    assert_eq!(
        Postgres.begin(Opening::Isolation(Isolation::RepeatableRead))?,
        Some("BEGIN ISOLATION LEVEL REPEATABLE READ")
    );
    assert_eq!(
        Postgres.begin(Opening::Isolation(Isolation::ReadCommitted))?,
        Some("BEGIN ISOLATION LEVEL READ COMMITTED")
    );
    assert_eq!(
        Postgres.begin(Opening::Isolation(Isolation::ReadUncommitted)),
        Err(StatementError::UnsupportedOpening {
            dialect: "postgres",
            opening: Opening::Isolation(Isolation::ReadUncommitted).name(),
        })
    );
    assert!(Postgres.begin(Opening::Mode(Mode::Immediate)).is_err());
    assert_eq!(Postgres.savepoint(), "SAVEPOINT ruststream_claim");
    assert_eq!(
        Postgres.rollback_to_savepoint(),
        "ROLLBACK TO SAVEPOINT ruststream_claim"
    );
    Ok(())
}

/// The text a table that names `Level` opens its transactions with: the bound holds where the
/// dialect opens the level.
fn begin_at<Level, D: Opens<Level>>(
    dialect: &D,
    opening: Opening,
) -> Result<Option<&'static str>, StatementError> {
    dialect.begin(opening)
}

#[test]
fn every_level_postgres_opens_is_one_its_begin_accepts() {
    let opened = [
        begin_at::<(), _>(&Postgres, Opening::Default),
        begin_at::<level::ReadCommitted, _>(
            &Postgres,
            Opening::Isolation(Isolation::ReadCommitted),
        ),
        begin_at::<level::RepeatableRead, _>(
            &Postgres,
            Opening::Isolation(Isolation::RepeatableRead),
        ),
        begin_at::<level::Serializable, _>(&Postgres, Opening::Isolation(Isolation::Serializable)),
    ];
    assert!(opened.iter().all(Result::is_ok), "{opened:?}");
}
