//! The statements the MySQL and MariaDB dialect builds for the row lock and lease forms.

#![cfg(feature = "mysql")]

use std::error::Error;
use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, Isolation, KeyPart, Lease, Mode, MySql, NameLimit, Opening,
    Opens, Param, Role, RowLock, Statement, StatementError, TableName, TableSpec, level,
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

/// A lease table inside a schema; rows are deleted when finished.
const LEASED: TableSpec<'static> = TableSpec::new(
    "email_jobs",
    Column::new("job_id"),
    Form::Lease(Column::new("locked_until")),
)
.within("app")
.group(Column::new("name"))
.retry_after(Column::new("retry_after"))
.attempt(Column::new("attempt"))
.payload(Column::new("payload"));

/// Only an id and a payload, in the default schema.
const BARE: TableSpec<'static> =
    TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));

/// Only an id, a lease and a payload, in the default schema.
const BARE_LEASED: TableSpec<'static> = TableSpec::new(
    "jobs",
    Column::new("job_id"),
    Form::Lease(Column::new("locked_until")),
)
.payload(Column::new("payload"));

/// Every role a claim by role reads, beside a group and a column of data.
const KEYED: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
        .group(Column::new("name"))
        .partition_key(Column::new("customer"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .headers(Column::new("meta"))
        .payload(Column::new("payload"))
        .data(&[Column::new("subject")]);

const JOB_KEY: &[KeyPart<'static>] = &[KeyPart::Column("job_id")];

fn quoted(ident: &str) -> String {
    let mut out = String::new();
    MySql.quote_into(ident, &mut out);
    out
}

#[test]
fn names_are_quoted_with_backticks_and_keep_their_case() {
    assert_eq!(quoted("email_jobs"), "`email_jobs`");
    assert_eq!(quoted("EmailJobs"), "`EmailJobs`");
    assert_eq!(quoted("email jobs"), "`email jobs`");
    assert_eq!(quoted("a`b"), "`a``b`");
    assert_eq!(quoted("``"), "``````");
    assert_eq!(quoted(r#"odd"name"#), r#"`odd"name`"#);
}

#[test]
fn every_placeholder_is_a_question_mark() {
    let mut out = String::new();
    MySql.placeholder_into(NonZeroUsize::MIN, &mut out);
    out.push(' ');
    MySql.placeholder_into(NonZeroUsize::MIN.saturating_add(11), &mut out);
    assert_eq!(out, "? ?");
    assert_eq!(MySql.name(), "mysql");
}

#[test]
fn the_row_lock_claim_skips_locked_rows() -> Result<(), StatementError> {
    let claim = MySql.lock_claim(&EMAILS, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        "SELECT `job_id`, `name`, `priority`, `retry_after`, `attempt`, `processed_at`, `payload` FROM `app`.`email_jobs` WHERE `name` = ? AND `retry_after` <= ? AND `processed_at` IS NULL ORDER BY `priority`, `retry_after`, `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(claim.params(), [Param::Group, Param::Now, Param::Limit]);
    Ok(())
}

#[test]
fn a_claim_of_ids_or_roles_selects_only_those_columns() -> Result<(), StatementError> {
    let ids = MySql.lock_claim(&EMAILS, ClaimShape::Ids)?;
    assert_eq!(
        ids.sql(),
        "SELECT `job_id` FROM `app`.`email_jobs` WHERE `name` = ? AND `retry_after` <= ? AND `processed_at` IS NULL ORDER BY `priority`, `retry_after`, `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(ids.params(), [Param::Group, Param::Now, Param::Limit]);

    let roles = MySql.lock_claim(&KEYED, ClaimShape::Roles)?;
    assert_eq!(
        roles.sql(),
        "SELECT `job_id` AS `id`, `customer` AS `partition_key`, `attempt` AS `attempt`, `meta` AS `headers`, `payload` AS `payload` FROM `email_jobs` WHERE `name` = ? AND `retry_after` <= ? ORDER BY `retry_after`, `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(roles.params(), [Param::Group, Param::Now, Param::Limit]);
    Ok(())
}

#[test]
fn a_claim_without_conditions_orders_by_id() -> Result<(), StatementError> {
    let claim = MySql.lock_claim(&BARE, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        "SELECT `job_id`, `payload` FROM `jobs` ORDER BY `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(claim.params(), [Param::Limit]);
    let flat = MySql.lock_claim(&BARE.selecting_all(), ClaimShape::Rows)?;
    assert_eq!(
        flat.sql(),
        "SELECT * FROM `jobs` ORDER BY `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    Ok(())
}

#[test]
fn names_that_need_quoting_survive_into_statements() -> Result<(), StatementError> {
    let spec = TableSpec::new("Email Jobs", Column::new("Job Id"), Form::RowLock)
        .within("Mail")
        .payload(Column::new("pay`load"));
    let claim = MySql.lock_claim(&spec, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        "SELECT `Job Id`, `pay``load` FROM `Mail`.`Email Jobs` ORDER BY `Job Id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    Ok(())
}

#[test]
fn the_lease_claim_selects_and_each_row_is_stamped() -> Result<(), StatementError> {
    let claim = MySql.lease_claim(&LEASED, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        "SELECT `job_id`, `name`, `retry_after`, `attempt`, `locked_until`, `payload` FROM `app`.`email_jobs` WHERE `name` = ? AND `retry_after` <= ? AND (`locked_until` IS NULL OR `locked_until` <= ?) ORDER BY `retry_after`, `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(
        claim.params(),
        [Param::Group, Param::Now, Param::LeaseNow, Param::Limit]
    );
    assert!(!MySql.claim_writes_lease());
    let stamp = MySql.stamp(&LEASED)?;
    assert_eq!(
        stamp.sql(),
        "UPDATE `app`.`email_jobs` SET `locked_until` = ?, `attempt` = `attempt` + 1 WHERE `job_id` = ? AND (`locked_until` IS NULL OR `locked_until` <= ?)",
    );
    assert_eq!(stamp.params(), [Param::Lease, Param::Id, Param::LeaseNow]);
    assert_eq!(
        MySql.stamp(&BARE_LEASED)?.sql(),
        "UPDATE `jobs` SET `locked_until` = ? WHERE `job_id` = ? AND (`locked_until` IS NULL OR `locked_until` <= ?)",
    );
    Ok(())
}

#[test]
fn a_lease_claim_of_ids_or_roles_is_the_same_select() -> Result<(), StatementError> {
    let ids = MySql.lease_claim(&LEASED, ClaimShape::Ids)?;
    assert_eq!(
        ids.sql(),
        "SELECT `job_id` FROM `app`.`email_jobs` WHERE `name` = ? AND `retry_after` <= ? AND (`locked_until` IS NULL OR `locked_until` <= ?) ORDER BY `retry_after`, `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    let roles = MySql.lease_claim(&LEASED, ClaimShape::Roles)?;
    assert_eq!(
        roles.sql(),
        "SELECT `job_id` AS `id`, `attempt` AS `attempt`, `payload` AS `payload` FROM `app`.`email_jobs` WHERE `name` = ? AND `retry_after` <= ? AND (`locked_until` IS NULL OR `locked_until` <= ?) ORDER BY `retry_after`, `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(
        roles.params(),
        [Param::Group, Param::Now, Param::LeaseNow, Param::Limit]
    );
    Ok(())
}

#[test]
fn ack_and_discard_mark_or_delete_the_row_in_the_row_lock_form() -> Result<(), StatementError> {
    for statement in [MySql.ack(&EMAILS)?, MySql.discard(&EMAILS)?] {
        assert_eq!(
            statement.sql(),
            "UPDATE `app`.`email_jobs` SET `processed_at` = ? WHERE `job_id` = ?",
        );
        assert_eq!(statement.params(), [Param::Now, Param::Id]);
    }
    for statement in [MySql.ack(&BARE)?, MySql.discard(&BARE)?] {
        assert_eq!(statement.sql(), "DELETE FROM `jobs` WHERE `job_id` = ?");
        assert_eq!(statement.params(), [Param::Id]);
    }
    Ok(())
}

#[test]
fn ack_and_discard_in_the_lease_form_name_the_row_and_its_token() -> Result<(), StatementError> {
    for statement in [MySql.ack(&LEASED)?, MySql.discard(&LEASED)?] {
        assert_eq!(
            statement.sql(),
            "DELETE FROM `app`.`email_jobs` WHERE `job_id` = ? AND `locked_until` = ?",
        );
        assert_eq!(statement.params(), [Param::Id, Param::Held]);
    }
    let kept = LEASED.processed_at(Column::new("processed_at"));
    for statement in [MySql.ack(&kept)?, MySql.discard(&kept)?] {
        assert_eq!(
            statement.sql(),
            "UPDATE `app`.`email_jobs` SET `processed_at` = ?, `locked_until` = NULL WHERE `job_id` = ? AND `locked_until` = ?",
        );
        assert_eq!(statement.params(), [Param::Now, Param::Id, Param::Held]);
    }
    Ok(())
}

#[test]
fn a_retry_counts_the_attempt_in_the_row_lock_form() -> Result<(), StatementError> {
    let retry = MySql.retry(&EMAILS)?;
    assert_eq!(
        retry.as_ref().map(Statement::sql),
        Some("UPDATE `app`.`email_jobs` SET `attempt` = `attempt` + 1 WHERE `job_id` = ?"),
    );
    assert_eq!(
        retry.as_ref().map(Statement::params),
        Some([Param::Id].as_slice())
    );
    // The rollback releases the row, and there is no attempt to count.
    assert_eq!(MySql.retry(&BARE)?, None);
    Ok(())
}

#[test]
fn a_retry_in_the_lease_form_releases_the_lease() -> Result<(), StatementError> {
    let retry = MySql.retry(&LEASED)?;
    assert_eq!(
        retry.as_ref().map(Statement::sql),
        Some(
            "UPDATE `app`.`email_jobs` SET `locked_until` = NULL WHERE `job_id` = ? AND `locked_until` = ?"
        ),
    );
    assert_eq!(
        retry.as_ref().map(Statement::params),
        Some([Param::Id, Param::Held].as_slice())
    );
    Ok(())
}

#[test]
fn a_delayed_retry_sets_the_time_in_both_forms() -> Result<(), StatementError> {
    let locked = MySql.retry_after(&EMAILS)?;
    assert_eq!(
        locked.sql(),
        "UPDATE `app`.`email_jobs` SET `retry_after` = ?, `attempt` = `attempt` + 1 WHERE `job_id` = ?",
    );
    assert_eq!(locked.params(), [Param::RetryAfter, Param::Id]);
    let leased = MySql.retry_after(&LEASED)?;
    assert_eq!(
        leased.sql(),
        "UPDATE `app`.`email_jobs` SET `retry_after` = ?, `locked_until` = NULL WHERE `job_id` = ? AND `locked_until` = ?",
    );
    assert_eq!(leased.params(), [Param::RetryAfter, Param::Id, Param::Held]);
    assert_eq!(
        MySql.retry_after(&BARE),
        Err(StatementError::MissingRole {
            statement: "retry_after",
            role: Role::RetryAfter,
        })
    );
    Ok(())
}

#[test]
fn a_dead_letter_into_a_group_in_both_forms() -> Result<(), StatementError> {
    let locked = MySql.dead_letter_group(&EMAILS)?;
    assert_eq!(
        locked.sql(),
        "UPDATE `app`.`email_jobs` SET `name` = ? WHERE `job_id` = ?",
    );
    assert_eq!(locked.params(), [Param::Destination, Param::Id]);
    let leased = MySql.dead_letter_group(&LEASED)?;
    assert_eq!(
        leased.sql(),
        "UPDATE `app`.`email_jobs` SET `name` = ?, `locked_until` = NULL WHERE `job_id` = ? AND `locked_until` = ?",
    );
    assert_eq!(
        leased.params(),
        [Param::Destination, Param::Id, Param::Held]
    );
    assert_eq!(
        MySql.dead_letter_group(&BARE),
        Err(StatementError::MissingRole {
            statement: "dead_letter_group",
            role: Role::Group,
        })
    );
    Ok(())
}

#[test]
fn a_dead_letter_into_a_table_is_a_copy_then_a_delete() -> Result<(), Box<dyn Error>> {
    let moves = MySql.dead_letter_table(&LEASED, TableName::parse("archive.dead_jobs")?)?;
    assert_eq!(
        moves.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            "INSERT INTO `archive`.`dead_jobs` (`job_id`, `name`, `retry_after`, `attempt`, `locked_until`, `payload`) SELECT `job_id`, `name`, `retry_after`, `attempt`, NULL, `payload` FROM `app`.`email_jobs` WHERE `job_id` = ? AND `locked_until` = ?",
            "DELETE FROM `app`.`email_jobs` WHERE `job_id` = ? AND `locked_until` = ?",
        ]
    );
    assert_eq!(moves[0].params(), [Param::Id, Param::Held]);
    assert_eq!(moves[1].params(), [Param::Id, Param::Held]);
    Ok(())
}

#[test]
fn a_dead_letter_into_a_table_in_the_row_lock_form() -> Result<(), Box<dyn Error>> {
    let moves = MySql.dead_letter_table(&EMAILS, TableName::parse("app.jobs_dead")?)?;
    assert_eq!(
        moves.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            "INSERT INTO `app`.`jobs_dead` (`job_id`, `name`, `priority`, `retry_after`, `attempt`, `processed_at`, `payload`) SELECT `job_id`, `name`, `priority`, `retry_after`, `attempt`, `processed_at`, `payload` FROM `app`.`email_jobs` WHERE `job_id` = ?",
            "DELETE FROM `app`.`email_jobs` WHERE `job_id` = ?",
        ]
    );
    assert_eq!(moves[0].params(), [Param::Id]);
    assert_eq!(moves[1].params(), [Param::Id]);

    // A struct that flattens moves its row by position.
    let flat = MySql.dead_letter_table(&BARE.selecting_all(), TableName::parse("Jobs Dead")?)?;
    assert_eq!(
        flat.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            "INSERT INTO `Jobs Dead` SELECT * FROM `jobs` WHERE `job_id` = ?",
            "DELETE FROM `jobs` WHERE `job_id` = ?",
        ]
    );
    // The moved row arrives without a lease, and `*` cannot name the lease column.
    assert_eq!(
        MySql.dead_letter_table(&BARE_LEASED.selecting_all(), TableName::parse("dead_jobs")?),
        Err(StatementError::Flattened {
            statement: "dead_letter_table"
        })
    );
    Ok(())
}

#[test]
fn an_extension_writes_the_new_expiry_while_the_token_holds() -> Result<(), StatementError> {
    let extend = MySql.extend(&LEASED)?;
    assert_eq!(
        extend.sql(),
        "UPDATE `app`.`email_jobs` SET `locked_until` = ? WHERE `job_id` = ? AND `locked_until` = ?",
    );
    assert_eq!(extend.params(), [Param::Lease, Param::Id, Param::Held]);
    Ok(())
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
    let insert = MySql.insert(&spec)?;
    assert_eq!(
        insert.sql(),
        "INSERT INTO `app`.`email_jobs` (`name`, `retry_after`, `payload`, `subject`) VALUES (?, ?, ?, ?)",
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
fn an_insert_of_only_generated_columns_writes_an_empty_row() -> Result<(), StatementError> {
    let spec = TableSpec::new("jobs", Column::new("id").generated(), Form::RowLock);
    let insert = MySql.insert(&spec)?;
    assert_eq!(insert.sql(), "INSERT INTO `jobs` () VALUES ()");
    assert_eq!(insert.params(), []);
    assert_eq!(
        MySql.insert(&BARE.selecting_all()),
        Err(StatementError::Flattened {
            statement: "insert"
        })
    );
    Ok(())
}

#[test]
fn the_database_clock_reads_utc_timestamp() -> Result<(), StatementError> {
    let spec = EMAILS.database_clock();
    let claim = MySql.lock_claim(&spec, ClaimShape::Ids)?;
    assert_eq!(
        claim.sql(),
        "SELECT `job_id` FROM `app`.`email_jobs` WHERE `name` = ? AND `retry_after` <= UTC_TIMESTAMP(6) AND `processed_at` IS NULL ORDER BY `priority`, `retry_after`, `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(claim.params(), [Param::Group, Param::Limit]);

    let ack = MySql.ack(&spec)?;
    assert_eq!(
        ack.sql(),
        "UPDATE `app`.`email_jobs` SET `processed_at` = UTC_TIMESTAMP(6) WHERE `job_id` = ?",
    );
    assert_eq!(ack.params(), [Param::Id]);

    let retry_after = MySql.retry_after(&spec)?;
    assert_eq!(
        retry_after.sql(),
        "UPDATE `app`.`email_jobs` SET `retry_after` = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND, `attempt` = `attempt` + 1 WHERE `job_id` = ?",
    );
    assert_eq!(retry_after.params(), [Param::Delay, Param::Id]);
    Ok(())
}

#[test]
fn a_name_longer_than_64_characters_is_refused() -> Result<(), Box<dyn Error>> {
    let long = "n".repeat(65);
    let refused = |identifier: &str| StatementError::IdentifierTooLong {
        dialect: "mysql",
        identifier: identifier.to_owned(),
        limit: NameLimit::Characters(64),
    };

    let table = TableSpec::new(&long, Column::new("job_id"), Form::RowLock);
    assert_eq!(
        MySql.lock_claim(&table, ClaimShape::Rows),
        Err(refused(&long))
    );
    assert_eq!(MySql.ack(&table), Err(refused(&long)));
    assert_eq!(MySql.insert(&table), Err(refused(&long)));

    let schema = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).within(&long);
    assert_eq!(MySql.retry(&schema), Err(refused(&long)));

    let column =
        TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new(&long));
    assert_eq!(MySql.discard(&column), Err(refused(&long)));

    let target = format!("archive.{long}");
    assert_eq!(
        MySql.dead_letter_table(&BARE, TableName::parse(&target)?),
        Err(refused(&long))
    );

    // MySQL counts characters: 64 two-byte letters fit.
    let wide = "\u{e9}".repeat(64);
    let edge = TableSpec::new(&wide, Column::new(&wide), Form::RowLock).within(&wide);
    assert!(MySql.lock_claim(&edge, ClaimShape::Rows).is_ok());
    Ok(())
}

#[test]
fn a_fetch_by_a_list_of_ids_is_refused() {
    assert_eq!(
        MySql.fetch(&EMAILS),
        Err(StatementError::UnsupportedFetch { dialect: "mysql" })
    );
}

#[test]
fn the_lease_cannot_read_the_database_clock() {
    let clocked = LEASED.database_clock();
    let refused = StatementError::LeaseOnDatabaseClock { dialect: "mysql" };
    assert_eq!(
        MySql.lease_claim(&clocked, ClaimShape::Rows),
        Err(refused.clone())
    );
    assert_eq!(MySql.ack(&clocked), Err(refused.clone()));
    assert_eq!(MySql.extend(&clocked), Err(refused.clone()));
    assert_eq!(MySql.stamp(&clocked), Err(refused));
}

#[test]
fn forms_this_dialect_does_not_build_are_refused() -> Result<(), Box<dyn Error>> {
    let advisory = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(JOB_KEY));
    let unsupported = StatementError::UnsupportedForm {
        dialect: "mysql",
        form: "advisory lock",
    };
    assert_eq!(
        MySql.lock_claim(&advisory, ClaimShape::Rows),
        Err(StatementError::FormMismatch {
            statement: "lock_claim",
            form: "advisory lock",
        })
    );
    assert_eq!(
        MySql.lease_claim(&advisory, ClaimShape::Rows),
        Err(StatementError::FormMismatch {
            statement: "lease_claim",
            form: "advisory lock",
        })
    );
    assert_eq!(MySql.ack(&advisory), Err(unsupported.clone()));
    assert_eq!(MySql.retry(&advisory), Err(unsupported.clone()));
    assert_eq!(MySql.retry_after(&advisory), Err(unsupported.clone()));
    assert_eq!(MySql.discard(&advisory), Err(unsupported.clone()));
    assert_eq!(MySql.dead_letter_group(&advisory), Err(unsupported.clone()));
    assert_eq!(
        MySql.dead_letter_table(&advisory, TableName::parse("jobs_dead")?),
        Err(unsupported)
    );
    let other = |statement, form| StatementError::FormMismatch { statement, form };
    assert_eq!(
        MySql.extend(&advisory),
        Err(other("extend", "advisory lock"))
    );
    assert_eq!(MySql.stamp(&advisory), Err(other("stamp", "advisory lock")));

    // The lease statements serve lease tables, and the row lock claim the others.
    assert_eq!(
        MySql.lease_claim(&BARE, ClaimShape::Rows),
        Err(other("lease_claim", "row lock"))
    );
    assert_eq!(MySql.extend(&BARE), Err(other("extend", "row lock")));
    assert_eq!(MySql.stamp(&BARE), Err(other("stamp", "row lock")));
    assert_eq!(
        MySql.lock_claim(&LEASED, ClaimShape::Rows),
        Err(other("lock_claim", "lease"))
    );
    Ok(())
}

/// A ledger whose accounts keep their order: one row of an account in work, taken in claim order.
const LEDGER: TableSpec<'static> = TableSpec::new("ledger", Column::new("id"), Form::RowLock)
    .fifo_group(Column::new("account"))
    .retry_after(Column::new("retry_after"))
    .attempt(Column::new("attempt"))
    .processed_at(Column::new("processed_at"))
    .payload(Column::new("payload"));

/// The same ledger in the lease form.
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
fn a_fifo_claim_takes_the_head_of_its_group_or_nothing() -> Result<(), Box<dyn Error>> {
    let claim = MySql.lock_claim(&LEDGER, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        "SELECT `id`, `account`, `retry_after`, `attempt`, `processed_at`, `payload` FROM `ledger` WHERE `id` = (SELECT `id` FROM `ledger` WHERE `account` = ? AND `processed_at` IS NULL ORDER BY `retry_after`, `id` LIMIT 1) AND `retry_after` <= ? FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(claim.params(), [Param::Group, Param::Now]);
    Ok(())
}

#[test]
fn a_fifo_lease_claim_selects_the_head_alone() -> Result<(), Box<dyn Error>> {
    // The claim selects the head under a lock, and its transaction stamps it.
    let claim = MySql.lease_claim(&LEASED_LEDGER, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        "SELECT `id`, `account`, `retry_after`, `attempt`, `locked_until`, `processed_at`, `payload` FROM `ledger` WHERE `id` = (SELECT `id` FROM `ledger` WHERE `account` = ? AND `processed_at` IS NULL ORDER BY `retry_after`, `id` LIMIT 1) AND `retry_after` <= ? AND (`locked_until` IS NULL OR `locked_until` <= ?) FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(claim.params(), [Param::Group, Param::Now, Param::LeaseNow]);
    Ok(())
}

#[test]
fn a_claim_transaction_opens_at_read_committed_in_both_forms() -> Result<(), StatementError> {
    let read_committed = Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION");
    // A table that names no isolation level opens its row lock claim at READ COMMITTED.
    assert_eq!(MySql.begin(BARE.opening())?, read_committed);
    assert_eq!(MySql.begin_lease_claim(), read_committed);
    Ok(())
}

#[test]
fn transactions_open_at_the_declared_isolation() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        MySql.begin(Opening::Default)?,
        Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION")
    );
    assert_eq!(
        MySql.begin(Opening::Isolation(Isolation::ReadUncommitted))?,
        Some("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; START TRANSACTION")
    );
    assert_eq!(
        MySql.begin(Opening::Isolation(Isolation::ReadCommitted))?,
        Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION")
    );
    assert_eq!(
        MySql.begin(Opening::Isolation(Isolation::RepeatableRead))?,
        Some("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION")
    );
    assert_eq!(
        MySql.begin(Opening::Isolation(Isolation::Serializable))?,
        Some("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION")
    );
    assert_eq!(
        MySql.begin(Opening::Mode(Mode::Immediate)),
        Err(StatementError::UnsupportedOpening {
            dialect: "mysql",
            opening: Opening::Mode(Mode::Immediate).name(),
        })
    );
    assert_eq!(MySql.savepoint(), "SAVEPOINT ruststream_claim");
    assert_eq!(
        MySql.rollback_to_savepoint(),
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
fn every_level_mysql_opens_is_one_its_begin_accepts() {
    let opened = [
        begin_at::<(), _>(&MySql, Opening::Default),
        begin_at::<level::ReadUncommitted, _>(
            &MySql,
            Opening::Isolation(Isolation::ReadUncommitted),
        ),
        begin_at::<level::ReadCommitted, _>(&MySql, Opening::Isolation(Isolation::ReadCommitted)),
        begin_at::<level::RepeatableRead, _>(&MySql, Opening::Isolation(Isolation::RepeatableRead)),
        begin_at::<level::Serializable, _>(&MySql, Opening::Isolation(Isolation::Serializable)),
    ];
    assert!(opened.iter().all(Result::is_ok), "{opened:?}");
}

#[test]
fn the_server_reports_its_version() {
    assert_eq!(MySql.server_version(), Some("SELECT VERSION()"));
}

#[test]
fn the_server_floor_is_mysql_8_0_1_and_mariadb_10_6() {
    for accepted in [
        "8.0.1",
        "8.0.35",
        "8.4.2-log",
        "9.1.0",
        "10.6.0-MariaDB",
        "10.6.16-MariaDB-1:10.6.16+maria~ubu2004",
        "11.4.2-MariaDB",
        "10.6.1-mariadb-log",
    ] {
        assert_eq!(MySql.check_server(&EMAILS, accepted), Ok(()), "{accepted}");
    }
    for (refused, required) in [
        ("8.0.0", "MySQL 8.0.1"),
        ("5.7.44-log", "MySQL 8.0.1"),
        ("10.5.23-MariaDB", "MariaDB 10.6"),
        ("10.5.9-mariadb-log", "MariaDB 10.6"),
        ("garbage", "MySQL 8.0.1"),
        ("8.0", "MySQL 8.0.1"),
    ] {
        assert_eq!(
            MySql.check_server(&EMAILS, refused),
            Err(StatementError::ServerTooOld {
                dialect: "mysql",
                server: refused.to_owned(),
                required,
            }),
        );
    }
    // Both forms claim with `SKIP LOCKED`, so the floor holds for a lease table too.
    assert!(MySql.check_server(&LEASED, "5.7.44").is_err());
}
