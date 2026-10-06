//! The statements the MySQL and MariaDB dialect builds for the row lock form.

use std::error::Error;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, Isolation, MySql, Param, Role, RowLock, Statement,
    StatementError, TableName, TableSpec,
};

use crate::{BARE, BARE_LEASED, EMAILS, LEASED, LEASED_LEDGER};

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

/// A ledger whose accounts keep their order: one row of an account in work, taken in claim order.
const LEDGER: TableSpec<'static> = TableSpec::new("ledger", Column::new("id"), Form::RowLock)
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

/// The ledger without a finish mark: a finished row is deleted.
const BARE_LEDGER: TableSpec<'static> =
    TableSpec::new("ledger", Column::new("id"), Form::RowLock).fifo_group(Column::new("account"));

#[test]
fn a_fifo_claim_takes_its_group_first() -> Result<(), Box<dyn Error>> {
    // The group is the claim's when a locking read that skips held rows takes every unfinished
    // row of it.
    let guard = MySql
        .fifo_guard(&LEDGER)?
        .ok_or("a table with FIFO groups has a guard")?;
    assert_eq!(
        guard.sql(),
        "SELECT CAST((SELECT COUNT(*) FROM `ledger` WHERE `account` = ? AND `processed_at` IS NULL) = (SELECT COUNT(*) FROM `ledger` WHERE `account` = ? AND `processed_at` IS NULL FOR UPDATE SKIP LOCKED) AS SIGNED)",
    );
    assert_eq!(guard.params(), [Param::Group, Param::Group]);
    // The lease form takes its group the same way, for the claim's transaction.
    assert_eq!(MySql.fifo_guard(&LEASED_LEDGER)?, Some(guard));
    let bare = MySql
        .fifo_guard(&BARE_LEDGER)?
        .ok_or("a table with FIFO groups has a guard")?;
    assert_eq!(
        bare.sql(),
        "SELECT CAST((SELECT COUNT(*) FROM `ledger` WHERE `account` = ?) = (SELECT COUNT(*) FROM `ledger` WHERE `account` = ? FOR UPDATE SKIP LOCKED) AS SIGNED)",
    );
    // A table whose groups keep no order needs no guard.
    assert_eq!(MySql.fifo_guard(&EMAILS)?, None);
    assert_eq!(MySql.fifo_guard(&LEASED)?, None);
    Ok(())
}

#[test]
fn a_fifo_row_lock_table_claims_below_serializable() -> Result<(), Box<dyn Error>> {
    // Every read of a SERIALIZABLE transaction locks, so the guard's count of the group would wait
    // for the row another claim holds instead of answering at once.
    let serializable = LEDGER.isolation(Isolation::Serializable);
    assert_eq!(
        MySql.fifo_guard(&serializable),
        Err(StatementError::FifoAtSerializable { dialect: "mysql" })
    );
    // The levels below it keep the guard.
    for level in [
        Isolation::ReadUncommitted,
        Isolation::ReadCommitted,
        Isolation::RepeatableRead,
    ] {
        assert!(
            MySql.fifo_guard(&LEDGER.isolation(level))?.is_some(),
            "{level:?}"
        );
    }
    // A lease claim opens at READ COMMITTED whatever the table names, and keeps its guard.
    let leased = LEASED_LEDGER.isolation(Isolation::Serializable);
    assert!(MySql.fifo_guard(&leased)?.is_some());
    // A table whose groups keep no order takes no group at any level.
    assert_eq!(
        MySql.fifo_guard(&EMAILS.isolation(Isolation::Serializable))?,
        None
    );
    Ok(())
}
