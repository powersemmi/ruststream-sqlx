//! The statements the MySQL and MariaDB dialect builds for the lease form.

use std::error::Error;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Lease, MySql, Param, Statement, StatementError, TableName,
};

use crate::{BARE_LEASED, LEASED, LEASED_LEDGER};

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
fn a_fifo_lease_claim_selects_the_head_alone() -> Result<(), Box<dyn Error>> {
    // The claim selects the head under a lock, and its transaction stamps it; nothing while a row
    // of the group holds a lease.
    let claim = MySql.lease_claim(&LEASED_LEDGER, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        "SELECT `id`, `account`, `retry_after`, `attempt`, `locked_until`, `processed_at`, `payload` FROM `ledger` WHERE `id` = (SELECT `id` FROM `ledger` WHERE `account` = ? AND `processed_at` IS NULL ORDER BY `retry_after`, `id` LIMIT 1) AND `retry_after` <= ? AND (`locked_until` IS NULL OR `locked_until` <= ?) AND NOT EXISTS (SELECT 1 FROM `ledger` AS __work WHERE __work.`account` = ? AND __work.`locked_until` > ?) FOR UPDATE SKIP LOCKED",
    );
    assert_eq!(
        claim.params(),
        [
            Param::Group,
            Param::Now,
            Param::LeaseNow,
            Param::Group,
            Param::LeaseNow
        ]
    );
    Ok(())
}
