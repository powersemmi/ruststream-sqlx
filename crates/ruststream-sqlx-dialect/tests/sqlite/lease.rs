//! The statements the SQLite dialect builds for the lease form.

use std::error::Error;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, Lease, Param, Role, Sqlite, Statement, StatementError,
    TableName, TableSpec,
};

use crate::{BARE, EXPIRY, LEASED};

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

#[test]
fn the_lease_claim_is_one_update_that_returns_the_rows() -> Result<(), StatementError> {
    assert_eq!(
        Sqlite.lease_claim(&LEASED, ClaimShape::Rows)?.sql(),
        "UPDATE `email_jobs` SET `locked_until` = ?, `attempt` = `attempt` + 1 WHERE `job_id` IN (SELECT `job_id` FROM `email_jobs` WHERE `name` = ? AND `retry_after` <= ? AND `processed_at` IS NULL AND (`locked_until` IS NULL OR `locked_until` <= ?) ORDER BY `retry_after`, `job_id` LIMIT ?) RETURNING `job_id`, `name`, `retry_after`, `attempt` - 1 AS `attempt`, `locked_until`, `processed_at`, `payload`",
    );
    assert_eq!(
        Sqlite.lease_claim(&LEASED, ClaimShape::Rows)?.params(),
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

    let bare = Sqlite.lease_claim(&BARE, ClaimShape::Rows)?;
    assert_eq!(
        bare.sql(),
        "UPDATE `jobs` SET `locked_until` = ? WHERE `job_id` IN (SELECT `job_id` FROM `jobs` WHERE (`locked_until` IS NULL OR `locked_until` <= ?) ORDER BY `job_id` LIMIT ?) RETURNING `job_id`, `locked_until`, `payload`",
    );
    assert_eq!(bare.params(), [Param::Lease, Param::LeaseNow, Param::Limit]);
    Ok(())
}

#[test]
fn the_claim_orders_by_priority_and_names_the_schema() -> Result<(), StatementError> {
    let spec = LEASED.within("app").priority(Column::new("priority"));
    let claim = Sqlite.lease_claim(&spec, ClaimShape::Ids)?;
    assert_eq!(
        claim.sql(),
        "UPDATE `app`.`email_jobs` SET `locked_until` = ?, `attempt` = `attempt` + 1 WHERE `job_id` IN (SELECT `job_id` FROM `app`.`email_jobs` WHERE `name` = ? AND `retry_after` <= ? AND `processed_at` IS NULL AND (`locked_until` IS NULL OR `locked_until` <= ?) ORDER BY `priority`, `retry_after`, `job_id` LIMIT ?) RETURNING `job_id`",
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
    let roles = Sqlite.lease_claim(&KEYED, ClaimShape::Roles)?;
    assert_eq!(
        roles.sql(),
        "UPDATE `email_jobs` SET `locked_until` = ?, `tries` = `tries` + 1 WHERE `job_id` IN (SELECT `job_id` FROM `email_jobs` WHERE `name` = ? AND `retry_after` <= ? AND (`locked_until` IS NULL OR `locked_until` <= ?) ORDER BY `retry_after`, `job_id` LIMIT ?) RETURNING `job_id` AS `id`, `customer` AS `partition_key`, `tries` - 1 AS `attempt`, `meta` AS `headers`, `payload` AS `payload`",
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
    let rows = Sqlite.lease_claim(&KEYED, ClaimShape::Rows)?;
    assert!(
        rows.sql().ends_with("RETURNING `job_id`, `name`, `customer`, `retry_after`, `tries` - 1 AS `tries`, `locked_until`, `meta`, `payload`, `subject`"),
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
    let claim = Sqlite.lease_claim(&flat, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        "UPDATE `email_jobs` SET `locked_until` = ?, `attempt` = `attempt` + 1 WHERE `job_id` IN (SELECT `job_id` FROM `email_jobs` WHERE `name` = ? AND `retry_after` <= ? AND `processed_at` IS NULL AND (`locked_until` IS NULL OR `locked_until` <= ?) ORDER BY `retry_after`, `job_id` LIMIT ?) RETURNING *",
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
            "UPDATE `email_jobs` SET `processed_at` = ?, `locked_until` = NULL WHERE `job_id` = ? AND `locked_until` = ?",
        );
        assert_eq!(statement.params(), [Param::Now, Param::Id, Param::Held]);
    }
    for statement in [Sqlite.ack(&BARE)?, Sqlite.discard(&BARE)?] {
        assert_eq!(
            statement.sql(),
            "DELETE FROM `jobs` WHERE `job_id` = ? AND `locked_until` = ?",
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
            "UPDATE `email_jobs` SET `locked_until` = NULL WHERE `job_id` = ? AND `locked_until` = ?"
        ),
    );
    assert_eq!(
        retry.as_ref().map(Statement::params),
        Some([Param::Id, Param::Held].as_slice())
    );
    let later = Sqlite.retry_after(&LEASED)?;
    assert_eq!(
        later.sql(),
        "UPDATE `email_jobs` SET `retry_after` = ?, `locked_until` = NULL WHERE `job_id` = ? AND `locked_until` = ?",
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
        "UPDATE `email_jobs` SET `name` = ?, `locked_until` = NULL WHERE `job_id` = ? AND `locked_until` = ?",
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
            "INSERT INTO `archive`.`dead_jobs` (`job_id`, `name`, `retry_after`, `attempt`, `locked_until`, `processed_at`, `payload`) SELECT `job_id`, `name`, `retry_after`, `attempt`, NULL, `processed_at`, `payload` FROM `email_jobs` WHERE `job_id` = ? AND `locked_until` = ?",
            "DELETE FROM `email_jobs` WHERE `job_id` = ? AND `locked_until` = ?",
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
        "UPDATE `email_jobs` SET `locked_until` = ? WHERE `job_id` = ? AND `locked_until` = ?",
    );
    assert_eq!(extend.params(), [Param::Lease, Param::Id, Param::Held]);
    // A claim of the service's own leaves each row to this stamp, inside its transaction.
    let stamp = Sqlite.stamp(&LEASED)?;
    assert_eq!(
        stamp.sql(),
        "UPDATE `email_jobs` SET `locked_until` = ?, `attempt` = `attempt` + 1 WHERE `job_id` = ? AND (`locked_until` IS NULL OR `locked_until` <= ?)",
    );
    assert_eq!(stamp.params(), [Param::Lease, Param::Id, Param::LeaseNow]);
    Ok(())
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
fn a_fifo_lease_claim_returns_the_head_alone() -> Result<(), Box<dyn Error>> {
    // Nothing while a row of the group holds a lease: a row that entered ahead of the head in work
    // waits for it.
    let claim = Sqlite.lease_claim(&LEASED_LEDGER, ClaimShape::Rows)?;
    assert_eq!(
        claim.sql(),
        "UPDATE `ledger` SET `locked_until` = ?, `attempt` = `attempt` + 1 WHERE `id` IN (SELECT `id` FROM `ledger` WHERE `id` = (SELECT `id` FROM `ledger` WHERE `account` = ? AND `processed_at` IS NULL ORDER BY `retry_after`, `id` LIMIT 1) AND `retry_after` <= ? AND (`locked_until` IS NULL OR `locked_until` <= ?) AND NOT EXISTS (SELECT 1 FROM `ledger` AS __work WHERE __work.`account` = ? AND __work.`locked_until` > ?)) RETURNING `id`, `account`, `retry_after`, `attempt` - 1 AS `attempt`, `locked_until`, `processed_at`, `payload`",
    );
    assert_eq!(
        claim.params(),
        [
            Param::Lease,
            Param::Group,
            Param::Now,
            Param::LeaseNow,
            Param::Group,
            Param::LeaseNow
        ]
    );
    // One writer at a time keeps two claims apart, so the claim takes no group first.
    assert_eq!(Sqlite.fifo_guard(&LEASED_LEDGER)?, None);
    Ok(())
}

#[test]
fn the_lease_cannot_read_the_database_clock() {
    let clocked = LEASED.database_clock();
    let refused = StatementError::LeaseOnDatabaseClock { dialect: "sqlite" };
    assert_eq!(
        Sqlite.lease_claim(&clocked, ClaimShape::Rows),
        Err(refused.clone())
    );
    assert_eq!(Sqlite.ack(&clocked), Err(refused.clone()));
    assert_eq!(Sqlite.retry_after(&clocked), Err(refused.clone()));
    assert_eq!(Sqlite.extend(&clocked), Err(refused.clone()));
    assert_eq!(Sqlite.stamp(&clocked), Err(refused));
}
