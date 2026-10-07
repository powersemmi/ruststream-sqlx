//! The statements the SQLite dialect builds for the advisory lock form.

use std::error::Error;

use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Column, Dialect, Form, KeyPart, Lease, Param, Sqlite, Statement,
    StatementError, TableName, TableSpec,
};

use crate::{LEASED, LOCKED};

const JOB_KEY: &[KeyPart<'static>] = &[KeyPart::Column("job_id")];

/// `#[inbox(advisory_lock = "jobs-{job_id}")]`.
const PREFIXED_KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")];

/// Every role the advisory form reads; a lock on `jobs-{job_id}` holds a row.
const ADVISED: TableSpec<'static> =
    TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(PREFIXED_KEY))
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

/// The claim of an advisory table of an id alone, with the key `key`.
fn claim_keyed(key: &'static [KeyPart<'static>]) -> Result<Statement, StatementError> {
    Sqlite.advisory_claim(&TableSpec::new(
        "jobs",
        Column::new("job_id"),
        Form::Advisory(key),
    ))
}

#[test]
fn the_candidates_carry_their_keys() -> Result<(), StatementError> {
    // The process keeps the locks, so the select cannot tell a key in work.
    let claim = Sqlite.advisory_claim(&ADVISED)?;
    assert_eq!(
        claim.sql(),
        "SELECT `job_id`, CAST('jobs-' || ifnull(`job_id`, '') AS TEXT) AS `__lock` FROM `jobs` WHERE `name` = ? AND `retry_after` <= ? AND `processed_at` IS NULL ORDER BY `retry_after`, `job_id` LIMIT ?",
    );
    assert_eq!(claim.params(), [Param::Group, Param::Now, Param::Limit]);
    assert_eq!(Sqlite.lock(), None);
    assert_eq!(Sqlite.unlock(), None);
    Ok(())
}

#[test]
fn the_database_renders_the_key_from_its_parts() -> Result<(), StatementError> {
    // A key of one column renders without a literal.
    assert_eq!(
        claim_keyed(&[KeyPart::Column("job_id")])?.sql(),
        "SELECT `job_id`, CAST(ifnull(`job_id`, '') AS TEXT) AS `__lock` FROM `jobs` ORDER BY `job_id` LIMIT ?",
    );
    // A quote in a literal doubles; a backslash stays as it is.
    let quoted = claim_keyed(&[KeyPart::Literal(r"it's-a\b-"), KeyPart::Column("job_id")])?;
    assert!(
        quoted
            .sql()
            .contains(r"CAST('it''s-a\b-' || ifnull(`job_id`, '') AS TEXT) AS `__lock`"),
        "{}",
        quoted.sql()
    );
    // A key of no parts is the empty text: every row waits for one lock.
    let empty = claim_keyed(&[])?;
    assert!(
        empty.sql().contains("CAST('' AS TEXT) AS `__lock`"),
        "{}",
        empty.sql()
    );
    Ok(())
}

#[test]
fn the_take_counts_the_attempt_and_returns_the_row_as_it_was() -> Result<(), StatementError> {
    let rows = Sqlite.take(&ADVISED, ClaimShape::Rows)?;
    assert_eq!(
        rows.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            "UPDATE `jobs` SET `attempt` = `attempt` + 1 WHERE `job_id` = ? AND `name` = ? AND `retry_after` <= ? AND `processed_at` IS NULL RETURNING `job_id`, `name`, `retry_after`, `attempt` - 1 AS `attempt`, `processed_at`, `payload`",
        ]
    );
    assert_eq!(rows[0].params(), [Param::Id, Param::Group, Param::Now]);
    let roles = Sqlite.take(&ADVISED, ClaimShape::Roles)?;
    assert!(
        roles[0].sql().ends_with(
            "RETURNING `job_id` AS `id`, `attempt` - 1 AS `attempt`, `payload` AS `payload`"
        ),
        "{}",
        roles[0].sql()
    );
    // Without an attempt to count, the take reads the row while it is still claimable.
    let uncounted = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(PREFIXED_KEY))
        .group(Column::new("name"))
        .payload(Column::new("payload"));
    assert_eq!(
        Sqlite
            .take(&uncounted, ClaimShape::Roles)?
            .iter()
            .map(Statement::sql)
            .collect::<Vec<_>>(),
        [
            "SELECT `job_id` AS `id`, `payload` AS `payload` FROM `jobs` WHERE `job_id` = ? AND `name` = ?"
        ]
    );
    Ok(())
}

#[test]
fn the_advisory_form_settles_by_the_row_alone() -> Result<(), Box<dyn Error>> {
    for statement in [Sqlite.ack(&ADVISED)?, Sqlite.discard(&ADVISED)?] {
        assert_eq!(
            statement.sql(),
            "UPDATE `jobs` SET `processed_at` = ? WHERE `job_id` = ?"
        );
        assert_eq!(statement.params(), [Param::Now, Param::Id]);
    }
    // The claim counted the attempt, and the release of the key frees the row.
    assert_eq!(Sqlite.retry(&ADVISED)?, None);
    let retry_after = Sqlite.retry_after(&ADVISED)?;
    assert_eq!(
        retry_after.sql(),
        "UPDATE `jobs` SET `retry_after` = ? WHERE `job_id` = ?"
    );
    assert_eq!(retry_after.params(), [Param::RetryAfter, Param::Id]);
    let group = Sqlite.dead_letter_group(&ADVISED)?;
    assert_eq!(
        group.sql(),
        "UPDATE `jobs` SET `name` = ? WHERE `job_id` = ?"
    );
    let moves = Sqlite.dead_letter_table(&ADVISED, TableName::parse("jobs_dead")?)?;
    assert_eq!(
        moves.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            "INSERT INTO `jobs_dead` (`job_id`, `name`, `retry_after`, `attempt`, `processed_at`, `payload`) SELECT `job_id`, `name`, `retry_after`, `attempt`, `processed_at`, `payload` FROM `jobs` WHERE `job_id` = ?",
            "DELETE FROM `jobs` WHERE `job_id` = ?",
        ]
    );
    Ok(())
}

#[test]
fn the_advisory_statements_refuse_other_forms_and_fifo_groups() {
    let other = |statement, form| StatementError::FormMismatch { statement, form };
    let advisory = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(JOB_KEY));
    assert_eq!(
        Sqlite.lease_claim(&advisory, ClaimShape::Rows),
        Err(other("lease_claim", "advisory lock"))
    );
    assert_eq!(
        Sqlite.advisory_claim(&LEASED),
        Err(other("advisory_claim", "lease"))
    );
    assert_eq!(
        Sqlite.take(&LOCKED, ClaimShape::Rows),
        Err(other("take", "row lock"))
    );
    let fifo = ADVISED.fifo_group(Column::new("name"));
    let refused = StatementError::AdvisoryFifo { dialect: "sqlite" };
    assert_eq!(Sqlite.advisory_claim(&fifo), Err(refused.clone()));
    assert_eq!(Sqlite.take(&fifo, ClaimShape::Rows), Err(refused));
}
