//! The statements the MySQL and MariaDB dialect builds for the advisory lock form.

use std::error::Error;

use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Column, Dialect, Form, KeyPart, MySql, Param, Statement, StatementError,
    TableName, TableSpec,
};

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

/// The lock name MySQL takes for a key the expression `key` renders: the key itself, or its
/// SHA-256 where the server refuses it as a name.
fn guarded(key: &str) -> String {
    format!("IF(CHAR_LENGTH({key}) BETWEEN 1 AND 64, {key}, SHA2({key}, 256))")
}

/// The claim of an advisory table of an id alone, with the key `key`.
fn claim_keyed(key: &'static [KeyPart<'static>]) -> Result<Statement, StatementError> {
    MySql.advisory_claim(&TableSpec::new(
        "jobs",
        Column::new("job_id"),
        Form::Advisory(key),
    ))
}

#[test]
fn the_candidates_carry_their_guarded_keys_and_skip_the_keys_in_use() -> Result<(), StatementError>
{
    let claim = MySql.advisory_claim(&ADVISED)?;
    assert_eq!(
        claim.sql(),
        "SELECT `job_id`, IF(CHAR_LENGTH(CONCAT_WS('', LOWER(DATABASE()), '.', 'jobs-', `job_id`)) BETWEEN 1 AND 64, CONCAT_WS('', LOWER(DATABASE()), '.', 'jobs-', `job_id`), SHA2(CONCAT_WS('', LOWER(DATABASE()), '.', 'jobs-', `job_id`), 256)) AS `__lock` FROM `jobs` WHERE `name` = ? AND `retry_after` <= ? AND `processed_at` IS NULL AND IS_USED_LOCK(IF(CHAR_LENGTH(CONCAT_WS('', LOWER(DATABASE()), '.', 'jobs-', `job_id`)) BETWEEN 1 AND 64, CONCAT_WS('', LOWER(DATABASE()), '.', 'jobs-', `job_id`), SHA2(CONCAT_WS('', LOWER(DATABASE()), '.', 'jobs-', `job_id`), 256))) IS NULL ORDER BY `retry_after`, `job_id` LIMIT ?",
    );
    assert_eq!(claim.params(), [Param::Group, Param::Now, Param::Limit]);
    Ok(())
}

#[test]
fn the_database_renders_the_key_and_guards_its_length() -> Result<(), StatementError> {
    // A key of one column renders without a literal, and the probe is the only condition.
    let key = guarded("CONCAT_WS('', LOWER(DATABASE()), '.', `job_id`)");
    assert_eq!(
        claim_keyed(&[KeyPart::Column("job_id")])?.sql(),
        format!(
            "SELECT `job_id`, {key} AS `__lock` FROM `jobs` WHERE IS_USED_LOCK({key}) IS NULL ORDER BY `job_id` LIMIT ?"
        ),
    );
    // A quote in a literal doubles, and so does a backslash.
    let escaped = claim_keyed(&[KeyPart::Literal(r"it's-a\b-"), KeyPart::Column("job_id")])?;
    let key = guarded(r"CONCAT_WS('', LOWER(DATABASE()), '.', 'it''s-a\\b-', `job_id`)");
    assert!(
        escaped
            .sql()
            .starts_with(&format!("SELECT `job_id`, {key} AS `__lock`")),
        "{}",
        escaped.sql()
    );
    // A key of no parts is the empty text: every row waits for one lock, the database's own.
    let empty = claim_keyed(&[])?;
    let key = guarded("CONCAT_WS('', LOWER(DATABASE()), '.', '')");
    assert!(
        empty
            .sql()
            .starts_with(&format!("SELECT `job_id`, {key} AS `__lock`")),
        "{}",
        empty.sql()
    );
    Ok(())
}

#[test]
fn a_lock_name_starts_with_the_database_of_its_table() -> Result<(), StatementError> {
    // A table in a named database puts that name first; a table of the connection's default
    // database, the name the server reports for it.
    let within = MySql.advisory_claim(
        &TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(PREFIXED_KEY)).within("app"),
    )?;
    let key = guarded("CONCAT_WS('', LOWER('app'), '.', 'jobs-', `job_id`)");
    assert_eq!(
        within.sql(),
        format!(
            "SELECT `job_id`, {key} AS `__lock` FROM `app`.`jobs` WHERE IS_USED_LOCK({key}) IS NULL ORDER BY `job_id` LIMIT ?"
        ),
    );
    let key = guarded("CONCAT_WS('', LOWER(DATABASE()), '.', 'jobs-', `job_id`)");
    assert!(
        claim_keyed(PREFIXED_KEY)?
            .sql()
            .starts_with(&format!("SELECT `job_id`, {key} AS `__lock` FROM `jobs`")),
    );
    Ok(())
}

#[test]
fn the_lock_and_the_unlock_name_the_key_and_return_one_integer() {
    let lock = MySql.lock();
    assert_eq!(
        lock.as_ref().map(Statement::sql),
        Some("SELECT CAST(COALESCE(GET_LOCK(?, 0), 0) AS SIGNED)")
    );
    assert_eq!(
        lock.as_ref().map(Statement::params),
        Some([Param::Key].as_slice())
    );
    let unlock = MySql.unlock();
    assert_eq!(
        unlock.as_ref().map(Statement::sql),
        Some("SELECT CAST(COALESCE(RELEASE_LOCK(?), 0) AS SIGNED)")
    );
    assert_eq!(
        unlock.as_ref().map(Statement::params),
        Some([Param::Key].as_slice())
    );
}

#[test]
fn the_take_counts_the_attempt_then_reads_the_row_as_it_was() -> Result<(), StatementError> {
    let rows = MySql.take(&ADVISED, ClaimShape::Rows)?;
    assert_eq!(
        rows.iter().map(Statement::sql).collect::<Vec<_>>(),
        [
            "UPDATE `jobs` SET `attempt` = `attempt` + 1 WHERE `job_id` = ? AND `name` = ? AND `retry_after` <= ? AND `processed_at` IS NULL",
            "SELECT `job_id`, `name`, `retry_after`, `attempt` - 1 AS `attempt`, `processed_at`, `payload` FROM `jobs` WHERE `job_id` = ?",
        ]
    );
    assert_eq!(rows[0].params(), [Param::Id, Param::Group, Param::Now]);
    assert_eq!(rows[1].params(), [Param::Id]);
    let ids = MySql.take(&ADVISED, ClaimShape::Ids)?;
    assert_eq!(
        ids[1].sql(),
        "SELECT `job_id` FROM `jobs` WHERE `job_id` = ?"
    );
    let roles = MySql.take(&ADVISED, ClaimShape::Roles)?;
    assert_eq!(
        roles[1].sql(),
        "SELECT `job_id` AS `id`, `attempt` - 1 AS `attempt`, `payload` AS `payload` FROM `jobs` WHERE `job_id` = ?"
    );
    // `*` names no column, so the row reads with the attempt counted.
    let flat = MySql.take(&ADVISED.selecting_all(), ClaimShape::Rows)?;
    assert_eq!(flat[1].sql(), "SELECT * FROM `jobs` WHERE `job_id` = ?");
    Ok(())
}

#[test]
fn a_table_without_an_attempt_takes_by_reading_the_row() -> Result<(), StatementError> {
    let uncounted = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(PREFIXED_KEY))
        .within("app")
        .group(Column::new("name"))
        .payload(Column::new("payload"));
    let take = MySql.take(&uncounted, ClaimShape::Rows)?;
    assert_eq!(
        take.iter().map(Statement::sql).collect::<Vec<_>>(),
        ["SELECT `job_id`, `name`, `payload` FROM `app`.`jobs` WHERE `job_id` = ? AND `name` = ?"]
    );
    assert_eq!(take[0].params(), [Param::Id, Param::Group]);
    Ok(())
}

#[test]
fn the_advisory_form_settles_by_the_row_alone() -> Result<(), Box<dyn Error>> {
    for statement in [MySql.ack(&ADVISED)?, MySql.discard(&ADVISED)?] {
        assert_eq!(
            statement.sql(),
            "UPDATE `jobs` SET `processed_at` = ? WHERE `job_id` = ?"
        );
        assert_eq!(statement.params(), [Param::Now, Param::Id]);
    }
    // The claim counted the attempt, and the unlock frees the row.
    assert_eq!(MySql.retry(&ADVISED)?, None);
    let retry_after = MySql.retry_after(&ADVISED)?;
    assert_eq!(
        retry_after.sql(),
        "UPDATE `jobs` SET `retry_after` = ? WHERE `job_id` = ?"
    );
    assert_eq!(retry_after.params(), [Param::RetryAfter, Param::Id]);
    let clocked = MySql.retry_after(&ADVISED.database_clock())?;
    assert_eq!(
        clocked.sql(),
        "UPDATE `jobs` SET `retry_after` = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND WHERE `job_id` = ?"
    );
    let group = MySql.dead_letter_group(&ADVISED)?;
    assert_eq!(
        group.sql(),
        "UPDATE `jobs` SET `name` = ? WHERE `job_id` = ?"
    );
    let moves = MySql.dead_letter_table(&ADVISED, TableName::parse("jobs_dead")?)?;
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
fn an_advisory_fifo_group_is_refused() {
    let fifo = ADVISED.fifo_group(Column::new("name"));
    let refused = StatementError::AdvisoryFifo { dialect: "mysql" };
    assert_eq!(MySql.advisory_claim(&fifo), Err(refused.clone()));
    assert_eq!(MySql.take(&fifo, ClaimShape::Ids), Err(refused.clone()));
    assert_eq!(MySql.fifo_guard(&fifo), Err(refused));
}
