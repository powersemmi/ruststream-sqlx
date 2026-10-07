//! The statements the MySQL and MariaDB dialect builds for an outbox table.

use ruststream_sqlx_dialect::{
    Column, Dialect, Form, MySql, OutboxDialect, Param, StatementError, TableSpec,
};

/// An outbox table with a finish mark, on the database's clock, inside a schema.
const OUTBOX: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    .within("app")
    .group(Column::new("name"))
    .processed_at(Column::new("processed_at"))
    .headers(Column::new("headers"))
    .payload(Column::new("payload"))
    .database_clock();

/// The same table without the mark: a finished record is deleted.
const PLAIN: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    .group(Column::new("name"))
    .payload(Column::new("payload"))
    .database_clock();

const SELECT: &str = "`id`, `name`, `processed_at`, `headers`, `payload`";

#[test]
fn the_fetch_reads_an_unprocessed_record_by_its_id() -> Result<(), StatementError> {
    let fetch = MySql.outbox_fetch(&OUTBOX)?;
    assert_eq!(
        fetch.sql(),
        format!("SELECT {SELECT} FROM `app`.`outbox` WHERE `id` = ? AND `processed_at` IS NULL")
    );
    assert_eq!(fetch.params(), [Param::Id]);
    assert_eq!(
        MySql.outbox_fetch(&PLAIN)?.sql(),
        "SELECT `id`, `name`, `payload` FROM `outbox` WHERE `id` = ?"
    );
    Ok(())
}

#[test]
fn the_mark_is_the_dialects_acknowledgement() -> Result<(), StatementError> {
    let mark = MySql.outbox_mark(&OUTBOX)?;
    assert_eq!(
        mark.sql(),
        "UPDATE `app`.`outbox` SET `processed_at` = UTC_TIMESTAMP(6) WHERE `id` = ?"
    );
    assert_eq!(mark.params(), [Param::Id]);
    assert_eq!(mark, MySql.ack(&OUTBOX)?);
    let plain = MySql.outbox_mark(&PLAIN)?;
    assert_eq!(plain.sql(), "DELETE FROM `outbox` WHERE `id` = ?");
    assert_eq!(plain, MySql.ack(&PLAIN)?);
    Ok(())
}

#[test]
fn the_recovery_reads_the_unprocessed_records_of_one_name() -> Result<(), StatementError> {
    let recover = MySql.outbox_recover(&OUTBOX)?;
    assert_eq!(
        recover.sql(),
        format!("SELECT {SELECT} FROM `app`.`outbox` WHERE `name` = ? AND `processed_at` IS NULL")
    );
    assert_eq!(recover.params(), [Param::Group]);
    assert_eq!(
        MySql.outbox_recover(&PLAIN)?.sql(),
        "SELECT `id`, `name`, `payload` FROM `outbox` WHERE `name` = ?"
    );
    Ok(())
}
