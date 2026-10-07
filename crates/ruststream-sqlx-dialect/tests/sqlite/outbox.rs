//! The statements the SQLite dialect builds for an outbox table: the row lock form it refuses to
//! a queue does not apply, since a record is taken by its id.

use ruststream_sqlx_dialect::{
    Column, Form, OutboxDialect, Param, Sqlite, StatementError, TableSpec,
};

/// An outbox table with a finish mark, on the database's clock.
const OUTBOX: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    .group(Column::new("name"))
    .processed_at(Column::new("processed_at"))
    .headers(Column::new("headers"))
    .payload(Column::new("payload"))
    .database_clock();

/// The same table without the mark, inside a schema: a finished record is deleted.
const PLAIN: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    .within("main")
    .group(Column::new("name"))
    .payload(Column::new("payload"))
    .database_clock();

const SELECT: &str = "`id`, `name`, `processed_at`, `headers`, `payload`";

#[test]
fn the_fetch_reads_an_unprocessed_record_by_its_id() -> Result<(), StatementError> {
    let fetch = Sqlite.outbox_fetch(&OUTBOX)?;
    assert_eq!(
        fetch.sql(),
        format!("SELECT {SELECT} FROM `outbox` WHERE `id` = ? AND `processed_at` IS NULL")
    );
    assert_eq!(fetch.params(), [Param::Id]);
    assert_eq!(
        Sqlite.outbox_fetch(&PLAIN)?.sql(),
        "SELECT `id`, `name`, `payload` FROM `main`.`outbox` WHERE `id` = ?"
    );
    Ok(())
}

#[test]
fn the_mark_writes_the_databases_clock_or_deletes() -> Result<(), StatementError> {
    let mark = Sqlite.outbox_mark(&OUTBOX)?;
    assert_eq!(
        mark.sql(),
        "UPDATE `outbox` SET `processed_at` = strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now') \
         WHERE `id` = ?"
    );
    assert_eq!(mark.params(), [Param::Id]);
    let plain = Sqlite.outbox_mark(&PLAIN)?;
    assert_eq!(plain.sql(), "DELETE FROM `main`.`outbox` WHERE `id` = ?");
    Ok(())
}

#[test]
fn the_recovery_reads_the_unprocessed_records_of_one_name() -> Result<(), StatementError> {
    let recover = Sqlite.outbox_recover(&OUTBOX)?;
    assert_eq!(
        recover.sql(),
        format!("SELECT {SELECT} FROM `outbox` WHERE `name` = ? AND `processed_at` IS NULL")
    );
    assert_eq!(recover.params(), [Param::Group]);
    assert_eq!(
        Sqlite.outbox_recover(&PLAIN)?.sql(),
        "SELECT `id`, `name`, `payload` FROM `main`.`outbox` WHERE `name` = ?"
    );
    Ok(())
}
