//! The statements the Postgres dialect builds for an outbox table.

use ruststream_sqlx_dialect::{
    Column, Dialect, Form, OutboxDialect, Param, Postgres, Role, StatementError, TableSpec,
};

const DATA: &[Column<'static>] = &[Column::new("created_at")];

/// An outbox table with a finish mark, on the database's clock.
const OUTBOX: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    .group(Column::new("name"))
    .processed_at(Column::new("processed_at"))
    .headers(Column::new("headers"))
    .payload(Column::new("payload"))
    .database_clock()
    .data(DATA);

/// The same table without the mark: a finished record is deleted.
const PLAIN: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    .group(Column::new("name"))
    .payload(Column::new("payload"))
    .database_clock();

const SELECT: &str = r#""id", "name", "processed_at", "headers", "payload", "created_at""#;

#[test]
fn the_fetch_reads_an_unprocessed_record_by_its_id() -> Result<(), StatementError> {
    let fetch = Postgres.outbox_fetch(&OUTBOX)?;
    assert_eq!(
        fetch.sql(),
        format!(r#"SELECT {SELECT} FROM "outbox" WHERE "id" = $1 AND "processed_at" IS NULL"#)
    );
    assert_eq!(fetch.params(), [Param::Id]);
    let plain = Postgres.outbox_fetch(&PLAIN)?;
    assert_eq!(
        plain.sql(),
        r#"SELECT "id", "name", "payload" FROM "outbox" WHERE "id" = $1"#
    );
    assert_eq!(plain.params(), [Param::Id]);
    Ok(())
}

#[test]
fn the_mark_is_the_dialects_acknowledgement() -> Result<(), StatementError> {
    let mark = Postgres.outbox_mark(&OUTBOX)?;
    assert_eq!(
        mark.sql(),
        r#"UPDATE "outbox" SET "processed_at" = statement_timestamp() WHERE "id" = $1"#
    );
    assert_eq!(mark.params(), [Param::Id]);
    assert_eq!(mark, Postgres.ack(&OUTBOX)?);
    let plain = Postgres.outbox_mark(&PLAIN)?;
    assert_eq!(plain.sql(), r#"DELETE FROM "outbox" WHERE "id" = $1"#);
    assert_eq!(plain, Postgres.ack(&PLAIN)?);
    Ok(())
}

#[test]
fn the_recovery_reads_the_unprocessed_records_of_one_name() -> Result<(), StatementError> {
    let recover = Postgres.outbox_recover(&OUTBOX)?;
    assert_eq!(
        recover.sql(),
        format!(r#"SELECT {SELECT} FROM "outbox" WHERE "name" = $1 AND "processed_at" IS NULL"#)
    );
    assert_eq!(recover.params(), [Param::Group]);
    let plain = Postgres.outbox_recover(&PLAIN)?;
    assert_eq!(
        plain.sql(),
        r#"SELECT "id", "name", "payload" FROM "outbox" WHERE "name" = $1"#
    );
    Ok(())
}

#[test]
fn a_table_inside_a_schema_is_qualified() -> Result<(), StatementError> {
    let spec = OUTBOX.within("app");
    assert!(
        Postgres
            .outbox_fetch(&spec)?
            .sql()
            .contains(r#" FROM "app"."outbox" WHERE"#)
    );
    assert!(
        Postgres
            .outbox_mark(&spec)?
            .sql()
            .starts_with(r#"UPDATE "app"."outbox" SET"#)
    );
    assert!(
        Postgres
            .outbox_recover(&spec)?
            .sql()
            .contains(r#" FROM "app"."outbox" WHERE"#)
    );
    Ok(())
}

#[test]
fn the_recovery_needs_the_name() {
    let unnamed =
        TableSpec::new("outbox", Column::new("id"), Form::RowLock).payload(Column::new("payload"));
    assert_eq!(
        Postgres.outbox_recover(&unnamed),
        Err(StatementError::MissingRole {
            statement: "outbox_recover",
            role: Role::Group,
        })
    );
}

#[test]
fn an_outbox_name_longer_than_postgres_keeps_is_refused() {
    let long = "n".repeat(64);
    let spec = TableSpec::new(&long, Column::new("id"), Form::RowLock).group(Column::new("name"));
    assert!(matches!(
        Postgres.outbox_fetch(&spec),
        Err(StatementError::IdentifierTooLong { .. })
    ));
    assert!(matches!(
        Postgres.outbox_mark(&spec),
        Err(StatementError::IdentifierTooLong { .. })
    ));
    assert!(matches!(
        Postgres.outbox_recover(&spec),
        Err(StatementError::IdentifierTooLong { .. })
    ));
}
