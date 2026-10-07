//! The statements the Postgres dialect builds for the row lock, lease and advisory lock forms: a
//! module per form, and here what every form shares.

#![cfg(feature = "postgres")]

mod advisory;
mod lease;
mod outbox;
mod row_lock;

use std::error::Error;
use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, Isolation, Lease, Mode, NameLimit, Opening, Opens, Param,
    Postgres, RowLock, StatementError, TableName, TableSpec, level,
};

/// Only an id and a payload, in the default schema.
const BARE: TableSpec<'static> =
    TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));

const EXPIRY: Column<'static> = Column::new("locked_until");

/// Every role the lease form reads, in a table inside a schema.
const LEASED: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::Lease(EXPIRY))
        .within("app")
        .group(Column::new("name"))
        .priority(Column::new("priority"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

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
fn a_claim_transaction_opens_with_a_plain_begin() -> Result<(), StatementError> {
    // A table that names no isolation level opens its row lock claim with `BEGIN`.
    assert_eq!(Postgres.begin(BARE.opening())?, None);
    assert_eq!(Postgres.begin_lease_claim(), None);
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

#[test]
fn postgres_asks_nothing_of_its_server() -> Result<(), StatementError> {
    assert_eq!(Postgres.server_version(), None);
    Postgres.check_server(&LEASED, "17.2")?;
    Ok(())
}

#[test]
fn the_new_errors_name_what_to_do() {
    assert_eq!(
        StatementError::ServerTooOld {
            dialect: "mysql",
            server: "5.7.44".to_owned(),
            required: "MySQL 8.0.1"
        }
        .to_string(),
        "the mysql dialect needs MySQL 8.0.1 or later for this form; the server reports `5.7.44`"
    );
    assert_eq!(
        StatementError::UnsupportedFetch { dialect: "sqlite" }.to_string(),
        "the sqlite dialect cannot read rows by a list of ids: list `fetch` in `custom(..)` beside `claim`"
    );
    assert_eq!(
        StatementError::LeaseOnDatabaseClock {
            dialect: "postgres"
        }
        .to_string(),
        "the lease form computes its expiry from the crate's clock: a table on `DatabaseClock` cannot \
         declare `locked_until`"
    );
}
