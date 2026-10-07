//! The statements the MySQL and MariaDB dialect builds for the row lock, lease and advisory lock
//! forms: a module per form, and here what every form shares.

#![cfg(feature = "mysql")]

mod advisory;
mod lease;
mod outbox;
mod row_lock;

use std::error::Error;
use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Column, Dialect, Form, Isolation, KeyPart, Lease, Mode, MySql, NameLimit,
    Opening, Opens, Param, RowLock, StatementError, TableName, TableSpec, level,
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
    let group =
        TableSpec::new("ledger", Column::new("id"), Form::RowLock).fifo_group(Column::new(&long));
    assert_eq!(MySql.fifo_guard(&group), Err(refused(&long)));

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
fn the_statements_of_a_form_refuse_tables_of_other_forms() {
    let advisory = TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(JOB_KEY));
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
    assert_eq!(
        MySql.advisory_claim(&BARE),
        Err(other("advisory_claim", "row lock"))
    );
    assert_eq!(
        MySql.take(&LEASED, ClaimShape::Rows),
        Err(other("take", "lease"))
    );
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
