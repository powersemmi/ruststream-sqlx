//! The statements the SQLite dialect builds for the lease and advisory lock forms: a module per
//! form, and here what every form shares and the row lock form it refuses.

#![cfg(feature = "sqlite")]

mod advisory;
mod lease;

use std::error::Error;
use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{
    ClaimShape, Column, Dialect, Form, Isolation, Lease, Mode, Opening, Opens, Param, Sqlite,
    StatementError, TableName, TableSpec, level,
};

const EXPIRY: Column<'static> = Column::new("locked_until");

/// A lease table with a group, a delayed retry, an attempt and a finish mark.
const LEASED: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::Lease(EXPIRY))
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"));

/// Only an id, a lease and a payload: rows are deleted when finished.
const BARE: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::Lease(EXPIRY))
    .payload(Column::new("payload"));

/// The same queue in the row lock form, which SQLite has no locks for.
const LOCKED: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .payload(Column::new("payload"));

fn quoted(ident: &str) -> String {
    let mut out = String::new();
    Sqlite.quote_into(ident, &mut out);
    out
}

#[test]
fn names_are_quoted_with_backticks_and_keep_their_case() {
    // SQLite reads a backtick-quoted name as a name wherever it stands, and a double-quoted one
    // that matches no column as text.
    assert_eq!(quoted("email_jobs"), "`email_jobs`");
    assert_eq!(quoted("EmailJobs"), "`EmailJobs`");
    assert_eq!(quoted("email jobs"), "`email jobs`");
    assert_eq!(quoted("a`b"), "`a``b`");
    assert_eq!(quoted("``"), "``````");
    assert_eq!(quoted(r#"a"b"#), r#"`a"b`"#);
}

#[test]
fn every_placeholder_is_a_question_mark() {
    let mut out = String::new();
    Sqlite.placeholder_into(NonZeroUsize::MIN, &mut out);
    out.push(' ');
    Sqlite.placeholder_into(NonZeroUsize::MIN.saturating_add(11), &mut out);
    assert_eq!(out, "? ?");
    assert_eq!(Sqlite.name(), "sqlite");
}

#[test]
fn a_claim_of_the_services_own_opens_its_transaction_for_writing() {
    // The write lock is taken before the claim's select, so two claims never read one row.
    assert_eq!(Sqlite.begin_lease_claim(), Some("BEGIN IMMEDIATE"));
}

#[test]
fn transactions_open_in_the_declared_mode() -> Result<(), Box<dyn Error>> {
    assert_eq!(Sqlite.begin(Opening::Default)?, None);
    assert_eq!(
        Sqlite.begin(Opening::Mode(Mode::Immediate))?,
        Some("BEGIN IMMEDIATE")
    );
    assert_eq!(
        Sqlite.begin(Opening::Mode(Mode::Exclusive))?,
        Some("BEGIN EXCLUSIVE")
    );
    assert_eq!(
        Sqlite.begin(Opening::Mode(Mode::Deferred))?,
        Some("BEGIN DEFERRED")
    );
    assert_eq!(
        Sqlite.begin(Opening::Isolation(Isolation::Serializable)),
        Err(StatementError::UnsupportedOpening {
            dialect: "sqlite",
            opening: Opening::Isolation(Isolation::Serializable).name(),
        })
    );
    assert!(
        Sqlite
            .begin(Opening::Isolation(Isolation::ReadUncommitted))
            .is_err()
    );
    assert_eq!(Sqlite.savepoint(), "SAVEPOINT ruststream_claim");
    assert_eq!(
        Sqlite.rollback_to_savepoint(),
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
fn every_mode_sqlite_opens_is_one_its_begin_accepts() {
    let opened = [
        begin_at::<(), _>(&Sqlite, Opening::Default),
        begin_at::<level::Deferred, _>(&Sqlite, Opening::Mode(Mode::Deferred)),
        begin_at::<level::Immediate, _>(&Sqlite, Opening::Mode(Mode::Immediate)),
        begin_at::<level::Exclusive, _>(&Sqlite, Opening::Mode(Mode::Exclusive)),
    ];
    assert!(opened.iter().all(Result::is_ok), "{opened:?}");
}

#[test]
fn the_insert_writes_every_column_the_database_does_not_fill() -> Result<(), StatementError> {
    let data = [
        Column::new("subject"),
        Column::new("created_at").generated(),
    ];
    // An insert serves a table in any form: publishing into a row lock table needs no lock.
    let spec = TableSpec::new(
        "email_jobs",
        Column::new("job_id").generated(),
        Form::RowLock,
    )
    .group(Column::new("name"))
    .retry_after(Column::new("retry_after"))
    .attempt(Column::new("attempt").generated())
    .payload(Column::new("payload"))
    .data(&data);
    let insert = Sqlite.insert(&spec)?;
    assert_eq!(
        insert.sql(),
        "INSERT INTO `email_jobs` (`name`, `retry_after`, `payload`, `subject`) VALUES (?, ?, ?, ?)",
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
fn an_insert_of_only_generated_columns_writes_the_defaults() -> Result<(), StatementError> {
    let spec = TableSpec::new(
        "jobs",
        Column::new("id").generated(),
        Form::Lease(EXPIRY.generated()),
    );
    let insert = Sqlite.insert(&spec)?;
    assert_eq!(insert.sql(), "INSERT INTO `jobs` DEFAULT VALUES");
    assert_eq!(insert.params(), []);
    assert_eq!(
        Sqlite.insert(&BARE.selecting_all()),
        Err(StatementError::Flattened {
            statement: "insert"
        })
    );
    Ok(())
}

#[test]
fn a_name_of_any_length_is_kept() -> Result<(), Box<dyn Error>> {
    let long = "n".repeat(1000);
    let spec =
        TableSpec::new(&long, Column::new(&long), Form::Lease(Column::new(&long))).within(&long);
    let claim = Sqlite.lease_claim(&spec, ClaimShape::Ids)?;
    assert!(claim.sql().contains(&quoted(&long)), "{}", claim.sql());
    let target = format!("{long}.{long}");
    assert_eq!(
        Sqlite
            .dead_letter_table(&spec, TableName::parse(&target)?)?
            .len(),
        2
    );
    Ok(())
}

#[test]
fn the_row_lock_form_is_refused_for_every_statement() -> Result<(), Box<dyn Error>> {
    let refused = StatementError::UnsupportedForm {
        dialect: "sqlite",
        form: "row lock",
    };
    let other = |statement| StatementError::FormMismatch {
        statement,
        form: "row lock",
    };
    assert_eq!(
        Sqlite.lease_claim(&LOCKED, ClaimShape::Rows),
        Err(other("lease_claim"))
    );
    assert_eq!(
        Sqlite.lease_claim(&LOCKED, ClaimShape::Roles),
        Err(other("lease_claim"))
    );
    assert_eq!(Sqlite.ack(&LOCKED), Err(refused.clone()));
    assert_eq!(Sqlite.retry(&LOCKED), Err(refused.clone()));
    assert_eq!(Sqlite.retry_after(&LOCKED), Err(refused.clone()));
    assert_eq!(Sqlite.discard(&LOCKED), Err(refused.clone()));
    assert_eq!(Sqlite.dead_letter_group(&LOCKED), Err(refused.clone()));
    assert_eq!(
        Sqlite.dead_letter_table(&LOCKED, TableName::parse("jobs_dead")?),
        Err(refused.clone())
    );
    assert_eq!(Sqlite.extend(&LOCKED), Err(other("extend")));
    assert_eq!(Sqlite.stamp(&LOCKED), Err(other("stamp")));
    // A table on the database's clock is refused for its form all the same.
    assert_eq!(Sqlite.ack(&LOCKED.database_clock()), Err(refused));
    Ok(())
}

#[test]
fn a_fetch_by_a_list_of_ids_is_refused() {
    assert_eq!(
        Sqlite.fetch(&LEASED),
        Err(StatementError::UnsupportedFetch { dialect: "sqlite" })
    );
}

#[test]
fn sqlite_asks_nothing_of_its_server() -> Result<(), StatementError> {
    assert_eq!(Sqlite.server_version(), None);
    Sqlite.check_server(&LEASED, "3.50.4")?;
    Ok(())
}
