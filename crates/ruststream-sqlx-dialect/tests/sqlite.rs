//! The statements the SQLite dialect builds: the lease and advisory lock forms, and the form it
//! refuses.

#![cfg(feature = "sqlite")]

use std::error::Error;
use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Column, Dialect, Form, Isolation, KeyPart, Lease, Mode, Opening, Opens,
    Param, Role, Sqlite, Statement, StatementError, TableName, TableSpec, level,
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

/// The same queue in the row lock form, which SQLite has no locks for.
const LOCKED: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .payload(Column::new("payload"));

const JOB_KEY: &[KeyPart<'static>] = &[KeyPart::Column("job_id")];

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
fn a_fetch_by_a_list_of_ids_is_refused() {
    assert_eq!(
        Sqlite.fetch(&LEASED),
        Err(StatementError::UnsupportedFetch { dialect: "sqlite" })
    );
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

#[test]
fn sqlite_asks_nothing_of_its_server() -> Result<(), StatementError> {
    assert_eq!(Sqlite.server_version(), None);
    Sqlite.check_server(&LEASED, "3.50.4")?;
    Ok(())
}
