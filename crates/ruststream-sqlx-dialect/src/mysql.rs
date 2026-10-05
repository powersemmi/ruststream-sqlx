//! The built-in MySQL and MariaDB dialect.

use std::num::NonZeroUsize;

use crate::dialect::Dialect;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
use crate::table_name::TableName;
use crate::writer::{BuiltIn, SqlWriter};

/// How MySQL reads the current time: UTC to the microsecond, fixed when the statement starts, so
/// it does not follow the session's time zone.
const DATABASE_NOW: &str = "UTC_TIMESTAMP(6)";

/// How a claim locks the rows it takes: until its transaction ends, skipping the rows another
/// claim holds.
const LOCK: &str = " FOR UPDATE SKIP LOCKED";

/// How a claim's transaction opens: at READ COMMITTED, which takes no gap locks. Both statements
/// travel as one text query, in the round trip `BEGIN` would take.
const BEGIN_CLAIM: &str = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION";

/// The oldest release of a server whose claims skip locked rows, and how a refusal names it.
#[derive(Debug, Clone, Copy)]
struct Floor {
    release: (u32, u32, u32),
    name: &'static str,
}

/// MySQL 8.0.1 added `SKIP LOCKED`.
const MYSQL_FLOOR: Floor = Floor {
    release: (8, 0, 1),
    name: "MySQL 8.0.1",
};

/// MariaDB 10.6 added `SKIP LOCKED`.
const MARIADB_FLOOR: Floor = Floor {
    release: (10, 6, 0),
    name: "MariaDB 10.6",
};

/// MySQL and MariaDB: backtick-quoted names, `?` placeholders, rows claimed with
/// `FOR UPDATE SKIP LOCKED`.
///
/// It builds the statements of the row lock and lease forms and the insert, for MySQL 8.0.1 and
/// MariaDB 10.6 or later, the first versions that skip locked rows;
/// [`check_server`](Dialect::check_server) refuses an older server. A lease claim selects the
/// claimable rows, then stamps each one with its lease ([`stamp`](Dialect::stamp)) before its
/// transaction commits. A dead letter into a table copies the row, then deletes it, in one
/// transaction. Every name is quoted, so a name keeps its case and may hold any character; a name
/// over 64 characters, which MySQL refuses, is refused before it reaches the server. A table on
/// the database's clock reads `UTC_TIMESTAMP(6)`. Rows are not read by a list of ids, so a claim
/// of the service's own brings its own fetch.
///
/// A claim's transaction opens at READ COMMITTED ([`begin_claim`](Dialect::begin_claim)): under
/// the default REPEATABLE READ a locking read also locks the gaps between the rows it scans, so a
/// claim held for a handler would block every insert into the table until it ends. A server whose
/// binary log records statements (`binlog_format = STATEMENT`) refuses writes in such a
/// transaction; the row and mixed formats accept them.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, MySql, TableSpec};
///
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
///     .within("app")
///     .priority(Column::new("priority"))
///     .payload(Column::new("payload"));
///
/// let claim = MySql.claim(&JOBS, ClaimShape::Rows)?;
/// assert_eq!(
///     claim.sql(),
///     "SELECT `job_id`, `priority`, `payload` FROM `app`.`jobs` ORDER BY `priority`, `job_id` LIMIT ? FOR UPDATE SKIP LOCKED",
/// );
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
///
/// In the lease form the claim only selects, and its transaction stamps each row it took:
///
/// ```
/// use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, MySql, Param, TableSpec};
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
///         .attempt(Column::new("attempt"));
///
/// // What a broker prepares to claim leased rows: the select, and the stamp it runs per row.
/// let mut claiming = vec![MySql.claim(&JOBS, ClaimShape::Rows)?];
/// if !MySql.claim_writes_lease() {
///     claiming.push(MySql.stamp(&JOBS)?);
/// }
/// assert_eq!(
///     claiming[1].sql(),
///     "UPDATE `jobs` SET `locked_until` = ?, `attempt` = `attempt` + 1 WHERE `job_id` = ? AND (`locked_until` IS NULL OR `locked_until` <= ?)",
/// );
/// assert_eq!(claiming[1].params(), [Param::Lease, Param::Id, Param::LeaseNow]);
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
///
/// A subscription that starts on an older server stops and names the version it needs:
///
/// ```
/// use ruststream_sqlx_dialect::{Column, Dialect, Form, MySql, TableSpec};
///
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
///
/// // The broker reads the version with `MySql.server_version()` and checks the answer.
/// let checked = MySql
///     .check_server(&JOBS, "10.5.23-MariaDB")
///     .map_err(|refused| refused.to_string());
/// assert_eq!(
///     checked,
///     Err("the mysql dialect needs MariaDB 10.6 or later for this form; the server reports \
///          `10.5.23-MariaDB`"
///         .to_owned()),
/// );
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct MySql;

impl BuiltIn for MySql {
    /// MySQL and MariaDB count a name's characters, and refuse a longer one.
    const NAME_LIMIT: NameLimit = NameLimit::Characters(64);

    const DEFAULT_ROW: &'static str = " () VALUES ()";

    fn database_now(&self) -> &'static str {
        DATABASE_NOW
    }

    fn database_later(&self, sql: &mut SqlWriter<'_, Self>) {
        sql.push(DATABASE_NOW)
            .push(" + INTERVAL ")
            .param(Param::Delay)
            .push(" MICROSECOND");
    }
}

impl Dialect for MySql {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        out.push('`');
        for character in ident.chars() {
            if character == '`' {
                out.push('`');
            }
            out.push(character);
        }
        out.push('`');
    }

    fn placeholder_into(&self, _: NonZeroUsize, out: &mut String) {
        out.push('?');
    }

    fn claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> {
        // An update cannot return the rows it changed here, so both forms select the claimable
        // rows under a lock; in the lease form the claim's transaction then stamps each of them.
        self.claim_form(spec)?;
        let mut sql = SqlWriter::new(self);
        sql.claim(spec, shape, LOCK);
        Ok(sql.finish())
    }

    fn fetch(&self, _: &TableSpec<'_>) -> Result<Statement, StatementError> {
        // A statement is prepared once with fixed text, and MySQL binds no list as one parameter.
        Err(StatementError::UnsupportedFetch {
            dialect: self.name(),
        })
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.finish_statement(spec)
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        self.retry_statement(spec)
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.retry_after_statement(spec)
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.finish_statement(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.dead_letter_group_statement(spec)
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        self.movable(spec, target)?;
        // Neither server feeds a delete's rows into an insert, so the row is copied while the
        // delivery holds it, then deleted.
        let mut copy = SqlWriter::new(self);
        copy.push("INSERT INTO ").table_name(target);
        if !spec.selects_all() {
            copy.push(" (").columns(spec).push(")");
        }
        copy.push(" SELECT ")
            .moved_columns(spec)
            .push(" FROM ")
            .table(spec)
            .settled_row(spec);
        let mut delete = SqlWriter::new(self);
        delete.push("DELETE FROM ").table(spec).settled_row(spec);
        Ok(vec![copy.finish(), delete.finish()])
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.extend_statement(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.stamp_statement(spec)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.insert_statement(spec)
    }

    fn claim_writes_lease(&self) -> bool {
        false
    }

    fn server_version(&self) -> Option<&'static str> {
        Some("SELECT VERSION()")
    }

    fn check_server(&self, _: &TableSpec<'_>, version: &str) -> Result<(), StatementError> {
        // Both forms claim with `SKIP LOCKED`, so the floor holds for every table.
        let floor = if is_mariadb(version) {
            MARIADB_FLOOR
        } else {
            MYSQL_FLOOR
        };
        if release(version).is_some_and(|release| release >= floor.release) {
            return Ok(());
        }
        Err(StatementError::ServerTooOld {
            dialect: self.name(),
            server: version.to_owned(),
            required: floor.name,
        })
    }

    fn begin_claim(&self) -> Option<&'static str> {
        Some(BEGIN_CLAIM)
    }
}

/// Whether the server is MariaDB: its version names it, in any case.
fn is_mariadb(version: &str) -> bool {
    const MARIADB: &[u8] = b"mariadb";
    version
        .as_bytes()
        .windows(MARIADB.len())
        .any(|window| window.eq_ignore_ascii_case(MARIADB))
}

/// The `major.minor.patch` release a version opens with, or `None` when it opens with none.
fn release(version: &str) -> Option<(u32, u32, u32)> {
    let digits = version
        .split(|character: char| !character.is_ascii_digit() && character != '.')
        .next()
        .unwrap_or_default();
    let mut numbers = digits.split('.').map(|number| number.parse().ok());
    let mut next = || numbers.next().flatten();
    Some((next()?, next()?, next()?))
}
