//! The built-in MySQL and MariaDB dialect.

use std::num::NonZeroUsize;

use crate::advisory::Advisory;
use crate::dialect::Dialect;
use crate::form::{Form, KeyPart};
use crate::lease::Lease;
use crate::opening::{Isolation, Opening, Opens, level};
use crate::row_lock::RowLock;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
use crate::table_name::TableName;
use crate::writer::{BuiltIn, Probe, SqlWriter};

/// How MySQL reads the current time: UTC to the microsecond, fixed when the statement starts, so
/// it does not follow the session's time zone.
const DATABASE_NOW: &str = "UTC_TIMESTAMP(6)";

/// How a claim locks the rows it takes: until its transaction ends, skipping the rows another
/// claim holds.
const LOCK: &str = " FOR UPDATE SKIP LOCKED";

/// How a claim's transaction opens: at READ COMMITTED, which takes no gap locks. Both statements
/// travel as one text query, in the round trip `BEGIN` would take, and the level holds for this
/// transaction alone.
const BEGIN_CLAIM: &str = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION";

// The levels a table names open the same way: one round trip, the level for this transaction.
const BEGIN_READ_UNCOMMITTED: &str =
    "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; START TRANSACTION";
const BEGIN_REPEATABLE_READ: &str =
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION";
const BEGIN_SERIALIZABLE: &str = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION";

/// Tries the session's lock on a key, without waiting. `GET_LOCK` answers `NULL` on an error,
/// which reads as a lock not taken.
const TRY_LOCK: &str = "SELECT CAST(COALESCE(GET_LOCK(?, 0), 0) AS SIGNED)";

/// Releases the session's lock on a key. `RELEASE_LOCK` answers `NULL` for a lock no session
/// holds, which reads as a lock not held.
const UNLOCK: &str = "SELECT CAST(COALESCE(RELEASE_LOCK(?), 0) AS SIGNED)";

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
/// It builds the statements of the row lock form ([`RowLock`]), of the lease form ([`Lease`]), of
/// the advisory lock form ([`Advisory`]), and the insert, for MySQL 8.0.1 and MariaDB 10.6 or
/// later, the first versions that skip locked rows; [`check_server`](Dialect::check_server)
/// refuses an older server for a table in any form. A lease claim selects
/// the claimable rows, then stamps each one with its lease ([`stamp`](Lease::stamp)) before its
/// transaction commits. A dead letter into a table copies the row, then deletes it, in one
/// transaction. Every name is quoted, so a name keeps its case and may hold any character; a name
/// over 64 characters, which MySQL refuses, is refused before it reaches the server. A table on
/// the database's clock reads `UTC_TIMESTAMP(6)`. [`fetch`](Dialect::fetch) is refused, so a
/// claim of the service's own brings a fetch of its own.
///
/// A claim's transaction opens at READ COMMITTED in both forms ([`begin`](Dialect::begin) for a
/// table that names no level, [`begin_lease_claim`](Lease::begin_lease_claim)): under the
/// server's default REPEATABLE READ a locking read also locks the gaps between the rows it scans,
/// so a claim held for a handler would block every insert into the table until it ends. A server
/// whose binary log records statements (`binlog_format = STATEMENT`) refuses writes in such a
/// transaction; the row and mixed formats accept them. A row lock table that names a level opens
/// its claims at it, any of the four, READ UNCOMMITTED included; REPEATABLE READ and SERIALIZABLE
/// bring the gap locks back.
///
/// In the advisory lock form a row's key is the text `CONCAT_WS` renders from the key's parts, a
/// column without a value skipped. A session lock (`GET_LOCK`) holds the row, named by the table's
/// database, a dot and the key (`app.jobs-7`): the server names its locks server-wide, and the
/// database keeps the locks of two databases on one server apart. The server takes a lock name of
/// 1 to 64 characters, so a longer name is locked by its SHA-256, 64 characters of hex: the claim
/// selects the name it locks. The claim leaves out the names `IS_USED_LOCK` reports in use. An
/// update returns no rows here, so the take counts the attempt, then reads the row.
///
/// In a table with FIFO groups a claim's transaction takes its group
/// ([`fifo_guard`](Dialect::fifo_guard)) with a locking read of the group's unfinished rows that
/// skips the rows other transactions hold: the group is the claim's when the read took all of
/// them. The rows stay locked until the transaction ends, in the row lock form until the delivery
/// settles, and a write to one of them from elsewhere waits that long. Each claim reads every
/// unfinished row of its group, and the whole table where the group's column has no index. A row
/// lock table with FIFO groups claims below SERIALIZABLE: every read of a SERIALIZABLE
/// transaction locks the rows it reads, so the guard would wait for the group's row in work, and
/// it refuses such a table with [`StatementError::FifoAtSerializable`].
///
/// # Examples
///
/// A dialect of the service's own wraps MySQL and changes one statement:
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Dialect, MySql, Opening, Param, RowLock, Statement, StatementError, TableSpec,
/// };
///
/// /// MySQL, with an acknowledgement of the service's own.
/// #[derive(Debug)]
/// pub struct Audited;
///
/// impl Dialect for Audited {
///     fn name(&self) -> &'static str {
///         "audited"
///     }
///
///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         if spec.table() == "email_jobs" {
///             return Ok(Statement::new(
///                 "UPDATE `email_jobs` SET `name` = 'sent' WHERE `job_id` = ?",
///                 [Param::Id],
///             ));
///         }
///         MySql.ack(spec)
///     }
///
///     // What MySQL answers beside its statements stays MySQL's: claims open at READ COMMITTED, a
///     // FIFO group is taken before its claim, and an older server stops the subscription.
///     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
///         MySql.begin(opening)
///     }
///
///     fn fifo_guard(
///         &self,
///         spec: &TableSpec<'_>,
///     ) -> Result<Option<Statement>, StatementError> {
///         MySql.fifo_guard(spec)
///     }
///
///     fn server_version(&self) -> Option<&'static str> {
///         MySql.server_version()
///     }
///
///     fn check_server(
///         &self,
///         spec: &TableSpec<'_>,
///         version: &str,
///     ) -> Result<(), StatementError> {
///         MySql.check_server(spec, version)
///     }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { MySql.quote_into(ident, out); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { MySql.placeholder_into(index, out); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.fetch(spec) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { MySql.retry(spec) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.retry_after(spec) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.discard(spec) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.dead_letter_group(spec) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { MySql.dead_letter_table(spec, target) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { MySql.insert(spec) }
/// }
///
/// impl RowLock for Audited {
///     fn lock_claim(
///         &self,
///         spec: &TableSpec<'_>,
///         shape: ClaimShape,
///     ) -> Result<Statement, StatementError> {
///         MySql.lock_claim(spec, shape)
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct MySql;

impl BuiltIn for MySql {
    /// MySQL and MariaDB count a name's characters, and refuse a longer one.
    const NAME_LIMIT: Option<NameLimit> = Some(NameLimit::Characters(64));

    const ROW_LOCKS: bool = true;

    const DEFAULT_ROW: &'static str = " () VALUES ()";

    /// A backslash escapes the next character of a string literal, unless the server's SQL mode
    /// has `NO_BACKSLASH_ESCAPES`; under that mode a doubled backslash reads as two, the same in
    /// every statement, so a key stays one key.
    const BACKSLASH_ESCAPES: bool = true;

    const UPDATE_RETURNS: bool = false;

    const PROBE: Probe = Probe::Check("IS_USED_LOCK(", ") IS NULL");

    fn database_now(&self) -> &'static str {
        DATABASE_NOW
    }

    fn database_later(&self, sql: &mut SqlWriter<'_, Self>) {
        sql.push(DATABASE_NOW)
            .push(" + INTERVAL ")
            .param(Param::Delay)
            .push(" MICROSECOND");
    }

    fn render_lock_key(
        &self,
        sql: &mut SqlWriter<'_, Self>,
        schema: Option<&str>,
        key: &[KeyPart<'_>],
    ) {
        // The server names its locks server-wide, so the name starts with the table's database:
        // two databases on one server keep their locks apart, as Postgres keeps them per database.
        // The database's name is lowercased, so a table named with its database and the same table
        // reached through the connection's default name one lock alike, whatever case each writes.
        let text = |sql: &mut SqlWriter<'_, Self>| {
            sql.push("CONCAT_WS('', LOWER(");
            match schema {
                Some(schema) => sql.literal(schema),
                None => sql.push("DATABASE()"),
            };
            sql.push("), '.', ")
                .key_parts(key, ", ", |sql, column| {
                    sql.ident(column);
                })
                .push(")");
        };
        // MySQL refuses a lock name that is empty or longer than 64 characters, and MariaDB takes
        // no lock on an empty one: such a name is locked by its hash.
        sql.push("IF(CHAR_LENGTH(");
        text(sql);
        sql.push(") BETWEEN 1 AND 64, ");
        text(sql);
        sql.push(", SHA2(");
        text(sql);
        sql.push(", 256))");
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
        // Neither server feeds a delete's rows into an insert.
        self.copy_then_delete(spec, target)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.insert_statement(spec)
    }

    fn server_version(&self) -> Option<&'static str> {
        Some("SELECT VERSION()")
    }

    fn check_server(&self, _: &TableSpec<'_>, version: &str) -> Result<(), StatementError> {
        // The row lock and lease forms claim with `SKIP LOCKED`, and an advisory table shares
        // their floor: one floor for every table of a server.
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

    fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
        match opening {
            Opening::Default | Opening::Isolation(Isolation::ReadCommitted) => {
                Ok(Some(BEGIN_CLAIM))
            }
            Opening::Isolation(Isolation::ReadUncommitted) => Ok(Some(BEGIN_READ_UNCOMMITTED)),
            Opening::Isolation(Isolation::RepeatableRead) => Ok(Some(BEGIN_REPEATABLE_READ)),
            Opening::Isolation(Isolation::Serializable) => Ok(Some(BEGIN_SERIALIZABLE)),
            Opening::Mode(_) => Err(opening.refused(self.name())),
        }
    }

    fn fifo_guard(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        if !self.guards_group(spec)? {
            return Ok(None);
        }
        // Why a refusal: InnoDB turns every read of a SERIALIZABLE transaction into a locking
        // read, so the plain count below would wait for the row another claim holds instead of
        // answering at once. A lease claim opens at READ COMMITTED whatever the table names, so
        // only the row lock form claims at the table's level.
        if matches!(spec.form(), Form::RowLock)
            && spec.opening() == Opening::Isolation(Isolation::Serializable)
        {
            return Err(StatementError::FifoAtSerializable {
                dialect: self.name(),
            });
        }
        // A named lock belongs to the session here, not to the transaction. A locking read that
        // skips the rows other transactions hold takes every unfinished row of the group only
        // when no other transaction holds one, and its locks last until the transaction ends.
        let mut sql = SqlWriter::new(self);
        sql.push("SELECT CAST((")
            .group_count(spec, "")
            .push(") = (")
            .group_count(spec, LOCK)
            .push(") AS SIGNED)");
        Ok(Some(sql.finish()))
    }
}

impl Opens<level::ReadUncommitted> for MySql {}

impl Opens<level::ReadCommitted> for MySql {}

impl Opens<level::RepeatableRead> for MySql {}

impl Opens<level::Serializable> for MySql {}

impl RowLock for MySql {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        self.locked(spec, "lock_claim")?;
        let mut sql = SqlWriter::new(self);
        sql.claim(spec, shape, LOCK);
        Ok(sql.finish())
    }
}

impl Advisory for MySql {
    fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.advisory_claim_statement(spec)
    }

    fn lock(&self) -> Option<Statement> {
        Some(Statement::new(TRY_LOCK, [Param::Key]))
    }

    fn unlock(&self) -> Option<Statement> {
        Some(Statement::new(UNLOCK, [Param::Key]))
    }

    fn take(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError> {
        self.take_statements(spec, shape)
    }
}

impl Lease for MySql {
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        // An update cannot return the rows it changed here, so the claim selects the claimable
        // rows under a lock, and its transaction then stamps each of them.
        self.leased(spec, "lease_claim")?;
        let mut sql = SqlWriter::new(self);
        sql.claim(spec, shape, LOCK);
        Ok(sql.finish())
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.extend_statement(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.stamp_statement(spec)
    }

    fn claim_writes_lease(&self) -> bool {
        false
    }

    fn begin_lease_claim(&self) -> Option<&'static str> {
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
