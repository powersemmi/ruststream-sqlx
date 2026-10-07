//! `BuiltIn`: what a built-in dialect adds to `Dialect`, and the statements every built-in
//! dialect writes the same way, form by form.

use super::{Probe, SqlWriter};
use crate::column::Column;
use crate::dialect::Dialect;
use crate::form::{Form, KeyPart};
use crate::role::Role;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
use crate::table_name::TableName;

/// Whether `name` fits in `limit`, measured the way the database measures it.
fn fits(name: &str, limit: NameLimit) -> bool {
    match limit {
        NameLimit::Bytes(most) => name.len() <= usize::from(most),
        NameLimit::Characters(most) => name.chars().count() <= usize::from(most),
    }
}

/// The name of the column that plays `role`, which `statement` cannot do without.
fn required<'a>(
    spec: &TableSpec<'a>,
    role: Role,
    statement: &'static str,
) -> Result<&'a str, StatementError> {
    spec.column(role)
        .map(|column| column.name())
        .ok_or(StatementError::MissingRole { statement, role })
}

/// A form the built-in dialects build statements for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Built<'a> {
    /// The claim's transaction holds the row.
    RowLock,
    /// The expiry in this column holds the row.
    Lease(Column<'a>),
    /// A lock on the row's key, held by the delivery's session, holds the row.
    Advisory,
}

/// What a built-in dialect adds to [`Dialect`]: the longest name its database keeps, whether it
/// locks rows, how an insert of no column reads, the database's own clock, and how it renders,
/// probes and takes an advisory lock key.
///
/// The provided methods check a table against the dialect, and against a statement of one form
/// for the dialect's [`RowLock`](crate::RowLock), [`Lease`](crate::Lease) and
/// [`Advisory`](crate::Advisory) implementations, and build the statements every built-in dialect
/// writes the same way, through its quoting, placeholders and clock.
pub(crate) trait BuiltIn: Dialect {
    /// The longest name the database keeps; `None` where it keeps a name of any length.
    const NAME_LIMIT: Option<NameLimit>;

    /// Whether the database locks rows for a transaction, so the row lock form runs on it.
    const ROW_LOCKS: bool;

    /// What follows the table in an insert that writes no column, so every column takes its
    /// default.
    const DEFAULT_ROW: &'static str;

    /// Whether a backslash in a string literal escapes the character after it, so a literal
    /// doubles each backslash of its text.
    const BACKSLASH_ESCAPES: bool;

    /// Whether an update returns the rows it changed, so the take counts the attempt and reads the
    /// row in one statement.
    const UPDATE_RETURNS: bool;

    /// How the advisory claim leaves out the candidates whose key another session holds.
    const PROBE: Probe;

    /// What a read of a row subtracts from the attempt its statement counted, so the attempt
    /// reads as it was before, in the column's own type.
    const UNCOUNT: &'static str = " - 1";

    /// The database's current time.
    fn database_now(&self) -> &'static str;

    /// Writes the database's current time plus [`Param::Delay`] microseconds.
    fn database_later(&self, sql: &mut SqlWriter<'_, Self>);

    /// Writes the lock key of a row of a table in `schema` (`None` for the connection's default):
    /// the text the database renders from `key`'s parts, as the dialect locks it.
    fn render_lock_key(
        &self,
        sql: &mut SqlWriter<'_, Self>,
        schema: Option<&str>,
        key: &[KeyPart<'_>],
    );

    /// Refuses a name longer than the database keeps.
    fn names_fit<'a>(
        &self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), StatementError> {
        let Some(limit) = Self::NAME_LIMIT else {
            return Ok(());
        };
        names
            .into_iter()
            .find(|name| !fits(name, limit))
            .map_or(Ok(()), |name| {
                Err(StatementError::IdentifierTooLong {
                    dialect: self.name(),
                    identifier: name.to_owned(),
                    limit,
                })
            })
    }

    /// Every name a statement of `spec` writes.
    fn spec_fits(&self, spec: &TableSpec<'_>) -> Result<(), StatementError> {
        self.names_fit(
            spec.schema()
                .into_iter()
                .chain([spec.table()])
                .chain(spec.columns().map(|column| column.name())),
        )
    }

    /// The table's form, when the dialect builds it: the row lock where the database locks rows,
    /// a lease on the crate's clock, or an advisory lock.
    fn form<'a>(&self, spec: &TableSpec<'a>) -> Result<Built<'a>, StatementError> {
        self.spec_fits(spec)?;
        match spec.form() {
            Form::RowLock if Self::ROW_LOCKS => Ok(Built::RowLock),
            // Settlement matches the expiry the claim wrote, and the claim knows it only when the
            // crate's clock computes it.
            Form::Lease(_) if spec.uses_database_clock() => {
                Err(StatementError::LeaseOnDatabaseClock {
                    dialect: self.name(),
                })
            }
            Form::Lease(expiry) => Ok(Built::Lease(expiry)),
            Form::Advisory(_) => Ok(Built::Advisory),
            other => Err(StatementError::UnsupportedForm {
                dialect: self.name(),
                form: other.name(),
            }),
        }
    }

    /// The lock key of a table, for `statement`, a statement of the advisory lock form: the table
    /// takes its rows by advisory lock without FIFO groups, and its names and its key's fit.
    fn advised<'a>(
        &self,
        spec: &TableSpec<'a>,
        statement: &'static str,
    ) -> Result<&'a [KeyPart<'a>], StatementError> {
        self.spec_fits(spec)?;
        match spec.form() {
            // In this form the lock key keeps a group in order.
            Form::Advisory(_) if spec.is_fifo() => Err(StatementError::AdvisoryFifo {
                dialect: self.name(),
            }),
            Form::Advisory(key) => {
                self.names_fit(key.iter().filter_map(|part| match part {
                    KeyPart::Column(column) => Some(*column),
                    KeyPart::Literal(_) => None,
                }))?;
                Ok(key)
            }
            other => Err(StatementError::FormMismatch {
                statement,
                form: other.name(),
            }),
        }
    }

    /// Whether a claim of `spec` takes its group first: the table keeps its groups in order. Such
    /// a table is checked as its claim checks it: its names fit, and its form takes a head.
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    fn guards_group(&self, spec: &TableSpec<'_>) -> Result<bool, StatementError> {
        if !spec.is_fifo() {
            return Ok(false);
        }
        match self.form(spec)? {
            // In this form the lock key keeps a group in order.
            Built::Advisory => Err(StatementError::AdvisoryFifo {
                dialect: self.name(),
            }),
            Built::RowLock | Built::Lease(_) => Ok(true),
        }
    }

    /// Checks a table for `statement`, a statement of the row lock form: the table takes its rows
    /// by row lock, and its names fit.
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    fn locked(&self, spec: &TableSpec<'_>, statement: &'static str) -> Result<(), StatementError> {
        self.spec_fits(spec)?;
        match spec.form() {
            Form::RowLock => Ok(()),
            other => Err(StatementError::FormMismatch {
                statement,
                form: other.name(),
            }),
        }
    }

    /// The lease column of a table, for `statement`, a statement of the lease form: the table
    /// takes its rows by lease on the crate's clock, and its names fit.
    fn leased<'a>(
        &self,
        spec: &TableSpec<'a>,
        statement: &'static str,
    ) -> Result<Column<'a>, StatementError> {
        self.spec_fits(spec)?;
        match spec.form() {
            // Settlement matches the expiry the claim wrote, and the claim knows it only when the
            // crate's clock computes it.
            Form::Lease(_) if spec.uses_database_clock() => {
                Err(StatementError::LeaseOnDatabaseClock {
                    dialect: self.name(),
                })
            }
            Form::Lease(expiry) => Ok(expiry),
            other => Err(StatementError::FormMismatch {
                statement,
                form: other.name(),
            }),
        }
    }

    /// Checks that a row of `spec` can move into `target`: the form is built, a lease table
    /// names every column, and the target's names fit.
    fn movable(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<(), StatementError> {
        if matches!(self.form(spec)?, Built::Lease(_)) && spec.selects_all() {
            // The moved row arrives without a lease, and `*` cannot put `NULL` in the lease
            // column's place.
            return Err(StatementError::Flattened {
                statement: "dead_letter_table",
            });
        }
        self.names_fit(target.schema().into_iter().chain([target.table()]))
    }

    /// Acknowledgement and drop: the row is deleted, or marked finished.
    fn finish_statement(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.form(spec)?;
        let mut sql = SqlWriter::new(self);
        match spec.column(Role::ProcessedAt) {
            Some(processed_at) => sql
                .push("UPDATE ")
                .table(spec)
                .push(" SET ")
                .ident(processed_at.name())
                .push(" = ")
                .now(spec)
                .release(spec),
            None => sql.push("DELETE FROM ").table(spec),
        };
        sql.settled_row(spec);
        Ok(sql.finish())
    }

    /// The release for another attempt at once, or `None` when the release needs no statement.
    fn retry_statement(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        let form = self.form(spec)?;
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ").table(spec).push(" SET ");
        match form {
            // The claim counted the attempt; the release only frees the row.
            Built::Lease(expiry) => sql.ident(expiry.name()).push(" = NULL"),
            Built::RowLock => match spec.column(Role::Attempt) {
                Some(attempt) => sql.increment(attempt.name()),
                // The rollback releases the row, and there is no attempt to count.
                None => return Ok(None),
            },
            // The take counted the attempt, and the unlock frees the row.
            Built::Advisory => return Ok(None),
        };
        sql.settled_row(spec);
        Ok(Some(sql.finish()))
    }

    /// The release for another attempt after a delay.
    fn retry_after_statement(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let form = self.form(spec)?;
        let retry_after = required(spec, Role::RetryAfter, "retry_after")?;
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(retry_after)
            .push(" = ")
            .later(spec);
        match form {
            Built::RowLock => {
                if let Some(attempt) = spec.column(Role::Attempt) {
                    sql.push(", ").increment(attempt.name());
                }
            }
            // The claim counted the attempt.
            Built::Lease(_) => {
                sql.release(spec);
            }
            // The take counted the attempt, and the unlock frees the row.
            Built::Advisory => {}
        }
        sql.settled_row(spec);
        Ok(sql.finish())
    }

    /// The move of a row whose attempts are spent to another group.
    fn dead_letter_group_statement(
        &self,
        spec: &TableSpec<'_>,
    ) -> Result<Statement, StatementError> {
        self.form(spec)?;
        let group = required(spec, Role::Group, "dead_letter_group")?;
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(group)
            .push(" = ")
            .param(Param::Destination)
            .release(spec)
            .settled_row(spec);
        Ok(sql.finish())
    }

    /// The extension of a delivery's lease while the row still holds its token.
    fn extend_statement(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let expiry = self.leased(spec, "extend")?;
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(expiry.name())
            .push(" = ")
            .param(Param::Lease)
            .settled_row(spec);
        Ok(sql.finish())
    }

    /// The lease of one claimed row, written while no lease holds the row.
    fn stamp_statement(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let expiry = self.leased(spec, "stamp")?;
        let id = spec.id().name();
        let mut sql = SqlWriter::new(self);
        sql.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(expiry.name())
            .push(" = ")
            .param(Param::Lease);
        if let Some(attempt) = spec.column(Role::Attempt) {
            sql.push(", ").increment(attempt.name());
        }
        sql.push(" WHERE ")
            .ident(id)
            .push(" = ")
            .param(Param::Id)
            .push(" AND ")
            .lease_free(expiry.name());
        Ok(sql.finish())
    }

    /// The advisory claim: the id and the lock key of up to [`Param::Limit`] claimable rows in
    /// claim order, without the rows whose key another session holds where the database can tell.
    fn advisory_claim_statement(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let key = self.advised(spec, "advisory_claim")?;
        let mut sql = SqlWriter::new(self);
        sql.candidates(spec, key);
        Ok(sql.finish())
    }

    /// The take of a candidate whose key the session holds: the count of its attempt and the read
    /// of the row as it was before the count, while the row is still claimable; a read alone in a
    /// table without an attempt.
    fn take_statements(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Vec<Statement>, StatementError> {
        self.advised(spec, "take")?;
        let Some(attempt) = spec.column(Role::Attempt) else {
            let mut read = SqlWriter::new(self);
            read.push("SELECT ")
                .claimed_columns(spec, shape)
                .push(" FROM ")
                .table(spec)
                .taken_row(spec);
            return Ok(vec![read.finish()]);
        };
        let mut count = SqlWriter::new(self);
        count
            .push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .increment(attempt.name())
            .taken_row(spec);
        if Self::UPDATE_RETURNS {
            count.push(" RETURNING ").counted_columns(spec, shape);
            return Ok(vec![count.finish()]);
        }
        // The read runs only after the count changed the row, and the session's lock keeps every
        // other claim off it, so the id alone names the row.
        let mut read = SqlWriter::new(self);
        read.push("SELECT ")
            .counted_columns(spec, shape)
            .push(" FROM ")
            .table(spec)
            .push(" WHERE ")
            .ident(spec.id().name())
            .push(" = ")
            .param(Param::Id);
        Ok(vec![count.finish(), read.finish()])
    }

    /// The move of a row whose attempts are spent into another table, as two statements of one
    /// transaction: a copy of the row while the delivery holds it, then its delete. For a database
    /// that cannot feed a delete's rows into an insert.
    #[cfg(any(feature = "mysql", feature = "sqlite"))]
    fn copy_then_delete(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        self.movable(spec, target)?;
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

    /// The insert of a row: every column the database does not fill.
    fn insert_statement(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        self.spec_fits(spec)?;
        if spec.selects_all() {
            return Err(StatementError::Flattened {
                statement: "insert",
            });
        }
        let mut sql = SqlWriter::new(self);
        sql.push("INSERT INTO ").table(spec);
        let written: Vec<(usize, &str)> = spec
            .columns()
            .enumerate()
            .filter(|(_, column)| !column.is_generated())
            .map(|(position, column)| (position, column.name()))
            .collect();
        if written.is_empty() {
            sql.push(Self::DEFAULT_ROW);
            return Ok(sql.finish());
        }
        sql.push(" (");
        for (index, (_, name)) in written.iter().enumerate() {
            if index > 0 {
                sql.push(", ");
            }
            sql.ident(name);
        }
        sql.push(") VALUES (");
        for (index, (position, _)) in written.iter().enumerate() {
            if index > 0 {
                sql.push(", ");
            }
            sql.param(Param::Column(*position));
        }
        sql.push(")");
        Ok(sql.finish())
    }
}
