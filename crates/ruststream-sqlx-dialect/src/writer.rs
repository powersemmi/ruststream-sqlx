//! Statement text assembled through a dialect's quoting, placeholders and clock, and the
//! statements every built-in dialect writes the same way.

use std::num::NonZeroUsize;

use crate::column::Column;
use crate::dialect::Dialect;
use crate::form::Form;
use crate::role::Role;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
use crate::table_name::TableName;

/// The keys a claim orders by before the id, the most significant first.
const ORDER_KEYS: [Role; 2] = [Role::Priority, Role::RetryAfter];

/// The columns a reader that knows no struct reads, with the roles they play: `id`,
/// `partition_key`, `attempt`, `headers` and `payload`, in [`Role::ALL`] order and only where the
/// table has them.
fn read_by_role<'a>(spec: &TableSpec<'a>) -> impl Iterator<Item = (Role, Column<'a>)> {
    Role::ALL
        .iter()
        .filter(|role| {
            matches!(
                role,
                Role::Id | Role::PartitionKey | Role::Attempt | Role::Headers | Role::Payload
            )
        })
        .filter_map(|&role| spec.column(role).map(|column| (role, column)))
}

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
}

/// What a built-in dialect adds to [`Dialect`]: the longest name its database keeps, whether it
/// locks rows, how an insert of no column reads, and the database's own clock.
///
/// The provided methods check a table against the dialect, and against a statement of one form
/// for the dialect's [`RowLock`](crate::RowLock) and [`Lease`](crate::Lease) implementations, and
/// build the statements every built-in dialect writes the same way, through its quoting,
/// placeholders and clock.
pub(crate) trait BuiltIn: Dialect {
    /// The longest name the database keeps; `None` where it keeps a name of any length.
    const NAME_LIMIT: Option<NameLimit>;

    /// Whether the database locks rows for a transaction, so the row lock form runs on it.
    const ROW_LOCKS: bool;

    /// What follows the table in an insert that writes no column, so every column takes its
    /// default.
    const DEFAULT_ROW: &'static str;

    /// The database's current time.
    fn database_now(&self) -> &'static str;

    /// Writes the database's current time plus [`Param::Delay`] microseconds.
    fn database_later(&self, sql: &mut SqlWriter<'_, Self>);

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
    /// or a lease on the crate's clock.
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
            other => Err(StatementError::UnsupportedForm {
                dialect: self.name(),
                form: other.name(),
            }),
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

    /// Refuses a table with FIFO groups for a claim: the built-in claims skip locked rows and
    /// would skip a group's head.
    fn in_order(&self, spec: &TableSpec<'_>) -> Result<(), StatementError> {
        if spec.is_fifo() {
            return Err(StatementError::UnsupportedFifo {
                dialect: self.name(),
            });
        }
        Ok(())
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

/// One statement being written: the SQL text, and the parameters its placeholders bind so far.
pub(crate) struct SqlWriter<'d, D: ?Sized> {
    dialect: &'d D,
    sql: String,
    params: Vec<Param>,
}

impl<'d, D> SqlWriter<'d, D>
where
    D: BuiltIn + ?Sized,
{
    pub(crate) fn new(dialect: &'d D) -> Self {
        Self {
            dialect,
            sql: String::new(),
            params: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, text: &str) -> &mut Self {
        self.sql.push_str(text);
        self
    }

    pub(crate) fn ident(&mut self, name: &str) -> &mut Self {
        self.dialect.quote_into(name, &mut self.sql);
        self
    }

    /// The queue's table, qualified with its schema when it has one.
    pub(crate) fn table(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        self.qualified(spec.schema(), spec.table())
    }

    /// Another table, qualified with its schema when it has one.
    pub(crate) fn table_name(&mut self, name: TableName<'_>) -> &mut Self {
        self.qualified(name.schema(), name.table())
    }

    fn qualified(&mut self, schema: Option<&str>, table: &str) -> &mut Self {
        if let Some(schema) = schema {
            self.ident(schema).push(".");
        }
        self.ident(table)
    }

    /// The next placeholder, bound to `param`.
    pub(crate) fn param(&mut self, param: Param) -> &mut Self {
        let index = NonZeroUsize::MIN.saturating_add(self.params.len());
        self.dialect.placeholder_into(index, &mut self.sql);
        self.params.push(param);
        self
    }

    /// The current time: a bound [`Param::Now`], or the database's own clock when the table reads
    /// it.
    pub(crate) fn now(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if spec.uses_database_clock() {
            self.push(self.dialect.database_now())
        } else {
            self.param(Param::Now)
        }
    }

    /// The time a delayed retry comes back: a bound [`Param::RetryAfter`], or the database's
    /// clock plus [`Param::Delay`] microseconds.
    pub(crate) fn later(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if spec.uses_database_clock() {
            let dialect = self.dialect;
            dialect.database_later(self);
            self
        } else {
            self.param(Param::RetryAfter)
        }
    }

    /// Every column, or `*` when the struct flattens another.
    pub(crate) fn columns(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if spec.selects_all() {
            return self.push("*");
        }
        for (index, column) in spec.columns().enumerate() {
            if index > 0 {
                self.push(", ");
            }
            self.ident(column.name());
        }
        self
    }

    /// Every column as a moved row carries it into another table, `NULL` in place of the lease's
    /// expiry so the row arrives without a lease; `*` when the struct flattens another.
    pub(crate) fn moved_columns(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if spec.selects_all() {
            return self.push("*");
        }
        let expiry = spec.column(Role::LockedUntil).map(|column| column.name());
        for (index, column) in spec.columns().enumerate() {
            if index > 0 {
                self.push(", ");
            }
            if expiry == Some(column.name()) {
                self.push("NULL");
            } else {
                self.ident(column.name());
            }
        }
        self
    }

    /// The columns a reader that knows no struct reads, each under its role's attribute name:
    /// `id`, `partition_key`, `attempt`, `headers` and `payload`, in [`Role::ALL`] order and only
    /// where the table has them.
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    pub(crate) fn role_columns(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        self.roles_read(spec, false)
    }

    /// The columns a reader that knows no struct reads, under their roles' names, with the attempt
    /// one less than the row holds where `counted`: the rows of an update that counted it, read
    /// as they were before.
    fn roles_read(&mut self, spec: &TableSpec<'_>, counted: bool) -> &mut Self {
        for (index, (role, column)) in read_by_role(spec).enumerate() {
            if index > 0 {
                self.push(", ");
            }
            self.ident(column.name());
            if counted && role == Role::Attempt {
                self.push(" - 1");
            }
            self.push(" AS ").ident(role.attribute());
        }
        self
    }

    /// Every column of a row an update that counted its attempt returns, the attempt one less
    /// under its own name, so the row reads as it was before; `*` when the struct flattens
    /// another, whose columns the description cannot see.
    #[cfg(feature = "sqlite")]
    fn columns_before_count(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if spec.selects_all() {
            return self.push("*");
        }
        let attempt = spec.column(Role::Attempt).map(|column| column.name());
        for (index, column) in spec.columns().enumerate() {
            if index > 0 {
                self.push(", ");
            }
            self.ident(column.name());
            if attempt == Some(column.name()) {
                self.push(" - 1 AS ").ident(column.name());
            }
        }
        self
    }

    /// The names [`role_columns`](Self::role_columns) gives its columns, as a select over its
    /// rows reads them.
    #[cfg(feature = "postgres")]
    fn role_names(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        for (index, (role, _)) in read_by_role(spec).enumerate() {
            if index > 0 {
                self.push(", ");
            }
            self.ident(role.attribute());
        }
        self
    }

    /// The conditions a row meets to be claimed, each present only with its column: the
    /// subscription's group, a time that has come, no finish mark, no lease in force.
    pub(crate) fn claimable(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        let mut keyword = " WHERE ";
        if let Some(group) = spec.column(Role::Group) {
            self.push(keyword)
                .ident(group.name())
                .push(" = ")
                .param(Param::Group);
            keyword = " AND ";
        }
        if let Some(retry_after) = spec.column(Role::RetryAfter) {
            self.push(keyword)
                .ident(retry_after.name())
                .push(" <= ")
                .now(spec);
            keyword = " AND ";
        }
        if let Some(processed_at) = spec.column(Role::ProcessedAt) {
            self.push(keyword)
                .ident(processed_at.name())
                .push(" IS NULL");
            keyword = " AND ";
        }
        if let Some(expiry) = spec.column(Role::LockedUntil) {
            self.push(keyword).lease_free(expiry.name());
        }
        self
    }

    /// No lease holds the row: it has none, or its lease ended by [`Param::LeaseNow`].
    pub(crate) fn lease_free(&mut self, expiry: &str) -> &mut Self {
        self.push("(")
            .ident(expiry)
            .push(" IS NULL OR ")
            .ident(expiry)
            .push(" <= ")
            .param(Param::LeaseNow)
            .push(")")
    }

    /// The row a settlement names: its id, and in the lease form the delivery's token.
    pub(crate) fn settled_row(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        self.push(" WHERE ")
            .ident(spec.id().name())
            .push(" = ")
            .param(Param::Id)
            .held(spec)
    }

    /// In the lease form, the condition that the row still holds the delivery's lease
    /// ([`Param::Held`]), following a condition on the row's id; nothing in the other forms.
    pub(crate) fn held(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if let Some(expiry) = spec.column(Role::LockedUntil) {
            self.push(" AND ")
                .ident(expiry.name())
                .push(" = ")
                .param(Param::Held);
        }
        self
    }

    /// In the lease form, one more assignment that clears the lease; nothing in the other forms.
    pub(crate) fn release(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if let Some(expiry) = spec.column(Role::LockedUntil) {
            self.push(", ").ident(expiry.name()).push(" = NULL");
        }
        self
    }

    /// The claim order: `priority`, then `retry_after`, then the id, each key only with its
    /// column.
    pub(crate) fn claim_order(&mut self, spec: &TableSpec<'_>, id: &str) -> &mut Self {
        self.push(" ORDER BY ");
        for column in ORDER_KEYS.iter().filter_map(|&role| spec.column(role)) {
            self.ident(column.name()).push(", ");
        }
        self.ident(id)
    }

    /// The columns a claim of `shape` selects.
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    fn claimed_columns(&mut self, spec: &TableSpec<'_>, shape: ClaimShape) -> &mut Self {
        match shape {
            ClaimShape::Rows => self.columns(spec),
            ClaimShape::Ids => self.ident(spec.id().name()),
            ClaimShape::Roles => self.role_columns(spec),
        }
    }

    /// A claim's select after its columns: the claimable rows of the table in claim order, at
    /// most [`Param::Limit`] of them, locked by `lock`.
    fn claimed_rows(&mut self, spec: &TableSpec<'_>, lock: &str) -> &mut Self {
        self.push(" FROM ")
            .table(spec)
            .claimable(spec)
            .claim_order(spec, spec.id().name())
            .push(" LIMIT ")
            .param(Param::Limit)
            .push(lock)
    }

    /// The claim that selects rows and locks them with `lock`: the columns of `shape`, of the
    /// claimable rows in claim order.
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    pub(crate) fn claim(
        &mut self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
        lock: &str,
    ) -> &mut Self {
        self.push("SELECT ")
            .claimed_columns(spec, shape)
            .claimed_rows(spec, lock)
    }

    /// The lease claim in one statement: the claim's select under `lock`, an update that writes
    /// the claimed rows' lease ([`Param::Lease`]) into `expiry` and counts their attempt, and the
    /// rows as the select read them, in claim order.
    #[cfg(feature = "postgres")]
    pub(crate) fn lease_claim(
        &mut self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
        expiry: &str,
        lock: &str,
    ) -> &mut Self {
        let id = spec.id().name();
        // A claim by role names the id by its role.
        let claimed_id = match shape {
            ClaimShape::Roles => Role::Id.attribute(),
            ClaimShape::Rows | ClaimShape::Ids => id,
        };
        self.push("WITH __claimed AS (SELECT ")
            .claimed_columns(spec, shape);
        if matches!(shape, ClaimShape::Ids | ClaimShape::Roles) {
            // The rows return in claim order, so the select keeps its keys; whole rows have them.
            for column in ORDER_KEYS.iter().filter_map(|&role| spec.column(role)) {
                self.push(", ").ident(column.name());
            }
        }
        self.claimed_rows(spec, lock)
            .push("), __stamped AS (UPDATE ")
            .table(spec)
            .push(" AS __row SET ")
            .ident(expiry)
            .push(" = ")
            .param(Param::Lease);
        if let Some(attempt) = spec.column(Role::Attempt) {
            // Qualified: the claimed rows may carry an attempt of their own.
            self.push(", ")
                .ident(attempt.name())
                .push(" = __row.")
                .ident(attempt.name())
                .push(" + 1");
        }
        self.push(" FROM __claimed WHERE __row.")
            .ident(id)
            .push(" = __claimed.")
            .ident(claimed_id)
            .push(") SELECT ");
        match shape {
            ClaimShape::Rows => self.push("*"),
            ClaimShape::Ids => self.ident(id),
            ClaimShape::Roles => self.role_names(spec),
        };
        self.push(" FROM __claimed").claim_order(spec, claimed_id)
    }

    /// The lease claim as one update that returns the rows it took: it writes the lease
    /// ([`Param::Lease`]) into `expiry` and counts the attempt of the claimable rows a subquery
    /// picks in claim order, and returns the columns of `shape` as the rows were before.
    #[cfg(feature = "sqlite")]
    pub(crate) fn returning_claim(
        &mut self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
        expiry: &str,
    ) -> &mut Self {
        let id = spec.id().name();
        self.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(expiry)
            .push(" = ")
            .param(Param::Lease);
        if let Some(attempt) = spec.column(Role::Attempt) {
            self.push(", ").increment(attempt.name());
        }
        // The subquery takes no lock: the update holds the database's one write lock.
        self.push(" WHERE ")
            .ident(id)
            .push(" IN (SELECT ")
            .ident(id)
            .claimed_rows(spec, "")
            .push(") RETURNING ");
        match shape {
            ClaimShape::Rows => self.columns_before_count(spec),
            ClaimShape::Ids => self.ident(id),
            ClaimShape::Roles => self.roles_read(spec, true),
        }
    }

    /// `"column" = "column" + 1`.
    pub(crate) fn increment(&mut self, column: &str) -> &mut Self {
        self.ident(column).push(" = ").ident(column).push(" + 1")
    }

    pub(crate) fn finish(self) -> Statement {
        Statement::new(self.sql, self.params)
    }
}
