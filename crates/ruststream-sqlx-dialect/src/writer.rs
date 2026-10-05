//! Statement text assembled through a dialect's quoting and placeholders.

use std::num::NonZeroUsize;

use crate::column::Column;
use crate::dialect::Dialect;
use crate::role::Role;
use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Param, Statement};
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

/// One statement being written: the SQL text, and the parameters its placeholders bind so far.
pub(crate) struct SqlWriter<'d, D: ?Sized> {
    dialect: &'d D,
    sql: String,
    params: Vec<Param>,
}

impl<'d, D> SqlWriter<'d, D>
where
    D: Dialect + ?Sized,
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
    pub(crate) fn now(&mut self, spec: &TableSpec<'_>, database_now: &str) -> &mut Self {
        if spec.uses_database_clock() {
            self.push(database_now)
        } else {
            self.param(Param::Now)
        }
    }

    /// The time a delayed retry comes back: a bound [`Param::RetryAfter`], or the database's
    /// clock plus [`Param::Delay`] microseconds.
    pub(crate) fn later(&mut self, spec: &TableSpec<'_>, database_now: &str) -> &mut Self {
        if spec.uses_database_clock() {
            self.push(database_now)
                .push(" + ")
                .param(Param::Delay)
                .push(" * interval '1 microsecond'")
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
    pub(crate) fn role_columns(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        for (index, (role, column)) in read_by_role(spec).enumerate() {
            if index > 0 {
                self.push(", ");
            }
            self.ident(column.name())
                .push(" AS ")
                .ident(role.attribute());
        }
        self
    }

    /// The names [`role_columns`](Self::role_columns) gives its columns, as a select over its
    /// rows reads them.
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
    pub(crate) fn claimable(&mut self, spec: &TableSpec<'_>, database_now: &str) -> &mut Self {
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
                .now(spec, database_now);
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
    fn claimed_columns(&mut self, spec: &TableSpec<'_>, shape: ClaimShape) -> &mut Self {
        match shape {
            ClaimShape::Rows => self.columns(spec),
            ClaimShape::Ids => self.ident(spec.id().name()),
            ClaimShape::Roles => self.role_columns(spec),
        }
    }

    /// A claim's select after its columns: the claimable rows of the table in claim order, at
    /// most [`Param::Limit`] of them, locked by `lock`.
    fn claimed_rows(&mut self, spec: &TableSpec<'_>, database_now: &str, lock: &str) -> &mut Self {
        self.push(" FROM ")
            .table(spec)
            .claimable(spec, database_now)
            .claim_order(spec, spec.id().name())
            .push(" LIMIT ")
            .param(Param::Limit)
            .push(lock)
    }

    /// The claim that selects rows and locks them with `lock`: the columns of `shape`, of the
    /// claimable rows in claim order.
    pub(crate) fn claim(
        &mut self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
        database_now: &str,
        lock: &str,
    ) -> &mut Self {
        self.push("SELECT ")
            .claimed_columns(spec, shape)
            .claimed_rows(spec, database_now, lock)
    }

    /// The lease claim in one statement: the claim's select under `lock`, an update that writes
    /// the claimed rows' lease ([`Param::Lease`]) into `expiry` and counts their attempt, and the
    /// rows as the select read them, in claim order.
    pub(crate) fn lease_claim(
        &mut self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
        expiry: &str,
        database_now: &str,
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
        self.claimed_rows(spec, database_now, lock)
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

    /// `"column" = "column" + 1`.
    pub(crate) fn increment(&mut self, column: &str) -> &mut Self {
        self.ident(column).push(" = ").ident(column).push(" + 1")
    }

    pub(crate) fn finish(self) -> Statement {
        Statement::new(self.sql, self.params)
    }
}
