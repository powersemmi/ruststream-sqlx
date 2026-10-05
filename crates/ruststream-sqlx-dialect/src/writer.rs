//! Statement text assembled through a dialect's quoting and placeholders.

use std::num::NonZeroUsize;

use crate::dialect::Dialect;
use crate::spec::{Role, TableSpec};
use crate::statement::{Param, Statement};
use crate::table_name::TableName;

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

    /// The conditions a row meets to be claimed, each present only with its column: the
    /// subscription's group, a time that has come, no finish mark.
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
                .param(Param::Now);
            keyword = " AND ";
        }
        if let Some(processed_at) = spec.column(Role::ProcessedAt) {
            self.push(keyword)
                .ident(processed_at.name())
                .push(" IS NULL");
        }
        self
    }

    /// The claim order: `priority`, then `retry_after`, then the id, each key only with its
    /// column.
    pub(crate) fn claim_order(&mut self, spec: &TableSpec<'_>, id: &str) -> &mut Self {
        self.push(" ORDER BY ");
        for role in [Role::Priority, Role::RetryAfter] {
            if let Some(column) = spec.column(role) {
                self.ident(column.name()).push(", ");
            }
        }
        self.ident(id)
    }

    /// `"column" = "column" + 1`.
    pub(crate) fn increment(&mut self, column: &str) -> &mut Self {
        self.ident(column).push(" = ").ident(column).push(" + 1")
    }

    pub(crate) fn finish(self) -> Statement {
        Statement::new(self.sql, self.params)
    }
}
