//! Statement text assembled through a dialect's quoting, placeholders and clock, and the
//! statements every built-in dialect writes the same way.

use std::num::NonZeroUsize;

use crate::spec::TableSpec;
use crate::statement::{Param, Statement};
use crate::table_name::TableName;

mod advisory;
mod built_in;
mod claim;
mod columns;
mod fifo;
mod lease;
mod outbox;
mod settle;

pub(crate) use advisory::Probe;
pub(crate) use built_in::BuiltIn;
pub(crate) use outbox::OutboxWriter;

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

    /// `text` as a string literal: quoted, each quote doubled, and each backslash doubled where a
    /// backslash escapes.
    pub(crate) fn literal(&mut self, text: &str) -> &mut Self {
        self.sql.push('\'');
        for character in text.chars() {
            if character == '\'' || (character == '\\' && D::BACKSLASH_ESCAPES) {
                self.sql.push(character);
            }
            self.sql.push(character);
        }
        self.sql.push('\'');
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

    /// `"column" = "column" + 1`.
    pub(crate) fn increment(&mut self, column: &str) -> &mut Self {
        self.ident(column).push(" = ").ident(column).push(" + 1")
    }

    pub(crate) fn finish(self) -> Statement {
        Statement::new(self.sql, self.params)
    }
}
