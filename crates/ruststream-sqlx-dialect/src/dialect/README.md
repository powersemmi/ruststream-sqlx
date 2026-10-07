A database's SQL: how it quotes names and numbers placeholders, and the statements every form
of claiming runs to settle a row.

A dialect reads a [`TableSpec`] and answers with [`Statement`]s whose
[`Param`](crate::Param)s are bound in order. Statements are built while a subscription starts,
never per message. A dialect refuses with a [`StatementError`] what it does not build, and
never hands out a statement with other semantics instead.

The forms of claiming are traits of their own, each over this one: [`RowLock`](crate::RowLock)
builds the claim that locks rows for its transaction, [`Lease`](crate::Lease) the claim that
writes a lease and the lease's extension, [`Advisory`](crate::Advisory) the claim of candidates
with their lock keys, the lock, the unlock and the take. A dialect implements the traits of the
forms its database serves, and a table in another form does not compile against it. A dialect
of the service's own does the same: it implements this trait, then the trait of each form it
builds, and writes every statement in its database's SQL.

The settlements here serve every form the dialect builds. In the lease form each of them names
the row and the delivery's ownership token ([`Param::Held`](crate::Param::Held)), so a delivery
whose lease ran out, and whose row another claim took, changes nothing. In the advisory lock
form they name the row alone: the lock on its key keeps every other claim off it.

A dialect also opens transactions: [`begin`](Self::begin) gives the statement that opens one
at a table's isolation level or SQLite mode, and [`savepoint`](Self::savepoint) and
[`rollback_to_savepoint`](Self::rollback_to_savepoint) mark where a handler's writes start
and discard them. In a table with FIFO groups, a claim's transaction first takes the
subscription's group with the statement [`fifo_guard`](Self::fifo_guard) gives.

# Examples

A dialect for a database without a built-in one implements the trait itself:

```
use std::num::NonZeroUsize;

use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
# use ruststream_sqlx_dialect::TableName;

/// SQL Server, a database without a built-in dialect.
#[derive(Debug)]
pub struct Mssql;

impl Dialect for Mssql {
    fn name(&self) -> &'static str {
        "mssql"
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        out.push('[');
        out.push_str(&ident.replace(']', "]]"));
        out.push(']');
    }

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        out.push_str("@p");
        out.push_str(&index.to_string());
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let mut sql = String::from("DELETE FROM ");
        self.quote_into(spec.table(), &mut sql);
        sql.push_str(" WHERE ");
        self.quote_into(spec.id().name(), &mut sql);
        sql.push_str(" = ");
        self.placeholder_into(NonZeroUsize::MIN, &mut sql);
        Ok(Statement::new(sql, [Param::Id]))
    }

    // The statements this service never runs refuse the table.
#     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
#     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
#     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
#     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
#     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
#     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
#     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
}
```
