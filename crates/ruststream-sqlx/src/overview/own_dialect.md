## A dialect of the service's own

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use std::num::NonZeroUsize;

use ruststream_sqlx::dialect::{
    ClaimShape, Dialect, Param, Role, RowLock, Statement, StatementError, TableSpec,
};
# use ruststream_sqlx::dialect::{self, TableName};
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

// email_jobs: job_id INT8 PRIMARY KEY DEFAULT unique_rowid(), name STRING NOT NULL,
// payload BYTES NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

/// CockroachDB, reached through sqlx's Postgres driver.
#[derive(Debug)]
pub struct Cockroach;

impl Dialect for Cockroach {
    fn name(&self) -> &'static str {
        "cockroach"
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        out.push('"');
        out.push_str(&ident.replace('"', "\"\""));
        out.push('"');
    }

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        out.push('$');
        out.push_str(&index.to_string());
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let mut sql = String::from("DELETE FROM ");
        self.quote_into(spec.table(), &mut sql);
        sql.push_str(" WHERE ");
        self.quote_into(spec.id().name(), &mut sql);
        sql.push_str(" = $1");
        Ok(Statement::new(sql, [Param::Id]))
    }
#    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.fetch(spec) }
#    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { dialect::Postgres.retry(spec) }
#    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.retry_after(spec) }
#    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.discard(spec) }
#    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.dead_letter_group(spec) }
#    fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { dialect::Postgres.dead_letter_table(spec, target) }
#    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.insert(spec) }
}

// The row lock form, which `SendEmail` takes: the due rows of the subscription's group in claim
// order, locked for the claim's transaction, with the rows other claims hold passed over.
impl RowLock for Cockroach {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        if spec.is_fifo() {
            return Err(StatementError::UnsupportedFifo { dialect: self.name() });
        }
        let mut sql = String::from("SELECT ");
        match shape {
            ClaimShape::Ids => self.quote_into(spec.id().name(), &mut sql),
            ClaimShape::Rows if spec.selects_all() => sql.push('*'),
            ClaimShape::Rows => {
                for (i, column) in spec.columns().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    self.quote_into(column.name(), &mut sql);
                }
            }
            ClaimShape::Roles => {
                let roles = [
                    Role::Id,
                    Role::PartitionKey,
                    Role::Attempt,
                    Role::Headers,
                    Role::Payload,
                ];
                let played = roles
                    .into_iter()
                    .filter_map(|role| Some((role, spec.column(role)?)));
                for (i, (role, column)) in played.enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    self.quote_into(column.name(), &mut sql);
                    sql.push_str(" AS ");
                    self.quote_into(role.attribute(), &mut sql);
                }
            }
        }
        sql.push_str(" FROM ");
        self.quote_into(spec.table(), &mut sql);

        let mut params = Vec::new();
        let mut conditions = Vec::new();
        if let Some(group) = spec.column(Role::Group) {
            params.push(Param::Group);
            conditions.push((group, " = "));
        }
        if let Some(due) = spec.column(Role::RetryAfter) {
            params.push(Param::Now);
            conditions.push((due, " <= "));
        }
        for (i, (column, operator)) in conditions.into_iter().enumerate() {
            sql.push_str(if i == 0 { " WHERE " } else { " AND " });
            self.quote_into(column.name(), &mut sql);
            sql.push_str(operator);
            self.placeholder_into(NonZeroUsize::MIN.saturating_add(i), &mut sql);
        }
        if let Some(processed) = spec.column(Role::ProcessedAt) {
            sql.push_str(if params.is_empty() { " WHERE " } else { " AND " });
            self.quote_into(processed.name(), &mut sql);
            sql.push_str(" IS NULL");
        }

        sql.push_str(" ORDER BY ");
        for role in [Role::Priority, Role::RetryAfter] {
            if let Some(column) = spec.column(role) {
                self.quote_into(column.name(), &mut sql);
                sql.push_str(", ");
            }
        }
        self.quote_into(spec.id().name(), &mut sql);
        sql.push_str(" LIMIT ");
        self.placeholder_into(NonZeroUsize::MIN.saturating_add(params.len()), &mut sql);
        params.push(Param::Limit);
        sql.push_str(" FOR UPDATE SKIP LOCKED");
        Ok(Statement::new(sql, params))
    }
}

#[derive(Deserialize)]
pub struct Email {
    to: String,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    // A `SqlxBroker<Postgres, Cockroach>`: the dialect is part of the broker's type.
    RustStream::new(AppInfo::new("mailer", "0.1.0"))
        .with_broker(SqlxBroker::with_dialect(pool, Cockroach), |b| {
            b.include(send);
        })
}
# }
# fn main() {}
```

A dialect builds the statements of a broker's tables. [`SqlxBroker::new`] takes the one built
into the crate for its database, [`BuiltIn`]; [`SqlxBroker::with_dialect`] takes the service's
own, for a database without a built-in dialect. The dialect is the broker's second type
parameter, and each thing its tables can do is a trait it implements:

- [`Dialect`](dialect::Dialect): names, placeholders, the statement that opens a transaction at a
  table's level, the savepoint a [transactional](#transactional-mode) handler's writes start
  after, and the statements every form runs to settle a row, move a dead letter, fetch rows and
  insert one;
- [`RowLock`](dialect::RowLock): the row lock form and its claim;
- [`Lease`](dialect::Lease): the lease form, its claim, the extension of a lease in work and the
  stamp of a row a claim only selected;
- [`Advisory`](dialect::Advisory): the advisory lock form, its claim of candidates with their
  keys, the lock and the release of a key, and the take of a row whose key the session holds;
- [`Opens<Level>`](dialect::Opens): an isolation level or SQLite mode a table may declare;
- [`ByName<DB>`](ByName): subscriptions by name, the JSON headers and the times their rows hold.

A table in a form whose trait the dialect lacks does not compile, and neither does a by-name
mount on a dialect without [`ByName`]; the error names the trait. A dialect refuses with a
[`StatementError`](dialect::StatementError) what it does not build, such as FIFO groups above,
and the subscription of such a table stops at startup. The provided methods of
[`Dialect`](dialect::Dialect) and [`Lease`](dialect::Lease) state what their defaults mean for a database
without the feature. The statements are built once, when a subscription opens, so a message
costs the same as through a built-in dialect. A test addresses the broker by its type,
`tb.broker::<SqlxBroker<Postgres, Cockroach>>()`.

One statement of a built-in dialect written the service's own way is an event of the table
instead (see [An event of the service's own](#an-event-of-the-services-own)).
