## A dialect of the service's own

```no_run
# #[cfg(feature = "postgres")]
# mod demo {
use std::num::NonZeroUsize;

use ruststream::HeaderMap;
use ruststream_sqlx::dialect::{
    self, ClaimShape, Dialect, Opening, Param, RowLock, Statement, StatementError, TableName,
    TableSpec,
};
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{BuiltIn, ByName, NamedTime};
use serde::Deserialize;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArguments, PgValueRef};
use sqlx::{PgPool, Postgres};

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

/// Postgres, with an acknowledgement of the service's own: a sent email stays in its table, in
/// the `sent` group, for an audit.
#[derive(Debug)]
pub struct Audited;

impl Dialect for Audited {
    fn name(&self) -> &'static str {
        "audited"
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        if spec.table() == "email_jobs" {
            return Ok(Statement::new(
                r#"UPDATE "email_jobs" SET "name" = 'sent' WHERE "job_id" = $1"#,
                [Param::Id],
            ));
        }
        dialect::Postgres.ack(spec)
    }

    // The provided methods the built-in dialect overrides: its transactions' opening and the
    // guard of a FIFO group. A wrapper of `dialect::MySql` delegates `server_version` and
    // `check_server` too, and one that serves leases the `Lease` hooks.
    fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
        dialect::Postgres.begin(opening)
    }

    fn fifo_guard(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        dialect::Postgres.fifo_guard(spec)
    }

    // Every other statement is the built-in dialect's.
    fn quote_into(&self, ident: &str, out: &mut String) {
        dialect::Postgres.quote_into(ident, out);
    }
#    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { dialect::Postgres.placeholder_into(index, out) }
#    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.fetch(spec) }
#    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { dialect::Postgres.retry(spec) }
#    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.retry_after(spec) }
#    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.discard(spec) }
#    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.dead_letter_group(spec) }
#    fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { dialect::Postgres.dead_letter_table(spec, target) }
#    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.insert(spec) }
}

// The row lock form, which `SendEmail` takes.
impl RowLock for Audited {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        dialect::Postgres.lock_claim(spec, shape)
    }
}

// Subscriptions by name, as `#[subscriber("emails")]` would mount.
impl ByName<Postgres> for Audited {
    fn headers(value: PgValueRef<'_>) -> Result<HeaderMap, BoxDynError> {
        <BuiltIn<Postgres> as ByName<Postgres>>::headers(value)
    }

    fn bind_time(arguments: &mut PgArguments, time: NamedTime) -> Result<(), sqlx::Error> {
        <BuiltIn<Postgres> as ByName<Postgres>>::bind_time(arguments, time)
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
    // A `SqlxBroker<Postgres, Audited>`: the dialect is part of the broker's type.
    RustStream::new(AppInfo::new("mailer", "0.1.0"))
        .with_broker(SqlxBroker::with_dialect(pool, Audited), |b| {
            b.include(send);
        })
}
# }
# fn main() {}
```

A dialect builds the statements of a broker's tables. [`SqlxBroker::new`] takes the one built
into the crate for its database, [`BuiltIn`]; [`SqlxBroker::with_dialect`] takes the service's
own, for a database whose sqlx driver lives outside sqlx, or to write a statement its own way.
The dialect is the broker's second type parameter, and each thing its tables can do is a trait it
implements:

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
mount on a dialect without [`ByName`]; the error names the trait. A dialect that wraps a built-in
one writes the statements it changes and delegates the rest: the statements to
[`dialect::Postgres`], [`dialect::MySql`] or [`dialect::Sqlite`], the by-name binding to
[`BuiltIn<DB>`](BuiltIn). It delegates the provided methods the wrapped dialect overrides as well
(`begin`, `fifo_guard`, `server_version`, `check_server`, and on [`Lease`](dialect::Lease)
`claim_writes_lease`, `claim_counts_attempt` and `begin_lease_claim`): a default left in their
place drops the FIFO guard, MySQL's READ COMMITTED claims and its server version check. Its
statements are built once, when a subscription opens, so a message costs the same as through the
built-in dialect. A test addresses the broker by its type,
`tb.broker::<SqlxBroker<Postgres, Audited>>()`.

