use std::num::NonZeroUsize;

use ruststream::{Connected, SubscriptionSource};
use ruststream_sqlx::dialect::{self, Dialect, Statement, StatementError, TableName, TableSpec};
use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
use sqlx::Postgres;

/// A dialect of the service's own that takes no advisory locks.
#[derive(Debug)]
struct WithoutLocks;

impl Dialect for WithoutLocks {
    fn name(&self) -> &'static str {
        "without-locks"
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        dialect::Postgres.quote_into(ident, out);
    }

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        dialect::Postgres.placeholder_into(index, out);
    }

    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.fetch(spec)
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.ack(spec)
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        dialect::Postgres.retry(spec)
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.retry_after(spec)
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.discard(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.dead_letter_group(spec)
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        dialect::Postgres.dead_letter_table(spec, target)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.insert(spec)
    }
}

/// What a mount asks of a descriptor on a broker that builds its statements with `WithoutLocks`.
fn subscribes<Source>(source: Source)
where
    Source: SubscriptionSource<Connected<SqlxBroker<Postgres, WithoutLocks>>>,
{
    let _ = source;
}

/// A lock key: the table takes its rows by advisory lock.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", advisory_lock = "jobs-{id}")]
struct Advised {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

fn main() {
    subscribes(InboxQueue::<Advised>::new("jobs"));
}
