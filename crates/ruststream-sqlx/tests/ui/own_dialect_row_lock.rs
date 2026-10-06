use std::num::NonZeroUsize;

use ruststream::{Connected, SubscriptionSource};
use ruststream_sqlx::dialect::{
    self, ClaimShape, Dialect, Lease, Statement, StatementError, TableName, TableSpec,
};
use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
use sqlx::Postgres;

/// A dialect of the service's own that builds the lease form alone.
#[derive(Debug)]
struct LeasesOnly;

impl Dialect for LeasesOnly {
    fn name(&self) -> &'static str {
        "leases-only"
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

impl Lease for LeasesOnly {
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        dialect::Postgres.lease_claim(spec, shape)
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.extend(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.stamp(spec)
    }
}

/// What a mount asks of a descriptor on a broker that builds its statements with `LeasesOnly`.
fn subscribes<Source>(source: Source)
where
    Source: SubscriptionSource<Connected<SqlxBroker<Postgres, LeasesOnly>>>,
{
    let _ = source;
}

/// No `locked_until` and no `advisory_lock`: the table takes rows by row lock.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
struct Locked {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

fn main() {
    subscribes(InboxQueue::<Locked>::new("jobs"));
}
