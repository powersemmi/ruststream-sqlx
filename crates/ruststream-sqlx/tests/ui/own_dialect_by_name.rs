use std::num::NonZeroUsize;

use ruststream::Subscribe;
use ruststream_sqlx::dialect::{
    self, ClaimShape, Dialect, RowLock, Statement, StatementError, TableName, TableSpec,
};
use ruststream_sqlx::ConnectedSqlxBroker;
use sqlx::Postgres;

/// A dialect of the service's own that builds the row lock form and binds nothing by name.
#[derive(Debug)]
struct LocksOnly;

impl Dialect for LocksOnly {
    fn name(&self) -> &'static str {
        "locks-only"
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

impl RowLock for LocksOnly {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        dialect::Postgres.lock_claim(spec, shape)
    }
}

/// What a mount of `#[subscriber("name")]` asks of the connected broker.
fn opens_by_name<Connected: Subscribe>(connected: &Connected) {
    let _ = connected;
}

fn by_name(connected: &ConnectedSqlxBroker<Postgres, LocksOnly>) {
    opens_by_name(connected);
}

fn main() {
    let _ = by_name;
}
