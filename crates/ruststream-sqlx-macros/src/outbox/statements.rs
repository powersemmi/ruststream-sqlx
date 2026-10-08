//! The check of an outbox record's table against each built-in dialect the macros are built with:
//! the registry builds the same statements when the record is registered, and a table no dialect
//! can run fails here, on the table's name, while the service compiles.

#[cfg(feature = "mysql")]
use ruststream_sqlx_dialect::MySql;
#[cfg(feature = "postgres")]
use ruststream_sqlx_dialect::Postgres;
#[cfg(feature = "sqlite")]
use ruststream_sqlx_dialect::Sqlite;
use ruststream_sqlx_dialect::{Column, Form, OutboxDialect, Statement, StatementError, TableSpec};
use syn::LitStr;

use super::parse::{OutboxRole, Record};

/// One statement of a dialect.
type Build = fn(&dyn OutboxDialect, &TableSpec<'_>) -> Result<Statement, StatementError>;

/// The dialects the macros are built with.
const DIALECTS: &[&dyn OutboxDialect] = &[
    #[cfg(feature = "postgres")]
    &Postgres,
    #[cfg(feature = "mysql")]
    &MySql,
    #[cfg(feature = "sqlite")]
    &Sqlite,
];

/// Builds the record's statements with every dialect the macros are built with, as the registry
/// will. The name maps to the description's group column, and the mark reads the database's
/// clock, so `processed_at` may be any time type.
pub(crate) fn check(record: &Record<'_>) -> syn::Result<()> {
    let column = |role| {
        record
            .playing(role)
            .map(|(_, column)| Column::new(&column.name))
    };
    let table = record.table.name.value();
    let schema = record.table.schema.as_ref().map(LitStr::value);
    let required = |role| Column::new(&record.required(role).1.name);
    let mut spec = TableSpec::new(&table, required(OutboxRole::Id), Form::RowLock)
        .group(required(OutboxRole::Name))
        .payload(required(OutboxRole::Payload))
        .database_clock();
    if let Some(schema) = &schema {
        spec = spec.within(schema);
    }
    if let Some(headers) = column(OutboxRole::Headers) {
        spec = spec.headers(headers);
    }
    if let Some(processed_at) = column(OutboxRole::ProcessedAt) {
        spec = spec.processed_at(processed_at);
    }
    let data: Vec<Column<'_>> = record
        .columns()
        .filter(|(_, column)| column.role.is_none())
        .map(|(_, column)| Column::new(&column.name))
        .collect();
    spec = spec.data(&data);
    if record.flattens() {
        spec = spec.selecting_all();
    }
    let statements: [Build; 3] = [
        |dialect, spec| dialect.outbox_fetch(spec),
        |dialect, spec| dialect.outbox_mark(spec),
        |dialect, spec| dialect.outbox_recover(spec),
    ];
    for dialect in DIALECTS {
        for statement in statements {
            statement(*dialect, &spec)
                .map_err(|err| syn::Error::new(record.table.name.span(), err.to_string()))?;
        }
    }
    Ok(())
}
