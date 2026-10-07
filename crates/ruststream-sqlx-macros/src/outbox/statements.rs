//! The default statements of an outbox record, built at compile time with each built-in dialect
//! the macros are built with: the fetch, the mark (`Ack` and `Discard`) and the recovery.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
#[cfg(feature = "mysql")]
use ruststream_sqlx_dialect::MySql;
#[cfg(feature = "postgres")]
use ruststream_sqlx_dialect::Postgres;
#[cfg(feature = "sqlite")]
use ruststream_sqlx_dialect::Sqlite;
use ruststream_sqlx_dialect::{Column, Form, OutboxDialect, Statement, StatementError, TableSpec};
use syn::LitStr;

use super::parse::{OutboxRole, Record};

/// The built-in dialects, each under the name of its field in the crate's `OutboxSql`: every one
/// of them, so the generated value names each field.
const FIELDS: [&str; 3] = ["postgres", "mysql", "sqlite"];

/// The dialects the macros are built with, each under the name of its field in `OutboxSql`.
const DIALECTS: &[(&str, &dyn OutboxDialect)] = &[
    #[cfg(feature = "postgres")]
    ("postgres", &Postgres),
    #[cfg(feature = "mysql")]
    ("mysql", &MySql),
    #[cfg(feature = "sqlite")]
    ("sqlite", &Sqlite),
];

/// The three statements, each an `OutboxSql` value.
pub(crate) struct Statements {
    pub(crate) fetch: TokenStream2,
    pub(crate) mark: TokenStream2,
    pub(crate) recover: TokenStream2,
}

/// Builds the record's statements with every dialect the macros are built with. The name maps to
/// the description's group column, and the mark reads the database's clock, so `processed_at`
/// may be any time type.
pub(crate) fn statements(record: &Record<'_>) -> syn::Result<Statements> {
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
    let build =
        |statement: fn(&dyn OutboxDialect, &TableSpec<'_>) -> Result<Statement, StatementError>| {
            DIALECTS
                .iter()
                .map(|(field, dialect)| {
                    statement(*dialect, &spec)
                        .map(|statement| (*field, statement))
                        .map_err(|err| syn::Error::new(record.table.name.span(), err.to_string()))
                })
                .collect::<syn::Result<Vec<_>>>()
                .map(|statements| sql(&statements))
        };
    Ok(Statements {
        fetch: build(|dialect, spec| dialect.outbox_fetch(spec))?,
        mark: build(|dialect, spec| dialect.outbox_mark(spec))?,
        recover: build(|dialect, spec| dialect.outbox_recover(spec))?,
    })
}

/// The `OutboxSql` value of `statements`: each built-in dialect's text, `None` where the macros
/// were built without that dialect.
fn sql(statements: &[(&'static str, Statement)]) -> TokenStream2 {
    let fields = FIELDS.iter().map(|field| {
        let name = format_ident!("{field}");
        statements
            .iter()
            .find(|(built, _)| built == field)
            .map(|(_, statement)| statement.sql())
            .map_or_else(
                || quote!(#name: ::core::option::Option::None),
                |text| quote!(#name: ::core::option::Option::Some(#text)),
            )
    });
    quote!(::ruststream_sqlx::__private::OutboxSql { #(#fields),* })
}
