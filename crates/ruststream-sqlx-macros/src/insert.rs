//! The insert the derive builds at compile time, with each built-in dialect the macros are built
//! with.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
#[cfg(feature = "mysql")]
use ruststream_sqlx_dialect::MySql;
#[cfg(feature = "postgres")]
use ruststream_sqlx_dialect::Postgres;
#[cfg(feature = "sqlite")]
use ruststream_sqlx_dialect::Sqlite;
use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Role, Statement, TableSpec};
use syn::{DeriveInput, Generics, LitStr, WherePredicate, parse_quote};

use crate::parse::{ColumnField, Field, Inbox};

/// The built-in dialects, each under the name of its field in the crate's `InsertSql`: every one
/// of them, so the generated value names each field.
const FIELDS: [&str; 3] = ["postgres", "mysql", "sqlite"];

/// The dialects the macros are built with, each under the name of its field in `InsertSql`.
const DIALECTS: &[(&str, &dyn Dialect)] = &[
    #[cfg(feature = "postgres")]
    ("postgres", &Postgres),
    #[cfg(feature = "mysql")]
    ("mysql", &MySql),
    #[cfg(feature = "sqlite")]
    ("sqlite", &Sqlite),
];

/// A column of the description the dialect reads.
fn column(column: &ColumnField) -> Column<'_> {
    let built = Column::new(&column.name);
    if column.generated {
        built.generated()
    } else {
        built
    }
}

/// The insert of the table `columns` describe, built by each dialect the macros are built with
/// and named by its field in `InsertSql`; none without an id.
fn statements(
    inbox: &Inbox<'_>,
    columns: &[(&Field<'_>, &ColumnField)],
) -> syn::Result<Vec<(&'static str, Statement)>> {
    let Some((_, id)) = columns
        .iter()
        .find(|(_, column)| column.role == Some(Role::Id))
    else {
        return Ok(Vec::new());
    };
    let form = columns
        .iter()
        .find(|(_, column)| column.role == Some(Role::LockedUntil))
        .map_or(Form::RowLock, |(_, expiry)| Form::Lease(column(expiry)));
    let table = inbox.table.name.value();
    let mut spec = TableSpec::new(&table, column(id), form);
    let schema = inbox.table.schema.as_ref().map(LitStr::value);
    if let Some(schema) = &schema {
        spec = spec.within(schema);
    }
    for (_, slot) in columns {
        let built = column(slot);
        spec = match slot.role {
            Some(Role::Group) if slot.fifo.is_some() => spec.fifo_group(built),
            Some(Role::Group) => spec.group(built),
            Some(Role::PartitionKey) => spec.partition_key(built),
            Some(Role::Priority) => spec.priority(built),
            Some(Role::RetryAfter) => spec.retry_after(built),
            Some(Role::Attempt) => spec.attempt(built),
            Some(Role::ProcessedAt) => spec.processed_at(built),
            Some(Role::Headers) => spec.headers(built),
            Some(Role::Payload) => spec.payload(built),
            _ => spec,
        };
    }
    let data: Vec<Column<'_>> = columns
        .iter()
        .filter(|(_, slot)| slot.role.is_none())
        .map(|(_, slot)| column(slot))
        .collect();
    let spec = spec.data(&data);
    DIALECTS
        .iter()
        .map(|(field, dialect)| {
            dialect
                .insert(&spec)
                .map(|statement| (*field, statement))
                .map_err(|err| syn::Error::new(inbox.table.name.span(), err.to_string()))
        })
        .collect()
}

/// The `InsertSql` value of `statements`: each built-in dialect's insert, `None` where the macros
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
    quote!(::ruststream_sqlx::__private::InsertSql { #(#fields),* })
}

/// The struct's columns in the order of [`TableSpec::columns`], which an insert's
/// [`Param::Column`] counts in: every role in `Role::ALL` order, then the data columns.
pub(crate) fn ordered<'i, 'a>(
    columns: &[(&'i Field<'a>, &'i ColumnField)],
) -> Vec<(&'i Field<'a>, &'i ColumnField)> {
    let mut ordered: Vec<(&Field<'_>, &ColumnField)> = Role::ALL
        .iter()
        .filter_map(|role| {
            columns
                .iter()
                .find(|(_, slot)| slot.role == Some(*role))
                .copied()
        })
        .collect();
    ordered.extend(
        columns
            .iter()
            .filter(|(_, slot)| slot.role.is_none())
            .copied(),
    );
    ordered
}

/// What binds `params` in order, a field of the struct each, and the bound each field's type puts
/// on the connection's database.
fn binds(
    inbox: &Inbox<'_>,
    columns: &[(&Field<'_>, &ColumnField)],
    params: &[Param],
) -> syn::Result<(Vec<TokenStream2>, Vec<WherePredicate>)> {
    let ordered = ordered(columns);
    let p = quote!(::ruststream_sqlx::__private);
    let database = quote!(<__C as #p::OnConnection>::Database);
    let mut binds = Vec::new();
    let mut predicates = Vec::new();
    for param in params {
        let Param::Column(position) = param else {
            return Err(syn::Error::new(
                inbox.table.name.span(),
                format!("the insert binds {param:?}, which a struct has no field for"),
            ));
        };
        let (field, slot) = ordered[*position];
        let ident = field.ident;
        let ty = field.ty;
        let (value, bound_ty) = if slot.json {
            (
                quote!(#p::sqlx::types::Json(&self.#ident)),
                quote!(#p::sqlx::types::Json<&'__x #ty>),
            )
        } else {
            (quote!(&self.#ident), quote!(&'__x #ty))
        };
        predicates.push(parse_quote!(
            for<'__q, '__x> <#bound_ty as #p::Via<__C>>::Is:
                #p::sqlx::Encode<'__q, #database> + #p::sqlx::Type<#database>
        ));
        binds.push(quote!(#p::put::<#database, _>(&mut arguments, #value)?;));
    }
    Ok((binds, predicates))
}

/// `Insert<C>` for the connection of each built-in dialect the macros are built with, its
/// statement built now; none for a struct that flattens another, or with no dialect built in.
pub(crate) fn insert(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
) -> syn::Result<Option<TokenStream2>> {
    if inbox.flattens() {
        return Ok(None);
    }
    let columns: Vec<(&Field<'_>, &ColumnField)> = inbox.columns().collect();
    let statements = statements(inbox, &columns)?;
    let Some((first_dialect, first)) = statements.first() else {
        return Ok(None);
    };
    // One list of binds serves every dialect, so every dialect binds the same parameters.
    if let Some((dialect, statement)) = statements
        .iter()
        .find(|(_, statement)| statement.params() != first.params())
    {
        return Err(syn::Error::new(
            inbox.table.name.span(),
            format!(
                "the {first_dialect} insert binds {:?} and the {dialect} insert binds {:?}: the \
                 generated insert binds one list for every database",
                first.params(),
                statement.params(),
            ),
        ));
    }
    let (binds, predicates) = binds(inbox, &columns, first.params())?;
    let sql = sql(&statements);

    let p = quote!(::ruststream_sqlx::__private);
    let database = quote!(<__C as #p::OnConnection>::Database);
    let name = &input.ident;
    let row = name.to_string();
    let mut generics = generics.clone();
    generics.params.push(parse_quote!(__C));
    let where_clause = generics.make_where_clause();
    where_clause
        .predicates
        .push(parse_quote!(__C: #p::OnConnection + ::core::marker::Send));
    where_clause.predicates.extend(predicates);
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();
    Ok(Some(quote! {
        impl #impl_generics ::ruststream_sqlx::Insert<__C> for #name #ty_generics #where_clause {
            fn insert<'__c>(
                &'__c self,
                conn: &'__c mut __C,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #p::sqlx::Error>>
                   + ::core::marker::Send + '__c {
                const SQL: #p::InsertSql<'static> = #sql;
                async move {
                    let ::core::option::Option::Some(sql) = #p::OnConnection::insert_sql(&*conn, &SQL)
                    else {
                        return ::core::result::Result::Err(#p::no_insert(#row));
                    };
                    let mut arguments =
                        <<#database as #p::sqlx::Database>::Arguments as ::core::default::Default>::default();
                    #(#binds)*
                    <#database as #p::QueueDatabase>::execute(
                        #p::OnConnection::connection(conn),
                        sql,
                        arguments,
                    )
                    .await?;
                    ::core::result::Result::Ok(())
                }
            }
        }
    }))
}
