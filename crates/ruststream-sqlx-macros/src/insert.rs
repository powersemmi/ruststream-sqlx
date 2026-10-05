//! The insert the derive builds at compile time with the Postgres dialect.

use proc_macro2::TokenStream as TokenStream2;
#[cfg(feature = "postgres")]
use quote::quote;
#[cfg(feature = "postgres")]
use syn::parse_quote;
use syn::{DeriveInput, Generics};

use crate::parse::Inbox;
#[cfg(feature = "postgres")]
use crate::parse::{ColumnField, Field};

/// A column of the description the dialect reads.
#[cfg(feature = "postgres")]
fn column(column: &ColumnField) -> ruststream_sqlx_dialect::Column<'_> {
    let built = ruststream_sqlx_dialect::Column::new(&column.name);
    if column.generated {
        built.generated()
    } else {
        built
    }
}

/// The insert of the table `columns` describe, built by the Postgres dialect; `None` without an id.
#[cfg(feature = "postgres")]
fn statement(
    inbox: &Inbox<'_>,
    columns: &[(&Field<'_>, &ColumnField)],
) -> syn::Result<Option<ruststream_sqlx_dialect::Statement>> {
    use ruststream_sqlx_dialect::{Column, Dialect, Form, Postgres, Role, TableSpec};

    let Some((_, id)) = columns
        .iter()
        .find(|(_, column)| column.role == Some(Role::Id))
    else {
        return Ok(None);
    };
    let form = columns
        .iter()
        .find(|(_, column)| column.role == Some(Role::LockedUntil))
        .map_or(Form::RowLock, |(_, expiry)| Form::Lease(column(expiry)));
    let table = inbox.table.name.value();
    let mut spec = TableSpec::new(&table, column(id), form);
    let schema = inbox.table.schema.as_ref().map(syn::LitStr::value);
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
    Postgres
        .insert(&spec.data(&data))
        .map(Some)
        .map_err(|err| syn::Error::new(inbox.table.name.span(), err.to_string()))
}

/// `Insert<C>` for Postgres connections, with the statement built now; none for a struct that
/// flattens another.
#[cfg(feature = "postgres")]
pub(crate) fn insert(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
) -> syn::Result<Option<TokenStream2>> {
    use ruststream_sqlx_dialect::{Param, Role};

    if inbox.flattens() {
        return Ok(None);
    }
    let columns: Vec<(&Field<'_>, &ColumnField)> = inbox.columns().collect();
    let Some(statement) = statement(inbox, &columns)? else {
        return Ok(None);
    };

    // `TableSpec::columns` lists every role in `Role::ALL` order, then the data columns.
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

    let p = quote!(::ruststream_sqlx::__private);
    let mut predicates: Vec<syn::WherePredicate> =
        vec![parse_quote!(__C: #p::OnPostgres + ::core::marker::Send)];
    let mut binds = Vec::new();
    for param in statement.params() {
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
                #p::sqlx::Encode<'__q, #p::Postgres> + #p::sqlx::Type<#p::Postgres>
        ));
        binds.push(quote!(#p::put::<#p::Postgres, _>(&mut arguments, #value)?;));
    }
    let sql = statement.sql();

    let name = &input.ident;
    let mut generics = generics.clone();
    generics.params.push(parse_quote!(__C));
    generics.make_where_clause().predicates.extend(predicates);
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();
    Ok(Some(quote! {
        impl #impl_generics ::ruststream_sqlx::Insert<__C> for #name #ty_generics #where_clause {
            fn insert<'__c>(
                &'__c self,
                conn: &'__c mut __C,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #p::sqlx::Error>>
                   + ::core::marker::Send + '__c {
                async move {
                    let mut arguments =
                        <<#p::Postgres as #p::sqlx::Database>::Arguments as ::core::default::Default>::default();
                    #(#binds)*
                    <#p::Postgres as #p::QueueDatabase>::execute(
                        #p::OnPostgres::connection(conn),
                        #sql,
                        arguments,
                    )
                    .await?;
                    ::core::result::Result::Ok(())
                }
            }
        }
    }))
}

/// No insert without the Postgres dialect.
#[cfg(not(feature = "postgres"))]
pub(crate) fn insert(
    _input: &DeriveInput,
    _generics: &Generics,
    _inbox: &Inbox<'_>,
) -> syn::Result<Option<TokenStream2>> {
    Ok(None)
}
