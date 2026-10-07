//! `#[derive(InboxHeaders)]`: a struct that describes the queue table of a message assembled from
//! it. The struct's description and its per-row half of the contract, the header map of its fields
//! without a role, and the generated insert.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use ruststream_sqlx_dialect::Role;
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics, parse_quote};

use crate::check::{self, Errors};
use crate::events::{LeaseTime, RowParts, base_predicates, row_parts};
use crate::inbox::{Description, LeaseParts, bounded_generics, description};
use crate::parse::{self, Field, Inbox, Storage};
use crate::{insert, template};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let inbox = parse::headers_table(input)?;
    refuse_message_parts(input, &inbox)?;
    let (id_field, id_column) = check::check(input, &inbox)?;
    let key = template::advisory_key(&inbox)?;
    let generics = bounded_generics(input, id_field.ty);
    let p = quote!(::ruststream_sqlx::__private);
    let r = quote!(::ruststream_sqlx);
    let dialect = quote!(::ruststream_sqlx::dialect);
    let Description {
        spec,
        form_type,
        opening_type,
        lease:
            LeaseParts {
                row: lease_row,
                item_check,
                ..
            },
    } = description(
        input,
        &generics,
        &inbox,
        id_column,
        key.as_deref(),
        &quote!(#p::HeadersLease),
    );
    let name = &input.ident;
    let id_type = id_field.ty;
    let (header_map, unfit_header) = header_map(&inbox);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let row = headers_row(input, &generics, &inbox, id_field);
    let insert = insert::insert(input, &generics, &inbox)?;
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #r::InboxHeaders for #name #ty_generics #where_clause {
            const SPEC: #dialect::TableSpec<'static> = #spec;
            type Form = #form_type;
            type Opening = #opening_type;
            type Id = #id_type;

            fn header_map(&self) -> #p::HeaderMap {
                #header_map
            }

            fn unfit_header(headers: &#p::HeaderMap) -> ::core::option::Option<&str> {
                #unfit_header
            }
        }

        #lease_row

        #item_check

        #row

        #insert
    })
}

/// Refuses what belongs to the message struct, which flattens this one: `custom(..)`, the
/// `payload` and `headers` roles, and a flattened field, whose columns the description cannot
/// see.
fn refuse_message_parts(input: &DeriveInput, inbox: &Inbox<'_>) -> syn::Result<()> {
    let name = &input.ident;
    let mut errors = Errors::default();
    if let Some(span) = inbox.table.custom.listed {
        errors.push(syn::Error::new(
            span,
            format!(
                "`custom(..)` lists the events of the message, which a handler takes: put it on \
                 the message struct whose `#[field(headers)]` field flattens `{name}`"
            ),
        ));
    }
    for field in &inbox.fields {
        let ident = field.ident;
        match &field.storage {
            Storage::Column(column) if column.role == Some(Role::Payload) => {
                errors.push(syn::Error::new(
                    ident.span(),
                    format!(
                        "`{ident}` plays `payload`, and a headers struct holds no message: the \
                         handler takes the message struct that flattens `{name}`, as in row mode"
                    ),
                ));
            }
            Storage::Column(column) if column.role == Some(Role::Headers) => {
                errors.push(syn::Error::new(
                    ident.span(),
                    format!(
                        "`{ident}` plays `headers` in a headers struct: every field of `{name}` \
                         without a role is a header already, so drop the role"
                    ),
                ));
            }
            Storage::Flattened | Storage::Headers => errors.push(syn::Error::new(
                ident.span(),
                format!(
                    "`{ident}` flattens a struct whose columns `{name}` cannot see: a headers \
                     struct names every column of the queue table it describes"
                ),
            )),
            Storage::Column(_) | Storage::Skipped => {}
        }
    }
    errors.finish()
}

/// The header map of the fields without a role, each under its column's name, and the refusal
/// of a published header that none of them is named for.
fn header_map(inbox: &Inbox<'_>) -> (TokenStream2, TokenStream2) {
    let p = quote!(::ruststream_sqlx::__private);
    let headers: Vec<(&Field<'_>, &str)> = inbox
        .columns()
        .filter(|(_, column)| column.role.is_none())
        .map(|(field, column)| (field, column.name.as_str()))
        .collect();
    let count = headers.len();
    // Each put is spanned at the field's type, so a type without `HeaderField` is reported there.
    let puts = headers.iter().map(|(field, column)| {
        let ident = field.ident;
        quote_spanned!(field.ty.span()=> #p::put_header(&mut headers, #column, &self.#ident);)
    });
    let names = headers.iter().map(|(_, column)| column);
    (
        quote! {
            let mut headers = #p::HeaderMap::with_capacity(#count);
            #(#puts)*
            headers
        },
        quote!(#p::unnamed_header(headers, &[#(#names),*])),
    )
}

/// The struct's `HeadersRow<DB>`: the per-row half of a flat struct's `Events<DB>`, built for the
/// message that flattens it.
fn headers_row(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
    id: &Field<'_>,
) -> TokenStream2 {
    let p = quote!(::ruststream_sqlx::__private);
    let r = quote!(::ruststream_sqlx);
    let name = &input.ident;
    let id_ident = id.ident;
    let mut predicates = base_predicates(id.ty);
    let row = row_parts(inbox, &quote!(__Row), LeaseTime::OfField, &mut predicates);
    let bind = row.bind(None);
    let RowParts {
        token,
        key,
        attempt,
        read_attempt,
        leasing,
        ..
    } = &row;
    let mut generics = generics.clone();
    generics.params.push(parse_quote!(__DB));
    generics.make_where_clause().predicates.extend(predicates);
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics #p::HeadersRow<__DB> for #name #ty_generics #where_clause {
            type Token = #token;

            fn id(&self) -> &<Self as #r::InboxHeaders>::Id {
                &self.#id_ident
            }

            fn partition_key(&self) -> ::core::option::Option<&[u8]> {
                #key
            }

            fn attempt(&self) -> ::core::option::Option<u64> {
                #attempt
            }

            fn read_attempt(
                row: &<__DB as #p::sqlx::Database>::Row,
                queue: &'static #p::Queue,
            ) -> ::core::option::Option<u64> {
                #read_attempt
            }

            fn bind<__Row>(
                param: #p::Param,
                arguments: &mut <__DB as #p::sqlx::Database>::Arguments,
                values: &#p::Values<'_, __DB, __Row>,
            ) -> ::core::result::Result<bool, #p::sqlx::Error>
            where
                __Row: #p::Events<__DB, Token = #token>
                    + #p::QueueRow<Id = <Self as #r::InboxHeaders>::Id>,
            {
                #bind
            }

            fn lease(
                queue: &'static #p::Queue,
                now: #p::Now,
            ) -> ::core::result::Result<
                #p::Leasing<<Self as #p::HeadersRow<__DB>>::Token>,
                #p::sqlx::Error,
            > {
                #leasing
            }
        }
    }
}

#[cfg(test)]
mod tests;
