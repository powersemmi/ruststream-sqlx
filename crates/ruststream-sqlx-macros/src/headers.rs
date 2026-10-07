//! `#[derive(InboxHeaders)]`: a struct that describes the queue table of a message assembled from
//! it. The struct's description in the manual form, the header map of its fields without a role,
//! the accessors of the roles it plays, and the generated insert.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use ruststream_sqlx_dialect::Role;
use syn::DeriveInput;
use syn::spanned::Spanned;

use crate::check::{self, Errors};
use crate::inbox::{Description, accessors, bounded_generics, describe, valid_generics};
use crate::parse::{self, Field, Inbox, Storage};
use crate::{insert, template};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let inbox = parse::headers_table(input)?;
    refuse_message_parts(input, &inbox)?;
    let (id_field, id_column) = check::check(input, &inbox)?;
    let key = template::advisory_key(&inbox)?;
    let r = quote!(::ruststream_sqlx);
    let description = describe(
        &inbox,
        id_column,
        key.as_deref(),
        &[(quote!(.header_fields()), quote!(#r::spec::HeaderFields))],
    );
    let generics = valid_generics(input, bounded_generics(input, id_field.ty), &description);
    let settings = description.settings();
    let chain = &description.chain;
    let name = &input.ident;
    let id_type = id_field.ty;
    let id_ident = id_field.ident;
    let (names, header_map) = header_map(&inbox);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let valid = valid(input, inbox.table.clock.as_ref(), &description);
    let rows = accessors(input, &generics, &inbox);
    let insert = insert::insert(input, &generics, &inbox)?;
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #r::InboxHeaders for #name #ty_generics #where_clause {
            type Id = #id_type;
            type Settings = #settings;
            const TABLE: #r::InboxSpec<Self::Settings> = #chain;
            const NAMES: &'static [&'static str] = #names;

            fn id(&self) -> &#id_type {
                &self.#id_ident
            }

            fn header_map(&self) -> #r::__private::HeaderMap {
                #header_map
            }
        }

        #valid

        #rows

        #insert
    })
}

/// The rules across the table's settings, held where the struct names its clock: a message's
/// description extends this one, and the struct's own clock is what may break them.
fn valid(
    input: &DeriveInput,
    clock: Option<&syn::Path>,
    description: &Description,
) -> Option<TokenStream2> {
    // The struct's own checks judge every other setting. A clock that is a parameter of the
    // struct is judged where the message is mounted.
    if clock.is_none() || !input.generics.params.is_empty() {
        return None;
    }
    let table = description.table_type();
    let span = description.span;
    Some(quote_spanned! {span=>
        const _: () = {
            #[allow(dead_code)]
            fn __valid<__Table: ::ruststream_sqlx::spec::Valid>() {}
            #[allow(dead_code)]
            fn __check() {
                __valid::<#table>();
            }
        };
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

/// The names of the fields without a role, each its column's, and the header map that puts
/// each under its name.
fn header_map(inbox: &Inbox<'_>) -> (TokenStream2, TokenStream2) {
    let headers: Vec<(&Field<'_>, &str)> = inbox
        .columns()
        .filter(|(_, column)| column.role.is_none())
        .map(|(field, column)| (field, column.name.as_str()))
        .collect();
    let count = headers.len();
    // Each put is spanned at the field's type, so a type without `HeaderField` is reported there.
    let put = quote!(::ruststream_sqlx::put_header);
    let puts = headers.iter().map(|(field, column)| {
        let ident = field.ident;
        quote_spanned!(field.ty.span()=> #put(&mut headers, #column, &self.#ident);)
    });
    let names = headers.iter().map(|(_, column)| column);
    (
        quote!(&[#(#names),*]),
        quote! {
            let mut headers = ::ruststream_sqlx::__private::HeaderMap::with_capacity(#count);
            #(#puts)*
            headers
        },
    )
}

#[cfg(test)]
mod tests;
