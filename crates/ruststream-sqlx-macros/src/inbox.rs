//! The derive's expansion: the manual form a service would write by hand. `impl InboxTable` with
//! the `InboxSpec` chain and its type, the accessor impls of the roles the fields play, the row
//! mode's `Input`, and the insert. The crate's blanket impls over `InboxTable` write the rest.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, Generics, parse_quote, parse_quote_spanned};

use crate::parse;
use crate::{check, insert, template};

mod assembled;
mod rows;
mod table;

pub(crate) use rows::{accessors, carried};
pub(crate) use table::{Description, describe, own_events};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    // A struct that flattens a headers struct is a message assembled from it; any other is flat.
    if let Ok(fields) = parse::fields(input)
        && let Some(headers) = fields
            .iter()
            .position(|field| matches!(field.storage, parse::Storage::Headers))
    {
        return assembled::expand(input, &fields, headers);
    }
    let inbox = parse::inbox(input)?;
    let (id_field, id_column) = check::check(input, &inbox)?;
    let key = template::advisory_key(&inbox)?;
    let description = describe(&inbox, id_column, key.as_deref(), &[]);
    let generics = valid_generics(input, bounded_generics(input, id_field.ty), &description);
    let table = inbox_table(input, &generics, &description, id_field);
    let rows = accessors(input, &generics, &inbox);
    let insert = insert::insert(input, &generics, &inbox)?;
    Ok(quote!(#table #rows #insert))
}

/// `impl InboxTable` for a flat struct: its id, and its description as the chain and its type.
fn inbox_table(
    input: &DeriveInput,
    generics: &Generics,
    description: &Description,
    id: &parse::Field<'_>,
) -> TokenStream2 {
    let name = &input.ident;
    let table = description.table_type();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let chain = &description.chain;
    let id_type = id.ty;
    let id_ident = id.ident;
    quote! {
        #[automatically_derived]
        impl #impl_generics ::ruststream_sqlx::InboxTable for #name #ty_generics #where_clause {
            type Id = #id_type;
            type Table = #table;
            const TABLE: Self::Table = #chain;

            fn id(&self) -> &#id_type {
                &self.#id_ident
            }
        }
    }
}

/// `generics` with the rules across the table's settings, for a generic struct: a setting may name
/// a parameter (`clock = Source`), and the table and every impl of the row hold where the
/// parameter keeps the rules.
pub(crate) fn valid_generics(
    input: &DeriveInput,
    mut generics: Generics,
    description: &Description,
) -> Generics {
    if !input.generics.params.is_empty() {
        let table = description.table_type();
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote_spanned!(description.span=>
                #table: ::ruststream_sqlx::spec::Valid
            ));
    }
    generics
}

/// The struct's generics with what every impl of the row needs of them: `Send + Sync + 'static`
/// of the struct, and of the id type, which logs also print and a lease subscription copies.
pub(crate) fn bounded_generics(input: &DeriveInput, id_type: &syn::Type) -> Generics {
    let name = &input.ident;
    let mut generics = input.generics.clone();
    if !generics.params.is_empty() {
        let (_, ty_generics, _) = input.generics.split_for_impl();
        let predicates = &mut generics.make_where_clause().predicates;
        predicates.push(parse_quote!(
            #name #ty_generics: ::core::marker::Send + ::core::marker::Sync + 'static
        ));
        predicates.push(parse_quote!(
            #id_type: ::core::clone::Clone
                + ::core::fmt::Debug
                + ::core::marker::Send
                + ::core::marker::Sync
                + 'static
        ));
    }
    generics
}

#[cfg(test)]
mod tests;
