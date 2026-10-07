//! The derive's expansion: the `QueueRow` and `InboxRow` impls this module generates, followed by
//! the message mode, the events and the insert of their own modules.

use heck::ToUpperCamelCase;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use ruststream_sqlx_dialect::{Opening, Role};
use syn::{DeriveInput, Generics, parse_quote};

use crate::mode::Mode;
use crate::parse::{self, ColumnField, Field, Inbox};
use crate::template::{self, KeyItem};
use crate::{check, events, insert};

mod lease;

use lease::{LeaseParts, lease_parts};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let inbox = parse::inbox(input)?;
    let id = check::check(input, &inbox)?;
    let key = template::advisory_key(&inbox)?;
    let generics = bounded_generics(input, id.0.ty);
    let mode = Mode::of(&inbox);
    let row = generate(input, &generics, &inbox, id, key.as_deref(), &mode);
    let lane = mode.impls(input, &generics);
    let contract = events::events(input, &generics, &inbox, id.0);
    let insert = insert::insert(input, &generics, &inbox)?;
    Ok(quote!(#row #lane #contract #insert))
}

fn generate(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
    (id_field, id_column): (&Field<'_>, &ColumnField),
    key: Option<&[KeyItem]>,
    mode: &Mode<'_, '_>,
) -> TokenStream2 {
    let dialect = quote!(::ruststream_sqlx::dialect);
    let column = |column: &ColumnField| {
        let name = &column.name;
        let generated = column.generated.then(|| quote!(.generated()));
        quote!(#dialect::Column::new(#name) #generated)
    };
    let id = column(id_column);
    let lease = inbox
        .columns()
        .find(|(_, column)| column.role == Some(Role::LockedUntil));
    let private = quote!(::ruststream_sqlx::__private);
    // The form twice: as the description's value, and as the type a subscription checks against
    // its database.
    let (form, form_type) = match (key, lease) {
        (Some(key), _) => {
            let parts = key.iter().map(|item| match item {
                KeyItem::Literal(text) => quote!(#dialect::KeyPart::Literal(#text)),
                KeyItem::Column(column) => quote!(#dialect::KeyPart::Column(#column)),
            });
            (
                quote!(#dialect::Form::Advisory(&[#(#parts),*])),
                quote!(#private::AdvisoryForm),
            )
        }
        (None, Some((_, expiry))) => {
            let expiry = column(expiry);
            (
                quote!(#dialect::Form::Lease(#expiry)),
                quote!(#private::LeaseForm),
            )
        }
        (None, None) => (
            quote!(#dialect::Form::RowLock),
            quote!(#private::RowLockForm),
        ),
    };
    let slots = inbox.columns().filter_map(|(_, slot)| {
        let role = slot.role?;
        if matches!(role, Role::Id | Role::LockedUntil) {
            return None;
        }
        // Each role's builder on `TableSpec` is named as `#[field(..)]` spells the role.
        let builder = if slot.fifo.is_some() {
            format_ident!("fifo_group")
        } else {
            format_ident!("{}", role.attribute())
        };
        let slot = column(slot);
        Some(quote!(.#builder(#slot)))
    });
    let data: Vec<_> = inbox
        .columns()
        .filter(|(_, column)| column.role.is_none())
        .map(|(_, data)| column(data))
        .collect();
    let data = (!data.is_empty()).then(|| quote!(.data(&[#(#data),*])));
    let table = &inbox.table.name;
    let within = inbox
        .table
        .schema
        .as_ref()
        .map(|schema| quote!(.within(#schema)));
    let selecting_all = inbox.flattens().then(|| quote!(.selecting_all()));
    let (opening, opening_type) = opening(inbox.table.opening);

    let name = &input.ident;
    let id_type = id_field.ty;
    let spec = quote!(#dialect::TableSpec::new(#table, #id, #form) #within #(#slots)* #data #selecting_all #opening);
    let LeaseParts {
        row: lease_row,
        item_check,
        spec_check,
    } = lease_parts(input, generics, inbox, lease.map(|(field, _)| field));
    let spec = match &inbox.table.clock {
        Some(clock) => quote!({
            #spec_check
            let spec = #spec;
            if <#clock as ::ruststream_sqlx::TimeSource>::DATABASE {
                spec.database_clock()
            } else {
                spec
            }
        }),
        None => spec,
    };
    let lane = mode.lane();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics ::ruststream_sqlx::__private::QueueRow for #name #ty_generics #where_clause {
            type Id = #id_type;
            type Lane = #lane;
        }

        #[automatically_derived]
        impl #impl_generics ::ruststream_sqlx::InboxRow for #name #ty_generics #where_clause {
            const SPEC: #dialect::TableSpec<'static> = #spec;
            type Form = #form_type;
            type Opening = #opening_type;
        }

        #lease_row

        #item_check
    }
}

/// What the table's transactions open at, twice: as the description's builder step, and as the
/// type a subscription requires its dialect to open (`Opens`). A table that names neither a level
/// nor a mode opens at its database's default, which every dialect opens, as `()`.
fn opening(opening: Opening) -> (Option<TokenStream2>, TokenStream2) {
    let dialect = quote!(::ruststream_sqlx::dialect);
    // A variant of `Isolation` or `Mode` and its type in `level` share one name: the word the
    // attribute takes, in upper camel case.
    let named = |word: &str| format_ident!("{}", word.to_upper_camel_case());
    match opening {
        Opening::Isolation(level) => {
            let name = named(level.attribute());
            (
                Some(quote!(.isolation(#dialect::Isolation::#name))),
                quote!(#dialect::level::#name),
            )
        }
        Opening::Mode(mode) => {
            let name = named(mode.attribute());
            (
                Some(quote!(.mode(#dialect::Mode::#name))),
                quote!(#dialect::level::#name),
            )
        }
        _ => (None, quote!(())),
    }
}

/// The struct's generics with what every impl of the row needs of them: `Send + Sync + 'static`
/// of the struct, and of the id type, which logs also print and a lease subscription copies.
fn bounded_generics(input: &DeriveInput, id_type: &syn::Type) -> Generics {
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
