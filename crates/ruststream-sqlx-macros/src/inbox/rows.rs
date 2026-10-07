//! The roles a delivery reads off a row, one accessor impl each, and the message mode: a struct
//! whose `#[field(payload)]` field holds the message lends its bytes through `PayloadRow`; a
//! struct without one hands its handler the row itself, on the core's carried lane.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use ruststream_sqlx_dialect::Role;
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics, Ident, parse_quote_spanned};

use crate::parse::{Inbox, playing};

/// The accessor impls of the roles the struct's fields play: `KeyRow`, `AttemptRow`, `HeaderRow`
/// and `PayloadRow`. Each names the field's type spanned at the field, so a type that cannot
/// play the role is reported there.
pub(crate) fn accessors(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
) -> TokenStream2 {
    let name = &input.ident;
    // Why each column's bound is a higher-ranked where clause: it names no parameter of the
    // impl, and rustc refuses a bound that names none and fails where it is written. As a where
    // clause, a field type that cannot play the role rules the accessor out, and the table fails
    // where it is mounted, as every role's column type does on a database that cannot bind it.
    let accessor = |role: Role,
                    accessor: &str,
                    column: Option<&str>,
                    items: &dyn Fn(&Ident, TokenStream2) -> TokenStream2| {
        playing(inbox, role).map(|field| {
            let accessor = format_ident!("{accessor}");
            let ty = field.ty;
            let spanned = quote_spanned!(ty.span()=> #ty);
            let mut generics = generics.clone();
            if let Some(column) = column {
                let column = format_ident!("{column}");
                generics.make_where_clause().predicates.push(parse_quote_spanned!(ty.span()=>
                    for<'__c> #ty: ::ruststream_sqlx::#column
                ));
            }
            let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
            let items = items(field.ident, spanned);
            quote! {
                #[automatically_derived]
                impl #impl_generics ::ruststream_sqlx::#accessor for #name #ty_generics #where_clause {
                    #items
                }
            }
        })
    };
    let key = accessor(
        Role::PartitionKey,
        "KeyRow",
        Some("KeyColumn"),
        &|ident, ty| {
            quote! {
                type Key = #ty;

                fn partition_key(&self) -> &#ty {
                    &self.#ident
                }
            }
        },
    );
    let attempt = accessor(
        Role::Attempt,
        "AttemptRow",
        Some("AttemptColumn"),
        &|ident, ty| {
            quote! {
                type Attempt = #ty;

                fn attempt(&self) -> &#ty {
                    &self.#ident
                }
            }
        },
    );
    let headers = accessor(
        Role::Headers,
        "HeaderRow",
        Some("HeaderColumn"),
        &|ident, ty| {
            quote! {
                type Column = #ty;

                fn headers_mut(&mut self) -> &mut #ty {
                    &mut self.#ident
                }
            }
        },
    );
    let message = accessor(Role::Payload, "PayloadRow", None, &|ident, ty| {
        let bytes =
            quote_spanned!(ty.span()=> ::core::convert::AsRef::<[u8]>::as_ref(&self.#ident));
        quote! {
            type Column = #ty;

            fn payload(&self) -> &[u8] {
                #bytes
            }
        }
    })
    .unwrap_or_else(|| carried(input, generics));
    quote!(#key #attempt #headers #message)
}

/// `Input` on the core's carried lane, for a struct without a payload field. The lane asks
/// `Clone` of the row, because the test harness keeps a copy of each value. The bound sits at the
/// struct's name: a struct without `Clone` gets one error there, rustc's, with its help to derive
/// it. A message of the crate's own could only come as a second error beside it, since rustc
/// checks the lane's bound wherever the impl is written.
pub(crate) fn carried(input: &DeriveInput, generics: &Generics) -> TokenStream2 {
    let name = &input.ident;
    let (_, ty_generics, _) = generics.split_for_impl();
    let mut bounded = generics.clone();
    bounded
        .make_where_clause()
        .predicates
        .push(parse_quote_spanned!(name.span()=> #name #ty_generics: ::core::clone::Clone));
    let (impl_generics, _, where_clause) = bounded.split_for_impl();
    let private = quote!(::ruststream_sqlx::__private);
    quote! {
        #[automatically_derived]
        impl #impl_generics #private::Input for #name #ty_generics #where_clause {
            type Axis = #private::SoloCarried<Self>;
        }
    }
}
