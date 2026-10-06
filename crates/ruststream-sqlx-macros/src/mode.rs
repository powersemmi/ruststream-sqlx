//! The message mode: how a delivery hands a row's message to its handler. In payload mode the
//! struct's `#[field(payload)]` field holds the message, and `PayloadRow` lends its bytes to the
//! codec.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use ruststream_sqlx_dialect::Role;
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics};

use crate::parse::{Inbox, playing};

/// `PayloadRow`, for a struct with a payload field.
pub(crate) fn payload_row(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
) -> Option<TokenStream2> {
    let field = playing(inbox, Role::Payload)?;
    let ident = field.ident;
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let bytes =
        quote_spanned!(field.ty.span()=> ::core::convert::AsRef::<[u8]>::as_ref(&self.#ident));
    Some(quote! {
        impl #impl_generics ::ruststream_sqlx::PayloadRow for #name #ty_generics #where_clause {
            fn payload(&self) -> &[u8] {
                #bytes
            }
        }
    })
}
