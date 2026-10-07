//! The message mode: how a delivery hands a row's message to its handler. In payload mode the
//! struct's `#[field(payload)]` field holds the message, and `PayloadRow` lends its bytes to the
//! codec. In row mode, a struct without that field, the handler takes the row itself: the struct
//! rides the core's carried lane.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use ruststream_sqlx_dialect::Role;
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics, parse_quote_spanned};

use crate::parse::{Field, Inbox, playing};

/// The struct's message mode, chosen by the presence of a payload field.
pub(crate) enum Mode<'i, 'a> {
    /// The field that plays `payload` holds the message, which a codec decodes for the handler.
    Payload(&'i Field<'a>),
    /// No field holds a message: the handler takes the row itself.
    Row,
}

impl<'i, 'a> Mode<'i, 'a> {
    /// The mode of the struct `inbox` reads.
    pub(crate) fn of(inbox: &'i Inbox<'a>) -> Self {
        playing(inbox, Role::Payload).map_or(Self::Row, Self::Payload)
    }

    /// The lane `QueueRow::Lane` names.
    pub(crate) fn lane(&self) -> TokenStream2 {
        match self {
            Self::Payload(_) => quote!(::ruststream_sqlx::__private::PayloadLane),
            Self::Row => quote!(::ruststream_sqlx::__private::RowLane),
        }
    }

    /// What puts the struct on its lane: `PayloadRow` in payload mode, the core's `Input` on the
    /// carried lane in row mode.
    pub(crate) fn impls(&self, input: &DeriveInput, generics: &Generics) -> TokenStream2 {
        match self {
            Self::Payload(field) => payload_row(input, generics, field),
            Self::Row => carried(input, generics),
        }
    }
}

/// `PayloadRow`, for a struct whose `field` plays `payload`.
fn payload_row(input: &DeriveInput, generics: &Generics, field: &Field<'_>) -> TokenStream2 {
    let ident = field.ident;
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let bytes =
        quote_spanned!(field.ty.span()=> ::core::convert::AsRef::<[u8]>::as_ref(&self.#ident));
    quote! {
        impl #impl_generics ::ruststream_sqlx::PayloadRow for #name #ty_generics #where_clause {
            fn payload(&self) -> &[u8] {
                #bytes
            }
        }
    }
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
