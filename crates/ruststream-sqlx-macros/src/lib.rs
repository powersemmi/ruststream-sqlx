//! Procedural macros for [`ruststream-sqlx`](https://docs.rs/ruststream-sqlx), the SQL database
//! crate of the [RustStream](https://github.com/powersemmi/ruststream) messaging framework.
//!
//! A service depends on `ruststream-sqlx` rather than on this crate directly.

#![forbid(unsafe_code)]

mod check;
mod headers;
mod inbox;
mod insert;
mod naming;
mod outbox;
mod parse;
mod template;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Implemented in `ruststream-sqlx-macros` and used through `ruststream-sqlx`.
#[proc_macro_derive(Inbox, attributes(inbox, field, sqlx))]
pub fn derive_inbox(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    inbox::expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Implemented in `ruststream-sqlx-macros` and used through `ruststream-sqlx`.
#[proc_macro_derive(InboxHeaders, attributes(inbox, field, sqlx))]
pub fn derive_inbox_headers(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    headers::expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Implemented in `ruststream-sqlx-macros` and used through `ruststream-sqlx`.
#[proc_macro_derive(Outbox, attributes(outbox, field, sqlx))]
pub fn derive_outbox(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    outbox::expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Implemented in `ruststream-sqlx-macros` and used through `ruststream-sqlx`.
#[proc_macro]
pub fn outbox(item: TokenStream) -> TokenStream {
    outbox::registry::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
