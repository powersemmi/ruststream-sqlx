//! Procedural macros for [`ruststream-sqlx`](https://docs.rs/ruststream-sqlx), the SQL database
//! crate of the [RustStream](https://github.com/powersemmi/ruststream) messaging framework.
//!
//! A service depends on `ruststream-sqlx` rather than on this crate directly.

#![forbid(unsafe_code)]

mod events;
mod inbox;
mod insert;
mod naming;
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
