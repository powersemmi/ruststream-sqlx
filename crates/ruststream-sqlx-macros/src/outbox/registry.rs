//! `outbox!`: a registry with each name it tracks, registered under its record type.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, Ident, LitStr, Token, Type};

/// One `"name" => Record` of the list.
struct Registration {
    name: LitStr,
    record: Type,
}

/// What `outbox!` reads: the pool, where it is given, and the registrations.
struct Registrations {
    pool: Option<Expr>,
    entries: Vec<Registration>,
}

impl Parse for Registrations {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut pool = None;
        if input.peek(Ident) {
            let key: Ident = input.parse()?;
            if key != "pool" {
                return Err(syn::Error::new(
                    key.span(),
                    format!("unknown key `{key}`: `outbox!` takes `pool: <expr>` before the names"),
                ));
            }
            input.parse::<Token![:]>()?;
            pool = Some(input.parse()?);
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        let mut entries: Vec<Registration> = Vec::new();
        while !input.is_empty() {
            let name: LitStr = input.parse()?;
            input.parse::<Token![=>]>()?;
            let record: Type = input.parse()?;
            if entries
                .iter()
                .any(|entry| entry.name.value() == name.value())
            {
                return Err(syn::Error::new(
                    name.span(),
                    format!(
                        "`{}` is registered twice: a name has one record type",
                        name.value()
                    ),
                ));
            }
            entries.push(Registration { name, record });
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self { pool, entries })
    }
}

/// The registry `input` describes: `Outbox::new(pool)`, or `Outbox::deferred()` without a pool,
/// then one `register` per name, in the order written.
pub(crate) fn expand(input: TokenStream2) -> syn::Result<TokenStream2> {
    let Registrations { pool, entries } = syn::parse2(input)?;
    let registry = pool.map_or_else(
        || quote!(::ruststream_sqlx::Outbox::deferred()),
        |pool| quote!(::ruststream_sqlx::Outbox::new(#pool)),
    );
    let registrations = entries
        .iter()
        .map(|Registration { name, record }| quote!(.register::<#record>(#name)));
    Ok(quote!(#registry #(#registrations)*))
}
