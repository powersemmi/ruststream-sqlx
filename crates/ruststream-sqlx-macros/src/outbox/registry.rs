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

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream as TokenStream2;
    use quote::quote;

    use super::expand;

    fn expanded(input: TokenStream2) -> String {
        expand(input).map_or_else(|error| error.to_string(), |tokens| tokens.to_string())
    }

    #[test]
    fn outbox_registers_each_name_on_a_registry_with_its_pool() {
        assert_eq!(
            expanded(quote!(pool: pool.clone(), "orders" => Order, "refunds" => app::Refund,)),
            quote!(
                ::ruststream_sqlx::Outbox::new(pool.clone())
                    .register::<Order>("orders")
                    .register::<app::Refund>("refunds")
            )
            .to_string()
        );
    }

    #[test]
    fn outbox_without_a_pool_defers_it() {
        assert_eq!(
            expanded(quote!("orders" => Order)),
            quote!(::ruststream_sqlx::Outbox::deferred().register::<Order>("orders")).to_string()
        );
    }

    #[test]
    fn outbox_refuses_a_name_registered_twice() {
        assert_eq!(
            expanded(quote!("orders" => Order, "refunds" => Refund, "orders" => Refund)),
            "`orders` is registered twice: a name has one record type"
        );
    }

    #[test]
    fn outbox_refuses_a_key_other_than_pool() {
        assert_eq!(
            expanded(quote!(connection: pool, "orders" => Order)),
            "unknown key `connection`: `outbox!` takes `pool: <expr>` before the names"
        );
    }
}
