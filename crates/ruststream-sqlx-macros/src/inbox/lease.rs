//! What the lease form adds to the description: `LeaseRow`, and the refusal of a lease on the
//! database's clock.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics};

use crate::parse::{Field, Inbox};

/// What the lease form adds to a struct with a `locked_until` field.
pub(crate) struct LeaseParts {
    /// The `LeaseRow` impl.
    pub(crate) row: Option<TokenStream2>,
    /// The refusal of the database's clock, as an item of its own.
    pub(crate) item_check: Option<TokenStream2>,
    /// The same refusal inside `SPEC`, for a generic struct.
    pub(crate) spec_check: Option<TokenStream2>,
}

/// `lease_trait` (`LeaseRow`, or `HeadersLease` for a headers struct) for a struct whose `field`
/// plays `locked_until`, and the refusal of a lease on the database's clock.
pub(crate) fn lease_parts(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
    field: Option<&Field<'_>>,
    lease_trait: &TokenStream2,
) -> LeaseParts {
    let Some(field) = field else {
        return LeaseParts {
            row: None,
            item_check: None,
            spec_check: None,
        };
    };
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let ty = field.ty;
    let time = quote_spanned!(ty.span()=> <#ty as ::ruststream_sqlx::TimeColumn>::Time);
    let row = quote! {
        #[automatically_derived]
        impl #impl_generics #lease_trait for #name #ty_generics #where_clause {
            type Lease = #time;
        }
    };
    // The lease form computes its expiry from the crate's clock, so a lease table on the
    // database's clock is refused while it compiles: at item level, or inside `SPEC` where the
    // clock may name the struct's own parameters, which an item-level const cannot.
    let check = inbox.table.clock.as_ref().map(|clock| {
        let check = quote_spanned! {clock.span()=>
            ::core::assert!(
                !<#clock as ::ruststream_sqlx::TimeSource>::DATABASE,
                "the lease form computes its expiry from the crate's clock: drop \
                 `clock = DatabaseClock` or the `locked_until` field"
            );
        };
        (clock.span(), check)
    });
    let (item_check, spec_check) = match check {
        Some((span, check)) if input.generics.params.is_empty() => {
            (Some(quote_spanned!(span=> const _: () = { #check };)), None)
        }
        Some((_, check)) => (None, Some(check)),
        None => (None, None),
    };
    LeaseParts {
        row: Some(row),
        item_check,
        spec_check,
    }
}
