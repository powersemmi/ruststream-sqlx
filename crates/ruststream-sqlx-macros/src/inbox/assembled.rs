//! The expansion of a message assembled from a headers struct: a struct whose `#[field(headers)]`
//! field flattens a struct deriving `InboxHeaders`. The headers struct describes the queue table
//! and the per-row half of the contract; the message delegates to it, adds its own columns to the
//! description, and runs its own events on the carried lane, as row mode does.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{quote, quote_spanned};
use ruststream_sqlx_dialect::Role;
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics, Type, WherePredicate, parse_quote, parse_quote_spanned};

use crate::check::Errors;
use crate::events::{EventParts, base_predicates, event_parts};
use crate::mode::carried;
use crate::parse::{self, Custom, Field, Storage};

/// The impls of a message whose field at `headers` flattens its headers struct.
pub(crate) fn expand(
    input: &DeriveInput,
    fields: &[Field<'_>],
    headers: usize,
) -> syn::Result<TokenStream2> {
    let field = &fields[headers];
    let holder = field.ty;
    let holder_name = quote!(#holder).to_string().replace(' ', "");
    let mut errors = Errors::default();
    let custom = match parse::message_custom(input, &holder_name) {
        Ok(custom) => custom,
        Err(error) => {
            errors.push(error);
            Custom::default()
        }
    };
    check(fields, headers, &holder_name, custom, &mut errors);
    errors.finish()?;

    let p = quote!(::ruststream_sqlx::__private);
    let r = quote!(::ruststream_sqlx);
    let dialect = quote!(::ruststream_sqlx::dialect);
    let name = &input.ident;
    let ident = field.ident;
    let generics = bounded_generics(input, holder);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    // The uses of the headers struct's traits are spanned at its type, so a type that does not
    // derive `InboxHeaders` is reported at the field.
    let span = holder.span();
    let headers_of = quote_spanned!(span=> <#holder as ::ruststream_sqlx::InboxHeaders>);
    let lease_of = quote_spanned!(span=> <#holder as ::ruststream_sqlx::__private::HeadersLease>);

    // The message's own columns join the description, so the default fetch names them; a fetch
    // of the service's own reads them wherever they live, so the description stays the table's.
    let own: Vec<_> = fields
        .iter()
        .filter_map(Field::column)
        .map(|column| {
            let name = &column.name;
            quote!(#dialect::Column::new(#name))
        })
        .collect();
    let spec = if custom.fetch || own.is_empty() {
        quote!(#headers_of::SPEC)
    } else {
        quote!(#headers_of::SPEC.fetching(&[#(#own),*]))
    };
    // Why the bound is higher-ranked: it names no parameter of the impl, and rustc refuses a
    // bound that names none and fails where it is written. Higher-ranked, it is checked where a
    // lease is asked of the message instead, and names the headers struct to add the field to.
    let mut lease_generics = generics.clone();
    lease_generics
        .make_where_clause()
        .predicates
        .push(parse_quote_spanned!(span=> for<'__l> #holder: #p::HeadersLease));
    let (lease_impl, _, lease_where) = lease_generics.split_for_impl();
    let carried = carried(input, &generics);
    let forms = form_checks(&generics, holder, custom);
    let events = events(input, &generics, holder, ident, custom);
    let queue = quote! {
        #[automatically_derived]
        impl #impl_generics #p::QueueRow for #name #ty_generics #where_clause {
            type Id = #headers_of::Id;
            type Lane = #p::RowLane;
        }
    };
    let inbox_row = quote! {
        #[automatically_derived]
        impl #impl_generics #r::InboxRow for #name #ty_generics #where_clause {
            const SPEC: #dialect::TableSpec<'static> = #spec;
            type Form = #headers_of::Form;
            type Opening = #headers_of::Opening;
        }
    };
    let lease_row = quote! {
        #[automatically_derived]
        impl #lease_impl #r::LeaseRow for #name #ty_generics #lease_where {
            type Lease = #lease_of::Lease;
        }
    };
    let assembled = quote! {
        #[automatically_derived]
        impl #impl_generics #p::Assembled for #name #ty_generics #where_clause {
            fn header_map(&self) -> #p::HeaderMap {
                #headers_of::header_map(&self.#ident)
            }
        }
    };
    Ok(quote! {
        #queue
        #inbox_row
        #lease_row
        #assembled

        #carried

        #forms

        #events
    })
}

/// The rules of a message assembled from a headers struct: one headers struct, no role beside it
/// (the headers struct describes the table), columns the default fetch can name, and the service's
/// own `lock` listed with its `unlock`.
fn check(fields: &[Field<'_>], headers: usize, holder: &str, custom: Custom, errors: &mut Errors) {
    for (index, field) in fields.iter().enumerate() {
        let ident = field.ident;
        match &field.storage {
            Storage::Headers if index != headers => errors.push(syn::Error::new(
                ident.span(),
                format!(
                    "`{ident}` flattens a second headers struct: a message is assembled from one, \
                     `{holder}`"
                ),
            )),
            Storage::Column(column) => match column.role {
                Some(Role::Payload) => errors.push(syn::Error::new(
                    ident.span(),
                    format!(
                        "`{ident}` plays `payload` beside the headers struct `{holder}`: a message \
                         assembled from a headers struct is handed to its handler itself, as in \
                         row mode, so it holds no payload; drop the role"
                    ),
                )),
                Some(role) => errors.push(syn::Error::new(
                    ident.span(),
                    format!(
                        "`{ident}` plays `{role}` beside the headers struct `{holder}`, which \
                         describes the queue table: mark the field that plays `{role}` in \
                         `{holder}`"
                    ),
                )),
                None => {}
            },
            Storage::Flattened if !custom.fetch => errors.push(syn::Error::new(
                ident.span(),
                format!(
                    "`{ident}` flattens a struct whose columns the default fetch cannot name: \
                     list `fetch` in `#[inbox(custom(..))]` and read the message in the \
                     service's own `Fetch`"
                ),
            )),
            _ => {}
        }
    }
    match (custom.lock, custom.unlock) {
        (Some(span), None) => errors.push(syn::Error::new(
            span,
            "`lock` is listed without `unlock`: the service's own lock is released by its own \
             unlock, so list both in `custom(..)`",
        )),
        (None, Some(span)) => errors.push(syn::Error::new(
            span,
            "`unlock` is listed without `lock`: the service's own unlock releases what its own \
             lock took, so list both in `custom(..)`",
        )),
        _ => {}
    }
}

/// The message's generics with what every impl of it needs: `Send + Sync + 'static` of the
/// message, and `InboxHeaders` of the struct it flattens.
fn bounded_generics(input: &DeriveInput, holder: &Type) -> Generics {
    let name = &input.ident;
    let mut generics = input.generics.clone();
    if !generics.params.is_empty() {
        let (_, ty_generics, _) = input.generics.split_for_impl();
        let predicates = &mut generics.make_where_clause().predicates;
        predicates.push(parse_quote!(
            #name #ty_generics: ::core::marker::Send + ::core::marker::Sync + 'static
        ));
        predicates
            .push(parse_quote_spanned!(holder.span()=> #holder: ::ruststream_sqlx::InboxHeaders));
    }
    generics
}

/// The events the message lists in `custom(..)` that only some forms take, each held to the form
/// of its headers struct: `claim` outside the advisory lock form, `extend` in the lease form, and
/// `lock` and `unlock` in the advisory lock form. The derive cannot see the headers struct's form,
/// so the bound on it is what refuses the event, where `custom(..)` lists it.
fn form_checks(generics: &Generics, holder: &Type, custom: Custom) -> Option<TokenStream2> {
    let p = quote!(::ruststream_sqlx::__private);
    let r = quote!(::ruststream_sqlx);
    let listed = [
        (custom.claim, quote!(OwnClaim)),
        (custom.extend, quote!(OwnExtend)),
        (custom.lock, quote!(OwnLock)),
        (custom.unlock, quote!(OwnLock)),
    ];
    let checks: Vec<_> = listed
        .into_iter()
        .filter_map(|(span, takes)| {
            span.map(|span: Span| {
                let form = quote_spanned!(span=> <#holder as #r::InboxHeaders>::Form);
                quote_spanned! {span=>
                    #[allow(dead_code)]
                    fn __takes<__Form: #p::#takes + ?::core::marker::Sized>() {}
                    __takes::<#form>();
                }
            })
        })
        .collect();
    if checks.is_empty() {
        return None;
    }
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let checks = checks.iter().map(|check| quote!({ #check }));
    Some(quote! {
        const _: () = {
            #[allow(dead_code)]
            fn __forms #impl_generics () #where_clause {
                #(#checks)*
            }
        };
    })
}

/// The message's `Events<DB>`: the per-row methods delegated to its headers struct's
/// `HeadersRow<DB>`, its own events, and a header map built on the first read.
fn events(
    input: &DeriveInput,
    generics: &Generics,
    holder: &Type,
    ident: &syn::Ident,
    custom: Custom,
) -> TokenStream2 {
    let p = quote!(::ruststream_sqlx::__private);
    let r = quote!(::ruststream_sqlx);
    let name = &input.ident;
    let span = holder.span();
    let id_ty: Type = parse_quote_spanned!(span=> <#holder as #r::InboxHeaders>::Id);
    let mut predicates: Vec<WherePredicate> = base_predicates(&id_ty);
    predicates.push(parse_quote_spanned!(span=> #holder: #p::HeadersRow<__DB>));
    let events = event_parts(custom, &id_ty, &mut predicates);
    let shape = events.shape();
    let EventParts {
        ids_arm, methods, ..
    } = &events;
    let row = quote_spanned!(span=> <#holder as #p::HeadersRow<__DB>>);
    let headers_of = quote_spanned!(span=> <#holder as #r::InboxHeaders>);
    // The ids a claim of the service's own returned bind into the crate's fetch, which the
    // headers struct knows nothing of; every other parameter is the headers struct's.
    let bind = ids_arm.as_ref().map_or_else(
        || quote!(#row::bind::<Self>(param, arguments, values)),
        |arm| {
            quote! {
                let bound = match (param, values.event) {
                    #arm
                    _ => false,
                };
                if bound {
                    ::core::result::Result::Ok(true)
                } else {
                    #row::bind::<Self>(param, arguments, values)
                }
            }
        },
    );

    let mut generics = generics.clone();
    generics.params.push(parse_quote!(__DB));
    generics.make_where_clause().predicates.extend(predicates);
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();
    quote! {
        impl #impl_generics #p::Events<__DB> for #name #ty_generics #where_clause {
            #shape

            type Token = #row::Token;

            type Headers = #p::LazyHeaders;

            fn kinds() -> ::core::option::Option<#p::Kinds> {
                // A by-name subscription reads tables in payload mode; this one is in row mode.
                ::core::option::Option::None
            }

            fn id(&self) -> &<Self as #p::QueueRow>::Id {
                #row::id(&self.#ident)
            }

            fn take_headers(&mut self) -> #p::HeaderMap {
                // The header map is built from the headers struct on the first read instead.
                #p::HeaderMap::new()
            }

            fn unfit_header(headers: &#p::HeaderMap) -> ::core::option::Option<&str> {
                #headers_of::unfit_header(headers)
            }

            fn partition_key(&self) -> ::core::option::Option<&[u8]> {
                #row::partition_key(&self.#ident)
            }

            fn attempt(&self) -> ::core::option::Option<u64> {
                #row::attempt(&self.#ident)
            }

            fn read_attempt(
                row: &<__DB as #p::sqlx::Database>::Row,
                queue: &'static #p::Queue,
            ) -> ::core::option::Option<u64> {
                #row::read_attempt(row, queue)
            }

            fn bind(
                param: #p::Param,
                arguments: &mut <__DB as #p::sqlx::Database>::Arguments,
                values: &#p::Values<'_, __DB, Self>,
            ) -> ::core::result::Result<bool, #p::sqlx::Error> {
                #bind
            }

            fn lease(
                queue: &'static #p::Queue,
                now: #p::Now,
            ) -> ::core::result::Result<
                #p::Leasing<<Self as #p::Events<__DB>>::Token>,
                #p::sqlx::Error,
            > {
                #row::lease(queue, now)
            }

            #methods
        }
    }
}
