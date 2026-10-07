//! The expansion of a message assembled from a headers struct: a struct whose `#[field(headers)]`
//! field flattens a struct deriving `InboxHeaders`. The headers struct describes the queue table;
//! the message's `InboxTable` extends that description with its own columns and its own events,
//! reads its roles through the headers struct, and builds its header map from it.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{quote, quote_spanned};
use ruststream_sqlx_dialect::Role;
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics, Type, WherePredicate, parse_quote, parse_quote_spanned};

use super::{carried, own_events};
use crate::check::Errors;
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

    let r = quote!(::ruststream_sqlx);
    let spec = quote!(::ruststream_sqlx::spec);
    let name = &input.ident;
    let ident = field.ident;
    let generics = bounded_generics(input, holder);
    // The uses of the headers struct's traits are spanned at its type, so a type that does not
    // derive `InboxHeaders` is reported at the field.
    let span = holder.span();
    let headers_of = quote_spanned!(span=> <#holder as #r::InboxHeaders>);

    // The message's own columns join the description, so the default fetch names them; a fetch
    // of the service's own reads them wherever they live, so the description stays the table's.
    let own: Vec<_> = fields
        .iter()
        .filter_map(Field::column)
        .map(|column| {
            let name = &column.name;
            quote!(::ruststream_sqlx::dialect::Column::new(#name))
        })
        .collect();
    let fetching = (!custom.fetch && !own.is_empty()).then(|| quote!(.fetching(&[#(#own),*])));
    // The message's own events join the headers struct's settings, one `Push` each.
    let events = own_events(custom);
    let mut settings = quote!(#headers_of::Settings);
    let mut pushes: Vec<WherePredicate> = Vec::new();
    for event in &events {
        pushes.push(parse_quote!(#settings: #spec::Push<#event>));
        settings = quote!(<#settings as #spec::Push<#event>>::Out);
        pushes.push(parse_quote!(#settings: #spec::Declaration));
    }
    // The rules across the settings hold the message's own events to the headers struct's form,
    // which the derive cannot see: a broken one is reported where `custom(..)` lists the event.
    let rule_span = rule_span(custom);
    let table = quote_spanned!(rule_span.unwrap_or(span)=> ::ruststream_sqlx::InboxSpec<#settings>);
    let mut table_generics = generics.clone();
    let predicates = &mut table_generics.make_where_clause().predicates;
    if !input.generics.params.is_empty() {
        predicates.extend(pushes);
    }
    if rule_span.is_none() {
        // Why the bound is higher-ranked: without an event that only some forms take, the
        // message's settings keep the rules exactly where its headers struct's do, which the
        // headers struct's derive reports. Higher-ranked, the bound states that instead of
        // checking it a second time here, where a struct that does not derive `InboxHeaders`
        // would fail each rule on its own.
        predicates.push(parse_quote!(for<'__v> #table: #spec::Valid));
    }
    let (table_impl, ty_generics, table_where) = table_generics.split_for_impl();
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let carried = carried(input, &generics);
    let roles = roles(input, &generics, holder, ident);
    Ok(quote! {
        #[automatically_derived]
        impl #table_impl #r::InboxTable for #name #ty_generics #table_where {
            type Id = #headers_of::Id;
            type Table = #table;
            const TABLE: Self::Table = #headers_of::TABLE #fetching #(.own::<#events>())*;

            fn id(&self) -> &Self::Id {
                #headers_of::id(&self.#ident)
            }
        }

        #[automatically_derived]
        impl #impl_generics #r::HeaderFields for #name #ty_generics #where_clause {
            const NAMES: &'static [&'static str] = #headers_of::NAMES;

            fn header_map(&self) -> #r::__private::HeaderMap {
                #headers_of::header_map(&self.#ident)
            }
        }

        #roles

        #carried
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
/// message, and `InboxHeaders` of the struct it flattens. The second holds even of a message
/// without parameters: spanned at the field, it reports a struct that does not derive
/// `InboxHeaders` there, instead of in each item that reads it.
fn bounded_generics(input: &DeriveInput, holder: &Type) -> Generics {
    let name = &input.ident;
    let mut generics = input.generics.clone();
    let predicates = &mut generics.make_where_clause().predicates;
    if !input.generics.params.is_empty() {
        let (_, ty_generics, _) = input.generics.split_for_impl();
        predicates.push(parse_quote!(
            #name #ty_generics: ::core::marker::Send + ::core::marker::Sync + 'static
        ));
    }
    predicates.push(parse_quote_spanned!(holder.span()=> #holder: ::ruststream_sqlx::InboxHeaders));
    generics
}

/// Where a rule across the settings is reported: the one event the message lists that only some
/// forms take (`claim`, `extend`, or `lock` with `unlock`), or `custom(..)` itself where it lists
/// more than one of them.
fn rule_span(custom: Custom) -> Option<Span> {
    let mut listed = [custom.claim, custom.extend, custom.lock.or(custom.unlock)]
        .into_iter()
        .flatten();
    let first = listed.next()?;
    if listed.next().is_some() {
        custom.listed
    } else {
        Some(first)
    }
}

/// The roles the message reads through its headers struct: the partition key and the attempt,
/// where the headers struct has the field.
fn roles(
    input: &DeriveInput,
    generics: &Generics,
    holder: &Type,
    ident: &syn::Ident,
) -> TokenStream2 {
    let r = quote!(::ruststream_sqlx);
    let name = &input.ident;
    let span = holder.span();
    let role = |accessor: TokenStream2, items: TokenStream2| {
        let mut generics = generics.clone();
        // Why the bound is higher-ranked: it names no parameter of the impl, and rustc refuses a
        // bound that names none and fails where it is written. Higher-ranked, it rules the impl
        // out where the headers struct has no such field, and the table sets no such role.
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote_spanned!(span=> for<'__r> #holder: #r::#accessor));
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
        quote! {
            #[automatically_derived]
            impl #impl_generics #r::#accessor for #name #ty_generics #where_clause {
                #items
            }
        }
    };
    let key = role(
        quote!(KeyRow),
        quote! {
            type Key = <#holder as #r::KeyRow>::Key;

            fn partition_key(&self) -> &Self::Key {
                #r::KeyRow::partition_key(&self.#ident)
            }
        },
    );
    let attempt = role(
        quote!(AttemptRow),
        quote! {
            type Attempt = <#holder as #r::AttemptRow>::Attempt;

            fn attempt(&self) -> &Self::Attempt {
                #r::AttemptRow::attempt(&self.#ident)
            }
        },
    );
    quote!(#key #attempt)
}
