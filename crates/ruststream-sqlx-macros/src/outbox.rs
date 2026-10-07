//! `#[derive(Outbox)]` and `outbox!`: a struct that describes a service's outbox table, its
//! `OutboxRow` impl and the default events, and the registry of the names it tracks.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics, Ident, WherePredicate, parse_quote};

mod parse;
pub(crate) mod registry;
mod statements;

use parse::{OutboxRole, Record};
use statements::Statements;

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let record = parse::record(input)?;
    let statements = statements::statements(&record)?;
    let row = row(input, &record);
    let events = events(input, &record, &statements);
    Ok(quote!(#row #events))
}

/// The struct's generics, bounded where it has parameters by what `OutboxRow` asks of the record
/// and its id; a struct without parameters meets those bounds at the trait itself.
fn row_generics(input: &DeriveInput, record: &Record<'_>) -> Generics {
    let mut generics = input.generics.clone();
    if generics.params.is_empty() {
        return generics;
    }
    let name = &input.ident;
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let id = record.required(OutboxRole::Id).0.ty;
    let where_clause = generics.make_where_clause();
    where_clause.predicates.push(parse_quote!(
        #name #ty_generics: ::core::marker::Send + ::core::marker::Sync + ::core::marker::Unpin
            + 'static
    ));
    where_clause.predicates.push(parse_quote!(
        #id: ::core::fmt::Display + ::core::str::FromStr + ::core::marker::Send
            + ::core::marker::Sync + 'static
    ));
    generics
}

/// `OutboxRow`: the id, the name through `AsRef<str>`, the payload through `AsRef<[u8]>`, and the
/// headers through `HeaderColumn` or an empty map. Each conversion is spanned on its field's type,
/// so a type that does not fit fails there.
fn row(input: &DeriveInput, record: &Record<'_>) -> TokenStream2 {
    let r = quote!(::ruststream_sqlx);
    let p = quote!(::ruststream_sqlx::__private);
    let name = &input.ident;
    let generics = row_generics(input, record);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let (id_field, _) = record.required(OutboxRole::Id);
    let id = id_field.ident;
    let id_type = id_field.ty;
    let (name_field, _) = record.required(OutboxRole::Name);
    let name_ident = name_field.ident;
    let name_value = quote_spanned!(name_field.ty.span()=>
        ::core::convert::AsRef::<str>::as_ref(&self.#name_ident)
    );
    let (payload_field, _) = record.required(OutboxRole::Payload);
    let payload_ident = payload_field.ident;
    let payload_value = quote_spanned!(payload_field.ty.span()=>
        ::core::convert::AsRef::<[u8]>::as_ref(&self.#payload_ident)
    );
    let headers = record.playing(OutboxRole::Headers).map_or_else(
        || quote!(#p::HeaderMap::new()),
        |(field, _)| {
            let ident = field.ident;
            quote_spanned!(field.ty.span()=> #r::HeaderColumn::take_headers(&mut self.#ident))
        },
    );
    // The mark is the database's to write and the outbox never reads it back, so a service's
    // `processed_at` field would read as unused; the record touches it once here.
    let processed = record.playing(OutboxRole::ProcessedAt).map(|(field, _)| {
        let ident = field.ident;
        quote!(let _ = &self.#ident;)
    });
    let retry_writes = record.table.custom.retry;
    quote! {
        #[automatically_derived]
        impl #impl_generics #r::OutboxRow for #name #ty_generics #where_clause {
            type Id = #id_type;

            const RETRY_WRITES: bool = #retry_writes;

            fn id(&self) -> &#id_type {
                #processed
                &self.#id
            }

            fn name(&self) -> &str {
                #name_value
            }

            fn payload(&self) -> &[u8] {
                #payload_value
            }

            fn take_headers(&mut self) -> #p::HeaderMap {
                #headers
            }
        }
    }
}

/// An event with a default statement.
#[derive(Clone, Copy)]
enum Event {
    Fetch,
    Ack,
    Discard,
    Recover,
}

impl Event {
    /// The event's name in `custom(..)` and in errors, which is its method's name too.
    const fn word(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::Ack => "ack",
            Self::Discard => "discard",
            Self::Recover => "recover",
        }
    }

    /// The event's trait in `ruststream_sqlx::outbox`.
    const fn trait_name(self) -> &'static str {
        match self {
            Self::Fetch => "Fetch",
            Self::Ack => "Ack",
            Self::Discard => "Discard",
            Self::Recover => "Recover",
        }
    }
}

/// The impls of every event `custom(..)` does not list, each generic over the connection's
/// database.
fn events(input: &DeriveInput, record: &Record<'_>, statements: &Statements) -> TokenStream2 {
    let custom = record.table.custom;
    let mut impls = Vec::new();
    for (event, listed, sql) in [
        (Event::Fetch, custom.fetch, &statements.fetch),
        (Event::Ack, custom.ack, &statements.mark),
        (Event::Discard, custom.discard, &statements.mark),
        (Event::Recover, custom.recover, &statements.recover),
    ] {
        if !listed {
            impls.push(event_impl(input, record, event, sql));
        }
    }
    if !custom.retry {
        impls.push(retry_impl(input, record));
    }
    quote!(#(#impls)*)
}

/// The struct's generics with the database parameter `__DB: OutboxDatabase`, and `predicates`.
fn event_generics(
    input: &DeriveInput,
    record: &Record<'_>,
    predicates: impl IntoIterator<Item = WherePredicate>,
) -> Generics {
    let mut generics = row_generics(input, record);
    generics.params.push(parse_quote!(__DB));
    let where_clause = generics.make_where_clause();
    where_clause
        .predicates
        .push(parse_quote!(__DB: ::ruststream_sqlx::OutboxDatabase));
    where_clause.predicates.extend(predicates);
    generics
}

/// The bind of the id and the bound it puts on the database: the id's own type, or `Json` of it
/// where the field reads its column through `#[sqlx(json)]`. The bound names the id's type, so a
/// type sqlx cannot bind fails with sqlx's own message.
fn id_bind(record: &Record<'_>) -> (TokenStream2, WherePredicate) {
    let p = quote!(::ruststream_sqlx::__private);
    let (field, column) = record.required(OutboxRole::Id);
    let ty = field.ty;
    let (value, predicate) = if column.json {
        (
            quote!(#p::sqlx::types::Json(id)),
            parse_quote!(
                for<'__q, '__x> #p::sqlx::types::Json<&'__x #ty>:
                    #p::sqlx::Encode<'__q, __DB> + #p::sqlx::Type<__DB>
            ),
        )
    } else {
        (
            quote!(id),
            parse_quote!(for<'__q> #ty: #p::sqlx::Encode<'__q, __DB> + #p::sqlx::Type<__DB>),
        )
    };
    (
        quote!(#p::sqlx::Arguments::add(&mut arguments, #value).map_err(#p::sqlx::Error::Encode)?;),
        predicate,
    )
}

/// The default `event`: its statement picked by the connection's database, its parameter bound,
/// and the statement run.
fn event_impl(
    input: &DeriveInput,
    record: &Record<'_>,
    event: Event,
    sql: &TokenStream2,
) -> TokenStream2 {
    let r = quote!(::ruststream_sqlx);
    let p = quote!(::ruststream_sqlx::__private);
    let database = quote!(<__DB as #p::sqlx::Database>);
    let name = &input.ident;
    let row = name.to_string();
    let word = event.word();
    let method = Ident::new(word, Span::call_site());
    let event_trait = Ident::new(event.trait_name(), Span::call_site());
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let id_type = record.required(OutboxRole::Id).0.ty;
    let (bind_id, id_bound) = id_bind(record);
    let reads: WherePredicate = parse_quote!(
        for<'__r> #name #ty_generics: #p::sqlx::FromRow<'__r, #database::Row>
    );
    let (predicates, parameter, output, run) = match event {
        Event::Fetch => (
            vec![id_bound, reads],
            quote!(id: &#id_type),
            quote!(::core::option::Option<Self>),
            quote! {
                #bind_id
                <__DB as #r::OutboxDatabase>::fetch_optional::<Self>(conn, sql, arguments).await
            },
        ),
        Event::Ack | Event::Discard => (
            vec![id_bound],
            quote!(id: &#id_type),
            quote!(()),
            quote! {
                #bind_id
                <__DB as #r::OutboxDatabase>::execute(conn, sql, arguments).await
            },
        ),
        Event::Recover => (
            vec![reads],
            quote!(name: &str),
            quote!(::std::vec::Vec<Self>),
            quote! {
                <__DB as #r::OutboxDatabase>::bind_name(&mut arguments, name)?;
                <__DB as #r::OutboxDatabase>::fetch_all::<Self>(conn, sql, arguments).await
            },
        ),
    };
    let generics = event_generics(input, record, predicates);
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics #r::outbox::#event_trait<__DB> for #name #ty_generics #where_clause {
            fn #method(
                conn: &mut #database::Connection,
                #parameter,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#output, #p::sqlx::Error>,
            > + ::core::marker::Send {
                const SQL: #p::OutboxSql<'static> = #sql;
                async move {
                    let ::core::option::Option::Some(sql) =
                        <__DB as #r::OutboxDatabase>::statement(&*conn, &SQL)
                    else {
                        return ::core::result::Result::Err(#p::no_outbox_statement(#row, #word));
                    };
                    let mut arguments =
                        <#database::Arguments as ::core::default::Default>::default();
                    #run
                }
            }
        }
    }
}

/// The default `Retry`: the record stays unprocessed, and no statement runs.
fn retry_impl(input: &DeriveInput, record: &Record<'_>) -> TokenStream2 {
    let r = quote!(::ruststream_sqlx);
    let p = quote!(::ruststream_sqlx::__private);
    let database = quote!(<__DB as #p::sqlx::Database>);
    let name = &input.ident;
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let id_type = record.required(OutboxRole::Id).0.ty;
    let generics = event_generics(input, record, []);
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics #r::outbox::Retry<__DB> for #name #ty_generics #where_clause {
            fn retry(
                conn: &mut #database::Connection,
                id: &#id_type,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #p::sqlx::Error>>
                   + ::core::marker::Send {
                let _ = (conn, id);
                ::core::future::ready(::core::result::Result::Ok(()))
            }
        }
    }
}

#[cfg(test)]
mod tests;
