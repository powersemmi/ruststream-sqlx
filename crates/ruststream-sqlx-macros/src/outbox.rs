//! `#[derive(Outbox)]` and `outbox!`: a struct that describes a service's outbox table through
//! `OutboxTable` and its builder, and the registry of the names it tracks.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics, parse_quote};

mod parse;
pub(crate) mod registry;
mod statements;

use parse::{OutboxRole, Record};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let record = parse::record(input)?;
    refuse_json_id(&record)?;
    statements::check(&record)?;
    let table = table(input, &record);
    let headers = header_row(input, &record);
    Ok(quote!(#table #headers))
}

/// The registry binds a record's id as the id's own type, so an id read through `Json` would bind
/// a value its column does not hold.
fn refuse_json_id(record: &Record<'_>) -> syn::Result<()> {
    let (field, column) = record.required(OutboxRole::Id);
    if column.json {
        return Err(syn::Error::new(
            field.ident.span(),
            "the outbox binds a record's id as itself: the `id` field reads its column without \
             `#[sqlx(json)]`",
        ));
    }
    Ok(())
}

/// The struct's generics, bounded where it has parameters by what `OutboxTable` asks of the
/// record and its id; a struct without parameters meets those bounds at the trait itself.
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

/// `Column::new(name)`.
fn column(name: &str) -> TokenStream2 {
    quote!(::ruststream_sqlx::dialect::Column::new(#name))
}

/// The builder chain of `TABLE` and its type: the required columns, the schema, the columns no
/// role reads, the roles that add a marker, and an `own` per event `custom(..)` lists.
fn description(record: &Record<'_>) -> (TokenStream2, TokenStream2) {
    let spec = quote!(::ruststream_sqlx::outbox::spec);
    let table = &record.table.name;
    let required = |role| column(&record.required(role).1.name);
    let (id, name, payload) = (
        required(OutboxRole::Id),
        required(OutboxRole::Name),
        required(OutboxRole::Payload),
    );
    let mut chain = quote!(::ruststream_sqlx::OutboxSpec::new(#table, #id, #name, #payload));
    let mut markers = Vec::new();
    if let Some(schema) = &record.table.schema {
        chain = quote!(#chain.within(#schema));
    }
    let data: Vec<TokenStream2> = record
        .columns()
        .filter(|(_, column)| column.role.is_none())
        .map(|(_, data)| column(&data.name))
        .collect();
    if !data.is_empty() {
        chain = quote!(#chain.data(&[#(#data),*]));
    }
    if record.flattens() {
        chain = quote!(#chain.selecting_all());
    }
    if let Some((_, headers)) = record.playing(OutboxRole::Headers) {
        let headers = column(&headers.name);
        chain = quote!(#chain.headers(#headers));
        markers.push(quote!(#spec::Headers));
    }
    if let Some((_, processed_at)) = record.playing(OutboxRole::ProcessedAt) {
        let processed_at = column(&processed_at.name);
        chain = quote!(#chain.processed_at(#processed_at));
        markers.push(quote!(#spec::ProcessedAt));
    }
    let custom = record.table.custom;
    for (listed, event) in [
        (custom.fetch, quote!(Fetch)),
        (custom.ack, quote!(Ack)),
        (custom.retry, quote!(Retry)),
        (custom.discard, quote!(Discard)),
        (custom.recover, quote!(Recover)),
    ] {
        if listed {
            chain = quote!(#chain.own::<#spec::own::#event>());
            markers.push(quote!(#spec::own::#event));
        }
    }
    (
        chain,
        quote!(::ruststream_sqlx::OutboxSpec<(#(#markers,)*)>),
    )
}

/// `OutboxTable`: the table's description, the id, the name through `AsRef<str>` and the payload
/// through `AsRef<[u8]>`. Each conversion is spanned on its field's type, so a type that does not
/// fit fails there.
fn table(input: &DeriveInput, record: &Record<'_>) -> TokenStream2 {
    let r = quote!(::ruststream_sqlx);
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
    let (chain, described) = description(record);
    // The mark is the database's to write and the outbox never reads it back, so a service's
    // `processed_at` field would read as unused; the record touches it once here.
    let processed = record.playing(OutboxRole::ProcessedAt).map(|(field, _)| {
        let ident = field.ident;
        quote!(let _ = &self.#ident;)
    });
    quote! {
        #[automatically_derived]
        impl #impl_generics #r::OutboxTable for #name #ty_generics #where_clause {
            type Id = #id_type;
            type Table = #described;

            const TABLE: Self::Table = #chain;

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
        }
    }
}

/// `HeaderRow` for the field playing `headers`, its type spanned on the field, so a type that
/// does not hold headers fails there.
fn header_row(input: &DeriveInput, record: &Record<'_>) -> Option<TokenStream2> {
    let (field, _) = record.playing(OutboxRole::Headers)?;
    let r = quote!(::ruststream_sqlx);
    let name = &input.ident;
    let generics = row_generics(input, record);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let ident = field.ident;
    let ty = field.ty;
    let column = quote_spanned!(ty.span()=> type Column = #ty;);
    Some(quote! {
        #[automatically_derived]
        impl #impl_generics #r::HeaderRow for #name #ty_generics #where_clause {
            #column

            fn headers_mut(&mut self) -> &mut #ty {
                &mut self.#ident
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::{DeriveInput, parse_quote};

    #[test]
    fn the_derive_writes_the_manual_description_and_no_statement() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox", schema = "app", custom(fetch, retry))]
            struct Order {
                #[field(id)]
                id: i64,
                #[field(name)]
                #[sqlx(rename = "channel")]
                name: String,
                #[field(payload)]
                payload: Vec<u8>,
                #[field(headers)]
                headers: Option<String>,
                #[field(processed_at)]
                processed_at: Option<String>,
                created_at: String,
            }
        };
        let expanded = super::expand(&input)?;
        let column = |name: &str| quote!(::ruststream_sqlx::dialect::Column::new(#name));
        let (id, channel, payload, created_at, headers, processed_at) = (
            column("id"),
            column("channel"),
            column("payload"),
            column("created_at"),
            column("headers"),
            column("processed_at"),
        );
        let spec = quote!(::ruststream_sqlx::outbox::spec);
        let expected = quote! {
            #[automatically_derived]
            impl ::ruststream_sqlx::OutboxTable for Order {
                type Id = i64;
                type Table = ::ruststream_sqlx::OutboxSpec<(
                    #spec::Headers,
                    #spec::ProcessedAt,
                    #spec::own::Fetch,
                    #spec::own::Retry,
                )>;

                const TABLE: Self::Table =
                    ::ruststream_sqlx::OutboxSpec::new("outbox", #id, #channel, #payload)
                        .within("app")
                        .data(&[#created_at])
                        .headers(#headers)
                        .processed_at(#processed_at)
                        .own::<#spec::own::Fetch>()
                        .own::<#spec::own::Retry>();

                fn id(&self) -> &i64 {
                    let _ = &self.processed_at;
                    &self.id
                }

                fn name(&self) -> &str {
                    ::core::convert::AsRef::<str>::as_ref(&self.name)
                }

                fn payload(&self) -> &[u8] {
                    ::core::convert::AsRef::<[u8]>::as_ref(&self.payload)
                }
            }

            #[automatically_derived]
            impl ::ruststream_sqlx::HeaderRow for Order {
                type Column = Option<String>;

                fn headers_mut(&mut self) -> &mut Option<String> {
                    &mut self.headers
                }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
        Ok(())
    }

    #[test]
    fn an_id_read_through_json_is_refused_on_its_field() {
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox")]
            struct Order {
                #[field(id)]
                #[sqlx(json)]
                id: Key,
                #[field(name)]
                name: String,
                #[field(payload)]
                payload: Vec<u8>,
            }
        };
        let error = super::expand(&input).map_or_else(|error| error.to_string(), |_| String::new());
        assert_eq!(
            error,
            "the outbox binds a record's id as itself: the `id` field reads its column without \
             `#[sqlx(json)]`"
        );
    }
}
