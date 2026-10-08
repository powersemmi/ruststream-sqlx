//! `#[derive(InboxHeaders)]`: a struct that describes the queue table of a message assembled from
//! it. The struct's description in the manual form, the header map of its fields without a role,
//! the accessors of the roles it plays, and the generated insert.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use ruststream_sqlx_dialect::Role;
use syn::DeriveInput;
use syn::spanned::Spanned;

use crate::check::{self, Errors};
use crate::inbox::{Description, accessors, bounded_generics, describe, valid_generics};
use crate::parse::{self, Field, Inbox, Storage};
use crate::{checked, insert, template};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let inbox = parse::headers_table(input)?;
    refuse_message_parts(input, &inbox)?;
    let (id_field, id_column) = check::check(input, &inbox)?;
    let key = template::advisory_key(&inbox)?;
    let r = quote!(::ruststream_sqlx);
    let description = describe(
        &inbox,
        id_column,
        key.as_deref(),
        &[(quote!(.header_fields()), quote!(#r::spec::HeaderFields))],
    );
    let generics = valid_generics(input, bounded_generics(input, id_field.ty), &description);
    let settings = description.settings();
    let chain = &description.chain;
    let name = &input.ident;
    let id_type = id_field.ty;
    let id_ident = id_field.ident;
    let (names, header_map) = header_map(&inbox);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let valid = valid(input, inbox.table.clock.as_ref(), &description);
    let rows = accessors(input, &generics, &inbox);
    let insert = insert::insert(input, &generics, &inbox)?;
    let checked = checked::item(
        input,
        &generics,
        &inbox,
        (id_field, id_column),
        key.as_deref(),
        checked::Layout::Headers,
    )?;
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #r::InboxHeaders for #name #ty_generics #where_clause {
            type Id = #id_type;
            type Settings = #settings;
            const TABLE: #r::InboxSpec<Self::Settings> = #chain;
            const NAMES: &'static [&'static str] = #names;

            fn id(&self) -> &#id_type {
                &self.#id_ident
            }

            fn header_map(&self) -> #r::__private::HeaderMap {
                #header_map
            }
        }

        #valid

        #rows

        #insert

        #checked
    })
}

/// The rules across the table's settings, held where the struct names its clock: a message's
/// description extends this one, and the struct's own clock is what may break them.
fn valid(
    input: &DeriveInput,
    clock: Option<&syn::Path>,
    description: &Description,
) -> Option<TokenStream2> {
    // The struct's own checks judge every other setting. A clock that is a parameter of the
    // struct is judged where the message is mounted.
    if clock.is_none() || !input.generics.params.is_empty() {
        return None;
    }
    let table = description.table_type();
    let span = description.span;
    Some(quote_spanned! {span=>
        const _: () = {
            #[allow(dead_code)]
            fn __valid<__Table: ::ruststream_sqlx::spec::Valid>() {}
            #[allow(dead_code)]
            fn __check() {
                __valid::<#table>();
            }
        };
    })
}

/// Refuses what belongs to the message struct, which flattens this one: `custom(..)`, the
/// `payload` and `headers` roles, and a flattened field, whose columns the description cannot
/// see.
fn refuse_message_parts(input: &DeriveInput, inbox: &Inbox<'_>) -> syn::Result<()> {
    let name = &input.ident;
    let mut errors = Errors::default();
    if let Some(span) = inbox.table.custom.listed {
        errors.push(syn::Error::new(
            span,
            format!(
                "`custom(..)` lists the events of the message, which a handler takes: put it on \
                 the message struct whose `#[field(headers)]` field flattens `{name}`"
            ),
        ));
    }
    for field in &inbox.fields {
        let ident = field.ident;
        match &field.storage {
            Storage::Column(column) if column.role == Some(Role::Payload) => {
                errors.push(syn::Error::new(
                    ident.span(),
                    format!(
                        "`{ident}` plays `payload`, and a headers struct holds no message: the \
                         handler takes the message struct that flattens `{name}`, as in row mode"
                    ),
                ));
            }
            Storage::Column(column) if column.role == Some(Role::Headers) => {
                errors.push(syn::Error::new(
                    ident.span(),
                    format!(
                        "`{ident}` plays `headers` in a headers struct: every field of `{name}` \
                         without a role is a header already, so drop the role"
                    ),
                ));
            }
            Storage::Flattened | Storage::Headers => errors.push(syn::Error::new(
                ident.span(),
                format!(
                    "`{ident}` flattens a struct whose columns `{name}` cannot see: a headers \
                     struct names every column of the queue table it describes"
                ),
            )),
            Storage::Column(_) | Storage::Skipped => {}
        }
    }
    errors.finish()
}

/// The names of the fields without a role, each its column's, and the header map that puts
/// each under its name.
fn header_map(inbox: &Inbox<'_>) -> (TokenStream2, TokenStream2) {
    let headers: Vec<(&Field<'_>, &str)> = inbox
        .columns()
        .filter(|(_, column)| column.role.is_none())
        .map(|(field, column)| (field, column.name.as_str()))
        .collect();
    let count = headers.len();
    // Each put is spanned at the field's type, so a type without `HeaderField` is reported there.
    let put = quote!(::ruststream_sqlx::put_header);
    let puts = headers.iter().map(|(field, column)| {
        let ident = field.ident;
        quote_spanned!(field.ty.span()=> #put(&mut headers, #column, &self.#ident);)
    });
    let names = headers.iter().map(|(_, column)| column);
    (
        quote!(&[#(#names),*]),
        quote! {
            let mut headers = ::ruststream_sqlx::__private::HeaderMap::with_capacity(#count);
            #(#puts)*
            headers
        },
    )
}

#[cfg(test)]
mod tests {
    use syn::{DeriveInput, parse_quote};

    use super::expand;

    fn errors(input: &DeriveInput) -> Vec<String> {
        expand(input).map_or_else(
            |error| error.into_iter().map(|error| error.to_string()).collect(),
            |_| Vec::new(),
        )
    }

    #[test]
    fn what_belongs_to_the_message_is_refused_on_the_headers_struct() {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", custom(fetch))]
            struct Head {
                #[field(id)] id: i64,
                #[field(payload)] body: Vec<u8>,
                #[field(headers)] meta: Json<Map>,
                #[sqlx(flatten)] extra: Extra,
            }
        };
        assert_eq!(
            errors(&input),
            [
                "`custom(..)` lists the events of the message, which a handler takes: put it on the \
                 message struct whose `#[field(headers)]` field flattens `Head`",
                "`body` plays `payload`, and a headers struct holds no message: the handler takes \
                 the message struct that flattens `Head`, as in row mode",
                "`meta` plays `headers` in a headers struct: every field of `Head` without a role is \
                 a header already, so drop the role",
                "`extra` flattens a struct whose columns `Head` cannot see: a headers struct names \
                 every column of the queue table it describes",
            ]
        );
        let untabled: DeriveInput = parse_quote! { struct Head { #[field(id)] id: i64 } };
        assert_eq!(
            errors(&untabled),
            ["#[derive(InboxHeaders)] needs the table: add `#[inbox(table = \"..\")]`"]
        );
    }

    #[test]
    fn the_fields_without_a_role_are_the_headers_under_their_columns_names() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs")]
            #[sqlx(rename_all = "camelCase")]
            struct Head {
                #[field(id, generated)] job_id: i64,
                #[field(group)] name: String,
                tenant_name: String,
                #[sqlx(rename = "trace")] trace_id: Option<String>,
                #[sqlx(skip)] cache: u8,
            }
        };
        let impls = expand(&input)?.to_string().replace(' ', "");
        for expected in [
            "impl::ruststream_sqlx::InboxHeadersforHead{typeId=i64;\
             typeSettings=(::ruststream_sqlx::spec::HeaderFields,);\
             constTABLE:::ruststream_sqlx::InboxSpec<Self::Settings>=\
             ::ruststream_sqlx::InboxSpec::new(\"jobs\",\
             ::ruststream_sqlx::dialect::Column::new(\"jobId\").generated())",
            ".group(::ruststream_sqlx::dialect::Column::new(\"name\"))\
             .data(&[::ruststream_sqlx::dialect::Column::new(\"tenantName\"),\
             ::ruststream_sqlx::dialect::Column::new(\"trace\")]).header_fields();",
            "fnid(&self)->&i64{&self.job_id}",
            "constNAMES:&'static[&'staticstr]=&[\"tenantName\",\"trace\"];",
            "::ruststream_sqlx::put_header(&mutheaders,\"tenantName\",&self.tenant_name);",
            "::ruststream_sqlx::put_header(&mutheaders,\"trace\",&self.trace_id);",
        ] {
            assert!(impls.contains(expected), "{expected}\n{impls}");
        }
        assert!(!impls.contains("\"cache\""), "a skipped field is no header");
        for machinery in ["HeadersRow", "HeadersLease", "Events"] {
            assert!(!impls.contains(machinery), "{machinery}: {impls}");
        }
        // The insert is built with each dialect the macros are built with.
        #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
        assert!(
            impls.contains("impl<__C>"),
            "the headers struct gets the generated insert"
        );
        Ok(())
    }
}
