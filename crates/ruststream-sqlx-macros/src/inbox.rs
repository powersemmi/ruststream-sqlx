//! The derive's expansion: the manual form a service would write by hand. `impl InboxTable` with
//! the `InboxSpec` chain and its type, the accessor impls of the roles the fields play, the row
//! mode's `Input`, and the insert. The crate's blanket impls over `InboxTable` write the rest.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, Generics, parse_quote, parse_quote_spanned};

use crate::parse;
use crate::{check, checked, insert, template};

mod assembled;
mod rows;
mod table;

pub(crate) use rows::{accessors, carried};
pub(crate) use table::{Description, describe, own_events};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    // A struct that flattens a headers struct is a message assembled from it; any other is flat.
    if let Ok(fields) = parse::fields(input)
        && let Some(headers) = fields
            .iter()
            .position(|field| matches!(field.storage, parse::Storage::Headers))
    {
        return assembled::expand(input, &fields, headers);
    }
    let inbox = parse::inbox(input)?;
    let (id_field, id_column) = check::check(input, &inbox)?;
    let key = template::advisory_key(&inbox)?;
    let description = describe(&inbox, id_column, key.as_deref(), &[]);
    let generics = valid_generics(input, bounded_generics(input, id_field.ty), &description);
    let table = inbox_table(input, &generics, &description, id_field);
    let rows = accessors(input, &generics, &inbox);
    let insert = insert::insert(input, &generics, &inbox)?;
    let checked = checked::item(
        input,
        &generics,
        &inbox,
        (id_field, id_column),
        key.as_deref(),
        checked::Layout::Flat,
    )?;
    Ok(quote!(#table #rows #insert #checked))
}

/// `impl InboxTable` for a flat struct: its id, and its description as the chain and its type.
fn inbox_table(
    input: &DeriveInput,
    generics: &Generics,
    description: &Description,
    id: &parse::Field<'_>,
) -> TokenStream2 {
    let name = &input.ident;
    let table = description.table_type();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let chain = &description.chain;
    let id_type = id.ty;
    let id_ident = id.ident;
    quote! {
        #[automatically_derived]
        impl #impl_generics ::ruststream_sqlx::InboxTable for #name #ty_generics #where_clause {
            type Id = #id_type;
            type Table = #table;
            const TABLE: Self::Table = #chain;

            fn id(&self) -> &#id_type {
                &self.#id_ident
            }
        }
    }
}

/// `generics` with the rules across the table's settings, for a generic struct: a setting may name
/// a parameter (`clock = Source`), and the table and every impl of the row hold where the
/// parameter keeps the rules.
pub(crate) fn valid_generics(
    input: &DeriveInput,
    mut generics: Generics,
    description: &Description,
) -> Generics {
    if !input.generics.params.is_empty() {
        let table = description.table_type();
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote_spanned!(description.span=>
                #table: ::ruststream_sqlx::spec::Valid
            ));
    }
    generics
}

/// The struct's generics with what every impl of the row needs of them: `Send + Sync + 'static`
/// of the struct, and of the id type, which logs also print and a lease subscription copies.
pub(crate) fn bounded_generics(input: &DeriveInput, id_type: &syn::Type) -> Generics {
    let name = &input.ident;
    let mut generics = input.generics.clone();
    if !generics.params.is_empty() {
        let (_, ty_generics, _) = input.generics.split_for_impl();
        let predicates = &mut generics.make_where_clause().predicates;
        predicates.push(parse_quote!(
            #name #ty_generics: ::core::marker::Send + ::core::marker::Sync + 'static
        ));
        predicates.push(parse_quote!(
            #id_type: ::core::clone::Clone
                + ::core::fmt::Debug
                + ::core::marker::Send
                + ::core::marker::Sync
                + 'static
        ));
    }
    generics
}

#[cfg(test)]
pub(crate) mod tests {
    use syn::{DeriveInput, parse_quote};

    use super::expand;

    pub(crate) fn errors(input: &DeriveInput) -> Vec<String> {
        expand(input).map_or_else(
            |error| error.into_iter().map(|error| error.to_string()).collect(),
            |_| Vec::new(),
        )
    }

    /// The expansion without whitespace, so a test reads it as the source would be written.
    pub(crate) fn expanded(input: &DeriveInput) -> syn::Result<String> {
        Ok(expand(input)?.to_string().replace(' ', ""))
    }

    #[test]
    fn a_flat_struct_expands_to_the_manual_form() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", schema = "app", custom(ack, retry))]
            struct Job {
                #[field(id, generated)] id: i64,
                #[field(payload)] payload: Vec<u8>,
                #[field(group, fifo = true)] name: String,
                #[field(partition_key)] tenant: String,
                #[field(attempt, generated)] attempt: i16,
                #[field(locked_until)] locked_until: Option<DateTime<Utc>>,
                #[field(retry_after)] retry_after: Option<DateTime<Utc>>,
                note: String,
            }
        };
        let impls = expanded(&input)?;
        for expected in [
            "impl::ruststream_sqlx::InboxTableforJob{typeId=i64;",
            // The markers in the canonical order: the form, the roles, the clock and opening, the
            // service's own events.
            "typeTable=::ruststream_sqlx::InboxSpec<(\
             ::ruststream_sqlx::spec::Lease<<Option<DateTime<Utc>>as::ruststream_sqlx::TimeColumn>::Time>,\
             ::ruststream_sqlx::spec::Fifo,\
             ::ruststream_sqlx::spec::Key,\
             ::ruststream_sqlx::spec::RetryAfter<Option<DateTime<Utc>>>,\
             ::ruststream_sqlx::spec::Attempt,\
             ::ruststream_sqlx::spec::Payload,\
             ::ruststream_sqlx::spec::own::Ack,\
             ::ruststream_sqlx::spec::own::Retry,)>;",
            "constTABLE:Self::Table=::ruststream_sqlx::InboxSpec::new(\"jobs\",\
             ::ruststream_sqlx::dialect::Column::new(\"id\").generated()).within(\"app\")\
             .lease(::ruststream_sqlx::dialect::Column::new(\"locked_until\"))\
             .fifo_group(::ruststream_sqlx::dialect::Column::new(\"name\"))\
             .partition_key(::ruststream_sqlx::dialect::Column::new(\"tenant\"))\
             .retry_after(::ruststream_sqlx::dialect::Column::new(\"retry_after\"))\
             .attempt(::ruststream_sqlx::dialect::Column::new(\"attempt\").generated())\
             .payload(::ruststream_sqlx::dialect::Column::new(\"payload\"))\
             .data(&[::ruststream_sqlx::dialect::Column::new(\"note\")])\
             .own::<::ruststream_sqlx::spec::own::Ack>()\
             .own::<::ruststream_sqlx::spec::own::Retry>();",
            "fnid(&self)->&i64{&self.id}",
            "impl::ruststream_sqlx::PayloadRowforJob{typeColumn=Vec<u8>;",
            "impl::ruststream_sqlx::KeyRowforJobwherefor<'__c>String:::ruststream_sqlx::KeyColumn{typeKey=String;fnpartition_key(&self)->&String{&self.tenant}}",
            "impl::ruststream_sqlx::AttemptRowforJobwherefor<'__c>i16:::ruststream_sqlx::AttemptColumn{typeAttempt=i16;fnattempt(&self)->&i16{&self.attempt}}",
        ] {
            assert!(impls.contains(expected), "{expected}\n{impls}");
        }
        // The crate's blanket impls write the rest of the contract.
        for machinery in ["Events", "QueueRow", "InboxRow", "LeaseRow", "Input"] {
            assert!(!impls.contains(machinery), "{machinery}: {impls}");
        }
        Ok(())
    }

    #[test]
    fn a_generic_struct_holds_its_impls_where_its_settings_keep_the_rules() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", clock = Source)]
            struct Job<Source> { #[field(id)] id: i64, #[sqlx(skip)] source: PhantomData<Source> }
        };
        let impls = expanded(&input)?;
        let valid = "::ruststream_sqlx::InboxSpec<(::ruststream_sqlx::spec::Clock<Source>,)>:\
                     ::ruststream_sqlx::spec::Valid";
        for header in ["InboxTableforJob<Source>", "__private::InputforJob<Source>"] {
            let at = impls
                .find(header)
                .ok_or_else(|| syn::Error::new(input.ident.span(), header))?;
            let clause = &impls[at..at + impls[at..].find('{').unwrap_or_default()];
            assert!(clause.contains(valid), "{header}: {clause}");
        }
        Ok(())
    }
}
