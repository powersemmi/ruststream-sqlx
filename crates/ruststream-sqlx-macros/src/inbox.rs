//! The `QueueRow` and `InboxRow` impls `#[derive(Inbox)]` generates.

use heck::ToUpperCamelCase;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use ruststream_sqlx_dialect::{Opening, Role};
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics, parse_quote};

use crate::parse::{self, ColumnField, Field, Inbox};
use crate::template::{self, Piece};
use crate::{events, insert};

/// One part of the advisory lock key, with every field already turned into its column.
enum KeyItem {
    Literal(String),
    Column(String),
}

/// The errors of one derive, reported together.
#[derive(Default)]
struct Errors(Option<syn::Error>);

impl Errors {
    fn push(&mut self, error: syn::Error) {
        match &mut self.0 {
            Some(errors) => errors.combine(error),
            None => self.0 = Some(error),
        }
    }

    fn finish(self) -> syn::Result<()> {
        self.0.map_or(Ok(()), Err)
    }
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let inbox = parse::inbox(input)?;
    let id = check(input, &inbox)?;
    let key = advisory_key(&inbox)?;
    let generics = bounded_generics(input, id.0.ty);
    let row = generate(input, &generics, &inbox, id, key.as_deref());
    let payload = events::payload_row(input, &generics, &inbox);
    let contract = events::events(input, &generics, &inbox, id.0);
    let insert = insert::insert(input, &generics, &inbox)?;
    Ok(quote!(#row #payload #contract #insert))
}

/// The rules `TableSpec`'s types leave to the struct: one id, one field per role, one field per
/// column, one form, FIFO groups and a claim of the service's own outside the advisory lock form,
/// `extend` in the lease form. Every broken rule is reported at once, on the field or the event
/// that breaks it; the field playing `id` comes back.
fn check<'i, 'a>(
    input: &DeriveInput,
    inbox: &'i Inbox<'a>,
) -> syn::Result<(&'i Field<'a>, &'i ColumnField)> {
    let table = inbox.table.name.value();
    let missing_id = || {
        syn::Error::new(
            input.ident.span(),
            format!(
                "table `{table}` has no `id` field: mark the field that identifies a row with \
                 `#[field(id)]`"
            ),
        )
    };
    let mut errors = Errors::default();
    let columns: Vec<_> = inbox.columns().collect();
    let id = columns
        .iter()
        .copied()
        .find(|(_, column)| column.role == Some(Role::Id));
    if id.is_none() {
        errors.push(missing_id());
    }
    for (index, (field, column)) in columns.iter().enumerate() {
        let earlier = &columns[..index];
        if let Some(role) = column.role
            && let Some((first, _)) = earlier.iter().find(|(_, other)| other.role == Some(role))
        {
            errors.push(syn::Error::new(
                field.ident.span(),
                format!(
                    "`{}` plays `{role}` already: a role belongs to one field",
                    first.ident
                ),
            ));
        }
        if let Some((first, _)) = earlier.iter().find(|(_, other)| other.name == column.name) {
            errors.push(syn::Error::new(
                field.ident.span(),
                format!(
                    "`{}` reads column `{}`, which `{}` reads already: a column is named in one \
                     place",
                    field.ident, column.name, first.ident
                ),
            ));
        }
        if inbox.table.advisory_lock.is_some() {
            if column.role == Some(Role::LockedUntil) {
                errors.push(syn::Error::new(
                    field.ident.span(),
                    "`locked_until` selects the lease form and `advisory_lock` the advisory lock \
                     form: a table has one form",
                ));
            }
            if let Some(span) = column.fifo {
                let group = field.ident.unraw();
                errors.push(syn::Error::new(
                    span,
                    format!(
                        "`fifo = true` does not combine with `advisory_lock`: drop it and put the \
                         group's field into the lock key, as in \
                         `advisory_lock = \"{table}-{{{group}}}\"`"
                    ),
                ));
            }
        }
    }
    if inbox.table.advisory_lock.is_some()
        && let Some(span) = inbox.table.custom.claim
    {
        errors.push(syn::Error::new(
            span,
            "the advisory lock form selects its candidates with their keys itself: drop `claim` \
             from `custom(..)`",
        ));
    }
    if let Some(span) = inbox.table.custom.extend
        && !columns
            .iter()
            .any(|(_, column)| column.role == Some(Role::LockedUntil))
    {
        errors.push(syn::Error::new(
            span,
            "`extend` is an event of the lease form: add `#[field(locked_until)]` or drop \
             `extend` from `custom(..)`",
        ));
    }
    errors.finish()?;
    id.ok_or_else(missing_id)
}

/// The lock key the template names, with each field resolved to the column it reads.
fn advisory_key(inbox: &Inbox<'_>) -> syn::Result<Option<Vec<KeyItem>>> {
    let Some(template) = &inbox.table.advisory_lock else {
        return Ok(None);
    };
    let mut key = Vec::new();
    for piece in template::parse(template)? {
        match piece {
            Piece::Literal(text) => key.push(KeyItem::Literal(text)),
            Piece::Field(name) => {
                let Some(field) = inbox.field_named(&name) else {
                    return Err(syn::Error::new(
                        template.span(),
                        format!("the lock key names `{name}`, which is not a field of the struct"),
                    ));
                };
                let Some(column) = field.column() else {
                    return Err(syn::Error::new(
                        template.span(),
                        format!("the lock key names `{name}`, a field without a column"),
                    ));
                };
                key.push(KeyItem::Column(column.name.clone()));
            }
        }
    }
    Ok(Some(key))
}

fn generate(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
    (id_field, id_column): (&Field<'_>, &ColumnField),
    key: Option<&[KeyItem]>,
) -> TokenStream2 {
    let dialect = quote!(::ruststream_sqlx::dialect);
    let column = |column: &ColumnField| {
        let name = &column.name;
        let generated = column.generated.then(|| quote!(.generated()));
        quote!(#dialect::Column::new(#name) #generated)
    };
    let id = column(id_column);
    let lease = inbox
        .columns()
        .find(|(_, column)| column.role == Some(Role::LockedUntil));
    let private = quote!(::ruststream_sqlx::__private);
    // The form twice: as the description's value, and as the type a subscription checks against
    // its database.
    let (form, form_type) = match (key, lease) {
        (Some(key), _) => {
            let parts = key.iter().map(|item| match item {
                KeyItem::Literal(text) => quote!(#dialect::KeyPart::Literal(#text)),
                KeyItem::Column(column) => quote!(#dialect::KeyPart::Column(#column)),
            });
            (
                quote!(#dialect::Form::Advisory(&[#(#parts),*])),
                quote!(#private::AdvisoryForm),
            )
        }
        (None, Some((_, expiry))) => {
            let expiry = column(expiry);
            (
                quote!(#dialect::Form::Lease(#expiry)),
                quote!(#private::LeaseForm),
            )
        }
        (None, None) => (
            quote!(#dialect::Form::RowLock),
            quote!(#private::RowLockForm),
        ),
    };
    let slots = inbox.columns().filter_map(|(_, slot)| {
        let role = slot.role?;
        if matches!(role, Role::Id | Role::LockedUntil) {
            return None;
        }
        // Each role's builder on `TableSpec` is named as `#[field(..)]` spells the role.
        let builder = if slot.fifo.is_some() {
            format_ident!("fifo_group")
        } else {
            format_ident!("{}", role.attribute())
        };
        let slot = column(slot);
        Some(quote!(.#builder(#slot)))
    });
    let data: Vec<_> = inbox
        .columns()
        .filter(|(_, column)| column.role.is_none())
        .map(|(_, data)| column(data))
        .collect();
    let data = (!data.is_empty()).then(|| quote!(.data(&[#(#data),*])));
    let table = &inbox.table.name;
    let within = inbox
        .table
        .schema
        .as_ref()
        .map(|schema| quote!(.within(#schema)));
    let selecting_all = inbox.flattens().then(|| quote!(.selecting_all()));
    let (opening, opening_type) = opening(inbox.table.opening);

    let name = &input.ident;
    let id_type = id_field.ty;
    let spec = quote!(#dialect::TableSpec::new(#table, #id, #form) #within #(#slots)* #data #selecting_all #opening);
    let LeaseParts {
        row: lease_row,
        item_check,
        spec_check,
    } = lease_parts(input, generics, inbox, lease.map(|(field, _)| field));
    let spec = match &inbox.table.clock {
        Some(clock) => quote!({
            #spec_check
            let spec = #spec;
            if <#clock as ::ruststream_sqlx::TimeSource>::DATABASE {
                spec.database_clock()
            } else {
                spec
            }
        }),
        None => spec,
    };
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics ::ruststream_sqlx::__private::QueueRow for #name #ty_generics #where_clause {
            type Id = #id_type;
        }

        #[automatically_derived]
        impl #impl_generics ::ruststream_sqlx::InboxRow for #name #ty_generics #where_clause {
            const SPEC: #dialect::TableSpec<'static> = #spec;
            type Form = #form_type;
            type Opening = #opening_type;
        }

        #lease_row

        #item_check
    }
}

/// What the table's transactions open at, twice: as the description's builder step, and as the
/// type a subscription requires its dialect to open (`Opens`). A table that names neither a level
/// nor a mode opens at its database's default, which every dialect opens, as `()`.
fn opening(opening: Opening) -> (Option<TokenStream2>, TokenStream2) {
    let dialect = quote!(::ruststream_sqlx::dialect);
    // A variant of `Isolation` or `Mode` and its type in `level` share one name: the word the
    // attribute takes, in upper camel case.
    let named = |word: &str| format_ident!("{}", word.to_upper_camel_case());
    match opening {
        Opening::Isolation(level) => {
            let name = named(level.attribute());
            (
                Some(quote!(.isolation(#dialect::Isolation::#name))),
                quote!(#dialect::level::#name),
            )
        }
        Opening::Mode(mode) => {
            let name = named(mode.attribute());
            (
                Some(quote!(.mode(#dialect::Mode::#name))),
                quote!(#dialect::level::#name),
            )
        }
        _ => (None, quote!(())),
    }
}

/// What the lease form adds to a struct with a `locked_until` field.
struct LeaseParts {
    /// The `LeaseRow` impl.
    row: Option<TokenStream2>,
    /// The refusal of the database's clock, as an item of its own.
    item_check: Option<TokenStream2>,
    /// The same refusal inside `SPEC`, for a generic struct.
    spec_check: Option<TokenStream2>,
}

/// `LeaseRow` for a struct whose `field` plays `locked_until`, and the refusal of a lease on the
/// database's clock.
fn lease_parts(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
    field: Option<&Field<'_>>,
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
        impl #impl_generics ::ruststream_sqlx::LeaseRow for #name #ty_generics #where_clause {
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

/// The struct's generics with what every impl of the row needs of them: `Send + Sync + 'static`
/// of the struct, and of the id type, which logs also print and a lease subscription copies.
fn bounded_generics(input: &DeriveInput, id_type: &syn::Type) -> Generics {
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
mod tests {
    use quote::format_ident;
    use syn::{DeriveInput, parse_quote};

    use super::expand;

    fn errors(input: &DeriveInput) -> Vec<String> {
        expand(input).map_or_else(
            |error| error.into_iter().map(|error| error.to_string()).collect(),
            |_| Vec::new(),
        )
    }

    #[test]
    fn a_declared_opening_reaches_the_description_and_the_type() -> syn::Result<()> {
        let cases = [
            (
                "isolation",
                "read_uncommitted",
                "Isolation",
                "ReadUncommitted",
            ),
            ("isolation", "read_committed", "Isolation", "ReadCommitted"),
            (
                "isolation",
                "repeatable_read",
                "Isolation",
                "RepeatableRead",
            ),
            ("isolation", "serializable", "Isolation", "Serializable"),
            ("mode", "deferred", "Mode", "Deferred"),
            ("mode", "immediate", "Mode", "Immediate"),
            ("mode", "exclusive", "Mode", "Exclusive"),
        ];
        for (key, word, kind, name) in cases {
            let (key, word) = (format_ident!("{key}"), format_ident!("{word}"));
            let input: DeriveInput = parse_quote! {
                #[inbox(table = "jobs", #key = #word)]
                struct Job { #[field(id)] id: i64 }
            };
            let impls = expand(&input)?.to_string();
            assert!(
                impls.contains(&format!(
                    ". {key} (:: ruststream_sqlx :: dialect :: {kind} :: {name})"
                )),
                "{key} = {word}: {impls}"
            );
            assert!(
                impls.contains(&format!(
                    "type Opening = :: ruststream_sqlx :: dialect :: level :: {name} ;"
                )),
                "{key} = {word}: {impls}"
            );
        }
        // A table that names neither opens at its database's default, which every dialect opens.
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs")]
            struct Job { #[field(id)] id: i64 }
        };
        let impls = expand(&input)?.to_string();
        assert!(impls.contains("type Opening = () ;"), "{impls}");
        assert!(
            !impls.contains(". isolation (") && !impls.contains(". mode ("),
            "{impls}"
        );
        Ok(())
    }

    #[test]
    fn the_rules_the_types_leave_to_the_struct_are_checked() {
        let cases: [(DeriveInput, &str); 9] = [
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(payload)] payload: Vec<u8> } },
                "table `jobs` has no `id` field: mark the field that identifies a row with \
                 `#[field(id)]`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] job_id: i64, #[field(id)] id: i64 } },
                "`job_id` plays `id` already: a role belongs to one field",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] job_id: i64, #[sqlx(rename = "job_id")] legacy_id: i64 } },
                "`legacy_id` reads column `job_id`, which `job_id` reads already: a column is named \
                 in one place",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", advisory_lock = "jobs-{job_id}")] struct Job { #[field(id)] job_id: i64, #[field(locked_until)] locked_until: Option<i64> } },
                "`locked_until` selects the lease form and `advisory_lock` the advisory lock form: \
                 a table has one form",
            ),
            (
                parse_quote! { #[inbox(table = "ledger", advisory_lock = "ledger-{id}")] struct Entry { #[field(id)] id: i64, #[field(group, fifo = true)] account: String } },
                "`fifo = true` does not combine with `advisory_lock`: drop it and put the group's \
                 field into the lock key, as in `advisory_lock = \"ledger-{account}\"`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", advisory_lock = "jobs-{tenant}")] struct Job { #[field(id)] job_id: i64 } },
                "the lock key names `tenant`, which is not a field of the struct",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", advisory_lock = "jobs-{tenant}")] struct Job { #[field(id)] job_id: i64, #[sqlx(skip)] tenant: String } },
                "the lock key names `tenant`, a field without a column",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", custom(extend))] struct Job { #[field(id)] job_id: i64 } },
                "`extend` is an event of the lease form: add `#[field(locked_until)]` or drop \
                 `extend` from `custom(..)`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", advisory_lock = "jobs-{job_id}", custom(fetch, claim))] struct Job { #[field(id)] job_id: i64 } },
                "the advisory lock form selects its candidates with their keys itself: drop \
                 `claim` from `custom(..)`",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(errors(&input), [expected]);
        }
    }

    #[test]
    fn every_broken_rule_is_reported_at_once() {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs")]
            struct Job {
                #[field(payload)]
                payload: Vec<u8>,
                #[sqlx(rename = "payload")]
                body: Vec<u8>,
            }
        };
        assert_eq!(
            errors(&input),
            [
                "table `jobs` has no `id` field: mark the field that identifies a row with \
                 `#[field(id)]`",
                "`body` reads column `payload`, which `payload` reads already: a column is named \
                 in one place",
            ]
        );
    }
}
