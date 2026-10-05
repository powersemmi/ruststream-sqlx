//! The `InboxRow` impl `#[derive(Inbox)]` generates.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use ruststream_sqlx_dialect::Role;
use syn::ext::IdentExt;
use syn::{DeriveInput, parse_quote};

use crate::parse::{self, ColumnField, Field, Inbox};
use crate::template::{self, Piece};

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
    Ok(generate(input, &inbox, id, key.as_deref()))
}

/// The rules `TableSpec`'s types leave to the struct: one id, one field per role, one field per
/// column, one form, FIFO groups outside the advisory lock form. Every broken rule is reported at
/// once, on the field that breaks it; the field playing `id` comes back.
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
    let form = match (key, lease) {
        (Some(key), _) => {
            let parts = key.iter().map(|item| match item {
                KeyItem::Literal(text) => quote!(#dialect::KeyPart::Literal(#text)),
                KeyItem::Column(column) => quote!(#dialect::KeyPart::Column(#column)),
            });
            quote!(#dialect::Form::Advisory(&[#(#parts),*]))
        }
        (None, Some((_, expiry))) => {
            let expiry = column(expiry);
            quote!(#dialect::Form::Lease(#expiry))
        }
        (None, None) => quote!(#dialect::Form::RowLock),
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

    let name = &input.ident;
    let id_type = id_field.ty;
    let mut generics = input.generics.clone();
    if !generics.params.is_empty() {
        let (_, ty_generics, _) = input.generics.split_for_impl();
        let predicates = &mut generics.make_where_clause().predicates;
        predicates.push(parse_quote!(
            #name #ty_generics: ::core::marker::Send + ::core::marker::Sync + 'static
        ));
        predicates.push(parse_quote!(
            #id_type: ::core::marker::Send + ::core::marker::Sync + 'static
        ));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics ::ruststream_sqlx::InboxRow for #name #ty_generics #where_clause {
            const SPEC: #dialect::TableSpec<'static> =
                #dialect::TableSpec::new(#table, #id, #form)
                    #within #(#slots)* #data #selecting_all;
            type Id = #id_type;
        }
    }
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
    fn the_rules_the_types_leave_to_the_struct_are_checked() {
        let cases: [(DeriveInput, &str); 7] = [
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
