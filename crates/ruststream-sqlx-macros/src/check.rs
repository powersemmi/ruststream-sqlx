//! The rules a struct deriving `Inbox` keeps beyond what `TableSpec`'s types check, every broken
//! one reported at once.

use ruststream_sqlx_dialect::Role;
use syn::DeriveInput;
use syn::ext::IdentExt;

use crate::parse::{ColumnField, Field, Inbox};

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

/// The rules `TableSpec`'s types leave to the struct: one id, one field per role, one field per
/// column, one form, FIFO groups and a claim of the service's own outside the advisory lock form,
/// `extend` in the lease form, `lock` and `unlock` together in the advisory lock form. Every broken
/// rule is reported at once, on the field or the event that breaks it; the field playing `id` comes
/// back.
pub(crate) fn check<'i, 'a>(
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
    check_lock(inbox, &mut errors);
    errors.finish()?;
    id.ok_or_else(missing_id)
}

/// The service's own `lock` and `unlock`: events of the advisory lock form, listed together, since
/// the service's unlock is what releases the lock its own lock took. Without `advisory_lock` each
/// listed one is refused; beside it, one listed without the other.
fn check_lock(inbox: &Inbox<'_>, errors: &mut Errors) {
    let custom = inbox.table.custom;
    let listed = [("lock", custom.lock), ("unlock", custom.unlock)];
    if inbox.table.advisory_lock.is_none() {
        for (event, span) in listed {
            if let Some(span) = span {
                errors.push(syn::Error::new(
                    span,
                    format!(
                        "`{event}` is an event of the advisory lock form: add \
                         `advisory_lock = \"..\"` or drop `{event}` from `custom(..)`"
                    ),
                ));
            }
        }
        return;
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
