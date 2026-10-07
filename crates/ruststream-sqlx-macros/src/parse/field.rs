//! `#[field(..)]`: the role a field plays and whether the database fills its column in, read
//! together with sqlx's attributes into where the field's value comes from.

use proc_macro2::Span;
use ruststream_sqlx_dialect::Role;
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{Attribute, LitBool};

use super::sqlx::{column_name, sqlx_field};
use super::{ColumnField, Field, Storage};
use crate::naming::RenameAll;

/// What `#[field(..)]` says about one field, with the span of each word for errors.
#[derive(Default)]
struct Marks {
    role: Option<(Role, Span)>,
    generated: Option<Span>,
    fifo: Option<(bool, Span)>,
}

pub(super) fn field(field: &syn::Field, rename_all: Option<RenameAll>) -> syn::Result<Field<'_>> {
    let ident = field.ident.as_ref().expect("a named field has a name");
    let sqlx = sqlx_field(&field.attrs)?;
    let marks = marks(&field.attrs)?;
    let without_column = if sqlx.skip {
        Some(("`#[sqlx(skip)]`", Storage::Skipped))
    } else if sqlx.flatten {
        Some(("`#[sqlx(flatten)]`", Storage::Flattened))
    } else {
        None
    };
    if let Some((attribute, _)) = &without_column {
        if let Some((role, span)) = marks.role {
            return Err(syn::Error::new(
                span,
                format!(
                    "{attribute} leaves `{ident}` without a column, so it cannot play `{role}`"
                ),
            ));
        }
        if let Some(span) = marks.generated {
            return Err(syn::Error::new(
                span,
                format!(
                    "{attribute} leaves `{ident}` without a column for the database to fill in"
                ),
            ));
        }
    }
    let role = marks.role.map(|(role, _)| role);
    if let Some((_, span)) = marks.fifo
        && role != Some(Role::Group)
    {
        return Err(syn::Error::new(
            span,
            "`fifo` belongs to the `group` role: `#[field(group, fifo = true)]`",
        ));
    }
    let storage = match without_column {
        Some((_, storage)) => storage,
        None => Storage::Column(ColumnField {
            name: column_name(ident, sqlx.rename.as_ref(), rename_all)?,
            role,
            generated: marks.generated.is_some(),
            fifo: marks.fifo.and_then(|(fifo, span)| fifo.then_some(span)),
            json: sqlx.json,
            try_from: sqlx.try_from,
        }),
    };
    Ok(Field {
        ident,
        ty: &field.ty,
        storage,
    })
}

fn marks(attrs: &[Attribute]) -> syn::Result<Marks> {
    let mut marks = Marks::default();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("field")) {
        let mut named = false;
        attr.parse_nested_meta(|meta| {
            named = true;
            let span = meta.path.span();
            let word = meta
                .path
                .get_ident()
                .map(|ident| ident.unraw().to_string())
                .unwrap_or_default();
            if word == "generated" {
                if marks.generated.replace(span).is_some() {
                    return Err(meta.error("`generated` is given twice"));
                }
            } else if word == "fifo" {
                let fifo: LitBool = meta.value()?.parse()?;
                if marks.fifo.replace((fifo.value(), span)).is_some() {
                    return Err(meta.error("`fifo` is given twice"));
                }
            } else if let Some(role) = Role::from_attribute(&word) {
                if let Some((first, _)) = marks.role.replace((role, span)) {
                    return Err(meta.error(format!(
                        "this field already plays `{first}`: a field plays one role"
                    )));
                }
            } else {
                return Err(meta.error(format!(
                    "unknown `#[field(..)]` option: expected a role ({}), `generated` or `fifo`",
                    role_list()
                )));
            }
            Ok(())
        })?;
        if !named {
            return Err(syn::Error::new_spanned(
                attr,
                "`#[field(..)]` names nothing: give it a role, `generated`, or both",
            ));
        }
    }
    Ok(marks)
}

fn role_list() -> String {
    Role::ALL
        .iter()
        .map(|role| format!("`{role}`"))
        .collect::<Vec<_>>()
        .join(", ")
}
