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
    // A flattened field that plays `headers` holds a headers struct: the headers layout.
    let headers_struct = sqlx.flatten && matches!(marks.role, Some((Role::Headers, _)));
    if headers_struct {
        if let Some(span) = marks.generated {
            return Err(syn::Error::new(
                span,
                format!(
                    "`{ident}` holds a headers struct, which describes the queue table: mark its \
                     generated columns there"
                ),
            ));
        }
        return Ok(Field {
            ident,
            ty: &field.ty,
            storage: Storage::Headers,
        });
    }
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

#[cfg(test)]
mod tests {
    use ruststream_sqlx_dialect::Role;
    use syn::{DeriveInput, parse_quote};

    use crate::parse::tests::error;
    use crate::parse::{Storage, fields, inbox};

    fn errors(input: &DeriveInput) -> Vec<String> {
        inbox(input).map_or_else(
            |error| error.into_iter().map(|error| error.to_string()).collect(),
            |_| Vec::new(),
        )
    }

    #[test]
    fn roles_and_modifiers_land_on_their_fields() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "email_jobs")]
            struct SendEmail {
                #[field(id, generated)]
                job_id: i64,
                #[field(group, fifo = true)]
                name: String,
                #[field(generated)]
                created_at: i64,
                subject: String,
            }
        };
        let inbox = inbox(&input)?;
        let roles: Vec<_> = inbox.columns().map(|(_, column)| column.role).collect();
        assert_eq!(roles, [Some(Role::Id), Some(Role::Group), None, None]);
        let generated: Vec<_> = inbox
            .columns()
            .map(|(_, column)| column.generated)
            .collect();
        assert_eq!(generated, [true, false, true, false]);
        let fifo: Vec<_> = inbox
            .columns()
            .map(|(_, column)| column.fifo.is_some())
            .collect();
        assert_eq!(fifo, [false, true, false, false]);
        Ok(())
    }

    #[test]
    fn every_field_reports_its_own_problem() {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs")]
            struct Job {
                #[field(identity)]
                id: i64,
                #[field(priority, fifo = true)]
                priority: i16,
            }
        };
        assert_eq!(
            errors(&input),
            [
                "unknown `#[field(..)]` option: expected a role (`id`, `group`, `partition_key`, \
                 `priority`, `retry_after`, `attempt`, `locked_until`, `processed_at`, \
                 `headers`, `payload`), `generated` or `fifo`",
                "`fifo` belongs to the `group` role: `#[field(group, fifo = true)]`",
            ]
        );
    }

    #[test]
    fn a_flattened_headers_field_holds_a_headers_struct() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            struct OrderJob {
                #[field(headers)]
                #[sqlx(flatten)]
                headers: OrderHeaders,
                note: Option<String>,
            }
        };
        let fields = fields(&input)?;
        assert!(matches!(fields[0].storage, Storage::Headers));
        assert_eq!(
            fields[1].column().map(|column| column.name.as_str()),
            Some("note")
        );
        let generated: DeriveInput = parse_quote! {
            #[inbox(table = "jobs")]
            struct Job { #[field(headers, generated)] #[sqlx(flatten)] headers: Headers }
        };
        assert_eq!(
            error(&generated),
            "`headers` holds a headers struct, which describes the queue table: mark its generated \
             columns there"
        );
        Ok(())
    }
}
