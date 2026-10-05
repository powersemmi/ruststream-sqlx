//! Reads a struct deriving `Inbox`: its table, sqlx's naming attributes and the roles of its
//! fields.

use proc_macro2::{Span, TokenTree};
use ruststream_sqlx_dialect::Role;
use syn::ext::IdentExt;
use syn::parse::ParseStream;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Fields, Ident, LitBool, LitStr, Token, Type};

use crate::naming::RenameAll;

/// `#[inbox(..)]`: the table, its schema and its advisory lock key.
pub(crate) struct Table {
    pub(crate) name: LitStr,
    pub(crate) schema: Option<LitStr>,
    pub(crate) advisory_lock: Option<LitStr>,
}

/// Where a field's value comes from.
pub(crate) enum Storage {
    /// A column of the table.
    Column(ColumnField),
    /// `#[sqlx(skip)]`: the field is not read from the row.
    Skipped,
    /// `#[sqlx(flatten)]`: another struct reads its own columns.
    Flattened,
}

/// A field that reads a column: the column's name in the database and what `#[field(..)]` says.
pub(crate) struct ColumnField {
    pub(crate) name: String,
    pub(crate) role: Option<Role>,
    pub(crate) generated: bool,
    /// Where `fifo = true` is written, on the field that plays `group`.
    pub(crate) fifo: Option<Span>,
}

/// One field and where its value comes from.
pub(crate) struct Field<'a> {
    pub(crate) ident: &'a Ident,
    pub(crate) ty: &'a Type,
    pub(crate) storage: Storage,
}

impl Field<'_> {
    /// The column the field reads, if it reads one.
    pub(crate) const fn column(&self) -> Option<&ColumnField> {
        match &self.storage {
            Storage::Column(column) => Some(column),
            Storage::Skipped | Storage::Flattened => None,
        }
    }
}

/// The struct as the derive reads it.
pub(crate) struct Inbox<'a> {
    pub(crate) table: Table,
    pub(crate) fields: Vec<Field<'a>>,
}

impl<'a> Inbox<'a> {
    /// The field that names the advisory lock key's placeholder, by its name in Rust.
    pub(crate) fn field_named(&self, name: &str) -> Option<&Field<'a>> {
        self.fields.iter().find(|field| field.ident.unraw() == name)
    }

    /// The fields that read a column, with their columns, in field order.
    pub(crate) fn columns(&self) -> impl Iterator<Item = (&Field<'a>, &ColumnField)> {
        self.fields
            .iter()
            .filter_map(|field| field.column().map(|column| (field, column)))
    }

    /// Whether a field flattens another struct, whose columns the derive cannot see.
    pub(crate) fn flattens(&self) -> bool {
        self.fields
            .iter()
            .any(|field| matches!(field.storage, Storage::Flattened))
    }
}

/// Reads the struct, reporting every field's problem at once.
pub(crate) fn inbox(input: &DeriveInput) -> syn::Result<Inbox<'_>> {
    let named = named_fields(input)?;
    let table = table(input)?;
    let rename_all = rename_all(&input.attrs)?;
    let mut fields = Vec::new();
    let mut errors: Option<syn::Error> = None;
    for field in named {
        match self::field(field, rename_all) {
            Ok(field) => fields.push(field),
            Err(error) => match &mut errors {
                Some(errors) => errors.combine(error),
                None => errors = Some(error),
            },
        }
    }
    if let Some(errors) = errors {
        return Err(errors);
    }
    Ok(Inbox { table, fields })
}

fn named_fields(input: &DeriveInput) -> syn::Result<&Punctuated<syn::Field, Token![,]>> {
    const MESSAGE: &str = "#[derive(Inbox)] describes a table: it takes a struct with named fields";
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => Ok(&fields.named),
            Fields::Unnamed(fields) => Err(syn::Error::new_spanned(fields, MESSAGE)),
            Fields::Unit => Err(syn::Error::new_spanned(&input.ident, MESSAGE)),
        },
        Data::Enum(_) | Data::Union(_) => Err(syn::Error::new_spanned(&input.ident, MESSAGE)),
    }
}

fn table(input: &DeriveInput) -> syn::Result<Table> {
    let mut name = None;
    let mut schema = None;
    let mut advisory_lock = None;
    for attr in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("inbox"))
    {
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            let slot = match key.as_str() {
                "table" => &mut name,
                "schema" => &mut schema,
                "advisory_lock" => &mut advisory_lock,
                _ => {
                    return Err(meta.error(
                        "unknown `#[inbox(..)]` option: expected `table`, `schema` or \
                         `advisory_lock`",
                    ));
                }
            };
            let value: LitStr = meta.value()?.parse()?;
            if value.value().is_empty() {
                return Err(syn::Error::new(value.span(), format!("`{key}` is empty")));
            }
            if slot.replace(value).is_some() {
                return Err(meta.error(format!("`{key}` is given twice")));
            }
            Ok(())
        })?;
    }
    let Some(name) = name else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Inbox)] needs the table: add `#[inbox(table = \"..\")]`",
        ));
    };
    Ok(Table {
        name,
        schema,
        advisory_lock,
    })
}

fn rename_all(attrs: &[Attribute]) -> syn::Result<Option<RenameAll>> {
    let mut casing = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("sqlx")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                casing = Some(RenameAll::parse(&meta.value()?.parse()?)?);
            } else {
                skip_value(meta.input)?;
            }
            Ok(())
        })?;
    }
    Ok(casing)
}

/// What `#[sqlx(..)]` says about one field.
#[derive(Default)]
struct SqlxField {
    rename: Option<LitStr>,
    skip: bool,
    flatten: bool,
}

/// What `#[field(..)]` says about one field, with the span of each word for errors.
#[derive(Default)]
struct Marks {
    role: Option<(Role, Span)>,
    generated: Option<Span>,
    fifo: Option<(bool, Span)>,
}

fn field(field: &syn::Field, rename_all: Option<RenameAll>) -> syn::Result<Field<'_>> {
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
        }),
    };
    Ok(Field {
        ident,
        ty: &field.ty,
        storage,
    })
}

/// The column sqlx reads for a field: its `rename` as written, else its name without `r#`,
/// recased by the struct's `rename_all`.
fn column_name(
    ident: &Ident,
    rename: Option<&LitStr>,
    rename_all: Option<RenameAll>,
) -> syn::Result<String> {
    if let Some(rename) = rename {
        let name = rename.value();
        if name.is_empty() {
            return Err(syn::Error::new(rename.span(), "the column name is empty"));
        }
        return Ok(name);
    }
    let name = ident.unraw().to_string();
    Ok(match rename_all {
        Some(casing) => casing.apply(&name),
        None => name,
    })
}

fn sqlx_field(attrs: &[Attribute]) -> syn::Result<SqlxField> {
    let mut sqlx = SqlxField::default();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("sqlx")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                sqlx.rename = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("skip") {
                sqlx.skip = true;
            } else if meta.path.is_ident("flatten") {
                sqlx.flatten = true;
            } else {
                skip_value(meta.input)?;
            }
            Ok(())
        })?;
    }
    Ok(sqlx)
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

/// Consumes what follows a key of `#[sqlx(..)]` this derive does not read (`json(nullable)`,
/// `try_from = "i64"`, `default`), so sqlx's own options pass through untouched.
fn skip_value(input: ParseStream<'_>) -> syn::Result<()> {
    while !input.is_empty() && !input.peek(Token![,]) {
        input.parse::<TokenTree>()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ruststream_sqlx_dialect::Role;
    use syn::{DeriveInput, parse_quote};

    use super::{Inbox, Storage, inbox};

    fn columns(inbox: &Inbox<'_>) -> Vec<Option<String>> {
        inbox
            .fields
            .iter()
            .map(|field| field.column().map(|column| column.name.clone()))
            .collect()
    }

    fn error(input: &DeriveInput) -> String {
        inbox(input).map_or_else(|error| error.to_string(), |_| String::new())
    }

    fn errors(input: &DeriveInput) -> Vec<String> {
        inbox(input).map_or_else(
            |error| error.into_iter().map(|error| error.to_string()).collect(),
            |_| Vec::new(),
        )
    }

    #[test]
    fn the_table_attribute_names_table_schema_and_key() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "email_jobs", schema = "app", advisory_lock = "jobs-{job_id}")]
            struct SendEmail {
                #[field(id)]
                job_id: i64,
            }
        };
        let inbox = inbox(&input)?;
        assert_eq!(inbox.table.name.value(), "email_jobs");
        assert_eq!(
            inbox.table.schema.map(|schema| schema.value()),
            Some("app".to_owned())
        );
        assert_eq!(
            inbox.table.advisory_lock.map(|key| key.value()),
            Some("jobs-{job_id}".to_owned())
        );
        Ok(())
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
    fn column_names_follow_sqlx() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "email_jobs")]
            #[sqlx(rename_all = "camelCase")]
            struct SendEmail {
                #[field(id)]
                job_id: i64,
                #[sqlx(rename = "queue_name")]
                #[field(group)]
                group_name: String,
                r#type: String,
                #[sqlx(skip)]
                cache: Vec<u8>,
                #[sqlx(flatten)]
                extra: Extra,
                #[sqlx(json(nullable), try_from = "i64", default)]
                attachments: Vec<String>,
            }
        };
        assert_eq!(
            columns(&inbox(&input)?),
            [
                Some("jobId".to_owned()),
                Some("queue_name".to_owned()),
                Some("type".to_owned()),
                None,
                None,
                Some("attachments".to_owned()),
            ]
        );
        let inbox = inbox(&input)?;
        assert!(matches!(inbox.fields[3].storage, Storage::Skipped));
        assert!(matches!(inbox.fields[4].storage, Storage::Flattened));
        assert!(inbox.flattens());
        Ok(())
    }

    #[test]
    fn a_placeholder_finds_a_raw_field_by_its_plain_name() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", advisory_lock = "jobs-{type}")]
            struct Job {
                #[field(id)]
                id: i64,
                r#type: String,
            }
        };
        let inbox = inbox(&input)?;
        assert_eq!(
            inbox
                .field_named("type")
                .and_then(|field| field.column())
                .map(|column| column.name.as_str()),
            Some("type")
        );
        assert!(inbox.field_named("kind").is_none());
        Ok(())
    }

    #[test]
    fn misuse_of_the_attributes_is_reported() {
        let cases: [(DeriveInput, &str); 15] = [
            (
                parse_quote! { struct Job { #[field(id)] id: i64 } },
                "#[derive(Inbox)] needs the table: add `#[inbox(table = \"..\")]`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", queue = "emails")] struct Job { #[field(id)] id: i64 } },
                "unknown `#[inbox(..)]` option: expected `table`, `schema` or `advisory_lock`",
            ),
            (
                parse_quote! { #[inbox(table = "")] struct Job { #[field(id)] id: i64 } },
                "`table` is empty",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(identity)] id: i64 } },
                "unknown `#[field(..)]` option: expected a role (`id`, `group`, `partition_key`, \
                 `priority`, `retry_after`, `attempt`, `locked_until`, `processed_at`, \
                 `headers`, `payload`), `generated` or `fifo`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id, group)] id: i64 } },
                "this field already plays `id`: a field plays one role",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id, fifo = true)] id: i64 } },
                "`fifo` belongs to the `group` role: `#[field(group, fifo = true)]`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] #[sqlx(skip)] id: i64 } },
                "`#[sqlx(skip)]` leaves `id` without a column, so it cannot play `id`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job(i64); },
                "#[derive(Inbox)] describes a table: it takes a struct with named fields",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] #[inbox(table = "jobs_v2")] struct Job { #[field(id)] id: i64 } },
                "`table` is given twice",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] #[sqlx(rename = "")] id: i64 } },
                "the column name is empty",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] id: i64, #[field(headers)] #[sqlx(flatten)] headers: Headers } },
                "`#[sqlx(flatten)]` leaves `headers` without a column, so it cannot play `headers`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] id: i64, #[field(generated)] #[sqlx(skip)] created_at: i64 } },
                "`#[sqlx(skip)]` leaves `created_at` without a column for the database to fill in",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id, generated)] #[field(generated)] id: i64 } },
                "`generated` is given twice",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] id: i64, #[field(group, fifo = true, fifo = true)] name: String } },
                "`fifo` is given twice",
            ),
            (
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] id: i64, #[field()] payload: Vec<u8> } },
                "`#[field(..)]` names nothing: give it a role, `generated`, or both",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(error(&input), expected);
        }
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
}
