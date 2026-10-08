//! Reads a struct deriving `Inbox`: its table, sqlx's naming attributes and the roles of its
//! fields.

use proc_macro2::Span;
use ruststream_sqlx_dialect::Role;
use syn::ext::IdentExt;
use syn::punctuated::Punctuated;
use syn::{Data, DeriveInput, Fields, Ident, Token, Type};

mod custom;
mod field;
pub(crate) mod sqlx;
mod table;

pub(crate) use custom::Custom;
use field::field;
use sqlx::rename_all;
pub(crate) use table::message_custom;
use table::{Table, table};

/// Where a field's value comes from.
pub(crate) enum Storage {
    /// A column of the table.
    Column(ColumnField),
    /// `#[sqlx(skip)]`: the field is not read from the row.
    Skipped,
    /// `#[sqlx(flatten)]`: another struct reads its own columns.
    Flattened,
    /// `#[field(headers)] #[sqlx(flatten)]`: a headers struct describes the queue table, and this
    /// struct is a message assembled from it.
    Headers,
}

/// A field that reads a column: the column's name in the database and what `#[field(..)]` says.
pub(crate) struct ColumnField {
    pub(crate) name: String,
    pub(crate) role: Option<Role>,
    pub(crate) generated: bool,
    /// Where `fifo = true` is written, on the field that plays `group`.
    pub(crate) fifo: Option<Span>,
    /// `#[sqlx(json)]`: sqlx reads and writes the column through `Json`.
    pub(crate) json: bool,
    /// `#[sqlx(try_from = "..")]`: the type sqlx decodes the column as, then converts into the
    /// field's.
    // Boxed: a type is large, and every field's storage would carry its size.
    pub(crate) try_from: Option<Box<Type>>,
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
            Storage::Skipped | Storage::Flattened | Storage::Headers => None,
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
            .any(|field| matches!(field.storage, Storage::Flattened | Storage::Headers))
    }
}

/// The field that plays `role`, if one does.
pub(crate) fn playing<'i, 'a>(inbox: &'i Inbox<'a>, role: Role) -> Option<&'i Field<'a>> {
    inbox
        .columns()
        .find(|(_, column)| column.role == Some(role))
        .map(|(field, _)| field)
}

/// Reads the struct, reporting every field's problem at once.
pub(crate) fn inbox(input: &DeriveInput) -> syn::Result<Inbox<'_>> {
    inbox_of(input, "Inbox")
}

/// Reads a struct deriving `InboxHeaders`, as [`inbox`] reads one deriving `Inbox`.
pub(crate) fn headers_table(input: &DeriveInput) -> syn::Result<Inbox<'_>> {
    inbox_of(input, "InboxHeaders")
}

fn inbox_of<'a>(input: &'a DeriveInput, derive: &str) -> syn::Result<Inbox<'a>> {
    let named = named_fields(input, derive)?;
    let table = table(input, derive)?;
    let fields = read_fields(input, named)?;
    Ok(Inbox { table, fields })
}

/// Reads the struct's fields alone, reporting every field's problem at once.
pub(crate) fn fields(input: &DeriveInput) -> syn::Result<Vec<Field<'_>>> {
    read_fields(input, named_fields(input, "Inbox")?)
}

fn read_fields<'a>(
    input: &'a DeriveInput,
    named: &'a Punctuated<syn::Field, Token![,]>,
) -> syn::Result<Vec<Field<'a>>> {
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
    Ok(fields)
}

fn named_fields<'a>(
    input: &'a DeriveInput,
    derive: &str,
) -> syn::Result<&'a Punctuated<syn::Field, Token![,]>> {
    let message =
        format!("#[derive({derive})] describes a table: it takes a struct with named fields");
    let message = message.as_str();
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => Ok(&fields.named),
            Fields::Unnamed(fields) => Err(syn::Error::new_spanned(fields, message)),
            Fields::Unit => Err(syn::Error::new_spanned(&input.ident, message)),
        },
        Data::Enum(_) | Data::Union(_) => Err(syn::Error::new_spanned(&input.ident, message)),
    }
}

#[cfg(test)]
mod tests {
    use syn::{DeriveInput, parse_quote};

    use super::inbox;

    pub(super) fn error(input: &DeriveInput) -> String {
        inbox(input).map_or_else(|error| error.to_string(), |_| String::new())
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
        let cases: [(DeriveInput, &str); 22] = [
            (
                parse_quote! { struct Job { #[field(id)] id: i64 } },
                "#[derive(Inbox)] needs the table: add `#[inbox(table = \"..\")]`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", queue = "emails")] struct Job { #[field(id)] id: i64 } },
                "unknown `#[inbox(..)]` option: expected `table`, `schema`, `advisory_lock`, \
                 `isolation`, `mode`, `custom` or `clock`",
            ),
            (
                parse_quote! { #[inbox(table = "")] struct Job { #[field(id)] id: i64 } },
                "`table` is empty",
            ),
            (
                parse_quote! { #[inbox(table = "app.jobs")] struct Job { #[field(id)] id: i64 } },
                "`table` holds a dot: name the table alone, and its schema with `schema = \"..\"`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", schema = "db.app")] struct Job { #[field(id)] id: i64 } },
                "`schema` holds a dot: name the schema alone, without its database or table",
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
                parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] id: i64, #[field(payload)] #[sqlx(flatten)] body: Body } },
                "`#[sqlx(flatten)]` leaves `body` without a column, so it cannot play `payload`",
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
            (
                parse_quote! { #[inbox(table = "jobs", custom(lease))] struct Job { #[field(id)] id: i64 } },
                "unknown event in `custom(..)`: expected `claim`, `fetch`, `ack`, `retry`, \
                 `retry_after`, `discard`, `dead_letter`, `extend`, `lock` or `unlock`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", custom(publish))] struct Job { #[field(id)] id: i64 } },
                "`publish` has no default to hand over: implement `Publish` for the struct \
                 without listing it",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", custom(ack, ack))] struct Job { #[field(id)] id: i64 } },
                "`ack` is listed twice",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", custom(ack), custom(fetch))] struct Job { #[field(id)] id: i64 } },
                "`custom` is given twice",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", clock = A, clock = B)] struct Job { #[field(id)] id: i64 } },
                "`clock` is given twice",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(error(&input), expected);
        }
    }
}
