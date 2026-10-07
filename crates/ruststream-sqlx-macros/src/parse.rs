//! Reads a struct deriving `Inbox`: its table, sqlx's naming attributes and the roles of its
//! fields.

use proc_macro2::Span;
use ruststream_sqlx_dialect::Role;
use syn::ext::IdentExt;
use syn::punctuated::Punctuated;
use syn::{Data, DeriveInput, Fields, Ident, Token, Type};

mod custom;
mod field;
mod sqlx;
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
mod tests;
