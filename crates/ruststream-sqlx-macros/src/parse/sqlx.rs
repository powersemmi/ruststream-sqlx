//! sqlx's own attributes, as sqlx reads them: `rename_all` on the struct, and `rename`, `skip`,
//! `flatten`, `json` and `try_from` on a field.

use proc_macro2::TokenTree;
use syn::ext::IdentExt;
use syn::parse::ParseStream;
use syn::{Attribute, Ident, LitStr, Token, Type};

use crate::naming::RenameAll;

pub(super) fn rename_all(attrs: &[Attribute]) -> syn::Result<Option<RenameAll>> {
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
pub(super) struct SqlxField {
    pub(super) rename: Option<LitStr>,
    pub(super) skip: bool,
    pub(super) flatten: bool,
    pub(super) json: bool,
    pub(super) try_from: Option<Box<Type>>,
}

/// The column sqlx reads for a field: its `rename` as written, else its name without `r#`,
/// recased by the struct's `rename_all`.
pub(super) fn column_name(
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

pub(super) fn sqlx_field(attrs: &[Attribute]) -> syn::Result<SqlxField> {
    let mut sqlx = SqlxField::default();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("sqlx")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                sqlx.rename = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("skip") {
                sqlx.skip = true;
            } else if meta.path.is_ident("flatten") {
                sqlx.flatten = true;
            } else if meta.path.is_ident("json") {
                sqlx.json = true;
                skip_value(meta.input)?;
            } else if meta.path.is_ident("try_from") {
                let decoded: LitStr = meta.value()?.parse()?;
                sqlx.try_from = Some(Box::new(decoded.parse()?));
            } else {
                skip_value(meta.input)?;
            }
            Ok(())
        })?;
    }
    Ok(sqlx)
}

/// Consumes what follows a key of `#[sqlx(..)]` this derive does not read (`json(nullable)`,
/// `default`), so sqlx's own options pass through untouched.
fn skip_value(input: ParseStream<'_>) -> syn::Result<()> {
    while !input.is_empty() && !input.peek(Token![,]) {
        input.parse::<TokenTree>()?;
    }
    Ok(())
}
