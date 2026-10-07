//! `#[inbox(..)]` on the struct: the table, its schema, its advisory lock key, what its
//! transactions open at, the events the service implements itself and the clock.

use proc_macro2::{TokenStream as TokenStream2, TokenTree};
use ruststream_sqlx_dialect::{Isolation, Mode, Opening};
use syn::ext::IdentExt;
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{DeriveInput, LitStr, Token};

use super::custom::Custom;

/// `#[inbox(..)]`: the table, its schema, its advisory lock key and what its transactions open at.
pub(crate) struct Table {
    pub(crate) name: LitStr,
    pub(crate) schema: Option<LitStr>,
    pub(crate) advisory_lock: Option<LitStr>,
    /// `isolation = ..` or `mode = ..`; [`Opening::Default`] where the struct names neither.
    pub(crate) opening: Opening,
    pub(crate) custom: Custom,
    pub(crate) clock: Option<syn::Path>,
}

/// The isolation levels `isolation = ..` names, each read as [`Isolation::attribute`] spells it.
const ISOLATIONS: [Isolation; 4] = [
    Isolation::ReadUncommitted,
    Isolation::ReadCommitted,
    Isolation::RepeatableRead,
    Isolation::Serializable,
];

/// The SQLite modes `mode = ..` names, each read as [`Mode::attribute`] spells it.
const MODES: [Mode; 3] = [Mode::Deferred, Mode::Immediate, Mode::Exclusive];

/// Reads `isolation = <level>` or `mode = <mode>` into `opening`, which holds what the attributes
/// read before it. Every refusal points at the value.
fn opening(key: &str, meta: &ParseNestedMeta<'_>, opening: &mut Opening) -> syn::Result<()> {
    let (word, value) = word(meta)?;
    let refused = |message: &str| {
        value.as_ref().map_or_else(
            || meta.error(message),
            |value| syn::Error::new_spanned(value, message),
        )
    };
    let word = word.as_deref();
    let read = if key == "isolation" {
        ISOLATIONS
            .into_iter()
            .find(|level| Some(level.attribute()) == word)
            .map(Opening::Isolation)
            .ok_or_else(|| {
                refused(
                    "unknown isolation level: expected `read_uncommitted`, `read_committed`, \
                     `repeatable_read` or `serializable`",
                )
            })?
    } else {
        MODES
            .into_iter()
            .find(|mode| Some(mode.attribute()) == word)
            .map(Opening::Mode)
            .ok_or_else(|| {
                refused("unknown mode: expected `deferred`, `immediate` or `exclusive` (SQLite)")
            })?
    };
    match (*opening, read) {
        (Opening::Default, _) => {
            *opening = read;
            Ok(())
        }
        (Opening::Isolation(_), Opening::Isolation(_)) | (Opening::Mode(_), Opening::Mode(_)) => {
            Err(refused(&format!("`{key}` is given twice")))
        }
        _ => Err(refused(
            "a table declares `isolation` (Postgres, MySQL, MariaDB) or `mode` (SQLite), not both",
        )),
    }
}

/// The value after `=`, up to the next comma: the word it is, where it is one, and its tokens,
/// where it has any, for an error to point at.
fn word(meta: &ParseNestedMeta<'_>) -> syn::Result<(Option<String>, Option<TokenStream2>)> {
    let input = meta.value()?;
    let mut value = TokenStream2::new();
    while !input.is_empty() && !input.peek(Token![,]) {
        value.extend([input.parse::<TokenTree>()?]);
    }
    let mut tokens = value.clone().into_iter();
    let word = match (tokens.next(), tokens.next()) {
        (Some(TokenTree::Ident(word)), None) => Some(word.unraw().to_string()),
        _ => None,
    };
    Ok((word, (!value.is_empty()).then_some(value)))
}

pub(super) fn table(input: &DeriveInput, derive: &str) -> syn::Result<Table> {
    // A dot would read as a schema in one place and as part of a quoted name in another (a
    // dead-letter `TableName` splits on it), so `table` and `schema` each name one thing.
    const DOTTED_TABLE: &str =
        "`table` holds a dot: name the table alone, and its schema with `schema = \"..\"`";
    const DOTTED_SCHEMA: &str =
        "`schema` holds a dot: name the schema alone, without its database or table";
    let mut name = None;
    let mut schema = None;
    let mut advisory_lock = None;
    let mut opening = Opening::Default;
    let mut custom = Custom::default();
    let mut custom_seen = false;
    let mut clock = None;
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
            if key == "custom" {
                if custom_seen {
                    return Err(meta.error("`custom` is given twice"));
                }
                custom_seen = true;
                custom.listed = Some(meta.path.span());
                return meta.parse_nested_meta(|event| custom.list(&event));
            }
            if key == "clock" {
                let path: syn::Path = meta.value()?.parse()?;
                if clock.replace(path).is_some() {
                    return Err(meta.error("`clock` is given twice"));
                }
                return Ok(());
            }
            if key == "isolation" || key == "mode" {
                return self::opening(&key, &meta, &mut opening);
            }
            let (slot, dotted) = match key.as_str() {
                "table" => (&mut name, Some(DOTTED_TABLE)),
                "schema" => (&mut schema, Some(DOTTED_SCHEMA)),
                "advisory_lock" => (&mut advisory_lock, None),
                _ => {
                    return Err(meta.error(
                        "unknown `#[inbox(..)]` option: expected `table`, `schema`, \
                         `advisory_lock`, `isolation`, `mode`, `custom` or `clock`",
                    ));
                }
            };
            let value: LitStr = meta.value()?.parse()?;
            if value.value().is_empty() {
                return Err(syn::Error::new(value.span(), format!("`{key}` is empty")));
            }
            if let Some(message) = dotted
                && value.value().contains('.')
            {
                return Err(syn::Error::new(value.span(), message));
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
            format!("#[derive({derive})] needs the table: add `#[inbox(table = \"..\")]`"),
        ));
    };
    Ok(Table {
        name,
        schema,
        advisory_lock,
        opening,
        custom,
        clock,
    })
}

/// `#[inbox(..)]` on a message assembled from a headers struct: `custom(..)` alone, since the
/// headers struct `headers` names describes the table. Every other option is refused where it is
/// written.
pub(crate) fn message_custom(input: &DeriveInput, headers: &str) -> syn::Result<Custom> {
    let mut custom = Custom::default();
    let mut errors: Option<syn::Error> = None;
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
            if key == "custom" {
                if custom.listed.is_some() {
                    return Err(meta.error("`custom` is given twice"));
                }
                custom.listed = Some(meta.path.span());
                return meta.parse_nested_meta(|event| custom.list(&event));
            }
            let error = meta.error(format!(
                "`{key}` describes the queue table, which the headers struct `{headers}` \
                 describes: put it on `{headers}`'s `#[inbox(..)]`"
            ));
            match &mut errors {
                Some(errors) => errors.combine(error),
                None => errors = Some(error),
            }
            // The value is read past, so the next option is read in turn.
            if meta.input.peek(Token![=]) {
                word(&meta)?;
            } else if !meta.input.is_empty() && !meta.input.peek(Token![,]) {
                meta.parse_nested_meta(|_| Ok(())).ok();
            }
            Ok(())
        })?;
    }
    errors.map_or(Ok(custom), Err)
}
