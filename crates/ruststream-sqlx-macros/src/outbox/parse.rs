//! Reads a struct deriving `Outbox`: `#[outbox(..)]`, sqlx's naming attributes and the roles of
//! its fields. The outbox has its own roles, so its field reader is its own; the column names come
//! from sqlx's attributes as the inbox derive reads them.

use std::fmt;

use proc_macro2::Span;
use syn::ext::IdentExt;
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Fields, Ident, LitStr, Type};

use crate::check::Errors;
use crate::naming::RenameAll;
use crate::parse::sqlx::{column_name, rename_all, sqlx_field};

/// The role a field of an outbox record plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutboxRole {
    /// The record's identity: the value the id header carries.
    Id,
    /// The name the record was published under.
    Name,
    /// The published bytes.
    Payload,
    /// The published headers.
    Headers,
    /// The time the record was processed; the mark sets it instead of deleting the record.
    ProcessedAt,
}

impl OutboxRole {
    /// Every role, in the order the errors list them.
    const ALL: [Self; 5] = [
        Self::Id,
        Self::Name,
        Self::Payload,
        Self::Headers,
        Self::ProcessedAt,
    ];

    /// The role's name inside `#[field(..)]`.
    const fn attribute(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
            Self::Payload => "payload",
            Self::Headers => "headers",
            Self::ProcessedAt => "processed_at",
        }
    }
}

impl fmt::Display for OutboxRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.attribute())
    }
}

/// The events of `#[outbox(custom(..))]`: the ones the service implements itself.
// One switch per event the crate can hand over.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct OutboxCustom {
    pub(crate) fetch: bool,
    pub(crate) ack: bool,
    pub(crate) retry: bool,
    pub(crate) discard: bool,
    pub(crate) recover: bool,
}

impl OutboxCustom {
    /// Reads one event of `custom(..)`.
    fn list(&mut self, event: &ParseNestedMeta<'_>) -> syn::Result<()> {
        let word = event
            .path
            .get_ident()
            .map(ToString::to_string)
            .unwrap_or_default();
        if word == "publish" {
            return Err(event.error(
                "`publish` has no default to hand over: implement `outbox::Publish` for the \
                 record without listing it",
            ));
        }
        let slot = match word.as_str() {
            "fetch" => &mut self.fetch,
            "ack" => &mut self.ack,
            "retry" => &mut self.retry,
            "discard" => &mut self.discard,
            "recover" => &mut self.recover,
            _ => {
                return Err(event.error(
                    "unknown event in `custom(..)`: expected `fetch`, `ack`, `retry`, `discard` \
                     or `recover`",
                ));
            }
        };
        if std::mem::replace(slot, true) {
            return Err(event.error(format!("`{word}` is listed twice")));
        }
        Ok(())
    }
}

/// `#[outbox(..)]`: the table, its schema and the events the service implements itself.
pub(crate) struct OutboxTable {
    pub(crate) name: LitStr,
    pub(crate) schema: Option<LitStr>,
    pub(crate) custom: OutboxCustom,
}

/// A field that reads a column.
pub(crate) struct OutboxColumn {
    pub(crate) name: String,
    pub(crate) role: Option<OutboxRole>,
    /// `#[sqlx(json)]`: sqlx reads and writes the column through `Json`.
    pub(crate) json: bool,
}

/// Where a field's value comes from.
pub(crate) enum OutboxStorage {
    /// A column of the table.
    Column(OutboxColumn),
    /// `#[sqlx(skip)]`: the field reads no column.
    Skipped,
    /// `#[sqlx(flatten)]`: another struct reads its own columns, which the derive cannot see.
    Flattened,
}

/// One field and where its value comes from.
pub(crate) struct OutboxField<'a> {
    pub(crate) ident: &'a Ident,
    pub(crate) ty: &'a Type,
    pub(crate) storage: OutboxStorage,
}

/// The struct as the derive reads it.
pub(crate) struct Record<'a> {
    pub(crate) table: OutboxTable,
    pub(crate) fields: Vec<OutboxField<'a>>,
}

impl<'a> Record<'a> {
    /// The fields that read a column, with their columns, in field order.
    pub(crate) fn columns(&self) -> impl Iterator<Item = (&OutboxField<'a>, &OutboxColumn)> {
        self.fields.iter().filter_map(|field| match &field.storage {
            OutboxStorage::Column(column) => Some((field, column)),
            OutboxStorage::Skipped | OutboxStorage::Flattened => None,
        })
    }

    /// The field that plays `role`, with its column, if one does.
    pub(crate) fn playing(&self, role: OutboxRole) -> Option<(&OutboxField<'a>, &OutboxColumn)> {
        self.columns().find(|(_, column)| column.role == Some(role))
    }

    /// The field that plays `role`, one of the roles [`record`] requires.
    ///
    /// # Panics
    ///
    /// Panics for a role nothing plays: [`record`] refuses a struct without `id`, `name` and
    /// `payload`, so only another role can be missing.
    pub(crate) fn required(&self, role: OutboxRole) -> (&OutboxField<'a>, &OutboxColumn) {
        self.playing(role)
            .expect("`record` refuses a struct without its required roles")
    }

    /// Whether a field flattens another struct, whose columns the derive cannot see.
    pub(crate) fn flattens(&self) -> bool {
        self.fields
            .iter()
            .any(|field| matches!(field.storage, OutboxStorage::Flattened))
    }
}

/// Reads the struct and checks it, reporting every problem at once.
pub(crate) fn record(input: &DeriveInput) -> syn::Result<Record<'_>> {
    const NAMED: &str = "#[derive(Outbox)] describes a table: it takes a struct with named fields";
    let named = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(fields) => return Err(syn::Error::new_spanned(fields, NAMED)),
            Fields::Unit => return Err(syn::Error::new_spanned(&input.ident, NAMED)),
        },
        Data::Enum(_) | Data::Union(_) => {
            return Err(syn::Error::new_spanned(&input.ident, NAMED));
        }
    };
    let table = table(input)?;
    let casing = rename_all(&input.attrs)?;
    let mut errors = Errors::default();
    let mut fields = Vec::new();
    for field in named {
        match self::field(field, casing) {
            Ok(field) => fields.push(field),
            Err(error) => errors.push(error),
        }
    }
    errors.finish()?;
    let record = Record { table, fields };
    check(input, &record)?;
    Ok(record)
}

/// The rules beyond one field: the required roles, one field per role, one field per column.
fn check(input: &DeriveInput, record: &Record<'_>) -> syn::Result<()> {
    let table = record.table.name.value();
    let mut errors = Errors::default();
    let required = [
        (OutboxRole::Id, "identifies a record"),
        (
            OutboxRole::Name,
            "holds the name a record was published under",
        ),
        (OutboxRole::Payload, "holds the published bytes"),
    ];
    for (role, what) in required {
        if record.playing(role).is_none() {
            errors.push(syn::Error::new(
                input.ident.span(),
                format!(
                    "table `{table}` has no `{role}` field: mark the field that {what} with \
                     `#[field({role})]`"
                ),
            ));
        }
    }
    let columns: Vec<_> = record.columns().collect();
    for (index, (field, column)) in columns.iter().enumerate() {
        let earlier = &columns[..index];
        if let Some(role) = column.role
            && let Some((first, _)) = earlier.iter().find(|(_, other)| other.role == Some(role))
        {
            errors.push(syn::Error::new(
                field.ident.span(),
                format!(
                    "`{}` plays `{role}`, which `{}` plays already: a role is played by one field",
                    field.ident.unraw(),
                    first.ident.unraw()
                ),
            ));
        }
        if let Some((first, _)) = earlier.iter().find(|(_, other)| other.name == column.name) {
            errors.push(syn::Error::new(
                field.ident.span(),
                format!(
                    "`{}` reads column `{}`, which `{}` reads already",
                    field.ident.unraw(),
                    column.name,
                    first.ident.unraw()
                ),
            ));
        }
    }
    errors.finish()
}

fn table(input: &DeriveInput) -> syn::Result<OutboxTable> {
    // A dot would read as a schema, so `table` and `schema` each name one thing.
    const DOTTED_TABLE: &str =
        "`table` holds a dot: name the table alone, and its schema with `schema = \"..\"`";
    const DOTTED_SCHEMA: &str =
        "`schema` holds a dot: name the schema alone, without its database or table";
    let mut name = None;
    let mut schema = None;
    let mut custom = OutboxCustom::default();
    let mut custom_seen = false;
    for attr in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("outbox"))
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
                return meta.parse_nested_meta(|event| custom.list(&event));
            }
            let (slot, dotted) = match key.as_str() {
                "table" => (&mut name, DOTTED_TABLE),
                "schema" => (&mut schema, DOTTED_SCHEMA),
                _ => {
                    return Err(meta.error(
                        "unknown `#[outbox(..)]` option: expected `table`, `schema` or `custom`",
                    ));
                }
            };
            let value: LitStr = meta.value()?.parse()?;
            if value.value().is_empty() {
                return Err(syn::Error::new(value.span(), format!("`{key}` is empty")));
            }
            if value.value().contains('.') {
                return Err(syn::Error::new(value.span(), dotted));
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
            "#[derive(Outbox)] needs the table: add `#[outbox(table = \"..\")]`",
        ));
    };
    Ok(OutboxTable {
        name,
        schema,
        custom,
    })
}

fn field(field: &syn::Field, casing: Option<RenameAll>) -> syn::Result<OutboxField<'_>> {
    let ident = field.ident.as_ref().expect("a named field has a name");
    let sqlx = sqlx_field(&field.attrs)?;
    let role = role(&field.attrs)?;
    let without_column = if sqlx.skip {
        Some(("`#[sqlx(skip)]`", OutboxStorage::Skipped))
    } else if sqlx.flatten {
        Some(("`#[sqlx(flatten)]`", OutboxStorage::Flattened))
    } else {
        None
    };
    let storage = match without_column {
        Some((attribute, storage)) => {
            if let Some((role, span)) = role {
                return Err(syn::Error::new(
                    span,
                    format!(
                        "{attribute} leaves `{}` without a column, so it cannot play `{role}`",
                        ident.unraw()
                    ),
                ));
            }
            storage
        }
        None => OutboxStorage::Column(OutboxColumn {
            name: column_name(ident, sqlx.rename.as_ref(), casing)?,
            role: role.map(|(role, _)| role),
            json: sqlx.json,
        }),
    };
    Ok(OutboxField {
        ident,
        ty: &field.ty,
        storage,
    })
}

/// The role `#[field(..)]` gives a field, with where it is written.
fn role(attrs: &[Attribute]) -> syn::Result<Option<(OutboxRole, Span)>> {
    let mut role: Option<(OutboxRole, Span)> = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("field")) {
        let mut named = false;
        attr.parse_nested_meta(|meta| {
            named = true;
            let word = meta
                .path
                .get_ident()
                .map(|ident| ident.unraw().to_string())
                .unwrap_or_default();
            let Some(read) = OutboxRole::ALL
                .into_iter()
                .find(|role| role.attribute() == word)
            else {
                return Err(meta.error(
                    "unknown role of an outbox record: expected `id`, `name`, `payload`, \
                     `headers` or `processed_at`",
                ));
            };
            if let Some((first, _)) = role.replace((read, meta.path.span())) {
                return Err(meta.error(format!(
                    "this field already plays `{first}`: a field plays one role"
                )));
            }
            Ok(())
        })?;
        if !named {
            return Err(syn::Error::new_spanned(
                attr,
                "`#[field(..)]` names nothing: give it a role",
            ));
        }
    }
    Ok(role)
}

#[cfg(test)]
mod tests {
    use syn::{DeriveInput, parse_quote};

    use super::{OutboxRole, record};

    fn errors(input: &DeriveInput) -> Vec<String> {
        record(input).map_or_else(
            |error| error.into_iter().map(|error| error.to_string()).collect(),
            |_| Vec::new(),
        )
    }

    #[test]
    fn the_roles_land_on_their_fields_and_name_their_columns() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox", schema = "app")]
            #[sqlx(rename_all = "camelCase")]
            struct Order {
                #[field(id)]
                record_id: i64,
                #[field(name)]
                #[sqlx(rename = "channel")]
                name: String,
                #[field(payload)]
                payload: Vec<u8>,
                #[field(headers)]
                headers: Option<String>,
                #[field(processed_at)]
                processed_at: Option<String>,
                created_at: String,
                #[sqlx(skip)]
                note: String,
            }
        };
        let record = record(&input)?;
        assert_eq!(record.table.name.value(), "outbox");
        assert_eq!(
            record.table.schema.as_ref().map(syn::LitStr::value),
            Some("app".to_owned())
        );
        let columns: Vec<_> = record
            .columns()
            .map(|(field, column)| (field.ident.to_string(), column.name.clone(), column.role))
            .collect();
        assert_eq!(
            columns,
            [
                (
                    "record_id".to_owned(),
                    "recordId".to_owned(),
                    Some(OutboxRole::Id)
                ),
                (
                    "name".to_owned(),
                    "channel".to_owned(),
                    Some(OutboxRole::Name)
                ),
                (
                    "payload".to_owned(),
                    "payload".to_owned(),
                    Some(OutboxRole::Payload)
                ),
                (
                    "headers".to_owned(),
                    "headers".to_owned(),
                    Some(OutboxRole::Headers)
                ),
                (
                    "processed_at".to_owned(),
                    "processedAt".to_owned(),
                    Some(OutboxRole::ProcessedAt)
                ),
                ("created_at".to_owned(), "createdAt".to_owned(), None),
            ]
        );
        assert!(!record.flattens());
        Ok(())
    }

    #[test]
    fn custom_lists_the_events_the_service_writes() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox", custom(fetch, ack, retry, discard, recover))]
            struct Order {
                #[field(id)]
                id: i64,
                #[field(name)]
                name: String,
                #[field(payload)]
                payload: Vec<u8>,
            }
        };
        let custom = record(&input)?.table.custom;
        assert!(custom.fetch && custom.ack && custom.retry && custom.discard && custom.recover);
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox", custom(retry))]
            struct Order {
                #[field(id)]
                id: i64,
                #[field(name)]
                name: String,
                #[field(payload)]
                payload: Vec<u8>,
            }
        };
        let custom = record(&input)?.table.custom;
        assert!(custom.retry && !custom.fetch && !custom.ack && !custom.discard && !custom.recover);
        Ok(())
    }

    #[test]
    fn a_record_without_its_required_roles_names_each() {
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox")]
            struct Order {
                headers: Option<String>,
            }
        };
        assert_eq!(
            errors(&input),
            [
                "table `outbox` has no `id` field: mark the field that identifies a record with \
                 `#[field(id)]`",
                "table `outbox` has no `name` field: mark the field that holds the name a record \
                 was published under with `#[field(name)]`",
                "table `outbox` has no `payload` field: mark the field that holds the published \
                 bytes with `#[field(payload)]`",
            ]
        );
    }

    #[test]
    fn a_role_is_played_once() {
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox")]
            struct Order {
                #[field(id)]
                id: i64,
                #[field(payload, id)]
                payload: Vec<u8>,
            }
        };
        assert_eq!(
            errors(&input),
            ["this field already plays `payload`: a field plays one role"]
        );
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox")]
            struct Order {
                #[field(id)]
                id: i64,
                #[field(name)]
                name: String,
                #[field(name)]
                channel: String,
                #[field(payload)]
                payload: Vec<u8>,
            }
        };
        assert_eq!(
            errors(&input),
            ["`channel` plays `name`, which `name` plays already: a role is played by one field"]
        );
    }

    #[test]
    fn an_unknown_role_lists_the_outboxs_roles() {
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox")]
            struct Order {
                #[field(id)]
                id: i64,
                #[field(retry_after)]
                due: String,
            }
        };
        assert_eq!(
            errors(&input),
            [
                "unknown role of an outbox record: expected `id`, `name`, `payload`, `headers` or \
                 `processed_at`"
            ]
        );
    }

    #[test]
    fn a_column_is_named_once_and_a_role_needs_a_column() {
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox")]
            struct Order {
                #[field(id)]
                id: i64,
                #[field(name)]
                name: String,
                #[field(payload)]
                payload: Vec<u8>,
                #[sqlx(rename = "name")]
                channel: String,
                #[field(headers)]
                #[sqlx(skip)]
                headers: Option<String>,
            }
        };
        assert_eq!(
            errors(&input),
            ["`#[sqlx(skip)]` leaves `headers` without a column, so it cannot play `headers`",]
        );
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox")]
            struct Order {
                #[field(id)]
                id: i64,
                #[field(name)]
                name: String,
                #[field(payload)]
                payload: Vec<u8>,
                #[sqlx(rename = "name")]
                channel: String,
            }
        };
        assert_eq!(
            errors(&input),
            ["`channel` reads column `name`, which `name` reads already"]
        );
    }

    #[test]
    fn the_table_attribute_refuses_what_it_does_not_take() {
        let cases: [(DeriveInput, &str); 7] = [
            (
                parse_quote!(
                    struct Order {}
                ),
                "#[derive(Outbox)] needs the table: add `#[outbox(table = \"..\")]`",
            ),
            (
                parse_quote!(
                    #[outbox(table = "app.outbox")]
                    struct Order {}
                ),
                "`table` holds a dot: name the table alone, and its schema with `schema = \"..\"`",
            ),
            (
                parse_quote!(
                    #[outbox(table = "outbox", table = "other")]
                    struct Order {}
                ),
                "`table` is given twice",
            ),
            (
                parse_quote!(
                    #[outbox(table = "outbox", advisory_lock = "x")]
                    struct Order {}
                ),
                "unknown `#[outbox(..)]` option: expected `table`, `schema` or `custom`",
            ),
            (
                parse_quote!(
                    #[outbox(table = "outbox", custom(publish))]
                    struct Order {}
                ),
                "`publish` has no default to hand over: implement `outbox::Publish` for the record \
                 without listing it",
            ),
            (
                parse_quote!(
                    #[outbox(table = "outbox", custom(claim))]
                    struct Order {}
                ),
                "unknown event in `custom(..)`: expected `fetch`, `ack`, `retry`, `discard` or \
                 `recover`",
            ),
            (
                parse_quote!(
                    #[outbox(table = "outbox", custom(ack, ack))]
                    struct Order {}
                ),
                "`ack` is listed twice",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(errors(&input), [expected]);
        }
    }

    #[test]
    fn a_tuple_struct_is_refused() {
        let input: DeriveInput = parse_quote! {
            #[outbox(table = "outbox")]
            struct Order(i64);
        };
        assert_eq!(
            errors(&input),
            ["#[derive(Outbox)] describes a table: it takes a struct with named fields"]
        );
    }
}
