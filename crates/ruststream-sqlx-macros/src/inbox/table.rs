//! The table's description in the manual form: the `InboxSpec` builder chain `TABLE` holds and
//! the markers its type lists, both read off the struct's attributes and fields in one pass, so
//! the two agree by construction.

use heck::ToUpperCamelCase;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote, quote_spanned};
use ruststream_sqlx_dialect::{Opening, Role};
use syn::spanned::Spanned;

use crate::parse::{ColumnField, Custom, Field, Inbox};
use crate::template::KeyItem;

/// A table's description: the builder chain and the markers of its typed steps, in the order the
/// chain sets them.
pub(crate) struct Description {
    /// The markers `type Table` lists.
    pub(crate) markers: Vec<TokenStream2>,
    /// The `InboxSpec` builder chain.
    pub(crate) chain: TokenStream2,
    /// Where a broken rule across the settings is reported: the clock, the one setting the
    /// struct's own checks cannot judge, or the derive itself.
    pub(crate) span: Span,
}

impl Description {
    /// `InboxSpec<(..)>`, the type of the chain, spanned where a broken rule is reported.
    pub(crate) fn table_type(&self) -> TokenStream2 {
        let markers = &self.markers;
        quote_spanned!(self.span=> ::ruststream_sqlx::InboxSpec<(#(#markers,)*)>)
    }

    /// The tuple of markers alone.
    pub(crate) fn settings(&self) -> TokenStream2 {
        let markers = &self.markers;
        quote_spanned!(self.span=> (#(#markers,)*))
    }
}

/// One step of the chain: the setter, and the marker it adds where it is typed.
struct Steps {
    setters: Vec<TokenStream2>,
    markers: Vec<TokenStream2>,
}

impl Steps {
    fn push(&mut self, setter: TokenStream2, marker: Option<TokenStream2>) {
        self.setters.push(setter);
        self.markers.extend(marker);
    }
}

/// The `Column` value a field's column builds.
pub(crate) fn column(column: &ColumnField) -> TokenStream2 {
    let name = &column.name;
    let generated = column.generated.then(|| quote!(.generated()));
    quote!(::ruststream_sqlx::dialect::Column::new(#name) #generated)
}

/// The field's type, spanned at the field.
fn spanned(field: &Field<'_>) -> TokenStream2 {
    let ty = field.ty;
    quote_spanned!(ty.span()=> #ty)
}

/// The time a time field holds, spanned at its type, so a type that holds none is reported at
/// the field.
fn time(field: &Field<'_>) -> TokenStream2 {
    let ty = field.ty;
    quote_spanned!(ty.span()=> <#ty as ::ruststream_sqlx::TimeColumn>::Time)
}

/// The description of the table `inbox` reads, in the canonical order: the schema, the form, the
/// roles in `Role::ALL` order, the data columns, the clock, the opening and the service's own
/// events, then `trailing`.
pub(crate) fn describe(
    inbox: &Inbox<'_>,
    id: &ColumnField,
    key: Option<&[KeyItem]>,
    trailing: &[(TokenStream2, TokenStream2)],
) -> Description {
    let spec = quote!(::ruststream_sqlx::spec);
    let dialect = quote!(::ruststream_sqlx::dialect);
    let mut steps = Steps {
        setters: Vec::new(),
        markers: Vec::new(),
    };
    if let Some(schema) = &inbox.table.schema {
        steps.push(quote!(.within(#schema)), None);
    }
    let playing = |role: Role| {
        inbox
            .columns()
            .find(|(_, column)| column.role == Some(role))
    };
    if let Some(key) = key {
        let parts = key.iter().map(|item| match item {
            KeyItem::Literal(text) => quote!(#dialect::KeyPart::Literal(#text)),
            KeyItem::Column(column) => quote!(#dialect::KeyPart::Column(#column)),
        });
        steps.push(
            quote!(.advisory(&[#(#parts),*])),
            Some(quote!(#spec::Advisory)),
        );
    } else if let Some((field, expiry)) = playing(Role::LockedUntil) {
        let expiry = column(expiry);
        let time = time(field);
        steps.push(quote!(.lease(#expiry)), Some(quote!(#spec::Lease<#time>)));
    }
    for role in Role::ALL {
        if let Some((field, slot)) = playing(*role)
            && let Some((setter, marker)) = role_step(*role, field, slot)
        {
            steps.push(setter, marker);
        }
    }
    let data: Vec<_> = inbox
        .columns()
        .filter(|(_, column)| column.role.is_none())
        .map(|(_, data)| column(data))
        .collect();
    if !data.is_empty() {
        steps.push(quote!(.data(&[#(#data),*])), None);
    }
    if inbox.flattens() {
        steps.push(quote!(.selecting_all()), None);
    }
    let mut span = Span::call_site();
    if let Some(clock) = &inbox.table.clock {
        span = clock.span();
        steps.push(
            quote!(.clock::<#clock>()),
            Some(quote!(#spec::Clock<#clock>)),
        );
    }
    if let Some(level) = level(inbox.table.opening) {
        steps.push(
            quote!(.opens::<#level>()),
            Some(quote!(#spec::Opens<#level>)),
        );
    }
    for event in own_events(inbox.table.custom) {
        steps.push(quote!(.own::<#event>()), Some(event));
    }
    for (setter, marker) in trailing {
        steps.push(setter.clone(), Some(marker.clone()));
    }
    let table = &inbox.table.name;
    let id = column(id);
    let setters = &steps.setters;
    Description {
        markers: steps.markers,
        chain: quote!(::ruststream_sqlx::InboxSpec::new(#table, #id) #(#setters)*),
        span,
    }
}

/// The step of the role `slot` plays on `field`: its setter, and its marker where it is typed;
/// none for the id, which `new` takes, and the lease column, which the form takes.
fn role_step(
    role: Role,
    field: &Field<'_>,
    slot: &ColumnField,
) -> Option<(TokenStream2, Option<TokenStream2>)> {
    let spec = quote!(::ruststream_sqlx::spec);
    let value = column(slot);
    Some(match role {
        Role::Group if slot.fifo.is_some() => {
            (quote!(.fifo_group(#value)), Some(quote!(#spec::Fifo)))
        }
        Role::Group => (quote!(.group(#value)), None),
        Role::PartitionKey => (quote!(.partition_key(#value)), Some(quote!(#spec::Key))),
        Role::Priority => (quote!(.priority(#value)), None),
        // A time role names the field's own type: a type the database cannot bind rules the
        // table out where it is mounted on that database, not where it is described.
        Role::RetryAfter => {
            let ty = spanned(field);
            (
                quote!(.retry_after(#value)),
                Some(quote!(#spec::RetryAfter<#ty>)),
            )
        }
        // sqlx decodes the column as the `try_from` type where the field names one, and a row
        // that does not decode reads its attempt the same way.
        Role::Attempt => slot.try_from.as_deref().map_or_else(
            || (quote!(.attempt(#value)), Some(quote!(#spec::Attempt))),
            |decoded| {
                (
                    quote!(.attempt_from::<#decoded>(#value)),
                    Some(quote!(#spec::AttemptFrom<#decoded>)),
                )
            },
        ),
        Role::ProcessedAt => {
            let ty = spanned(field);
            (
                quote!(.processed_at(#value)),
                Some(quote!(#spec::ProcessedAt<#ty>)),
            )
        }
        Role::Headers => (quote!(.headers(#value)), Some(quote!(#spec::Headers))),
        Role::Payload => (quote!(.payload(#value)), Some(quote!(#spec::Payload))),
        _ => return None,
    })
}

/// The `level` marker of what the table's transactions open at; none for the database's
/// default, which every dialect opens.
fn level(opening: Opening) -> Option<TokenStream2> {
    // A variant of `Isolation` or `Mode` and its marker in `level` share one name: the word the
    // attribute takes, in upper camel case.
    let word = match opening {
        Opening::Isolation(level) => level.attribute(),
        Opening::Mode(mode) => mode.attribute(),
        _ => return None,
    };
    let name = format_ident!("{}", word.to_upper_camel_case());
    Some(quote!(::ruststream_sqlx::dialect::level::#name))
}

/// The markers of the events `custom(..)` lists, in the canonical order.
pub(crate) fn own_events(custom: Custom) -> Vec<TokenStream2> {
    let listed = [
        (custom.claim.is_some(), "Claim"),
        (custom.fetch, "Fetch"),
        (custom.ack, "Ack"),
        (custom.retry, "Retry"),
        (custom.retry_after, "RetryAfter"),
        (custom.discard, "Discard"),
        (custom.dead_letter, "DeadLetter"),
        (custom.extend.is_some(), "Extend"),
        (custom.lock.is_some(), "Lock"),
        (custom.unlock.is_some(), "Unlock"),
    ];
    listed
        .into_iter()
        .filter(|(listed, _)| *listed)
        .map(|(_, event)| {
            let event = format_ident!("{event}");
            quote!(::ruststream_sqlx::spec::own::#event)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use quote::format_ident;
    use syn::{DeriveInput, parse_quote};

    use crate::inbox::tests::expanded;

    #[test]
    fn a_declared_opening_and_clock_reach_the_chain_and_the_type() -> syn::Result<()> {
        let cases = [
            ("isolation", "read_uncommitted", "ReadUncommitted"),
            ("isolation", "read_committed", "ReadCommitted"),
            ("isolation", "repeatable_read", "RepeatableRead"),
            ("isolation", "serializable", "Serializable"),
            ("mode", "deferred", "Deferred"),
            ("mode", "immediate", "Immediate"),
            ("mode", "exclusive", "Exclusive"),
        ];
        for (key, word, name) in cases {
            let (key, word) = (format_ident!("{key}"), format_ident!("{word}"));
            let input: DeriveInput = parse_quote! {
                #[inbox(table = "jobs", #key = #word, clock = DatabaseClock)]
                struct Job { #[field(id)] id: i64 }
            };
            let impls = expanded(&input)?;
            let level = format!("::ruststream_sqlx::dialect::level::{name}");
            for expected in [
                format!(
                    "typeTable=::ruststream_sqlx::InboxSpec<(::ruststream_sqlx::spec::Clock<DatabaseClock>,\
                     ::ruststream_sqlx::spec::Opens<{level}>,)>;"
                ),
                format!(".clock::<DatabaseClock>().opens::<{level}>();"),
            ] {
                assert!(
                    impls.contains(&expected),
                    "{key} = {word}: {expected}\n{impls}"
                );
            }
        }
        // A table that names neither opens at its database's default on the crate's clock.
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs")]
            struct Job { #[field(id)] id: i64 }
        };
        let impls = expanded(&input)?;
        assert!(
            impls.contains("typeTable=::ruststream_sqlx::InboxSpec<()>;"),
            "{impls}"
        );
        Ok(())
    }

    #[test]
    fn a_converted_attempt_names_the_type_its_column_decodes_as() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs")]
            struct Job {
                #[field(id)] id: i64,
                #[field(attempt)] #[sqlx(try_from = "i16")] attempt: u16,
                #[field(processed_at)] done: Option<DateTime<Utc>>,
                #[field(priority)] rank: i16,
            }
        };
        let impls = expanded(&input)?;
        for expected in [
            "typeTable=::ruststream_sqlx::InboxSpec<(::ruststream_sqlx::spec::AttemptFrom<i16>,\
             ::ruststream_sqlx::spec::ProcessedAt<Option<DateTime<Utc>>>,)>;",
            ".priority(::ruststream_sqlx::dialect::Column::new(\"rank\"))\
             .attempt_from::<i16>(::ruststream_sqlx::dialect::Column::new(\"attempt\"))\
             .processed_at(::ruststream_sqlx::dialect::Column::new(\"done\"));",
            "typeAttempt=u16;",
        ] {
            assert!(impls.contains(expected), "{expected}\n{impls}");
        }
        Ok(())
    }

    #[test]
    fn an_advisory_lock_key_is_the_forms_setter() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "ledger", advisory_lock = "ledger-{account}")]
            struct Entry { #[field(id)] id: i64, #[sqlx(rename = "acct")] account: String }
        };
        let impls = expanded(&input)?;
        for expected in [
            "typeTable=::ruststream_sqlx::InboxSpec<(::ruststream_sqlx::spec::Advisory,)>;",
            ".advisory(&[::ruststream_sqlx::dialect::KeyPart::Literal(\"ledger-\"),\
             ::ruststream_sqlx::dialect::KeyPart::Column(\"acct\")])",
        ] {
            assert!(impls.contains(expected), "{expected}\n{impls}");
        }
        Ok(())
    }

    #[test]
    fn a_listed_lock_and_unlock_are_the_services_own_events() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", advisory_lock = "jobs-{id}", custom(lock, unlock))]
            struct Job { #[field(id)] id: i64 }
        };
        let impls = expanded(&input)?;
        for expected in [
            "::ruststream_sqlx::spec::own::Lock,::ruststream_sqlx::spec::own::Unlock,)>;",
            ".own::<::ruststream_sqlx::spec::own::Lock>().own::<::ruststream_sqlx::spec::own::Unlock>();",
        ] {
            assert!(impls.contains(expected), "{expected}\n{impls}");
        }
        Ok(())
    }
}
