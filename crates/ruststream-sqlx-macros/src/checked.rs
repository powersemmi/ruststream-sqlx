//! `#[inbox(checked, db = ..)]`: the statements a table's struct determines, built now by the
//! dialect the broker runs at startup and wrapped in `sqlx::query!` inside a function nothing
//! calls, so `cargo sqlx prepare` and the service's own build check them against the database.
//! The run-time path stays the manual form the derive emits beside it.

mod statements;

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote, quote_spanned};
use ruststream_sqlx_dialect::{Param, Role};
use syn::spanned::Spanned;
use syn::{DeriveInput, GenericParam, Generics, Type};

use self::statements::{Event, described, forms, statements};
use crate::check::Errors;
use crate::insert;
use crate::parse::{ColumnField, Field, Inbox, Storage};
use crate::template::KeyItem;

/// `checked` and `db = ..` as the attribute writes them, before they are judged.
#[derive(Default)]
pub(crate) struct Request {
    /// Where `checked` is written.
    pub(crate) checked: Option<Span>,
    /// The word `db = ..` names, where it is one word, and where its value is written.
    pub(crate) db: Option<(Option<String>, Span)>,
}

/// A built-in database whose dialect builds a checked struct's statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Db {
    Postgres,
    MySql,
    Sqlite,
}

impl Db {
    const ALL: [Self; 3] = [Self::Postgres, Self::MySql, Self::Sqlite];

    /// The word `db = ..` takes, which is also the name of the feature that builds the dialect.
    const fn word(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::Sqlite => "sqlite",
        }
    }

    /// The built-in databases the macros are built with.
    fn built() -> Vec<Self> {
        Self::ALL
            .into_iter()
            .filter(|db| forms(*db).is_some())
            .collect()
    }
}

/// A checked struct: the database its statements are checked against, and where `checked` is
/// written.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Checked {
    pub(crate) db: Db,
    pub(crate) span: Span,
}

/// Judges `request` against the databases `built` lists: `checked` names its database, the
/// database is a built-in one, and the macros are built with its dialect.
pub(crate) fn read(request: &Request, built: &[Db]) -> syn::Result<Option<Checked>> {
    match (request.checked, &request.db) {
        (None, None) => Ok(None),
        (Some(span), None) => Err(syn::Error::new(
            span,
            "`checked` checks the table's statements against a database at compile time: name \
             it with `db = postgres`, `db = mysql` or `db = sqlite`",
        )),
        (None, Some((_, span))) => Err(syn::Error::new(
            *span,
            "`db` names the database `checked` checks the statements against: write \
             `#[inbox(checked, db = ..)]`, or drop `db`",
        )),
        (Some(checked), Some((word, span))) => {
            let Some(db) = Db::ALL
                .into_iter()
                .find(|db| word.as_deref() == Some(db.word()))
            else {
                return Err(syn::Error::new(
                    *span,
                    "`db` names no built-in dialect: expected `postgres`, `mysql` or `sqlite`; a \
                     table served by a dialect of the service's own keeps the startup check, \
                     since a procedural macro cannot run that dialect",
                ));
            };
            if !built.contains(&db) {
                let word = db.word();
                return Err(syn::Error::new(
                    *span,
                    format!(
                        "`db = {word}` builds the statements with the {word} dialect, which this \
                         build leaves out: turn on the `{word}` feature of `ruststream-sqlx`"
                    ),
                ));
            }
            Ok(Some(Checked { db, span: checked }))
        }
    }
}

/// Which struct describes the table: a flat one, or the headers struct a message is assembled
/// from, which claims ids alone since the message reads its rows in its own `Fetch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Layout {
    Flat,
    Headers,
}

/// Whether `clock` reads the database's clock: the derive sees the clock's name alone, and a
/// constant the item emits holds that reading to the type's own answer.
fn database_clock(clock: &syn::Path) -> bool {
    clock
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "DatabaseClock")
}

/// The values the checked statements bind, each a parameter of the function that holds them,
/// typed as the run-time path binds it.
struct Binds<'i, 'a> {
    inbox: &'i Inbox<'a>,
    id: &'i Field<'a>,
    /// The parameters the statements use, in first use.
    used: Vec<(&'static str, TokenStream2)>,
}

impl Binds<'_, '_> {
    fn time(&self, role: Role) -> Option<TokenStream2> {
        self.inbox
            .columns()
            .find(|(_, slot)| slot.role == Some(role))
            .map(|(field, _)| {
                let ty = field.ty;
                quote!(<#ty as ::ruststream_sqlx::TimeColumn>::Time)
            })
    }

    /// Declares the parameter `name` of type `ty` once, and names it.
    fn take(&mut self, name: &'static str, ty: TokenStream2) -> TokenStream2 {
        if !self.used.iter().any(|(used, _)| *used == name) {
            self.used.push((name, ty));
        }
        let ident = format_ident!("{name}");
        quote!(#ident)
    }

    /// What binds `param` in a statement serving `event`.
    fn bind(
        &mut self,
        param: Param,
        event: Event,
        inserted: &[(&Field<'_>, &ColumnField)],
    ) -> Result<TokenStream2, String> {
        let unbound =
            || format!("the {event:?} statement binds {param:?}, which the table has no value for");
        Ok(match (param, event) {
            (Param::Id, _) => {
                let row = self.take("row", quote!(&Self));
                let ident = self.id.ident;
                quote!(#row.#ident)
            }
            (Param::Ids, _) => {
                let ty = self.id.ty;
                self.take("ids", quote!(&[#ty]))
            }
            (Param::Group, _) => self.take("group", quote!(&str)),
            (Param::Destination, _) => self.take("destination", quote!(&str)),
            (Param::Key, _) => self.take("key", quote!(&str)),
            (Param::Limit, _) => self.take("limit", quote!(i64)),
            (Param::Delay, _) => self.take("delay", quote!(i64)),
            (Param::Now, Event::Claim | Event::Guard | Event::Take | Event::Stamp)
            | (Param::RetryAfter, _) => {
                let ty = self.time(Role::RetryAfter).ok_or_else(unbound)?;
                self.take("retry_at", ty)
            }
            (Param::Now, Event::Ack | Event::Discard) => {
                let ty = self.time(Role::ProcessedAt).ok_or_else(unbound)?;
                self.take("processed_at", ty)
            }
            (Param::Lease | Param::LeaseNow | Param::Held, _) => {
                let ty = self.time(Role::LockedUntil).ok_or_else(unbound)?;
                self.take("lease", ty)
            }
            (Param::Column(position), Event::Insert) => {
                let (field, slot) = inserted.get(position).ok_or_else(unbound)?;
                let row = self.take("row", quote!(&Self));
                let ident = field.ident;
                // A JSON column binds through `Json` at run time; its type is sqlx's to judge.
                if slot.json {
                    quote!(::sqlx::types::Json(&#row.#ident) as _)
                } else {
                    quote!(#row.#ident)
                }
            }
            _ => return Err(unbound()),
        })
    }
}

/// The refusals of checked mode the struct's fields and settings show: a flattened field, whose
/// columns the derive cannot see, and a clock chosen where the struct is used.
fn refuse(input: &DeriveInput, inbox: &Inbox<'_>) -> syn::Result<()> {
    let name = &input.ident;
    let mut errors = Errors::default();
    for field in &inbox.fields {
        if matches!(field.storage, Storage::Flattened | Storage::Headers) {
            let ident = field.ident;
            errors.push(syn::Error::new(
                ident.span(),
                format!(
                    "`{ident}` flattens a struct whose columns `checked` cannot see: list its \
                     columns as fields of `{name}`, or drop `checked`"
                ),
            ));
        }
    }
    if let Some(clock) = &inbox.table.clock
        && let Some(ident) = clock.get_ident()
        && input
            .generics
            .params
            .iter()
            .any(|param| matches!(param, GenericParam::Type(ty) if ty.ident == *ident))
    {
        errors.push(syn::Error::new(
            clock.span(),
            format!(
                "`clock = {ident}` is chosen where `{name}` is used, and `checked` builds the \
                 statements now: name the clock's type, or drop `checked`"
            ),
        ));
    }
    errors.finish()
}

/// The checked item of a struct: one `sqlx::query!` per statement it determines, in a function
/// nothing calls; for a headers struct, also the constant that refuses a message reading its rows
/// with the crate's fetch. None for a struct without `checked`.
pub(crate) fn item(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
    id: (&Field<'_>, &ColumnField),
    key: Option<&[KeyItem]>,
    layout: Layout,
) -> syn::Result<Option<TokenStream2>> {
    let Some(checked) = read(&inbox.table.checked, &Db::built())? else {
        return Ok(None);
    };
    refuse(input, inbox)?;
    let Some(forms) = forms(checked.db) else {
        return Ok(None);
    };
    let table_span = inbox.table.name.span();
    let fail = |message: String| syn::Error::new(table_span, message);
    let statements = described(inbox, id.1, key, |spec| {
        statements(forms, spec, inbox.table.custom, layout)
    })
    .map_err(fail)?;
    let inserted = insert::ordered(&inbox.columns().collect::<Vec<_>>());
    let mut binds = Binds {
        inbox,
        id: id.0,
        used: Vec::new(),
    };
    let mut queries = Vec::new();
    for (event, statement) in &statements {
        let args = statement
            .params()
            .iter()
            .map(|param| binds.bind(*param, *event, &inserted))
            .collect::<Result<Vec<_>, _>>()
            .map_err(fail)?;
        let sql = statement.sql();
        // A lock's outcome is one unnamed value, which a record cannot name a field after on
        // every database (MySQL names the column after its expression).
        let query = if matches!(event, Event::Guard | Event::Lock | Event::Unlock) {
            quote!(query_scalar)
        } else {
            quote!(query)
        };
        queries.push(quote!(let _ = ::sqlx::#query!(#sql #(, #args)*);));
    }
    let params = binds.used.iter().map(|(name, ty)| {
        let ident = format_ident!("{name}");
        quote!(#ident: #ty)
    });
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let clock = clock_check(inbox, generics);
    let fetch = (layout == Layout::Headers).then(|| fetch_refusal(name, checked.span));
    Ok(Some(quote! {
        const _: () = {
            #[automatically_derived]
            impl #impl_generics #name #ty_generics #where_clause {
                #[allow(dead_code, clippy::too_many_arguments)]
                fn __ruststream_sqlx_checked(#(#params),*) {
                    #(#queries)*
                }
                #fetch
            }
            #clock
        };
    }))
}

/// Holds the derive's reading of the clock to the clock's own answer: the statements read the
/// database's clock where it is `DatabaseClock`, and a renamed import would read otherwise.
fn clock_check(inbox: &Inbox<'_>, generics: &Generics) -> Option<TokenStream2> {
    let clock = inbox.table.clock.as_ref()?;
    if !generics.params.is_empty() {
        return None;
    }
    let database = database_clock(clock);
    let message = format!(
        "`checked` read `clock = {}` as {} clock: name `DatabaseClock` by that name",
        quote!(#clock).to_string().replace(' ', ""),
        if database {
            "the database's"
        } else {
            "a clock on the host, not the database's"
        },
    );
    Some(quote_spanned! {clock.span()=>
        const _: () = ::core::assert!(
            <#clock as ::ruststream_sqlx::TimeSource>::DATABASE == #database,
            #message,
        );
    })
}

/// The constant a checked headers struct holds in place of the default every other one reads:
/// it stops the build of a message that reads the struct's rows with the crate's own fetch,
/// whose claim of whole rows `checked` cannot build.
fn fetch_refusal(name: &syn::Ident, span: Span) -> TokenStream2 {
    let message = format!(
        "`{name}` is `checked`, and its statements claim ids alone: list `fetch` in \
         `#[inbox(custom(..))]` on the message that flattens `{name}`, and read its rows in the \
         service's own `Fetch`"
    );
    quote_spanned! {span=>
        #[doc(hidden)]
        pub const __RUSTSTREAM_SQLX_FETCH: () = ::core::panic!(#message);
    }
}

/// What a message that reads its headers struct's rows with the crate's own fetch evaluates: the
/// headers struct's constant where it is `checked`, and a unit every other one gets from a trait.
///
/// Why a constant evaluated at build time: the message's derive cannot see whether its headers
/// struct is `checked`, so the rule is held where both meet, in the type `holder` names.
pub(crate) fn default_fetch(holder: &Type) -> TokenStream2 {
    quote_spanned! {holder.span()=>
        trait __DefaultFetch {
            const __RUSTSTREAM_SQLX_FETCH: () = ();
        }
        impl<__T: ?::core::marker::Sized> __DefaultFetch for __T {}
        let () = <#holder>::__RUSTSTREAM_SQLX_FETCH;
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::Span;
    use ruststream_sqlx_dialect::{
        ClaimShape, Column, Dialect, Form, Postgres, RowLock, TableSpec,
    };
    use syn::{DeriveInput, parse_quote};

    use super::statements::tests::checked_texts;
    use super::{Checked, Db, Request, read};
    use crate::headers::expand;
    use crate::inbox::tests::{errors, expanded};

    #[test]
    fn a_checked_headers_struct_claims_ids_and_refuses_the_crates_fetch()
    -> Result<(), Box<dyn std::error::Error>> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", checked, db = postgres)]
            struct Head {
                #[field(id)] id: i64,
                #[field(processed_at)] done: Option<DateTime<Utc>>,
                trace: String,
            }
        };
        let impls = expand(&input)?.to_string().replace(' ', "");
        let data = [Column::new("trace")];
        let spec = TableSpec::new("jobs", Column::new("id"), Form::RowLock)
            .processed_at(Column::new("done"))
            .data(&data);
        let mut expected: Vec<String> = [
            Postgres.lock_claim(&spec, ClaimShape::Ids)?,
            Postgres.ack(&spec)?,
            Postgres.discard(&spec)?,
            Postgres.insert(&spec)?,
        ]
        .iter()
        .chain(Postgres.retry(&spec)?.iter())
        .map(|statement| statement.sql().replace(' ', ""))
        .collect();
        let mut found = checked_texts(&impls);
        found.sort();
        expected.sort();
        expected.dedup();
        assert_eq!(found, expected, "{impls}");
        assert!(
            impls.contains("pubconst__RUSTSTREAM_SQLX_FETCH:()=::core::panic!(\"`Head`is`checked`"),
            "{impls}"
        );
        // A message that reads the rows with the crate's fetch evaluates that constant; one with
        // its own fetch does not.
        let message: DeriveInput = parse_quote! {
            struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head, note: String }
        };
        let fetched = "let()=<Head>::__RUSTSTREAM_SQLX_FETCH;";
        assert!(expanded(&message)?.contains(fetched));
        let own: DeriveInput = parse_quote! {
            #[inbox(custom(fetch))]
            struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head, note: String }
        };
        assert!(!expanded(&own)?.contains(fetched));
        Ok(())
    }

    #[test]
    fn the_clocks_reading_is_held_to_the_clocks_own_answer() -> syn::Result<()> {
        let database: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", clock = ruststream_sqlx::DatabaseClock, checked, db = postgres)]
            struct Job { #[field(id)] id: i64, #[field(retry_after)] at: DateTime<Utc> }
        };
        let impls = expanded(&database)?;
        assert!(
            impls.contains(
                "<ruststream_sqlx::DatabaseClockas::ruststream_sqlx::TimeSource>::DATABASE==true"
            ),
            "{impls}"
        );
        // The database's clock binds no time: the statements read it themselves.
        assert!(!impls.contains("retry_at:"), "{impls}");
        let host: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", clock = Frozen, checked, db = postgres)]
            struct Job { #[field(id)] id: i64, #[field(retry_after)] at: DateTime<Utc> }
        };
        let impls = expanded(&host)?;
        assert!(
            impls.contains("<Frozenas::ruststream_sqlx::TimeSource>::DATABASE==false"),
            "{impls}"
        );
        assert!(impls.contains("retry_at:"), "{impls}");
        Ok(())
    }

    #[test]
    fn a_struct_without_checked_emits_no_query() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs")]
            struct Job { #[field(id)] id: i64, #[field(payload)] payload: Vec<u8> }
        };
        let impls = expanded(&input)?;
        assert!(!impls.contains("query!"), "{impls}");
        assert!(!impls.contains("__ruststream_sqlx_checked"), "{impls}");
        Ok(())
    }

    #[test]
    fn misuse_of_checked_is_reported() {
        let cases: [(DeriveInput, &str); 10] = [
            (
                parse_quote! { #[inbox(table = "jobs", checked = true, db = postgres)] struct Job { #[field(id)] id: i64 } },
                "`checked` takes no value: `#[inbox(checked, db = ..)]`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", checked, checked, db = postgres)] struct Job { #[field(id)] id: i64 } },
                "`checked` is given twice",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", checked, db = postgres, db = mysql)] struct Job { #[field(id)] id: i64 } },
                "`db` is given twice",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", checked)] struct Job { #[field(id)] id: i64 } },
                "`checked` checks the table's statements against a database at compile time: \
                 name it with `db = postgres`, `db = mysql` or `db = sqlite`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", db = postgres)] struct Job { #[field(id)] id: i64 } },
                "`db` names the database `checked` checks the statements against: write \
                 `#[inbox(checked, db = ..)]`, or drop `db`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", checked, db = mssql)] struct Job { #[field(id)] id: i64 } },
                "`db` names no built-in dialect: expected `postgres`, `mysql` or `sqlite`; a \
                 table served by a dialect of the service's own keeps the startup check, since a \
                 procedural macro cannot run that dialect",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", checked, db = "postgres")] struct Job { #[field(id)] id: i64 } },
                "`db` names no built-in dialect: expected `postgres`, `mysql` or `sqlite`; a \
                 table served by a dialect of the service's own keeps the startup check, since a \
                 procedural macro cannot run that dialect",
            ),
            (
                parse_quote! {
                    #[inbox(table = "jobs", checked, db = postgres)]
                    struct Job { #[field(id)] id: i64, #[sqlx(flatten)] body: Body }
                },
                "`body` flattens a struct whose columns `checked` cannot see: list its columns as \
                 fields of `Job`, or drop `checked`",
            ),
            (
                parse_quote! {
                    #[inbox(table = "jobs", clock = Source, checked, db = postgres)]
                    struct Job<Source> { #[field(id)] id: i64, #[sqlx(skip)] source: PhantomData<Source> }
                },
                "`clock = Source` is chosen where `Job` is used, and `checked` builds the \
                 statements now: name the clock's type, or drop `checked`",
            ),
            (
                parse_quote! { #[inbox(table = "jobs", checked, db = sqlite)] struct Job { #[field(id)] id: i64 } },
                "the sqlite dialect claims no rows in the row lock form, which this table takes: \
                 claim them by lease, with a `#[field(locked_until)]` field, or by advisory lock, \
                 with `advisory_lock = \"..\"`",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(errors(&input), [expected], "{}", input.ident);
        }
    }

    #[test]
    fn a_database_the_build_leaves_out_names_its_feature() {
        let request = Request {
            checked: Some(Span::call_site()),
            db: Some((Some("mysql".to_owned()), Span::call_site())),
        };
        let refused = read(&request, &[Db::Postgres, Db::Sqlite])
            .err()
            .map(|error| error.to_string());
        assert_eq!(
            refused.as_deref(),
            Some(
                "`db = mysql` builds the statements with the mysql dialect, which this build \
                 leaves out: turn on the `mysql` feature of `ruststream-sqlx`"
            )
        );
        assert!(matches!(
            read(&request, &[Db::MySql]),
            Ok(Some(Checked { db: Db::MySql, .. }))
        ));
    }
}
