//! The statements a checked struct determines: the table's description as the derive's chain
//! builds it, and each statement the broker prepares at startup that the struct leaves to the
//! crate, as the database's built-in dialect writes it.

#[cfg(feature = "mysql")]
use ruststream_sqlx_dialect::MySql;
#[cfg(feature = "postgres")]
use ruststream_sqlx_dialect::Postgres;
#[cfg(feature = "sqlite")]
use ruststream_sqlx_dialect::Sqlite;
use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Column, Dialect, Form, KeyPart, Lease, Opening, Role, RowLock, Statement,
    StatementError, TableSpec,
};

use super::{Db, Layout, database_clock};
use crate::parse::{ColumnField, Custom, Inbox};
use crate::template::KeyItem;

/// The dialect of each form one database builds, as the broker reads them at startup.
#[derive(Clone, Copy)]
pub(super) struct Forms {
    dialect: &'static dyn Dialect,
    row_lock: Option<&'static dyn RowLock>,
    lease: Option<&'static dyn Lease>,
    advisory: Option<&'static dyn Advisory>,
}

/// The forms `db`'s built-in dialect serves; none where the macros are built without it.
pub(super) const fn forms(db: Db) -> Option<Forms> {
    match db {
        #[cfg(feature = "postgres")]
        Db::Postgres => Some(Forms {
            dialect: &Postgres,
            row_lock: Some(&Postgres),
            lease: Some(&Postgres),
            advisory: Some(&Postgres),
        }),
        #[cfg(feature = "mysql")]
        Db::MySql => Some(Forms {
            dialect: &MySql,
            row_lock: Some(&MySql),
            lease: Some(&MySql),
            advisory: Some(&MySql),
        }),
        #[cfg(feature = "sqlite")]
        Db::Sqlite => Some(Forms {
            dialect: &Sqlite,
            row_lock: None,
            lease: Some(&Sqlite),
            advisory: Some(&Sqlite),
        }),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

/// The event a statement serves, which decides the time a `Now` placeholder binds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Event {
    Claim,
    Guard,
    Fetch,
    Ack,
    Retry,
    RetryAfter,
    Discard,
    DeadLetter,
    Extend,
    Stamp,
    Lock,
    Unlock,
    Take,
    Insert,
}

/// The statements a subscription to `spec` prepares at startup that the struct determines, as
/// the broker builds them: the claim (and the claim by role a by-name subscription runs, for a
/// struct that leaves every event to the crate), the guard of a FIFO group, the fetch, the
/// settlements, the dead letter of a group, the lease's extension and stamp, the advisory lock
/// form's lock, unlock and take, and the insert. A statement the service writes itself is left
/// out; a dead-letter table is named where the struct is mounted, so its move is left to the
/// startup check.
pub(super) fn statements(
    forms: Forms,
    spec: &TableSpec<'_>,
    custom: Custom,
    layout: Layout,
) -> Result<Vec<(Event, Statement)>, String> {
    let dialect = forms.dialect;
    let refused = |err: StatementError| err.to_string();
    let shape = match layout {
        Layout::Headers => ClaimShape::Ids,
        Layout::Flat if custom.fetch => ClaimShape::Ids,
        Layout::Flat => ClaimShape::Rows,
    };
    // A struct that leaves every event to the crate is read by role where it is routed by name.
    let defaults = layout == Layout::Flat && custom.listed.is_none();
    let shapes: &[ClaimShape] = if defaults {
        &[shape, ClaimShape::Roles]
    } else {
        std::slice::from_ref(&shape)
    };
    let mut out: Vec<(Event, Statement)> = Vec::new();
    claims(forms, spec, custom, shapes, &mut out)?;
    if let Some(guard) = dialect.fifo_guard(spec).map_err(refused)? {
        out.push((Event::Guard, guard));
    }
    if custom.claim.is_some() && !custom.fetch {
        out.push((Event::Fetch, dialect.fetch(spec).map_err(refused)?));
    }
    if !custom.ack {
        out.push((Event::Ack, dialect.ack(spec).map_err(refused)?));
    }
    if !custom.retry
        && let Some(retry) = dialect.retry(spec).map_err(refused)?
    {
        out.push((Event::Retry, retry));
    }
    if !custom.retry_after && spec.column(Role::RetryAfter).is_some() {
        out.push((
            Event::RetryAfter,
            dialect.retry_after(spec).map_err(refused)?,
        ));
    }
    if !custom.discard {
        out.push((Event::Discard, dialect.discard(spec).map_err(refused)?));
    }
    if !custom.dead_letter && spec.column(Role::Group).is_some() {
        out.push((
            Event::DeadLetter,
            dialect.dead_letter_group(spec).map_err(refused)?,
        ));
    }
    if let Form::Lease(_) = spec.form()
        && let Some(lease) = forms.lease
    {
        if custom.extend.is_none() {
            out.push((Event::Extend, lease.extend(spec).map_err(refused)?));
        }
        if custom.claim.is_some() || !lease.claim_writes_lease() {
            out.push((Event::Stamp, lease.stamp(spec).map_err(refused)?));
        }
    }
    out.push((Event::Insert, dialect.insert(spec).map_err(refused)?));
    // One check per text: the claim by role of the advisory lock form selects as its claim does.
    let mut seen: Vec<String> = Vec::new();
    out.retain(|(_, statement)| {
        let fresh = !seen.iter().any(|sql| sql == statement.sql());
        if fresh {
            seen.push(statement.sql().to_owned());
        }
        fresh
    });
    Ok(out)
}

/// The claim in each of `shapes` the table's form runs, and the advisory lock form's take, lock
/// and unlock, onto `out`.
fn claims(
    forms: Forms,
    spec: &TableSpec<'_>,
    custom: Custom,
    shapes: &[ClaimShape],
    out: &mut Vec<(Event, Statement)>,
) -> Result<(), String> {
    let name = forms.dialect.name();
    let refused = |err: StatementError| err.to_string();
    let lacks = |form: &str| {
        format!("the {name} dialect claims no rows in the {form} form, which this table takes")
    };
    match spec.form() {
        Form::RowLock => {
            let row_lock = forms.row_lock.ok_or_else(|| {
                format!(
                    "{}: claim them by lease, with a `#[field(locked_until)]` field, or by \
                     advisory lock, with `advisory_lock = \"..\"`",
                    lacks("row lock")
                )
            })?;
            if custom.claim.is_none() {
                for shape in shapes {
                    out.push((
                        Event::Claim,
                        row_lock.lock_claim(spec, *shape).map_err(refused)?,
                    ));
                }
            }
        }
        Form::Lease(_) => {
            let lease = forms.lease.ok_or_else(|| lacks("lease"))?;
            if custom.claim.is_none() {
                for shape in shapes {
                    out.push((
                        Event::Claim,
                        lease.lease_claim(spec, *shape).map_err(refused)?,
                    ));
                }
            }
        }
        Form::Advisory(_) => {
            let advisory = forms.advisory.ok_or_else(|| lacks("advisory lock"))?;
            if custom.claim.is_none() {
                out.push((
                    Event::Claim,
                    advisory.advisory_claim(spec).map_err(refused)?,
                ));
            }
            for shape in shapes {
                let takes = advisory.take(spec, *shape).map_err(refused)?;
                if takes.is_empty() || takes.len() > 2 {
                    return Err(format!(
                        "the {name} dialect takes a candidate in {} statements, and the inbox \
                         runs one or two",
                        takes.len()
                    ));
                }
                out.extend(takes.into_iter().map(|take| (Event::Take, take)));
            }
            if custom.lock.is_none() {
                out.extend(advisory.lock().map(|lock| (Event::Lock, lock)));
            }
            if custom.unlock.is_none() {
                out.extend(advisory.unlock().map(|unlock| (Event::Unlock, unlock)));
            }
        }
        _ => {
            return Err(format!(
                "the {name} dialect claims no rows in this table's form"
            ));
        }
    }
    Ok(())
}

/// A column of the description the dialect reads.
fn column(slot: &ColumnField) -> Column<'_> {
    let built = Column::new(&slot.name);
    if slot.generated {
        built.generated()
    } else {
        built
    }
}

/// Builds the table's description from the struct as the derive's chain describes it, and hands
/// it to `with`: the schema, the form, the roles, the data columns, the clock and the opening.
pub(super) fn described<Out>(
    inbox: &Inbox<'_>,
    id: &ColumnField,
    key: Option<&[KeyItem]>,
    with: impl FnOnce(&TableSpec<'_>) -> Out,
) -> Out {
    let playing = |role: Role| {
        inbox
            .columns()
            .find(|(_, slot)| slot.role == Some(role))
            .map(|(_, slot)| slot)
    };
    let parts: Vec<KeyPart<'_>> = key
        .unwrap_or_default()
        .iter()
        .map(|item| match item {
            KeyItem::Literal(text) => KeyPart::Literal(text),
            KeyItem::Column(name) => KeyPart::Column(name),
        })
        .collect();
    let form = if key.is_some() {
        Form::Advisory(&parts)
    } else if let Some(expiry) = playing(Role::LockedUntil) {
        Form::Lease(column(expiry))
    } else {
        Form::RowLock
    };
    let table = inbox.table.name.value();
    let schema = inbox.table.schema.as_ref().map(syn::LitStr::value);
    let mut spec = TableSpec::new(&table, column(id), form);
    if let Some(schema) = &schema {
        spec = spec.within(schema);
    }
    for role in Role::ALL {
        let Some(slot) = playing(*role) else {
            continue;
        };
        let built = column(slot);
        spec = match role {
            Role::Group if slot.fifo.is_some() => spec.fifo_group(built),
            Role::Group => spec.group(built),
            Role::PartitionKey => spec.partition_key(built),
            Role::Priority => spec.priority(built),
            Role::RetryAfter => spec.retry_after(built),
            Role::Attempt => spec.attempt(built),
            Role::ProcessedAt => spec.processed_at(built),
            Role::Headers => spec.headers(built),
            Role::Payload => spec.payload(built),
            _ => spec,
        };
    }
    let data: Vec<Column<'_>> = inbox
        .columns()
        .filter(|(_, slot)| slot.role.is_none())
        .map(|(_, slot)| column(slot))
        .collect();
    if !data.is_empty() {
        spec = spec.data(&data);
    }
    if inbox.table.clock.as_ref().is_some_and(database_clock) {
        spec = spec.database_clock();
    }
    spec = match inbox.table.opening {
        Opening::Isolation(level) => spec.isolation(level),
        Opening::Mode(mode) => spec.mode(mode),
        _ => spec,
    };
    with(&spec)
}

#[cfg(test)]
pub(super) mod tests {
    use quote::format_ident;
    use ruststream_sqlx_dialect::{
        Advisory, ClaimShape, Column, Dialect, Form, KeyPart, Lease, MySql, Postgres, RowLock,
        Sqlite, Statement, TableSpec,
    };
    use syn::{DeriveInput, parse_quote};

    use crate::inbox::tests::expanded;

    /// The texts of the `sqlx::query!` and `sqlx::query_scalar!` calls of `expanded`, unescaped,
    /// in order.
    pub(in crate::checked) fn checked_texts(expanded: &str) -> Vec<String> {
        expanded
            .split("::sqlx::query")
            .skip(1)
            .filter_map(|rest| {
                rest.strip_prefix("!(\"")
                    .or_else(|| rest.strip_prefix("_scalar!(\""))
            })
            .map(|rest| {
                let mut text = String::new();
                let mut chars = rest.chars();
                while let Some(c) = chars.next() {
                    match c {
                        '\\' => text.extend(chars.next()),
                        '"' => break,
                        _ => text.push(c),
                    }
                }
                text
            })
            .collect()
    }

    /// The expansion's statements are `expected`, as the dialect builds them, each once.
    fn assert_texts(input: &DeriveInput, expected: &[Statement]) -> syn::Result<()> {
        let impls = expanded(input)?;
        // The expansion drops whitespace; so do the expected texts.
        let mut found = checked_texts(&impls);
        let mut expected: Vec<String> = expected
            .iter()
            .map(|statement| statement.sql().replace(' ', ""))
            .collect();
        found.sort();
        expected.sort();
        expected.dedup();
        assert_eq!(found, expected, "{impls}");
        Ok(())
    }

    #[test]
    fn a_checked_row_lock_struct_checks_each_statement_the_broker_prepares()
    -> Result<(), Box<dyn std::error::Error>> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", schema = "app", checked, db = postgres)]
            struct Job {
                #[field(id, generated)] id: i64,
                #[field(group)] name: String,
                #[field(retry_after)] retry_after: DateTime<Utc>,
                #[field(attempt, generated)] attempt: i16,
                #[field(processed_at)] processed_at: Option<DateTime<Utc>>,
                #[field(payload)] payload: Vec<u8>,
                note: String,
            }
        };
        // The description the chain of the manual form builds, by hand.
        let data = [Column::new("note")];
        let spec = TableSpec::new("jobs", Column::new("id").generated(), Form::RowLock)
            .within("app")
            .group(Column::new("name"))
            .retry_after(Column::new("retry_after"))
            .attempt(Column::new("attempt").generated())
            .processed_at(Column::new("processed_at"))
            .payload(Column::new("payload"))
            .data(&data);
        let mut expected = vec![
            Postgres.lock_claim(&spec, ClaimShape::Rows)?,
            Postgres.lock_claim(&spec, ClaimShape::Roles)?,
            Postgres.ack(&spec)?,
            Postgres.retry_after(&spec)?,
            Postgres.discard(&spec)?,
            Postgres.dead_letter_group(&spec)?,
            Postgres.insert(&spec)?,
        ];
        expected.extend(Postgres.retry(&spec)?);
        expected.extend(Postgres.fifo_guard(&spec)?);
        assert_texts(&input, &expected)?;
        // One text in full, as the database receives it.
        let impls = expanded(&input)?;
        assert!(
            impls.contains(
                r#"::sqlx::query!("UPDATE\"app\".\"jobs\"SET\"processed_at\"=$1WHERE\"id\"=$2",processed_at,row.id);"#
            ),
            "{impls}"
        );
        // Each value binds as the run-time path binds it.
        for parameter in [
            "row:&Self",
            "group:&str",
            "limit:i64",
            "retry_at:<DateTime<Utc>as::ruststream_sqlx::TimeColumn>::Time",
            "processed_at:<Option<DateTime<Utc>>as::ruststream_sqlx::TimeColumn>::Time",
            "destination:&str",
        ] {
            assert!(impls.contains(parameter), "{parameter}\n{impls}");
        }
        Ok(())
    }

    #[test]
    fn a_checked_lease_struct_checks_its_extension_on_every_database()
    -> Result<(), Box<dyn std::error::Error>> {
        let dialects: [(&str, &dyn Lease); 3] = [
            ("postgres", &Postgres),
            ("mysql", &MySql),
            ("sqlite", &Sqlite),
        ];
        for (db, dialect) in dialects {
            let db = format_ident!("{db}");
            let input: DeriveInput = parse_quote! {
                #[inbox(table = "jobs", checked, db = #db)]
                struct Job {
                    #[field(id)] id: i64,
                    #[field(locked_until)] locked_until: Option<DateTime<Utc>>,
                    #[field(payload)] payload: Vec<u8>,
                }
            };
            let spec = TableSpec::new(
                "jobs",
                Column::new("id"),
                Form::Lease(Column::new("locked_until")),
            )
            .payload(Column::new("payload"));
            let mut expected = vec![
                dialect.lease_claim(&spec, ClaimShape::Rows)?,
                dialect.lease_claim(&spec, ClaimShape::Roles)?,
                dialect.ack(&spec)?,
                dialect.discard(&spec)?,
                dialect.extend(&spec)?,
                dialect.insert(&spec)?,
            ];
            expected.extend(dialect.retry(&spec)?);
            if !dialect.claim_writes_lease() {
                expected.push(dialect.stamp(&spec)?);
            }
            assert_texts(&input, &expected)?;
            let impls = expanded(&input)?;
            assert!(
                impls
                    .contains("lease:<Option<DateTime<Utc>>as::ruststream_sqlx::TimeColumn>::Time"),
                "{db}: {impls}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_checked_advisory_struct_checks_its_lock_unlock_and_take()
    -> Result<(), Box<dyn std::error::Error>> {
        let dialects: [(&str, &dyn Advisory); 3] = [
            ("postgres", &Postgres),
            ("mysql", &MySql),
            ("sqlite", &Sqlite),
        ];
        for (db, dialect) in dialects {
            let db = format_ident!("{db}");
            let input: DeriveInput = parse_quote! {
                #[inbox(table = "ledger", advisory_lock = "ledger-{account}", checked, db = #db)]
                struct Entry {
                    #[field(id)] id: i64,
                    #[sqlx(rename = "acct")] account: String,
                    #[field(attempt)] attempt: i32,
                    #[field(payload)] payload: Vec<u8>,
                }
            };
            let key = [KeyPart::Literal("ledger-"), KeyPart::Column("acct")];
            let data = [Column::new("acct")];
            let spec = TableSpec::new("ledger", Column::new("id"), Form::Advisory(&key))
                .attempt(Column::new("attempt"))
                .payload(Column::new("payload"))
                .data(&data);
            let mut expected = vec![
                dialect.advisory_claim(&spec)?,
                dialect.ack(&spec)?,
                dialect.discard(&spec)?,
                dialect.insert(&spec)?,
            ];
            expected.extend(dialect.take(&spec, ClaimShape::Rows)?);
            expected.extend(dialect.take(&spec, ClaimShape::Roles)?);
            expected.extend(dialect.lock());
            expected.extend(dialect.unlock());
            expected.extend(dialect.retry(&spec)?);
            assert_texts(&input, &expected)?;
        }
        Ok(())
    }

    #[test]
    fn the_services_own_events_leave_their_statements_to_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", custom(claim, ack, discard), checked, db = postgres)]
            struct Job { #[field(id)] id: i64, #[field(payload)] payload: Vec<u8> }
        };
        let spec = TableSpec::new("jobs", Column::new("id"), Form::RowLock)
            .payload(Column::new("payload"));
        // A claim of the service's own leaves the rows to the crate's fetch, which binds the ids.
        let mut expected = vec![Postgres.fetch(&spec)?, Postgres.insert(&spec)?];
        expected.extend(Postgres.retry(&spec)?);
        assert_texts(&input, &expected)?;
        assert!(expanded(&input)?.contains("ids:&[i64]"));
        Ok(())
    }

    #[test]
    fn the_insert_is_the_text_the_runtime_insert_runs() -> syn::Result<()> {
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", checked, db = sqlite)]
            struct Job {
                #[field(id, generated)] id: i64,
                #[field(locked_until)] locked_until: Option<i64>,
                note: String,
                #[field(payload)] payload: Vec<u8>,
            }
        };
        let impls = expanded(&input)?;
        let runtime = impls
            .split("sqlite:::core::option::Option::Some(\"")
            .nth(1)
            .and_then(|rest| rest.split("\")").next())
            .unwrap_or_default()
            .replace("\\\"", "\"");
        assert!(runtime.starts_with("INSERTINTO"), "{impls}");
        assert!(
            checked_texts(&impls).contains(&runtime),
            "{runtime}\n{impls}"
        );
        Ok(())
    }
}
