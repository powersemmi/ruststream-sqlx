use quote::{ToTokens, format_ident};
use ruststream_sqlx_dialect::{Isolation, Mode, Opening, Role};
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
    // The type sqlx decodes a column as before it converts it, for a column read alone.
    let decoded = inbox.fields[5]
        .column()
        .and_then(|column| column.try_from.as_ref())
        .map(|ty| ty.to_token_stream().to_string());
    assert_eq!(decoded.as_deref(), Some("i64"));
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
fn the_table_opens_at_its_isolation_or_its_mode() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", isolation = serializable)]
        struct Job { #[field(id)] id: i64 }
    };
    assert_eq!(
        inbox(&input)?.table.opening,
        Opening::Isolation(Isolation::Serializable)
    );
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs")]
        #[inbox(mode = immediate)]
        struct Job { #[field(id)] id: i64 }
    };
    assert_eq!(inbox(&input)?.table.opening, Opening::Mode(Mode::Immediate));
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs")]
        struct Job { #[field(id)] id: i64 }
    };
    assert_eq!(inbox(&input)?.table.opening, Opening::Default);
    Ok(())
}

#[test]
fn every_level_and_every_mode_reads_as_the_dialect_names_it() -> syn::Result<()> {
    for level in [
        Isolation::ReadUncommitted,
        Isolation::ReadCommitted,
        Isolation::RepeatableRead,
        Isolation::Serializable,
    ] {
        let word = format_ident!("{}", level.attribute());
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", isolation = #word)]
            struct Job { #[field(id)] id: i64 }
        };
        assert_eq!(inbox(&input)?.table.opening, Opening::Isolation(level));
    }
    for mode in [Mode::Deferred, Mode::Immediate, Mode::Exclusive] {
        let word = format_ident!("{}", mode.attribute());
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", mode = #word)]
            struct Job { #[field(id)] id: i64 }
        };
        assert_eq!(inbox(&input)?.table.opening, Opening::Mode(mode));
    }
    Ok(())
}

#[test]
fn misuse_of_the_opening_is_reported() {
    const LEVELS: &str = "unknown isolation level: expected `read_uncommitted`, \
                          `read_committed`, `repeatable_read` or `serializable`";
    const MODES: &str = "unknown mode: expected `deferred`, `immediate` or `exclusive` (SQLite)";
    const BOTH: &str =
        "a table declares `isolation` (Postgres, MySQL, MariaDB) or `mode` (SQLite), not both";
    let cases: [(DeriveInput, &str); 10] = [
        (
            parse_quote! { #[inbox(table = "jobs", isolation = snapshot)] struct Job { #[field(id)] id: i64 } },
            LEVELS,
        ),
        (
            parse_quote! { #[inbox(table = "jobs", isolation = Serializable)] struct Job { #[field(id)] id: i64 } },
            LEVELS,
        ),
        (
            parse_quote! { #[inbox(table = "jobs", isolation = "serializable")] struct Job { #[field(id)] id: i64 } },
            LEVELS,
        ),
        (
            parse_quote! { #[inbox(table = "jobs", isolation = read-committed)] struct Job { #[field(id)] id: i64 } },
            LEVELS,
        ),
        (
            parse_quote! { #[inbox(table = "jobs", mode = wal)] struct Job { #[field(id)] id: i64 } },
            MODES,
        ),
        (
            parse_quote! { #[inbox(table = "jobs", mode = serializable)] struct Job { #[field(id)] id: i64 } },
            MODES,
        ),
        (
            parse_quote! { #[inbox(table = "jobs", isolation = serializable, mode = immediate)] struct Job { #[field(id)] id: i64 } },
            BOTH,
        ),
        (
            parse_quote! { #[inbox(table = "jobs", mode = immediate)] #[inbox(isolation = serializable)] struct Job { #[field(id)] id: i64 } },
            BOTH,
        ),
        (
            parse_quote! { #[inbox(table = "jobs", isolation = serializable, isolation = serializable)] struct Job { #[field(id)] id: i64 } },
            "`isolation` is given twice",
        ),
        (
            parse_quote! { #[inbox(table = "jobs", mode = immediate, mode = exclusive)] struct Job { #[field(id)] id: i64 } },
            "`mode` is given twice",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(error(&input), expected);
    }
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
fn custom_events_and_the_clock_are_read() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", custom(fetch, dead_letter), clock = crate::Offset)]
        struct Job { #[field(id)] id: i64 }
    };
    let inbox = inbox(&input)?;
    let custom = inbox.table.custom;
    assert!(custom.fetch && custom.dead_letter);
    assert!(custom.claim.is_none() && !custom.ack && !custom.retry);
    assert!(!custom.retry_after && !custom.discard && custom.extend.is_none());
    let leased: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", custom(extend, claim))]
        struct Job { #[field(id)] id: i64 }
    };
    let listed = self::inbox(&leased)?.table.custom;
    assert!(listed.extend.is_some() && listed.claim.is_some());
    let twice: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", custom(extend, extend))]
        struct Job { #[field(id)] id: i64 }
    };
    assert_eq!(error(&twice), "`extend` is listed twice");
    let claimed_twice: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", custom(claim, claim))]
        struct Job { #[field(id)] id: i64 }
    };
    assert_eq!(error(&claimed_twice), "`claim` is listed twice");
    let clock = inbox
        .table
        .clock
        .map(|path| quote::quote!(#path).to_string());
    assert_eq!(clock.as_deref(), Some("crate :: Offset"));
    Ok(())
}

#[test]
fn the_lock_and_the_unlock_are_read_where_they_are_listed() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", advisory_lock = "jobs-{id}", custom(unlock, lock))]
        struct Job { #[field(id)] id: i64 }
    };
    let custom = inbox(&input)?.table.custom;
    assert!(custom.lock.is_some() && custom.unlock.is_some());
    let neither: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", custom(ack))]
        struct Job { #[field(id)] id: i64 }
    };
    let custom = inbox(&neither)?.table.custom;
    assert!(custom.lock.is_none() && custom.unlock.is_none());
    for event in ["lock", "unlock"] {
        let event = format_ident!("{event}");
        let twice: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", custom(#event, #event))]
            struct Job { #[field(id)] id: i64 }
        };
        assert_eq!(error(&twice), format!("`{event}` is listed twice"));
    }
    Ok(())
}

#[test]
fn a_json_field_is_marked_for_the_insert() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs")]
        struct Job { #[field(id)] id: i64, #[sqlx(json(nullable))] body: Option<Body>, other: String }
    };
    let json: Vec<_> = inbox(&input)?
        .columns()
        .map(|(_, column)| column.json)
        .collect();
    assert_eq!(json, [false, true, false]);
    Ok(())
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
    let fields = super::fields(&input)?;
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
