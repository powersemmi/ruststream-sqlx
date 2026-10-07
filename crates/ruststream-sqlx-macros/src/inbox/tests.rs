use quote::format_ident;
use syn::{DeriveInput, parse_quote};

use super::expand;

fn errors(input: &DeriveInput) -> Vec<String> {
    expand(input).map_or_else(
        |error| error.into_iter().map(|error| error.to_string()).collect(),
        |_| Vec::new(),
    )
}

/// The expansion without whitespace, so a test reads it as the source would be written.
fn expanded(input: &DeriveInput) -> syn::Result<String> {
    Ok(expand(input)?.to_string().replace(' ', ""))
}

#[test]
fn a_flat_struct_expands_to_the_manual_form() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", schema = "app", custom(ack, retry))]
        struct Job {
            #[field(id, generated)] id: i64,
            #[field(payload)] payload: Vec<u8>,
            #[field(group, fifo = true)] name: String,
            #[field(partition_key)] tenant: String,
            #[field(attempt, generated)] attempt: i16,
            #[field(locked_until)] locked_until: Option<DateTime<Utc>>,
            #[field(retry_after)] retry_after: Option<DateTime<Utc>>,
            note: String,
        }
    };
    let impls = expanded(&input)?;
    for expected in [
        "impl::ruststream_sqlx::InboxTableforJob{typeId=i64;",
        // The markers in the canonical order: the form, the roles, the clock and opening, the
        // service's own events.
        "typeTable=::ruststream_sqlx::InboxSpec<(\
         ::ruststream_sqlx::spec::Lease<<Option<DateTime<Utc>>as::ruststream_sqlx::TimeColumn>::Time>,\
         ::ruststream_sqlx::spec::Fifo,\
         ::ruststream_sqlx::spec::Key,\
         ::ruststream_sqlx::spec::RetryAfter<Option<DateTime<Utc>>>,\
         ::ruststream_sqlx::spec::Attempt,\
         ::ruststream_sqlx::spec::Payload,\
         ::ruststream_sqlx::spec::own::Ack,\
         ::ruststream_sqlx::spec::own::Retry,)>;",
        "constTABLE:Self::Table=::ruststream_sqlx::InboxSpec::new(\"jobs\",\
         ::ruststream_sqlx::dialect::Column::new(\"id\").generated()).within(\"app\")\
         .lease(::ruststream_sqlx::dialect::Column::new(\"locked_until\"))\
         .fifo_group(::ruststream_sqlx::dialect::Column::new(\"name\"))\
         .partition_key(::ruststream_sqlx::dialect::Column::new(\"tenant\"))\
         .retry_after(::ruststream_sqlx::dialect::Column::new(\"retry_after\"))\
         .attempt(::ruststream_sqlx::dialect::Column::new(\"attempt\").generated())\
         .payload(::ruststream_sqlx::dialect::Column::new(\"payload\"))\
         .data(&[::ruststream_sqlx::dialect::Column::new(\"note\")])\
         .own::<::ruststream_sqlx::spec::own::Ack>()\
         .own::<::ruststream_sqlx::spec::own::Retry>();",
        "fnid(&self)->&i64{&self.id}",
        "impl::ruststream_sqlx::PayloadRowforJob{typeColumn=Vec<u8>;",
        "impl::ruststream_sqlx::KeyRowforJobwherefor<'__c>String:::ruststream_sqlx::KeyColumn{typeKey=String;fnpartition_key(&self)->&String{&self.tenant}}",
        "impl::ruststream_sqlx::AttemptRowforJobwherefor<'__c>i16:::ruststream_sqlx::AttemptColumn{typeAttempt=i16;fnattempt(&self)->&i16{&self.attempt}}",
    ] {
        assert!(impls.contains(expected), "{expected}\n{impls}");
    }
    // The crate's blanket impls write the rest of the contract.
    for machinery in ["Events", "QueueRow", "InboxRow", "LeaseRow", "Input"] {
        assert!(!impls.contains(machinery), "{machinery}: {impls}");
    }
    Ok(())
}

#[test]
fn a_struct_without_a_payload_field_puts_its_row_on_the_carried_lane() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs")]
        struct Job { #[field(id)] id: i64, #[field(headers)] meta: Json<Map>, note: String }
    };
    let impls = expanded(&input)?;
    for expected in [
        "typeTable=::ruststream_sqlx::InboxSpec<(::ruststream_sqlx::spec::Headers,)>;",
        "impl::ruststream_sqlx::__private::InputforJobwhereJob:::core::clone::Clone\
         {typeAxis=::ruststream_sqlx::__private::SoloCarried<Self>;}",
        "impl::ruststream_sqlx::HeaderRowforJobwherefor<'__c>Json<Map>:::ruststream_sqlx::HeaderColumn\
         {typeColumn=Json<Map>;\
         fnheaders_mut(&mutself)->&mutJson<Map>{&mutself.meta}}",
    ] {
        assert!(impls.contains(expected), "{expected}\n{impls}");
    }
    assert!(!impls.contains("PayloadRow"), "{impls}");
    Ok(())
}

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
fn a_generic_struct_holds_its_impls_where_its_settings_keep_the_rules() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", clock = Source)]
        struct Job<Source> { #[field(id)] id: i64, #[sqlx(skip)] source: PhantomData<Source> }
    };
    let impls = expanded(&input)?;
    let valid = "::ruststream_sqlx::InboxSpec<(::ruststream_sqlx::spec::Clock<Source>,)>:\
                 ::ruststream_sqlx::spec::Valid";
    for header in ["InboxTableforJob<Source>", "__private::InputforJob<Source>"] {
        let at = impls
            .find(header)
            .ok_or_else(|| syn::Error::new(input.ident.span(), header))?;
        let clause = &impls[at..at + impls[at..].find('{').unwrap_or_default()];
        assert!(clause.contains(valid), "{header}: {clause}");
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
fn the_rules_the_types_leave_to_the_struct_are_checked() {
    let cases: [(DeriveInput, &str); 9] = [
        (
            parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(payload)] payload: Vec<u8> } },
            "table `jobs` has no `id` field: mark the field that identifies a row with \
             `#[field(id)]`",
        ),
        (
            parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] job_id: i64, #[field(id)] id: i64 } },
            "`job_id` plays `id` already: a role belongs to one field",
        ),
        (
            parse_quote! { #[inbox(table = "jobs")] struct Job { #[field(id)] job_id: i64, #[sqlx(rename = "job_id")] legacy_id: i64 } },
            "`legacy_id` reads column `job_id`, which `job_id` reads already: a column is named \
             in one place",
        ),
        (
            parse_quote! { #[inbox(table = "jobs", advisory_lock = "jobs-{job_id}")] struct Job { #[field(id)] job_id: i64, #[field(locked_until)] locked_until: Option<i64> } },
            "`locked_until` selects the lease form and `advisory_lock` the advisory lock form: \
             a table has one form",
        ),
        (
            parse_quote! { #[inbox(table = "ledger", advisory_lock = "ledger-{id}")] struct Entry { #[field(id)] id: i64, #[field(group, fifo = true)] account: String } },
            "`fifo = true` does not combine with `advisory_lock`: drop it and put the group's \
             field into the lock key, as in `advisory_lock = \"ledger-{account}\"`",
        ),
        (
            parse_quote! { #[inbox(table = "jobs", advisory_lock = "jobs-{tenant}")] struct Job { #[field(id)] job_id: i64 } },
            "the lock key names `tenant`, which is not a field of the struct",
        ),
        (
            parse_quote! { #[inbox(table = "jobs", advisory_lock = "jobs-{tenant}")] struct Job { #[field(id)] job_id: i64, #[sqlx(skip)] tenant: String } },
            "the lock key names `tenant`, a field without a column",
        ),
        (
            parse_quote! { #[inbox(table = "jobs", custom(extend))] struct Job { #[field(id)] job_id: i64 } },
            "`extend` is an event of the lease form: add `#[field(locked_until)]` or drop \
             `extend` from `custom(..)`",
        ),
        (
            parse_quote! { #[inbox(table = "jobs", advisory_lock = "jobs-{job_id}", custom(fetch, claim))] struct Job { #[field(id)] job_id: i64 } },
            "the advisory lock form selects its candidates with their keys itself: drop \
             `claim` from `custom(..)`",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(errors(&input), [expected]);
    }
}

#[test]
fn the_services_lock_belongs_to_the_advisory_lock_form_and_comes_with_its_unlock() {
    let unlocked: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", custom(lock, unlock))]
        struct Job { #[field(id)] job_id: i64 }
    };
    assert_eq!(
        errors(&unlocked),
        [
            "`lock` is an event of the advisory lock form: add `advisory_lock = \"..\"` or \
             drop `lock` from `custom(..)`",
            "`unlock` is an event of the advisory lock form: add `advisory_lock = \"..\"` or \
             drop `unlock` from `custom(..)`",
        ]
    );
    let lock_alone: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", advisory_lock = "jobs-{job_id}", custom(lock))]
        struct Job { #[field(id)] job_id: i64 }
    };
    assert_eq!(
        errors(&lock_alone),
        [
            "`lock` is listed without `unlock`: the service's own lock is released by its own \
          unlock, so list both in `custom(..)`"
        ]
    );
    let unlock_alone: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", advisory_lock = "jobs-{job_id}", custom(unlock))]
        struct Job { #[field(id)] job_id: i64 }
    };
    assert_eq!(
        errors(&unlock_alone),
        [
            "`unlock` is listed without `lock`: the service's own unlock releases what its own \
          lock took, so list both in `custom(..)`"
        ]
    );
    let paired: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", advisory_lock = "jobs-{job_id}", custom(lock, unlock))]
        struct Job { #[field(id)] job_id: i64 }
    };
    assert_eq!(errors(&paired), Vec::<String>::new());
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

#[test]
fn every_broken_rule_is_reported_at_once() {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs")]
        struct Job {
            #[field(payload)]
            payload: Vec<u8>,
            #[sqlx(rename = "payload")]
            body: Vec<u8>,
        }
    };
    assert_eq!(
        errors(&input),
        [
            "table `jobs` has no `id` field: mark the field that identifies a row with \
             `#[field(id)]`",
            "`body` reads column `payload`, which `payload` reads already: a column is named \
             in one place",
        ]
    );
}

#[test]
fn a_message_assembled_from_a_headers_struct_keeps_the_tables_parts_on_it() {
    let cases: [(DeriveInput, &[&str]); 5] = [
        (
            parse_quote! { struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head, #[field(id)] id: i64 } },
            &[
                "`id` plays `id` beside the headers struct `Head`, which describes the queue table: \
               mark the field that plays `id` in `Head`",
            ],
        ),
        (
            parse_quote! { struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head, #[field(payload)] body: Vec<u8> } },
            &[
                "`body` plays `payload` beside the headers struct `Head`: a message assembled from a \
               headers struct is handed to its handler itself, as in row mode, so it holds no \
               payload; drop the role",
            ],
        ),
        (
            parse_quote! {
                #[inbox(table = "jobs", custom(fetch), advisory_lock = "jobs-{id}")]
                struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head }
            },
            &[
                "`table` describes the queue table, which the headers struct `Head` describes: \
                 put it on `Head`'s `#[inbox(..)]`",
                "`advisory_lock` describes the queue table, which the headers struct `Head` \
                 describes: put it on `Head`'s `#[inbox(..)]`",
            ],
        ),
        (
            parse_quote! {
                struct Job {
                    #[field(headers)] #[sqlx(flatten)] headers: Head,
                    #[field(headers)] #[sqlx(flatten)] more: Head,
                    #[sqlx(flatten)] order: Order,
                }
            },
            &[
                "`more` flattens a second headers struct: a message is assembled from one, `Head`",
                "`order` flattens a struct whose columns the default fetch cannot name: list \
                 `fetch` in `#[inbox(custom(..))]` and read the message in the service's own \
                 `Fetch`",
            ],
        ),
        (
            parse_quote! {
                #[inbox(custom(lock))]
                struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head }
            },
            &[
                "`lock` is listed without `unlock`: the service's own lock is released by its own \
               unlock, so list both in `custom(..)`",
            ],
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(errors(&input), expected, "{}", input.ident);
    }
}

#[test]
fn a_message_assembled_from_a_headers_struct_extends_its_description() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head, note: String, #[sqlx(skip)] cache: u8 }
    };
    let impls = expanded(&input)?;
    for expected in [
        "impl::ruststream_sqlx::InboxTableforJob",
        "typeId=<Headas::ruststream_sqlx::InboxHeaders>::Id;",
        "typeTable=::ruststream_sqlx::InboxSpec<<Headas::ruststream_sqlx::InboxHeaders>::Settings>;",
        "constTABLE:Self::Table=<Headas::ruststream_sqlx::InboxHeaders>::TABLE\
         .fetching(&[::ruststream_sqlx::dialect::Column::new(\"note\")]);",
        "fnid(&self)->&Self::Id{<Headas::ruststream_sqlx::InboxHeaders>::id(&self.headers)}",
        "impl::ruststream_sqlx::HeaderFieldsforJob",
        "impl::ruststream_sqlx::__private::InputforJob",
    ] {
        assert!(impls.contains(expected), "{expected}\n{impls}");
    }
    for machinery in ["Events", "QueueRow", "InboxRow", "LeaseRow"] {
        assert!(!impls.contains(machinery), "{machinery}: {impls}");
    }
    // The service's own fetch reads the message wherever its columns live: the description stays
    // the table's, and the message's own events join its settings.
    let fetched: DeriveInput = parse_quote! {
        #[inbox(custom(fetch, claim))]
        struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head, customer: String }
    };
    let impls = expanded(&fetched)?;
    for expected in [
        "typeTable=::ruststream_sqlx::InboxSpec<<<<Headas::ruststream_sqlx::InboxHeaders>::Settings\
         as::ruststream_sqlx::spec::Push<::ruststream_sqlx::spec::own::Claim>>::Out\
         as::ruststream_sqlx::spec::Push<::ruststream_sqlx::spec::own::Fetch>>::Out>;",
        "constTABLE:Self::Table=<Headas::ruststream_sqlx::InboxHeaders>::TABLE\
         .own::<::ruststream_sqlx::spec::own::Claim>().own::<::ruststream_sqlx::spec::own::Fetch>();",
    ] {
        assert!(impls.contains(expected), "{expected}\n{impls}");
    }
    Ok(())
}
