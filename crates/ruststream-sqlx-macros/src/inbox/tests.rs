use quote::format_ident;
use syn::{DeriveInput, parse_quote};

use super::expand;

fn errors(input: &DeriveInput) -> Vec<String> {
    expand(input).map_or_else(
        |error| error.into_iter().map(|error| error.to_string()).collect(),
        |_| Vec::new(),
    )
}

#[test]
fn a_declared_opening_reaches_the_description_and_the_type() -> syn::Result<()> {
    let cases = [
        (
            "isolation",
            "read_uncommitted",
            "Isolation",
            "ReadUncommitted",
        ),
        ("isolation", "read_committed", "Isolation", "ReadCommitted"),
        (
            "isolation",
            "repeatable_read",
            "Isolation",
            "RepeatableRead",
        ),
        ("isolation", "serializable", "Isolation", "Serializable"),
        ("mode", "deferred", "Mode", "Deferred"),
        ("mode", "immediate", "Mode", "Immediate"),
        ("mode", "exclusive", "Mode", "Exclusive"),
    ];
    for (key, word, kind, name) in cases {
        let (key, word) = (format_ident!("{key}"), format_ident!("{word}"));
        let input: DeriveInput = parse_quote! {
            #[inbox(table = "jobs", #key = #word)]
            struct Job { #[field(id)] id: i64 }
        };
        let impls = expand(&input)?.to_string();
        assert!(
            impls.contains(&format!(
                ". {key} (:: ruststream_sqlx :: dialect :: {kind} :: {name})"
            )),
            "{key} = {word}: {impls}"
        );
        assert!(
            impls.contains(&format!(
                "type Opening = :: ruststream_sqlx :: dialect :: level :: {name} ;"
            )),
            "{key} = {word}: {impls}"
        );
    }
    // A table that names neither opens at its database's default, which every dialect opens.
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs")]
        struct Job { #[field(id)] id: i64 }
    };
    let impls = expand(&input)?.to_string();
    assert!(impls.contains("type Opening = () ;"), "{impls}");
    assert!(
        !impls.contains(". isolation (") && !impls.contains(". mode ("),
        "{impls}"
    );
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
fn a_listed_lock_and_unlock_run_the_services_own() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", advisory_lock = "jobs-{id}", custom(lock, unlock))]
        struct Job { #[field(id)] id: i64 }
    };
    let impls = expand(&input)?.to_string();
    for expected in [
        "custom_lock : true",
        "custom_unlock : true",
        "Self : :: ruststream_sqlx :: Lock < __DB >",
        "Self : :: ruststream_sqlx :: Unlock < __DB >",
        "< Self as :: ruststream_sqlx :: Lock < __DB >> :: lock (conn , key)",
        "< Self as :: ruststream_sqlx :: Unlock < __DB >> :: unlock (conn , key)",
    ] {
        assert!(impls.contains(expected), "{expected}: {impls}");
    }
    // The crate's own lock and unlock run for a table that lists neither.
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", advisory_lock = "jobs-{id}")]
        struct Job { #[field(id)] id: i64 }
    };
    let impls = expand(&input)?.to_string();
    for expected in [
        "custom_lock : false",
        "custom_unlock : false",
        ":: ruststream_sqlx :: __private :: lock :: < __DB , Self > (conn , cx , key)",
        ":: ruststream_sqlx :: __private :: unlock :: < __DB , Self > (conn , cx , key)",
    ] {
        assert!(impls.contains(expected), "{expected}: {impls}");
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
fn a_messages_own_columns_join_its_headers_structs_description() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head, note: String, #[sqlx(skip)] cache: u8 }
    };
    let impls = expand(&input)?.to_string();
    assert!(
        impls.contains(
            "< Head as :: ruststream_sqlx :: InboxHeaders > :: SPEC . fetching (& [:: ruststream_sqlx :: dialect :: Column :: new (\"note\")])"
        ),
        "{impls}"
    );
    assert!(impls.contains("type Headers = :: ruststream_sqlx :: __private :: LazyHeaders ;"));
    // The service's own fetch reads the message wherever its columns live: the description stays
    // the table's.
    let fetched: DeriveInput = parse_quote! {
        #[inbox(custom(fetch, claim))]
        struct Job { #[field(headers)] #[sqlx(flatten)] headers: Head, customer: String }
    };
    let impls = expand(&fetched)?.to_string();
    assert!(
        impls.contains("const SPEC : :: ruststream_sqlx :: dialect :: TableSpec < 'static > = < Head as :: ruststream_sqlx :: InboxHeaders > :: SPEC ;"),
        "{impls}"
    );
    assert!(
        impls.contains("OwnClaim"),
        "the listed claim is held to the form: {impls}"
    );
    Ok(())
}
