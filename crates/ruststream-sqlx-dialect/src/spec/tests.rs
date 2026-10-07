//! What a description holds: a slot per role, its columns in order, its form, its groups,
//! its clock and its opening.

use std::any::Any;
use std::panic;

use super::{Column, Form, Role, TableSpec};
use crate::form::KeyPart;
use crate::opening::{Isolation, Mode, Opening};

const EMAILS: TableSpec<'static> = TableSpec::new(
    "email_jobs",
    Column::new("job_id").generated(),
    Form::RowLock,
)
.within("app")
.group(Column::new("name"))
.partition_key(Column::new("customer"))
.priority(Column::new("priority"))
.retry_after(Column::new("retry_after"))
.attempt(Column::new("attempt"))
.processed_at(Column::new("processed_at"))
.headers(Column::new("headers"))
.payload(Column::new("payload"))
.data(&[Column::new("subject")]);

const ID_KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("id")];

fn names(spec: &TableSpec<'_>) -> Vec<String> {
    spec.columns()
        .map(|column| column.name().to_owned())
        .collect()
}

#[test]
fn every_role_has_its_slot() {
    let expected = [
        (Role::Id, Some("job_id")),
        (Role::Group, Some("name")),
        (Role::PartitionKey, Some("customer")),
        (Role::Priority, Some("priority")),
        (Role::RetryAfter, Some("retry_after")),
        (Role::Attempt, Some("attempt")),
        (Role::LockedUntil, None),
        (Role::ProcessedAt, Some("processed_at")),
        (Role::Headers, Some("headers")),
        (Role::Payload, Some("payload")),
    ];
    for (role, column) in expected {
        assert_eq!(EMAILS.column(role).map(|column| column.name()), column);
    }
}

#[test]
fn columns_list_the_roles_in_order_then_the_data() {
    assert_eq!(
        names(&EMAILS),
        [
            "job_id",
            "name",
            "customer",
            "priority",
            "retry_after",
            "attempt",
            "processed_at",
            "headers",
            "payload",
            "subject",
        ]
    );
}

#[test]
fn a_spec_carries_what_its_builders_set() {
    assert_eq!(EMAILS.table(), "email_jobs");
    assert_eq!(EMAILS.schema(), Some("app"));
    assert_eq!(EMAILS.form(), Form::RowLock);
    assert!(!EMAILS.is_fifo());
    assert!(!EMAILS.selects_all());
    assert!(EMAILS.selecting_all().selects_all());
}

#[test]
fn a_fifo_group_is_a_group_that_keeps_its_order() {
    let base = TableSpec::new("jobs", Column::new("id"), Form::RowLock);
    let fifo = base.fifo_group(Column::new("account"));
    assert!(fifo.is_fifo());
    assert_eq!(
        fifo.column(Role::Group).map(|column| column.name()),
        Some("account")
    );
    assert!(!base.group(Column::new("account")).is_fifo());
}

/// What a panic says, whichever way its message was formatted.
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or_default()
}

#[test]
fn a_setting_given_twice_is_refused_naming_it() {
    type Twice = fn(TableSpec<'static>) -> TableSpec<'static>;
    const A: &[Column<'static>] = &[Column::new("a")];
    const B: &[Column<'static>] = &[Column::new("b")];
    let base = TableSpec::new("jobs", Column::new("id"), Form::RowLock);
    let cases: [(&str, Twice); 18] = [
        ("the schema", |spec| spec.within("app").within("ops")),
        ("`group`", |spec| {
            spec.group(Column::new("a")).group(Column::new("b"))
        }),
        ("`group`", |spec| {
            spec.group(Column::new("a")).fifo_group(Column::new("b"))
        }),
        ("`group`", |spec| {
            spec.fifo_group(Column::new("a")).group(Column::new("b"))
        }),
        ("`group`", |spec| {
            spec.fifo_group(Column::new("a"))
                .fifo_group(Column::new("b"))
        }),
        ("`partition_key`", |spec| {
            spec.partition_key(Column::new("a"))
                .partition_key(Column::new("b"))
        }),
        ("`priority`", |spec| {
            spec.priority(Column::new("a")).priority(Column::new("b"))
        }),
        ("`retry_after`", |spec| {
            spec.retry_after(Column::new("a"))
                .retry_after(Column::new("b"))
        }),
        ("`attempt`", |spec| {
            spec.attempt(Column::new("a")).attempt(Column::new("b"))
        }),
        ("`processed_at`", |spec| {
            spec.processed_at(Column::new("a"))
                .processed_at(Column::new("b"))
        }),
        ("`headers`", |spec| {
            spec.headers(Column::new("a")).headers(Column::new("b"))
        }),
        ("`payload`", |spec| {
            spec.payload(Column::new("a")).payload(Column::new("b"))
        }),
        ("data columns", |spec| spec.data(A).data(B)),
        ("data columns", |spec| spec.data(A).data(&[])),
        ("fetched columns", |spec| spec.fetching(A).fetching(B)),
        ("opening", |spec| {
            spec.isolation(Isolation::Serializable)
                .isolation(Isolation::ReadCommitted)
        }),
        ("opening", |spec| {
            spec.mode(Mode::Immediate).mode(Mode::Exclusive)
        }),
        ("opening", |spec| {
            spec.isolation(Isolation::Serializable)
                .mode(Mode::Immediate)
        }),
    ];
    for (setting, twice) in cases {
        let refused =
            panic::catch_unwind(|| twice(base)).expect_err("a setting given twice is refused");
        let message = panic_message(&*refused);
        assert!(
            message.contains(setting) && message.contains("twice"),
            "`{setting}`: {message}"
        );
    }
}

#[test]
fn a_switch_given_twice_stays_on() {
    let twice = EMAILS
        .selecting_all()
        .selecting_all()
        .database_clock()
        .database_clock();
    assert!(twice.selects_all());
    assert!(twice.uses_database_clock());
}

#[test]
fn an_empty_list_leaves_room_for_the_list() {
    const NOTE: &[Column<'static>] = &[Column::new("note")];
    let listed = EMAILS.fetching(&[]).fetching(NOTE);
    assert_eq!(
        listed.fetched_columns().first().map(Column::name),
        Some("note")
    );
    let bare = TableSpec::new("jobs", Column::new("id"), Form::RowLock);
    assert_eq!(
        bare.data(&[])
            .data(&[Column::new("subject")])
            .data_columns()
            .len(),
        1
    );
}

#[test]
fn the_lease_form_carries_the_locked_until_column() {
    let leased = TableSpec::new(
        "jobs",
        Column::new("id"),
        Form::Lease(Column::new("locked_until")),
    );
    assert_eq!(
        leased.column(Role::LockedUntil).map(|column| column.name()),
        Some("locked_until")
    );
    assert_eq!(names(&leased), ["id", "locked_until"]);
    let advisory = TableSpec::new("jobs", Column::new("id"), Form::Advisory(ID_KEY));
    assert_eq!(advisory.column(Role::LockedUntil), None);
}

#[test]
fn the_database_clock_is_a_switch_of_the_description() {
    assert!(!EMAILS.uses_database_clock());
    let on_database_time = EMAILS.database_clock();
    assert!(on_database_time.uses_database_clock());
    assert_eq!(names(&on_database_time), names(&EMAILS));
}

#[test]
fn a_table_opens_its_transactions_at_the_opening_it_names() {
    assert_eq!(EMAILS.opening(), Opening::Default);
    let serializable =
        TableSpec::new("jobs", Column::new("id"), Form::RowLock).isolation(Isolation::Serializable);
    assert_eq!(
        serializable.opening(),
        Opening::Isolation(Isolation::Serializable)
    );
    let immediate = TableSpec::new("jobs", Column::new("id"), Form::RowLock).mode(Mode::Immediate);
    assert_eq!(immediate.opening(), Opening::Mode(Mode::Immediate));
    assert_eq!(names(&immediate), ["id"], "an opening adds no column");
}

#[test]
fn a_message_assembled_from_the_table_reads_its_own_columns_after_the_data() {
    const ASSEMBLED: TableSpec<'static> =
        TableSpec::new("order_jobs", Column::new("job_id"), Form::RowLock)
            .group(Column::new("name"))
            .data(&[Column::new("tenant")])
            .fetching(&[Column::new("note")]);
    let assembled = ASSEMBLED;
    assert_eq!(names(&assembled), ["job_id", "name", "tenant", "note"]);
    assert!(!assembled.selects_all(), "every column is named");
    assert_eq!(
        assembled
            .data_columns()
            .iter()
            .map(Column::name)
            .collect::<Vec<_>>(),
        ["tenant"]
    );
    assert_eq!(
        assembled
            .fetched_columns()
            .iter()
            .map(Column::name)
            .collect::<Vec<_>>(),
        ["note"]
    );
}
