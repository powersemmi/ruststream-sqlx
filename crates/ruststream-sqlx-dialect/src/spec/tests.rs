//! What a description holds: a slot per role, its columns in order, its form, its groups,
//! its clock and its opening.

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
fn a_slot_holds_one_column() {
    let regrouped = EMAILS.group(Column::new("queue"));
    assert_eq!(
        regrouped.column(Role::Group).map(|column| column.name()),
        Some("queue")
    );
    assert_eq!(names(&regrouped).len(), names(&EMAILS).len());
}

#[test]
fn a_fifo_group_is_a_group_that_keeps_its_order() {
    let fifo = EMAILS.fifo_group(Column::new("account"));
    assert!(fifo.is_fifo());
    assert_eq!(
        fifo.column(Role::Group).map(|column| column.name()),
        Some("account")
    );
    assert!(!fifo.group(Column::new("account")).is_fifo());
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
fn a_table_opens_its_transactions_at_the_last_opening_it_names() {
    assert_eq!(EMAILS.opening(), Opening::Default);
    let serializable =
        TableSpec::new("jobs", Column::new("id"), Form::RowLock).isolation(Isolation::Serializable);
    assert_eq!(
        serializable.opening(),
        Opening::Isolation(Isolation::Serializable)
    );
    let immediate = serializable.mode(Mode::Immediate);
    assert_eq!(immediate.opening(), Opening::Mode(Mode::Immediate));
    assert_eq!(
        immediate.isolation(Isolation::ReadCommitted).opening(),
        Opening::Isolation(Isolation::ReadCommitted)
    );
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
    let unassembled = assembled.fetching(&[]);
    assert_eq!(names(&unassembled), ["job_id", "name", "tenant"]);
}
