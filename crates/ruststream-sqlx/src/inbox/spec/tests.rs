use std::any::TypeId;
use std::time::SystemTime;

use ruststream_sqlx_dialect::{Column, Form, KeyPart, Mode, TableSpec, level};

use super::{
    Advisory, Attempt, Clock, Declaration, Fifo, HeaderFields, Headers, Key, Lease, Opens, Payload,
    ProcessedAt, RetryAfter, Set, Unset, own,
};
use crate::{DatabaseClock, InboxSpec, SystemClock};

const DATA: &[Column<'static>] = &[Column::new("recipient"), Column::new("subject")];
const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("tenant")];

/// Whether two types are one.
fn same<Left: 'static, Right: 'static>() -> bool {
    TypeId::of::<Left>() == TypeId::of::<Right>()
}

type LeaseTable = InboxSpec<(
    Lease<SystemTime>,
    Payload,
    Attempt,
    RetryAfter<SystemTime>,
    ProcessedAt<SystemTime>,
    Key,
)>;

// The form is set after columns that the rebuilt description has to keep.
const LEASE_TABLE: LeaseTable = InboxSpec::new("email_jobs", Column::new("job_id").generated())
    .within("app")
    .group(Column::new("name"))
    .priority(Column::new("priority"))
    .data(DATA)
    .selecting_all()
    .lease(Column::new("locked_until"))
    .payload(Column::new("payload"))
    .attempt(Column::new("attempt").generated())
    .retry_after(Column::new("retry_after"))
    .processed_at(Column::new("processed_at"))
    .partition_key(Column::new("tenant"));

#[test]
fn a_lease_table_describes_what_the_dialect_builder_describes() {
    let expected = TableSpec::new(
        "email_jobs",
        Column::new("job_id").generated(),
        Form::Lease(Column::new("locked_until")),
    )
    .within("app")
    .group(Column::new("name"))
    .priority(Column::new("priority"))
    .data(DATA)
    .selecting_all()
    .payload(Column::new("payload"))
    .attempt(Column::new("attempt").generated())
    .retry_after(Column::new("retry_after"))
    .processed_at(Column::new("processed_at"))
    .partition_key(Column::new("tenant"));
    assert_eq!(LEASE_TABLE.spec(), expected);
}

#[test]
fn a_lease_table_folds_each_setting_into_its_own_slot() {
    type Settings = (
        Lease<SystemTime>,
        Payload,
        Attempt,
        RetryAfter<SystemTime>,
        ProcessedAt<SystemTime>,
        Key,
    );
    assert!(same::<
        <Settings as Declaration>::Form,
        Set<Lease<SystemTime>>,
    >());
    assert!(same::<<Settings as Declaration>::Message, Set<Payload>>());
    assert!(same::<<Settings as Declaration>::Key, Set<Key>>());
    assert!(same::<<Settings as Declaration>::Attempt, Set<Attempt>>());
    assert!(same::<
        <Settings as Declaration>::RetryAfter,
        Set<RetryAfter<SystemTime>>,
    >());
    assert!(same::<
        <Settings as Declaration>::ProcessedAt,
        Set<ProcessedAt<SystemTime>>,
    >());
    assert!(same::<<Settings as Declaration>::Headers, Unset>());
    assert!(same::<<Settings as Declaration>::Clock, Unset>());
    assert!(same::<<Settings as Declaration>::OwnAck, Unset>());
}

type AdvisoryTable = InboxSpec<(
    Clock<DatabaseClock>,
    Advisory,
    Opens<level::Immediate>,
    HeaderFields,
)>;

const ADVISORY_TABLE: AdvisoryTable = InboxSpec::new("order_jobs", Column::new("id"))
    .fetching(DATA)
    .clock::<DatabaseClock>()
    .advisory(KEY)
    .opens::<level::Immediate>()
    .header_fields();

#[test]
fn an_advisory_table_on_the_database_clock_describes_what_the_dialect_builder_describes() {
    let expected = TableSpec::new("order_jobs", Column::new("id"), Form::Advisory(KEY))
        .fetching(DATA)
        .database_clock()
        .mode(Mode::Immediate);
    assert_eq!(ADVISORY_TABLE.spec(), expected);
}

type FifoTable = InboxSpec<(Fifo, Headers, own::Ack, Clock<SystemClock>)>;

const FIFO_TABLE: FifoTable = InboxSpec::new("orders", Column::new("id").generated())
    .fifo_group(Column::new("customer"))
    .headers(Column::new("headers"))
    .own::<own::Ack>()
    .clock::<SystemClock>();

#[test]
fn a_fifo_row_lock_table_with_an_own_event_describes_what_the_dialect_builder_describes() {
    let expected = TableSpec::new("orders", Column::new("id").generated(), Form::RowLock)
        .fifo_group(Column::new("customer"))
        .headers(Column::new("headers"));
    assert_eq!(FIFO_TABLE.spec(), expected);
}

#[test]
fn an_own_event_folds_into_its_own_slot() {
    type Settings = (Fifo, Headers, own::Ack, Clock<SystemClock>);
    assert!(same::<<Settings as Declaration>::OwnAck, Set<own::Ack>>());
    assert!(same::<<Settings as Declaration>::Fifo, Set<Fifo>>());
    assert!(same::<<Settings as Declaration>::OwnClaim, Unset>());
    assert!(same::<<Settings as Declaration>::Form, Unset>());
}
