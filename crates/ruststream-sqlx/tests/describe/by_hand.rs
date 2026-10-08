//! A table described by hand with `InboxSpec`, as a service writes it: each chain of typed setters
//! describes the same table as the dialect's `TableSpec` builder.

use std::time::SystemTime;

use ruststream_sqlx::dialect::{Column, Form, KeyPart, Mode, TableSpec, level};
use ruststream_sqlx::spec::{
    Advisory, Attempt, Clock, Fifo, HeaderFields, Headers, Key, Lease, Opens, Payload, ProcessedAt,
    RetryAfter, own,
};
use ruststream_sqlx::{DatabaseClock, InboxSpec, SystemClock};

const DATA: &[Column<'static>] = &[Column::new("recipient"), Column::new("subject")];
const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("tenant")];

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
