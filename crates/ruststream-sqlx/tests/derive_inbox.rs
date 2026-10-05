//! What `#[derive(Inbox)]` reads from a struct: the table, the columns and their roles, the form.

#![cfg(feature = "inbox")]

use std::time::SystemTime;

use ruststream_sqlx::dialect::{Column, Form, KeyPart, Role, TableSpec};
use ruststream_sqlx::{Inbox, InboxRow};

/// The table of the derive's own documentation: every role the row lock form reads.
#[derive(Inbox)]
#[inbox(table = "email_jobs", schema = "app")]
#[cfg_attr(
    not(feature = "postgres"),
    expect(
        dead_code,
        reason = "a queue row is read by the broker, never by this test"
    )
)]
struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(priority)]
    priority: i16,
    #[field(retry_after)]
    retry_after: SystemTime,
    #[field(attempt)]
    attempt: i16,
    #[field(processed_at)]
    processed_at: Option<SystemTime>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[test]
fn a_flat_struct_describes_its_table() {
    assert_eq!(
        SendEmail::SPEC,
        TableSpec::new(
            "email_jobs",
            Column::new("job_id").generated(),
            Form::RowLock
        )
        .within("app")
        .group(Column::new("name"))
        .priority(Column::new("priority"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .processed_at(Column::new("processed_at"))
        .payload(Column::new("payload"))
    );
}

#[test]
fn the_id_type_is_the_id_fields_type() {
    fn id_of<Row: InboxRow<Id = i64>>() -> &'static str {
        Row::SPEC.table()
    }
    assert_eq!(id_of::<SendEmail>(), "email_jobs");
}

/// sqlx's naming rules: `rename` is taken as written, `rename_all` recases the rest, a raw
/// identifier loses its `r#`, and skipped fields read no column. A column of the data may be
/// filled in by the database.
#[derive(Inbox)]
#[inbox(table = "Email Jobs")]
#[sqlx(rename_all = "camelCase")]
#[expect(
    dead_code,
    reason = "a queue row is read by the broker, never by this test"
)]
struct Renamed {
    #[field(id)]
    job_id: i64,
    #[sqlx(rename = "queue_name")]
    #[field(group)]
    group_name: String,
    r#type: String,
    #[sqlx(skip)]
    cache: Vec<u8>,
    #[sqlx(json, default)]
    attachments: Vec<String>,
    #[field(generated)]
    created_at: SystemTime,
}

#[test]
fn column_names_follow_sqlx() {
    assert_eq!(
        Renamed::SPEC,
        TableSpec::new("Email Jobs", Column::new("jobId"), Form::RowLock)
            .group(Column::new("queue_name"))
            .data(&[
                Column::new("type"),
                Column::new("attachments"),
                Column::new("createdAt").generated(),
            ])
    );
}

/// The roles the other structs leave out: the partition key and the headers.
#[derive(Inbox)]
#[inbox(table = "orders")]
struct Keyed {
    #[field(id)]
    id: i64,
    #[field(partition_key)]
    customer: String,
    #[field(headers)]
    meta: Vec<u8>,
}

#[test]
fn the_partition_key_and_the_headers_reach_their_slots() {
    assert_eq!(
        Keyed::SPEC,
        TableSpec::new("orders", Column::new("id"), Form::RowLock)
            .partition_key(Column::new("customer"))
            .headers(Column::new("meta"))
    );
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
#[cfg_attr(
    not(feature = "postgres"),
    expect(
        dead_code,
        reason = "a queue row is read by the broker, never by this test"
    )
)]
struct Leased {
    #[field(id)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<SystemTime>,
}

#[test]
fn a_locked_until_field_selects_the_lease_form() {
    assert_eq!(
        Leased::SPEC.form(),
        Form::Lease(Column::new("locked_until"))
    );
}

#[derive(Inbox)]
#[inbox(table = "jobs", advisory_lock = "jobs-{tenant}-{type}")]
#[cfg_attr(
    not(feature = "postgres"),
    expect(
        dead_code,
        reason = "a queue row is read by the broker, never by this test"
    )
)]
struct Advisory {
    #[field(id)]
    id: i64,
    #[sqlx(rename = "tenant_id")]
    tenant: String,
    r#type: String,
}

#[test]
fn an_advisory_key_reads_the_columns_of_the_named_fields() {
    const KEY: &[KeyPart<'static>] = &[
        KeyPart::Literal("jobs-"),
        KeyPart::Column("tenant_id"),
        KeyPart::Literal("-"),
        KeyPart::Column("type"),
    ];
    assert_eq!(Advisory::SPEC.form(), Form::Advisory(KEY));
}

#[derive(Inbox)]
#[inbox(table = "ledger")]
#[cfg_attr(
    not(feature = "postgres"),
    expect(
        dead_code,
        reason = "a queue row is read by the broker, never by this test"
    )
)]
struct Ledger {
    #[field(id)]
    id: i64,
    #[field(group, fifo = true)]
    account: String,
}

#[test]
fn a_fifo_group_marks_the_table() {
    assert_eq!(
        Ledger::SPEC,
        TableSpec::new("ledger", Column::new("id"), Form::RowLock)
            .fifo_group(Column::new("account"))
    );
    assert!(Ledger::SPEC.is_fifo());
    assert_eq!(
        Ledger::SPEC.column(Role::Group).map(|column| column.name()),
        Some("account")
    );
}

/// The columns a flattened struct reads stay with that struct.
#[derive(Inbox)]
#[inbox(table = "jobs")]
#[expect(
    dead_code,
    reason = "a queue row is read by the broker, never by this test"
)]
struct Flattening {
    #[field(id)]
    id: i64,
    #[sqlx(flatten)]
    envelope: Envelope,
}

#[expect(
    dead_code,
    reason = "a queue row is read by the broker, never by this test"
)]
struct Envelope {
    subject: String,
}

#[test]
fn a_flattened_field_makes_statements_select_everything() {
    assert_eq!(
        Flattening::SPEC,
        TableSpec::new("jobs", Column::new("id"), Form::RowLock).selecting_all()
    );
}

/// A generic struct: the parameters reach the impl, the id type among them.
#[derive(Inbox)]
#[inbox(table = "jobs")]
#[cfg_attr(
    not(feature = "postgres"),
    expect(
        dead_code,
        reason = "a queue row is read by the broker, never by this test"
    )
)]
struct Generic<Key, Body> {
    #[field(id)]
    key: Key,
    #[sqlx(json)]
    body: Body,
}

#[test]
fn a_generic_struct_derives_for_every_instantiation() {
    fn key_of<Row: InboxRow<Id = Key>, Key>() -> &'static str {
        Row::SPEC.table()
    }
    assert_eq!(key_of::<Generic<i64, Vec<String>>, i64>(), "jobs");
    assert_eq!(key_of::<Generic<String, u8>, String>(), "jobs");
}
