use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::{Column, TableSpec};
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxRow, InboxSpec, InboxTable, PayloadRow};

/// The group's column is a data column too.
#[derive(sqlx::FromRow)]
struct Doubled {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Doubled {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id"))
        .lease(Column::new("locked_until"))
        .group(Column::new("name"))
        .payload(Column::new("payload"))
        .data(&[Column::new("name")]);

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Doubled {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

// A subscription reads the description when the service is built; `cargo check` evaluates a
// constant only where an item names it, as this one does.
const SPEC: TableSpec<'static> = <Doubled as InboxRow>::SPEC;

fn main() {
    let _ = SPEC;
}
