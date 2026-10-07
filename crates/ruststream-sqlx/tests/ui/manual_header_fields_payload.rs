use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{HeaderFields, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable};

/// A message assembled from header fields, with a payload.
struct AssembledWithPayload {
    id: i64,
}

impl InboxTable for AssembledWithPayload {
    type Id = i64;
    type Table = InboxSpec<(HeaderFields, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("orders", Column::new("id"))
        .header_fields()
        .payload(Column::new("body"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

fn main() {}
