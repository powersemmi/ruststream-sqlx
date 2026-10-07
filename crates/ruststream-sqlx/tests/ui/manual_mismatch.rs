use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable};

/// The chain sets the payload before the lease, the type lists them the other way round.
struct OutOfOrder {
    id: i64,
}

impl InboxTable for OutOfOrder {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id"))
        .payload(Column::new("payload"))
        .lease(Column::new("locked_until"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

fn main() {}
