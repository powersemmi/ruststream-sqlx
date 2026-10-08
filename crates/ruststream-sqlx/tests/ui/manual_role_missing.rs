use chrono::{DateTime, Utc};
use ruststream::{Connected, SubscriptionSource};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{InboxQueue, InboxSpec, InboxTable, PayloadRow, SqlxBroker};
use sqlx::Sqlite;

/// What a mount asks of a descriptor on the inbox broker.
fn subscribes<Source: SubscriptionSource<Connected<SqlxBroker<Sqlite>>>>(source: Source) {
    let _ = source;
}

/// An attempt column the row never reads.
#[derive(sqlx::FromRow)]
struct Uncounted {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Uncounted {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Attempt, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id"))
        .lease(Column::new("locked_until"))
        .attempt(Column::new("attempt").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Uncounted {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// A payload column the row never lends.
#[derive(sqlx::FromRow)]
struct Unlent {
    id: i64,
}

impl InboxTable for Unlent {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

fn main() {
    subscribes(InboxQueue::<Uncounted>::new("jobs"));
    subscribes(InboxQueue::<Unlent>::new("jobs"));
}
