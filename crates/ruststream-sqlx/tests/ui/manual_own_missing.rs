use chrono::{DateTime, Utc};
use ruststream::{Connected, SubscriptionSource};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload, own};
use ruststream_sqlx::{InboxQueue, InboxSpec, InboxTable, PayloadRow, SqlxBroker};
use sqlx::Sqlite;

/// What a mount asks of a descriptor on the inbox broker.
fn subscribes<Source: SubscriptionSource<Connected<SqlxBroker<Sqlite>>>>(source: Source) {
    let _ = source;
}

/// `ack` is set as the service's own and never implemented.
#[derive(sqlx::FromRow)]
struct Unacked {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Unacked {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload, own::Ack)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"))
        .own::<own::Ack>();

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Unacked {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

fn main() {
    subscribes(InboxQueue::<Unacked>::new("jobs"));
}
