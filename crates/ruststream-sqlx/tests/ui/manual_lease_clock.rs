use chrono::{DateTime, Utc};
use ruststream_sqlx::DatabaseClock;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Clock, Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable};

/// The lease form on the database's clock.
struct OnDatabaseTime {
    id: i64,
}

impl InboxTable for OnDatabaseTime {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload, Clock<DatabaseClock>)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"))
        .clock::<DatabaseClock>();

    fn id(&self) -> &i64 {
        &self.id
    }
}

fn main() {}
