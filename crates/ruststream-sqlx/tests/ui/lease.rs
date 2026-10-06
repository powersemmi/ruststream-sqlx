use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::{DatabaseClock, Inbox, InboxQueue, InboxRow};

#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
struct Locked {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", clock = DatabaseClock)]
struct OnDatabaseTime {
    #[field(id)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

fn main() {
    let _ = InboxQueue::<Locked>::new("jobs").lease(Duration::from_secs(30));
    let _ = <OnDatabaseTime as InboxRow>::SPEC;
}
