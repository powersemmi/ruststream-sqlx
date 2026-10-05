use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::TableSpec;
use ruststream_sqlx::{DatabaseClock, Inbox, InboxRow, SystemClock, TimeSource};

/// A lease table whose clock is a parameter: the database's clock is refused where the parameter
/// is known.
#[derive(Inbox)]
#[inbox(table = "jobs", clock = Source)]
struct OnAnyClock<Source: TimeSource> {
    #[field(id)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
    #[sqlx(skip)]
    source: PhantomData<Source>,
}

const ON_THE_HOST: TableSpec<'static> = <OnAnyClock<SystemClock> as InboxRow>::SPEC;
const ON_THE_DATABASE: TableSpec<'static> = <OnAnyClock<DatabaseClock> as InboxRow>::SPEC;

fn main() {
    let _ = (ON_THE_HOST, ON_THE_DATABASE);
}
