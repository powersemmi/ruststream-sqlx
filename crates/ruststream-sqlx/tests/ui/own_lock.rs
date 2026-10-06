use ruststream::{Connected, SubscriptionSource};
use ruststream_sqlx::{Inbox, InboxQueue, Lock, SqlxBroker};
use sqlx::{PgConnection, Postgres};

/// What a mount asks of a descriptor on the inbox broker.
fn subscribes<Source: SubscriptionSource<Connected<SqlxBroker<Postgres>>>>(source: Source) {
    let _ = source;
}

/// `lock` and `unlock` are listed as the service's own and neither is implemented.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", advisory_lock = "jobs-{id}", custom(lock, unlock))]
struct Unlocked {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

/// The lock is implemented and the unlock is not.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", advisory_lock = "jobs-{id}", custom(lock, unlock))]
struct HalfLocked {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Lock<Postgres> for HalfLocked {
    async fn lock(_: &mut PgConnection, _: &str) -> Result<bool, sqlx::Error> {
        Ok(true)
    }
}

fn main() {
    subscribes(InboxQueue::<Unlocked>::new("jobs"));
    subscribes(InboxQueue::<HalfLocked>::new("jobs"));
}
