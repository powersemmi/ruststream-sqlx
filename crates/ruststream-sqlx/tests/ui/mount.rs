use ruststream::{Connected, SubscriptionSource};
use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
use sqlx::Postgres;

/// What a mount asks of a descriptor on the inbox broker.
fn subscribes<Source: SubscriptionSource<Connected<SqlxBroker<Postgres>>>>(source: Source) {
    let _ = source;
}

/// No payload field: payload mode has no message to hand a handler.
#[derive(Inbox, sqlx::FromRow, Clone)]
#[inbox(table = "jobs")]
struct NoPayload {
    #[field(id)]
    id: i64,
}

/// `ack` is listed as the service's own and never implemented.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", custom(ack))]
struct Unacked {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

fn main() {
    subscribes(InboxQueue::<NoPayload>::new("jobs"));
    subscribes(InboxQueue::<Unacked>::new("jobs"));
}
