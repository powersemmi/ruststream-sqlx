use ruststream::{Connected, OutgoingMessage, SubscriptionSource};
use ruststream_sqlx::{Inbox, InboxQueue, Publish, SqlxBroker};
use sqlx::{PgConnection, PgPool, Postgres};

/// What a mount asks of a descriptor on the inbox broker.
fn subscribes<Source: SubscriptionSource<Connected<SqlxBroker<Postgres>>>>(source: Source) {
    let _ = source;
}

/// No payload field: a table in row mode, which a route has no column to write a message into.
#[derive(Inbox, sqlx::FromRow, Clone)]
#[inbox(table = "jobs")]
struct NoPayload {
    #[field(id)]
    id: i64,
}

impl Publish<Postgres> for NoPayload {
    async fn publish(_: &mut PgConnection, _: &OutgoingMessage<'_>) -> Result<(), sqlx::Error> {
        Ok(())
    }
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

/// A broker that routes a name to a table in row mode.
fn routed(pool: PgPool) -> SqlxBroker<Postgres> {
    SqlxBroker::new(pool).route::<NoPayload>("jobs")
}

fn main() {
    subscribes(InboxQueue::<Unacked>::new("jobs"));
    let _ = routed;
}
