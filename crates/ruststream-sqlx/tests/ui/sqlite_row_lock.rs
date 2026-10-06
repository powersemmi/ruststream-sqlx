use ruststream::SubscriptionSource;
use ruststream_sqlx::{ConnectedSqlxBroker, Inbox, InboxQueue};

#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
struct Locked {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

async fn open(connected: &ConnectedSqlxBroker<sqlx::Sqlite>) {
    let _ = InboxQueue::<Locked>::new("jobs").subscribe(connected).await;
}

fn main() {
    let _ = open;
}
