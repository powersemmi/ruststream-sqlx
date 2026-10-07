use ruststream_sqlx::Outbox;

#[derive(Outbox)]
#[outbox(table = "outbox", custom(publish))]
struct PublishListed {
    #[field(id)]
    id: i64,
}

#[derive(Outbox)]
#[outbox(table = "outbox", custom(claim))]
struct InboxEvent {
    #[field(id)]
    id: i64,
}

#[derive(Outbox)]
#[outbox(table = "outbox", custom(ack, ack))]
struct ListedTwice {
    #[field(id)]
    id: i64,
}

#[derive(Outbox)]
#[outbox(table = "outbox", advisory_lock = "outbox-{id}")]
struct InboxOption {
    #[field(id)]
    id: i64,
}

fn main() {}
