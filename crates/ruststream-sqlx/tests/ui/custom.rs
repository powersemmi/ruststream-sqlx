use ruststream_sqlx::Inbox;

#[derive(Inbox)]
#[inbox(table = "jobs", custom(lock))]
struct UnknownEvent {
    #[field(id)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs", custom(publish))]
struct PublishListed {
    #[field(id)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs", custom(ack, ack))]
struct ListedTwice {
    #[field(id)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs", clock = SystemClock, clock = SystemClock)]
struct TwoClocks {
    #[field(id)]
    id: i64,
}

fn main() {}
