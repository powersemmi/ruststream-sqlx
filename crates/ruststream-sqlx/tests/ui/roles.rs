use ruststream_sqlx::Inbox;

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct UnknownRole {
    #[field(identity)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct TwoRoles {
    #[field(id, group)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct EmptyField {
    #[field(id)]
    id: i64,
    #[field()]
    payload: Vec<u8>,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct GeneratedTwice {
    #[field(id, generated)]
    #[field(generated)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct FifoOutsideGroup {
    #[field(id)]
    id: i64,
    #[field(priority, fifo = true)]
    priority: i16,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct FifoTwice {
    #[field(id)]
    id: i64,
    #[field(group, fifo = true, fifo = true)]
    name: String,
}

fn main() {}
