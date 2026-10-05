use ruststream_sqlx::Inbox;

// A row without an identity cannot be settled.
#[derive(Inbox)]
#[inbox(table = "jobs")]
struct NoId {
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct TwoIds {
    #[field(id)]
    job_id: i64,
    #[field(id)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct OneColumnTwice {
    #[field(id)]
    job_id: i64,
    #[sqlx(rename = "job_id")]
    legacy_id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs", advisory_lock = "jobs-{job_id}")]
struct LeaseAndAdvisory {
    #[field(id)]
    job_id: i64,
    #[field(locked_until)]
    locked_until: Option<i64>,
}

// In the advisory lock form a group keeps its order through the lock key.
#[derive(Inbox)]
#[inbox(table = "ledger", advisory_lock = "ledger-{id}")]
struct FifoAndAdvisory {
    #[field(id)]
    id: i64,
    #[field(group, fifo = true)]
    account: String,
}

fn main() {}
