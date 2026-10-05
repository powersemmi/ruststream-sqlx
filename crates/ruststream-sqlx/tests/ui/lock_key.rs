use ruststream_sqlx::Inbox;

#[derive(Inbox)]
#[inbox(table = "jobs", advisory_lock = "jobs-{job_id")]
struct Unclosed {
    #[field(id)]
    job_id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs", advisory_lock = "jobs-}")]
struct Unopened {
    #[field(id)]
    job_id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs", advisory_lock = "jobs-{}")]
struct EmptyBraces {
    #[field(id)]
    job_id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs", advisory_lock = "jobs-{tenant}")]
struct UnknownField {
    #[field(id)]
    job_id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs", advisory_lock = "jobs-{tenant}")]
struct FieldWithoutColumn {
    #[field(id)]
    job_id: i64,
    #[sqlx(skip)]
    tenant: String,
}

fn main() {}
