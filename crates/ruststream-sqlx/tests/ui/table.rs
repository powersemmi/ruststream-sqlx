use ruststream_sqlx::Inbox;

// The table is the one thing the derive cannot guess.
#[derive(Inbox)]
struct NoTable {
    #[field(id)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs", queue = "emails")]
struct UnknownOption {
    #[field(id)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "")]
struct EmptyTable {
    #[field(id)]
    id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
#[inbox(table = "jobs_v2")]
struct TwoTables {
    #[field(id)]
    id: i64,
}

fn main() {}
