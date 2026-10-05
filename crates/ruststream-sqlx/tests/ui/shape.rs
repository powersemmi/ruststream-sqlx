use ruststream_sqlx::Inbox;

// A row is a struct with named fields: each field is a column.
#[derive(Inbox)]
#[inbox(table = "jobs")]
enum Job {
    Email { id: i64 },
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct Positional(i64, Vec<u8>);

fn main() {}
