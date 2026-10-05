use ruststream_sqlx::InboxRow;

// A plain struct: it never derived `Inbox`.
struct Job {
    id: i64,
}

fn queue_table<Row: InboxRow>() -> &'static str {
    Row::SPEC.table()
}

fn main() {
    let _ = Job { id: 1 }.id;
    queue_table::<Job>();
}
