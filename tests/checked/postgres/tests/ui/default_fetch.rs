use ruststream_sqlx::Inbox;
use ruststream_sqlx_checked_postgres::OrderHeaders;

// The checked headers struct's statements claim ids alone, and this message would read whole rows
// with the crate's fetch.
#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
struct Order {
    #[field(headers)]
    #[sqlx(flatten)]
    headers: OrderHeaders,
    total: i64,
}

fn main() {}
