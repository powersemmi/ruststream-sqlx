use ruststream_sqlx::{Inbox, InboxHeaders};

#[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
#[inbox(table = "order_jobs")]
struct OrderHeaders {
    #[field(id, generated)]
    job_id: i64,
    tenant: String,
}

#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
struct OrderJob {
    #[field(headers)]
    #[sqlx(flatten)]
    headers: OrderHeaders,
    #[field(id)]
    order_id: i64,
}

fn main() {}
