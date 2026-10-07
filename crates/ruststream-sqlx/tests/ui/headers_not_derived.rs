use ruststream_sqlx::Inbox;

/// A plain sqlx struct: it reads its columns, and describes no queue table.
#[derive(Debug, Clone, sqlx::FromRow)]
struct OrderHeaders {
    job_id: i64,
    tenant: String,
}

#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
struct OrderJob {
    #[field(headers)]
    #[sqlx(flatten)]
    headers: OrderHeaders,
    note: Option<String>,
}

fn main() {}
