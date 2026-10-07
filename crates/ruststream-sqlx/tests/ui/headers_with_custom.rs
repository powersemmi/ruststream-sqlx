use ruststream_sqlx::InboxHeaders;

#[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
#[inbox(table = "order_jobs", custom(fetch))]
struct OrderHeaders {
    #[field(id, generated)]
    job_id: i64,
    tenant: String,
}

fn main() {}
