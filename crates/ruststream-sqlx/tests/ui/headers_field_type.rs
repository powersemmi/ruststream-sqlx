use ruststream_sqlx::InboxHeaders;

/// A type of the service's own that the table keeps as text, and no header value.
#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(transparent)]
struct Region(String);

#[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
#[inbox(table = "order_jobs")]
struct OrderHeaders {
    #[field(id, generated)]
    job_id: i64,
    region: Region,
}

fn main() {}
