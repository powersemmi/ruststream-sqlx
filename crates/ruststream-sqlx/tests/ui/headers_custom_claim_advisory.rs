use ruststream_sqlx::{Claim, Inbox, InboxHeaders};
use sqlx::{PgConnection, Postgres};

#[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
#[inbox(table = "order_jobs", advisory_lock = "order-{job_id}")]
struct OrderHeaders {
    #[field(id, generated)]
    job_id: i64,
    tenant: String,
}

#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
#[inbox(custom(claim))]
struct OrderJob {
    #[field(headers)]
    #[sqlx(flatten)]
    headers: OrderHeaders,
    note: Option<String>,
}

impl Claim<Postgres> for OrderJob {
    async fn claim(_: &mut PgConnection, _: &str, _: i64) -> Result<Vec<i64>, sqlx::Error> {
        Ok(Vec::new())
    }
}

fn main() {}
