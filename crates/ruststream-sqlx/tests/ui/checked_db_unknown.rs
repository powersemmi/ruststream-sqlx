use ruststream_sqlx::Inbox;

#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs", checked, db = mssql)]
struct SendEmail {
    #[field(id)]
    job_id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

fn main() {}
