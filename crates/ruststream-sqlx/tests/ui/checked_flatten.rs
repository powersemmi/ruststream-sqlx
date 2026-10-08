use ruststream_sqlx::Inbox;

#[derive(Debug, sqlx::FromRow)]
struct Recipient {
    address: String,
    name: String,
}

#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs", checked, db = postgres)]
struct SendEmail {
    #[field(id)]
    job_id: i64,
    #[sqlx(flatten)]
    recipient: Recipient,
    #[field(payload)]
    payload: Vec<u8>,
}

fn main() {}
