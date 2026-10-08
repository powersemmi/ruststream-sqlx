use std::marker::PhantomData;

use ruststream_sqlx::Inbox;

#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs", clock = Source, checked, db = postgres)]
struct SendEmail<Source> {
    #[field(id)]
    job_id: i64,
    #[field(payload)]
    payload: Vec<u8>,
    #[sqlx(skip)]
    source: PhantomData<Source>,
}

fn main() {}
