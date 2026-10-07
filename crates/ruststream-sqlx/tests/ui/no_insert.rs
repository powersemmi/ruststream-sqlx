use ruststream_sqlx::{Inbox, Insert};
use sqlx::PgConnection;

#[derive(sqlx::FromRow, Clone)]
struct Envelope {
    subject: String,
}

#[derive(Inbox, sqlx::FromRow, Clone)]
#[inbox(table = "jobs")]
struct Flattening {
    #[field(id, generated)]
    id: i64,
    #[sqlx(flatten)]
    envelope: Envelope,
}

async fn write(job: &Flattening, conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    Insert::insert(job, conn).await
}

fn main() {
    let _ = write;
}
