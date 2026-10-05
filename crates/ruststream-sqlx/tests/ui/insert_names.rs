use ruststream_sqlx::Inbox;

// Postgres cuts a name over 63 bytes short without a word; the derive refuses it instead.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
struct TooLong {
    #[field(id)]
    id: i64,
    #[sqlx(rename = "a_column_name_that_runs_past_the_sixty_three_bytes_postgres_keeps")]
    payload: Vec<u8>,
}

fn main() {}
