use ruststream_sqlx::Outbox;
use sqlx::Postgres;

#[derive(Outbox, sqlx::FromRow)]
#[outbox(table = "outbox")]
struct Unrecorded {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

fn main() {
    let _ = Outbox::<Postgres>::deferred().register::<Unrecorded>("orders");
}
