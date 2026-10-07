use ruststream::OutgoingMessage;
use ruststream_sqlx::{Outbox, outbox};
use sqlx::{PgConnection, Postgres};

#[derive(Outbox, sqlx::FromRow)]
#[outbox(table = "outbox", custom(fetch))]
struct Untaken {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

impl outbox::Publish<Postgres> for Untaken {
    async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}

fn main() {
    let _ = Outbox::<Postgres>::deferred().register::<Untaken>("orders");
}
