use ruststream::OutgoingMessage;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::{ProcessedAt, own};
use ruststream_sqlx::outbox::{self, Outbox, OutboxSpec, OutboxTable, TrackedName};
use sqlx::{Sqlite, SqliteConnection};

#[derive(sqlx::FromRow)]
struct Event {
    id: i64,
    name: String,
    payload: Vec<u8>,
}

impl OutboxTable for Event {
    type Id = i64;
    // `own::Fetch` named, `outbox::Fetch<Sqlite>` not implemented.
    type Table = OutboxSpec<(ProcessedAt, own::Fetch)>;
    const TABLE: Self::Table = OutboxSpec::new(
        "outbox",
        Column::new("id"),
        Column::new("name"),
        Column::new("payload"),
    )
    .processed_at(Column::new("processed_at"))
    .own::<own::Fetch>();

    fn id(&self) -> &i64 {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl outbox::Publish<Sqlite> for Event {
    async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}

struct Orders;

impl TrackedName for Orders {
    const NAME: &'static str = "orders";
}

fn main() {
    let _ = Outbox::<Sqlite>::deferred().track::<Event, Orders>();
}
