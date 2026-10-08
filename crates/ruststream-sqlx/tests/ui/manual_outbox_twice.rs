use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::{ProcessedAt, own};
use ruststream_sqlx::outbox::{OutboxSpec, OutboxTable};

struct Marked {
    id: i64,
    name: String,
    payload: Vec<u8>,
}

impl OutboxTable for Marked {
    type Id = i64;
    type Table = OutboxSpec<(ProcessedAt, ProcessedAt)>;
    const TABLE: Self::Table = OutboxSpec::new(
        "outbox",
        Column::new("id"),
        Column::new("name"),
        Column::new("payload"),
    )
    .processed_at(Column::new("processed_at"))
    .processed_at(Column::new("done_at"));

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

struct Acked {
    id: i64,
    name: String,
    payload: Vec<u8>,
}

impl OutboxTable for Acked {
    type Id = i64;
    type Table = OutboxSpec<(own::Ack, own::Ack)>;
    const TABLE: Self::Table = OutboxSpec::new(
        "outbox",
        Column::new("id"),
        Column::new("name"),
        Column::new("payload"),
    )
    .own::<own::Ack>()
    .own::<own::Ack>();

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

fn main() {}
