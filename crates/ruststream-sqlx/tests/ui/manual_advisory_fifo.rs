use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::spec::{Advisory, Fifo};
use ruststream_sqlx::{InboxSpec, InboxTable};

const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("orders-"), KeyPart::Column("id")];

/// A FIFO group in the advisory lock form.
struct OrderedByLock {
    id: i64,
}

impl InboxTable for OrderedByLock {
    type Id = i64;
    type Table = InboxSpec<(Advisory, Fifo)>;
    const TABLE: Self::Table = InboxSpec::new("orders", Column::new("id"))
        .advisory(KEY)
        .fifo_group(Column::new("customer"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

fn main() {}
