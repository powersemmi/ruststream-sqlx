use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::spec::{Advisory, own};
use ruststream_sqlx::{InboxSpec, InboxTable};

const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("orders-"), KeyPart::Column("id")];

/// An extend of the service's own in the row lock form.
struct ExtendedRowLock {
    id: i64,
}

impl InboxTable for ExtendedRowLock {
    type Id = i64;
    type Table = InboxSpec<(own::Extend,)>;
    const TABLE: Self::Table = InboxSpec::new("orders", Column::new("id")).own::<own::Extend>();

    fn id(&self) -> &i64 {
        &self.id
    }
}

/// An extend of the service's own in the advisory lock form.
struct ExtendedLock {
    id: i64,
}

impl InboxTable for ExtendedLock {
    type Id = i64;
    type Table = InboxSpec<(Advisory, own::Extend)>;
    const TABLE: Self::Table = InboxSpec::new("orders", Column::new("id"))
        .advisory(KEY)
        .own::<own::Extend>();

    fn id(&self) -> &i64 {
        &self.id
    }
}

fn main() {}
