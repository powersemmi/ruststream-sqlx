use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::spec::{Advisory, own};
use ruststream_sqlx::{InboxSpec, InboxTable};

const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("orders-"), KeyPart::Column("id")];

/// A lock of the service's own without its unlock.
struct LockedOnly {
    id: i64,
}

impl InboxTable for LockedOnly {
    type Id = i64;
    type Table = InboxSpec<(Advisory, own::Lock)>;
    const TABLE: Self::Table = InboxSpec::new("orders", Column::new("id"))
        .advisory(KEY)
        .own::<own::Lock>();

    fn id(&self) -> &i64 {
        &self.id
    }
}

/// A lock and an unlock of the service's own in the row lock form.
struct LockedRows {
    id: i64,
}

impl InboxTable for LockedRows {
    type Id = i64;
    type Table = InboxSpec<(own::Lock, own::Unlock)>;
    const TABLE: Self::Table = InboxSpec::new("orders", Column::new("id"))
        .own::<own::Lock>()
        .own::<own::Unlock>();

    fn id(&self) -> &i64 {
        &self.id
    }
}

fn main() {}
