use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::spec::{Advisory, own};
use ruststream_sqlx::{InboxSpec, InboxTable};

const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("orders-"), KeyPart::Column("id")];

/// A claim of the service's own in the advisory lock form.
struct ClaimedByLock {
    id: i64,
}

impl InboxTable for ClaimedByLock {
    type Id = i64;
    type Table = InboxSpec<(Advisory, own::Claim)>;
    const TABLE: Self::Table = InboxSpec::new("orders", Column::new("id"))
        .advisory(KEY)
        .own::<own::Claim>();

    fn id(&self) -> &i64 {
        &self.id
    }
}

fn main() {}
