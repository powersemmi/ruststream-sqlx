use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::spec::{Advisory, Attempt, Lease, Payload, own};
use ruststream_sqlx::{InboxSpec, InboxTable};

const KEY: &[KeyPart<'static>] = &[KeyPart::Column("id")];

/// Two forms: the lease and the advisory lock.
struct TwoForms {
    id: i64,
}

impl InboxTable for TwoForms {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Advisory)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id"))
        .lease(Column::new("locked_until"))
        .advisory(KEY);

    fn id(&self) -> &i64 {
        &self.id
    }
}

/// `attempt` set twice.
struct TwoAttempts {
    id: i64,
}

impl InboxTable for TwoAttempts {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Payload, Attempt)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id"))
        .attempt(Column::new("attempt"))
        .payload(Column::new("payload"))
        .attempt(Column::new("tries"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

/// The service's own `Ack` set twice.
struct TwoAcks {
    id: i64,
}

impl InboxTable for TwoAcks {
    type Id = i64;
    type Table = InboxSpec<(own::Ack, own::Ack)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id"))
        .own::<own::Ack>()
        .own::<own::Ack>();

    fn id(&self) -> &i64 {
        &self.id
    }
}

fn main() {}
