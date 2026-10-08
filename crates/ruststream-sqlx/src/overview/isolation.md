# Isolation and mode

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

// payout_jobs: id BIGSERIAL PRIMARY KEY, payload BYTEA NOT NULL
// The database's transactions default to SERIALIZABLE; the claims of this table open at
// READ COMMITTED.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "payout_jobs", isolation = read_committed)]
pub struct SendPayout {
    #[field(id, generated)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Payout {
    account: String,
}

# async fn transfer(payout: &Payout) { let _ = &payout.account; }
#[subscriber(InboxQueue::<SendPayout>::new("payouts"))]
async fn pay(payout: &Payout) -> HandlerOutcome {
    transfer(payout).await;
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("payouts", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(pay);
    })
}
# }
# fn main() {}
```

By hand, `.opens::<Level>()` takes a marker of [`dialect::level`], and `Opens<Level>` goes into
the type:

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use ruststream_sqlx::dialect::{Column, level};
use ruststream_sqlx::spec::{Opens, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
# use ruststream_sqlx::prelude::*;
# use serde::Deserialize;
# use sqlx::PgPool;

#[derive(sqlx::FromRow)]
pub struct SendPayout {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for SendPayout {
    type Id = i64;
    type Table = InboxSpec<(Opens<level::ReadCommitted>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("payout_jobs", Column::new("id").generated())
        .opens::<level::ReadCommitted>()
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for SendPayout {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}
# #[derive(Deserialize)]
# pub struct Payout { account: String }
# #[subscriber(InboxQueue::<SendPayout>::new("payouts"))]
# async fn pay(payout: &Payout) -> HandlerOutcome {
#     let _ = &payout.account;
#     HandlerOutcome::ack()
# }
# pub fn app(pool: PgPool) -> RustStream {
#     RustStream::new(AppInfo::new("payouts", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
#         b.include(pay);
#     })
# }
# }
# fn main() {}
```

`isolation = <level>` in `#[inbox(..)]` declares an isolation level: `read_uncommitted`,
`read_committed`, `repeatable_read` or `serializable`. A SQLite table declares a mode instead, as in
`#[inbox(mode = immediate)]`: `deferred`, `immediate` or `exclusive`. SQLite runs every transaction
serializable. Its mode decides when a transaction takes the write lock ([`Mode`](dialect::Mode)). By
hand, the levels and the modes are the markers of [`dialect::level`], and a table opens at one of
them at most.

The declaration governs the transactions the crate opens for a delivery's work. In the row lock
form that is the claim's transaction: it holds the rows while their handler runs, and their
settlement commits it. In [transactional mode](#transactional-mode) it is also the transaction the
handler writes through, in every form. A table that declares neither opens them with a plain
`BEGIN` on Postgres and SQLite, at the database's default, and at READ COMMITTED on MySQL and
MariaDB.

Each database takes the levels it keeps: Postgres `read_committed`, `repeatable_read` and
`serializable`; MySQL and MariaDB all four; SQLite the three modes. Postgres runs READ UNCOMMITTED
as READ COMMITTED, so its dialect does not open it.

A subscription to a table at a level or mode its broker's dialect does not open does not compile,
and neither does a route into that table. The error names the dialect and the level, and lists
what each database opens. An `AnyPool` names its database only when the broker connects, so there
such a subscription stops when it starts, with [`SqlxBrokerError::Dialect`]. A dialect of the
service's own opens a level by implementing [`Opens`](dialect::Opens) for it, and its
[`Dialect::begin`](dialect::Dialect::begin) returns the statement that opens it.

On Postgres a row lock table at `repeatable_read` or `serializable` fails claims with
serialization errors when claims and settlements of its rows run at once. `read_committed` is the
practical level for the row lock form there.

On MySQL and MariaDB a row lock claim at `repeatable_read` or `serializable` keeps a lock on every
row it reads until its settlement. Without an index on the group's column it reads rows of other
groups as well, and a claim held for a handler keeps those rows from their own claims. An index on
that column lets a claim read its own group alone.

