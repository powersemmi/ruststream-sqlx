# Databases

```no_run
# #[cfg(all(feature = "sqlite", feature = "chrono"))]
# mod demo {
use chrono::{DateTime, Utc};
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::SqlitePool;

// thumbnail_jobs: id INTEGER PRIMARY KEY, attempt INTEGER NOT NULL DEFAULT 1,
// locked_until TEXT, payload BLOB NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "thumbnail_jobs")]
pub struct MakeThumbnail {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Image {
    path: String,
}

# async fn resize(image: &Image) -> bool { !image.path.is_empty() }
#[subscriber(InboxQueue::<MakeThumbnail>::new("thumbnails"))]
async fn thumbnail(image: &Image) -> HandlerOutcome {
    if resize(image).await {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

// The same struct serves a `PgPool` and a `MySqlPool` too: the broker takes the dialect of its
// pool, which builds its statements from the table's description.
pub fn app(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("thumbnails", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(thumbnail);
    })
}
# }
# fn main() {}
```

A built-in dialect builds a subscription's statements once, when it opens, from the table's
description. The insert of every enabled dialect is rendered at compile time, by the derive or by
[`dialect::insert`] for a table described by hand. A database whose sqlx driver lives outside sqlx
is served by a dialect of the service's own ([`SqlxBroker::with_dialect`]): a type that implements
[`Dialect`](dialect::Dialect), [`RowLock`](dialect::RowLock), [`Lease`](dialect::Lease) and
[`Advisory`](dialect::Advisory) for the forms it serves, and [`ByName`] for subscriptions by name.

The connections a subscription holds depend on its form and on transactional mode, alike on every
database ([Row locks, leases or advisory locks](#row-locks-leases-or-advisory-locks)).

## Postgres

`postgres` serves every form. The row lock and lease claims skip locked rows with `SKIP LOCKED`,
which Postgres 9.5 added. A lease claim is one statement: it locks the claimable rows, writes their
lease and returns them. A claim of the service's own may leave the
rows to the crate's fetch, which reads them by a list of ids. A table on the database's clock
reads `statement_timestamp()`. The advisory lock form and the claims of FIFO groups lock 64-bit
hashes made by `hashtextextended`, which Postgres 11 added. An older server refuses their
statements, so their subscriptions stop when they open.

A subscription on Postgres wakes on rows other processes write when the broker listens for
`pg_notify` ([`LISTEN/NOTIFY` on Postgres](#listennotify-on-postgres)). The advisory lock form
needs a direct connection or `PgBouncer` in session pooling
([The advisory lock form](#the-advisory-lock-form)). A row lock table works at `read_committed`;
the stricter levels fail claims with serialization errors ([Isolation and mode](#isolation-and-mode)).

## MySQL and MariaDB

`mysql` serves MySQL 8.0.1 and MariaDB 10.6 or later, in every form. Claims skip locked rows in
the row lock and lease forms, which those versions added, and an advisory table shares their
floor: a subscription reads the server's version when it opens, and an older server stops it with
[`SqlxBrokerError::ServerTooOld`]. A claim's transaction runs at READ COMMITTED unless a row lock
table declares another level ([Isolation and mode](#isolation-and-mode)). At READ COMMITTED a
claim held for a handler locks no gaps between rows and holds back no insert into its table. A
server whose binary log records statements (`binlog_format = STATEMENT`) refuses writes at that
level; the row and mixed formats accept them.

A lease claim selects its rows, stamps each one with its lease and commits. The expiry is a whole
second, so a `DATETIME` column without fractions holds the token exactly. A dead letter into a
table is two statements in one transaction. A claim of the service's own comes with a [`Fetch`]
of its own, because the crate reads rows by a list of ids on Postgres alone; a subscription
without one stops at startup.

Index a table's claim order: `(group, priority, retry_after, id)`, for the columns the table has.
A row lock or lease claim locks every row of its group that it reads before sorting them, so
without that index a claim held for a handler keeps the rest of its group from other claims. In the
row lock form the next claim passes over those rows to later ones, and the rows of a partition key
go into work out of order. In a table with FIFO groups a claim first locks the unfinished rows of
its group until its transaction ends, which in the row lock form is when the delivery settles. The same index keeps that read to
the group's own rows. A row lock table with FIFO groups that declares `serializable` stops its
subscription when it opens, with [`SqlxBrokerError::Dialect`]: at that level `InnoDB` locks every
row a read touches, and a claim would wait for the group's row in work.

## SQLite

`sqlite` serves the lease and advisory lock forms. SQLite locks the whole database for a writer,
so no claim can hold rows for a handler: its dialect implements no
[`RowLock`](dialect::RowLock), and a subscription to a table without `locked_until` or
`advisory_lock` does not compile. A lease claim is one `UPDATE .. RETURNING` that leases its rows,
and one writer at a time keeps two claims apart. The rows of one lease claim come in no
particular order. A claim of the service's own opens its transaction with `BEGIN IMMEDIATE` and
comes with a [`Fetch`] of its own. The advisory lock form keeps its locks in the process
([The advisory lock form](#the-advisory-lock-form)).

SQLite keeps times as text and compares them as text. `chrono` times sort exactly. `time` values
sort right only across seconds, so with them a lease may end up to a second late, and a delayed
retry may come back up to a second early or late ([`QueueTime`]). The generated document
describes the server by its protocol alone, with no host.

## An `AnyPool`

`any` serves an `AnyPool`. The broker picks the built-in dialect of the database the pool
reaches when it connects, among the dialects whose features are on; another backend stops
`connect` with [`SqlxBrokerError::Backend`]. A row holds the types `sqlx::Any` carries: integers,
text and bytes, with no time and no JSON. An `AnyPool` therefore serves the row lock form on its
Postgres and MySQL backends and the advisory lock form on all three, without `retry_after`,
`processed_at` or `headers`. A lease table is out of its reach, and on a SQLite backend a row
lock table stops its subscription at startup.

