# Row locks, leases or advisory locks

```no_run
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

// report_jobs: id BIGSERIAL PRIMARY KEY, attempt SMALLINT NOT NULL DEFAULT 1,
// locked_until TIMESTAMPTZ, payload BYTEA NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "report_jobs")]
pub struct RenderReport {
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
pub struct Report {
    id: u64,
}

# async fn render(report: &Report) { let _ = report.id; }
// A report takes minutes to render. The claim has committed, and the subscription extends the
// lease while the handler runs; after a crash the row waits out its minute, then returns.
#[subscriber(InboxQueue::<RenderReport>::new("reports").lease(Duration::from_secs(60)))]
async fn handle(report: &Report) -> HandlerOutcome {
    render(report).await;
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(handle);
    })
}
# }
# fn main() {}
```

By hand, `.lease(..)` selects the form, and `Lease<Time>` in the type gives the lease's time type:

```no_run
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
# use std::time::Duration;
# use ruststream_sqlx::prelude::*;
# use serde::Deserialize;
# use sqlx::PgPool;

#[derive(sqlx::FromRow)]
pub struct RenderReport {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for RenderReport {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("report_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for RenderReport {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for RenderReport {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}
# #[derive(Deserialize)]
# pub struct Report { id: u64 }
# #[subscriber(InboxQueue::<RenderReport>::new("reports").lease(Duration::from_secs(60)))]
# async fn handle(report: &Report) -> HandlerOutcome {
#     let _ = report.id;
#     HandlerOutcome::ack()
# }
# pub fn app(pool: PgPool) -> RustStream {
#     RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
#         b.include(handle);
#     })
# }
# }
# fn main() {}
```

A table takes its rows in one of three forms, and its description decides which.

A table with no form set takes its rows by row lock: no `locked_until` or `advisory_lock` on the
derive, no `.lease(..)` or `.advisory(..)` by hand. A claim locks each row with `FOR UPDATE SKIP
LOCKED` in a transaction that stays open while the handler runs, and the settlement's statement
commits that transaction. A row and its outcome change together, and the rows of a crashed process
return at once, when the database rolls its transactions back. The price is a connection of the pool
per message in work, held for the whole handler.

A `#[field(locked_until)]` field, or `.lease(..)` by hand, selects the lease form ([`LeaseRow`]).
The claim writes a lease into each row and commits at once, so the handler runs with no transaction
open and no connection held. Declare it for handlers that run long and for a pool that cannot spare
a connection per message in work.

`advisory_lock = ".."` in `#[inbox(..)]`, or `.advisory(..)` by hand, selects the advisory lock
form. A lock on each row's key holds the row. The connection that holds the delivery keeps that
lock, and no transaction stays open. Rows that share a key go into work one at a time. The locks of
a crashed process end with its connections, and its rows return. The price is a connection of the
pool per message in work, as in the row lock form.

SQLite has no row locks, so a SQLite table takes the lease or the advisory lock form.

A subscription mounted with `workers(n)` keeps a claim in flight for each free worker, so its
workers take rows at once instead of one after another. It never holds more than the pool's size
less one connection, so the pool always keeps a connection for the handlers' own queries, their
publishes and the settlements that take one. A claim beside another one starts only while the
pool has a connection to spare, so subscriptions that share a pool, and handlers that hold its
connections, leave each other room. A pool short of room runs fewer deliveries at once.

A subscription keeps one claim in flight where claims that run at once would cost more than they
give. On SQLite, which takes one writer at a time, they only collide. On a table whose groups keep
their order, or with a `partition_key` column for `workers_by_key(n)`, they could finish out of
claim order.

What a subscription holds depends on the form. In the row lock form each delivery in work and
each claim in flight holds a connection: with `workers(n)` up to n. In the lease form a claim and
a settlement each take a connection only for their statements, and each subscription takes one
each half lease to extend the leases in work. In the advisory lock form each delivery in work
holds a connection of its own until it settles, in a batch too. In
[transactional mode](#transactional-mode) each delivery in work holds a connection for the
transaction its handler writes through, in every form.

Size the pool for every subscription that holds connections: n for each `workers(n)`, one more for
the pool, and the connections the handlers take for their own queries and inserts. A publish takes
a connection of its own for its insert. In the lease form a pool of n + 1 lets every free worker
claim at once.

## The lease form

- The claim writes the lease's expiry into `locked_until`, counts the attempt and commits.
- The claim reads "now" once: the rows it finds due, the leases it finds ended and the expiry it
  writes start from that instant.
- The expiry is the delivery's ownership token: a settlement takes effect only while the row
  still holds it.
- The subscription extends the lease of every delivery in work each half lease, so a handler may
  run longer than its lease. Each extension moves the token forward.
- A lease is how long a crashed process's row stays out of the queue: thirty seconds, unless
  [`SqlxBroker::lease`] sets another for the broker or [`InboxQueue::lease`] for one
  subscription. It is whole seconds, at least one, and a shorter one is rounded up.
- Ownership ends with the lease, not with its holder. A lease runs out under a running handler
  only when its extensions fail or stop: the database out of reach, the subscription closed, the
  broker shut down. The next claim then takes the row while the first handler still runs, and the
  token stops the late one from settling: its settlement fails with
  [`SqlxBrokerError::LeaseLost`].
- A delivery dropped unsettled releases its row at once. A settlement whose statement fails keeps
  the row until the lease runs out, as a crash does.
- The claim counts the attempt, so a crash spends one too. A delivery reports the attempt as it
  stood before its claim, so the first delivery reads 1, as in the row lock form.
- The crate computes the expiry on the host, because every settlement names the expiry its claim
  wrote: a struct with `locked_until` on `clock = DatabaseClock` does not compile, and neither does
  a description with `Lease<Time>` beside `Clock<DatabaseClock>`.
- After the broker shuts down, a delivery in work keeps its lease and settles as before; the lease
  is no longer extended.

## The advisory lock form

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

// jobs: job_id BIGSERIAL PRIMARY KEY, attempt SMALLINT NOT NULL DEFAULT 1,
// payload BYTEA NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", advisory_lock = "jobs-{job_id}")]
pub struct Transcode {
    #[field(id, generated)]
    job_id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Video {
    path: String,
}

# async fn encode(video: &Video) -> bool { !video.path.is_empty() }
// A video takes minutes to encode. The delivery's connection keeps the job's lock while the
// handler runs, with no transaction open. If the process dies, the database ends that session,
// and the job returns.
#[subscriber(InboxQueue::<Transcode>::new("videos"))]
async fn transcode(video: &Video) -> HandlerOutcome {
    if encode(video).await {
        return HandlerOutcome::ack();
    }
    HandlerOutcome::retry()
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(transcode);
    })
}
# }
# fn main() {}
```

By hand, `.advisory(..)` takes the key as its parts, text and columns:

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::spec::{Advisory, Attempt, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
# use ruststream_sqlx::prelude::*;
# use serde::Deserialize;
# use sqlx::PgPool;

#[derive(sqlx::FromRow)]
pub struct Transcode {
    job_id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for Transcode {
    type Id = i64;
    type Table = InboxSpec<(Advisory, Attempt, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("job_id").generated())
        .advisory(&[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")])
        .attempt(Column::new("attempt").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.job_id
    }
}

impl AttemptRow for Transcode {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for Transcode {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}
# #[derive(Deserialize)]
# pub struct Video { path: String }
# #[subscriber(InboxQueue::<Transcode>::new("videos"))]
# async fn transcode(video: &Video) -> HandlerOutcome {
#     let _ = &video.path;
#     HandlerOutcome::ack()
# }
# pub fn app(pool: PgPool) -> RustStream {
#     RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
#         b.include(transcode);
#     })
# }
# }
# fn main() {}
```

`advisory_lock = "jobs-{job_id}"` names each row's lock key. A placeholder names a field, the key
reads its column, and the database renders the key as text. By hand, `KeyPart::Column` names the
column itself, and a column the table lacks stops the subscription at startup, when its statements
are prepared. Rows whose keys match go into work one at a time. A key on the row's id holds each row
alone. A key on another column, as in `advisory_lock = "accounts-{account}"`, keeps the rows of an
account in work one at a time. A key belongs to the database, not to its table. Two tables whose
keys match wait for each other, and a key that starts with its table's name keeps them apart.

A claim selects candidates: as many due rows of the queue as it may take, in claim order, each
with its key. It locks each candidate's key on a connection of its own, without waiting, and
passes over a key held elsewhere. Then it takes the row while the row is still claimable: the
take counts the attempt and reads the row. A row that another holder settled between the select
and the lock is passed over, and its lock released. The take commits the count, so a crash spends
an attempt, as in the lease form. A delivery reports the attempt as it stood before its claim.

A settlement runs its statement on the delivery's connection, where it commits on its own. Then
the lock is released, and the connection goes back to the pool. `retry()` runs no statement: the
claim counted the attempt, and the release returns the row. A settlement whose statement fails
still releases the lock, and the row returns as it was. A release that the database does not
confirm closes the connection instead, which ends the session and its locks.

A delivery dropped unsettled releases its lock and closes its connection, in a task on the
runtime the broker connected on, and its row returns. One timeout of five seconds bounds the
release and the close. Past it the connection drops, and the server ends the session and its lock
all the same.

`shutdown` releases the lock of every delivery in work. Each connection releases its key and goes
back to the pool, or closes where the database does not confirm the release. `shutdown` waits for
each settlement in flight and each connection still closing, and returns once the broker holds no
lock. A delivery whose lock it released settles no more: its settlement fails with
[`SqlxBrokerError::Closed`], and its row is back in the queue. The [`ClosedSqlxBroker`] it
returns counts the two outcomes. [`locks_released`](ClosedSqlxBroker::locks_released) counts the
locks released with their connections back in the pool, and
[`connections_closed`](ClosedSqlxBroker::connections_closed) the connections that held a lock and
closed instead. In a service the runtime drains the handlers before it shuts the broker down. A
handler aborted at the drain timeout drops its delivery, and `shutdown` waits for that connection
to close.

A delivery holds a connection of its own because its lock lives in that connection's session: a
delivery dropped unsettled closes its own connection and leaves the other rows alone. A
subscription with `workers(n)` holds up to n connections for its deliveries and its claims, and a
batch of n rows holds n. A batch takes the connections the pool gives at once: idle ones first, then
new ones while the pool has room. It ends where the pool is full, so a batch larger than the pool
shrinks instead of waiting. Handlers that publish need room for their inserts on top.

Each database keeps the locks in its own way, and keeps the locks of two databases apart:

- Postgres locks a 64-bit hash of the key (`hashtextextended`, Postgres 11 or later), and a claim
  leaves out the keys other sessions hold. Two keys with one hash wait for each other: a delay,
  never a double delivery. A lock lives in the database session that took it, so the form needs a
  direct connection or `PgBouncer` in session pooling. In transaction pooling the session that took
  a lock serves other clients between statements, and the lock goes with it.
- MySQL and MariaDB lock a name with `GET_LOCK`: the table's database in lower case, a dot and the
  key, as in `app.jobs-7`. A name longer than the 64 characters the server takes is locked by its
  SHA-256. A claim leaves out the names in use.
- SQLite has no locks that a session holds, so the process keeps the keys in work, by database
  and key. Two keys with one hash wait for each other here too. A claim's select cannot see the
  keys in work: it reads the first due rows alone and takes nothing while they are in work. A
  handler of single messages therefore has one delivery in work at a time, and with `workers(n)`
  each next row waits up to a poll interval. The registry serves one process per database file.
  Two processes on one file do not see each other's keys, and a row may be delivered twice.

### A lock of the service's own

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{Lock, Unlock};
use serde::Deserialize;
use sqlx::{PgConnection, PgPool, Postgres};

// payouts: id BIGSERIAL PRIMARY KEY, account_id BIGINT NOT NULL, payload BYTEA NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "payouts", advisory_lock = "{account_id}", custom(lock, unlock))]
pub struct Payout {
    #[field(id, generated)]
    id: i64,
    account_id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

// The billing system holds `pg_advisory_lock(account_id)` while it changes an account. The queue
// takes the same lock, so a payout never runs beside a billing run of its account.
impl Lock<Postgres> for Payout {
    async fn lock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT pg_try_advisory_lock($1::bigint)")
            .bind(key)
            .fetch_one(conn)
            .await
    }
}

impl Unlock<Postgres> for Payout {
    async fn unlock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT pg_advisory_unlock($1::bigint)")
            .bind(key)
            .fetch_one(conn)
            .await
    }
}

#[derive(Deserialize)]
pub struct Transfer {
    cents: i64,
}

# async fn transfer_to_bank(transfer: &Transfer) { let _ = transfer.cents; }
#[subscriber(InboxQueue::<Payout>::new("payouts"))]
async fn pay(transfer: &Transfer) -> HandlerOutcome {
    transfer_to_bank(transfer).await;
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

`custom(lock, unlock)` in `#[inbox(..)]` hands the lock and the release of each key to the service:
the struct implements [`Lock`] and [`Unlock`] for its database. By hand, the chain adds
`.own::<own::Lock>()` and `.own::<own::Unlock>()`, and the type lists `own::Lock` and `own::Unlock`
beside `Advisory`. The two come together, and only in the advisory lock form; a description that
breaks either rule does not compile. The dialect still selects the candidates and takes each row.
The lock tries without waiting and answers whether it took the key. The release answers whether the
session held the key, and one that answers `false` or fails closes the connection. The process keeps
no registry of keys for such a table, on SQLite too. The dialect's select leaves out only the keys
its own locks hold, so a claim reads the first due rows alone and takes nothing while their keys are
held. A handler of single messages therefore has one delivery in work at a time, and with
`workers(n)` each next row waits up to a poll interval.

A database the crate builds no dialect for takes the form through a dialect of the service's own
that implements [`Advisory`](dialect::Advisory): the claim of candidates, the lock, the release
and the take. On SQL Server its lock runs `sp_getapplock` with the session as the owner and no
wait, and its release runs `sp_releaseapplock`. Each of the two answers one 64-bit integer,
nonzero where it took or released the lock.

