SQL databases for the [RustStream](https://github.com/powersemmi/ruststream) messaging framework,
through [`sqlx`](https://docs.rs/sqlx).

The crate serves task queues kept in the service's own tables, on Postgres, MySQL, MariaDB and
SQLite. A struct of the service's own describes a queue table: [`Inbox`] reads the table from
`#[inbox(..)]`, the column names from sqlx's attributes and the role of each column from
`#[field(..)]`. [`SqlxBroker`] serves the queues of the service's sqlx pool as a RustStream
broker: a subscription claims rows, a handler's outcome settles them, and a publish writes a row
through the service's own SQL.

The `inbox` feature turns the broker on, and a service adds what its database and its tables
need:

- `postgres`, `mysql` (MySQL and MariaDB) and `sqlite`: a built-in dialect and its sqlx driver;
- `any`: an `AnyPool`, served by the dialect of the database it reaches;
- `chrono` or `time`: the time columns; `json`: the headers column;
- `testing`: the broker's in-process mode for `TestApp`; `asyncapi`: what a subscription adds to
  the generated document.

Where things are:

- [`Inbox`]: what a struct declares, the roles a column plays, the compile errors it meets.
- [`SqlxBroker`] and [`InboxQueue`]: the broker and its subscriptions, described below.
- [`Repository`] and [`Routed`]: publishing into tables.
- [`keys`]: what a handler reads off a delivery.
- [`dialect`]: the SQL each database runs, and the traits a dialect of the service's own
  implements.

# The inbox broker

```no_run
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::{PgConnection, PgPool, Postgres};

// email_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT, retry_after TIMESTAMPTZ DEFAULT now(),
// attempt SMALLINT DEFAULT 1, processed_at TIMESTAMPTZ, payload BYTEA
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(retry_after, generated)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(processed_at, generated)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for SendEmail {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO email_jobs (name, payload) VALUES ($1, $2)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Deserialize)]
pub struct Email {
    to: String,
}

# async fn deliver(_: &Email) -> bool { true }
#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    if deliver(email).await {
        return HandlerOutcome::ack();
    }
    // The handler owns the backoff: a minute per attempt.
    HandlerOutcome::retry_after(Duration::from_secs(60 * attempt.unwrap_or(1)))
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendEmail>("emails"),
        |b| {
            b.include(send);
        },
    )
}
# }
# fn main() {}
```

[`SqlxBroker`] serves the queues in the tables of the service's pool, and the pool stays the
service's. An [`InboxQueue`] subscription reads one queue: its name selects a group where the
table has a `group` column, and addresses the whole table where it has none. A subscription
claims rows when its stream is polled; after a claim that found fewer rows than it asked for, it
waits the poll interval, one second unless set. The bytes reach the codec lent from the row.

What a handler answers decides the row's fate:

- `ack()` deletes the row, or sets `processed_at` where the table has it;
- `retry()` returns the row to the queue at once, and its next delivery reads one more attempt;
- `retry_after(d)` hides the row until `retry_after` comes; a table without that column returns
  it at once, and the runtime logs a warning;
- `drop()` finishes the row as `ack()` does;
- `max_attempts(n)` on the mount reads `attempt`. At the cap, `dead_letter(..)` moves the row to
  another group or into a table with the same columns; without a destination the row is
  finished.

"Now" comes from [`SystemClock`] unless the struct names another source:
`#[inbox(clock = DatabaseClock)]` reads the database's clock, and a service's own [`Clock`] fits
there too. Hosts that bind "now" keep their clocks in step.

# Row locks or leases

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

A table takes its rows in one of two forms, and its struct decides which.

Without `locked_until` a claim takes its rows by row lock. It locks each row with
`FOR UPDATE SKIP LOCKED` in a transaction that stays open while the handler runs, and the
settlement's statement commits that transaction. A row and its outcome change together, and the
rows of a crashed process return at once, when the database rolls its transactions back. The
price is a connection of the pool per message in work, held for the whole handler.

A `#[field(locked_until)]` field selects the lease form ([`LeaseRow`]). The claim writes a lease
into each row and commits at once, so the handler runs with no transaction open and no
connection held. Declare it for handlers that run long, for a pool that cannot spare a connection
per message in work, and on SQLite, which has no row locks.

A publish takes a connection of its own for its insert. In the row lock form a subscription with
`workers(n)` holds up to n + 1 connections, and handlers that publish need room for their inserts
on top: a pool without that room makes them wait for its `acquire_timeout`. In the lease form a
claim and a settlement each take a connection only for their statements, and each subscription
takes one each half lease to extend the leases in work.

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
  wrote: a struct with `locked_until` on `clock = DatabaseClock` does not compile.
- After the broker shuts down, a delivery in work keeps its lease and settles as before; the lease
  is no longer extended.

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

// The same struct serves a `PgPool` and a `MySqlPool` too: the derive builds each dialect's
// statements, and the broker takes the dialect of its pool.
pub fn app(pool: SqlitePool) -> RustStream {
    RustStream::new(AppInfo::new("thumbnails", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(thumbnail);
    })
}
# }
# fn main() {}
```

A built-in dialect builds a subscription's statements once, when it opens; the derive builds the
insert of every enabled dialect at compile time. A database whose sqlx driver lives outside sqlx
is served by a dialect of the service's own ([`SqlxBroker::with_dialect`]): a type that
implements [`Dialect`](dialect::Dialect), [`RowLock`](dialect::RowLock) and
[`Lease`](dialect::Lease) for the forms it serves, and [`ByName`] for subscriptions by name.

## Postgres

`postgres` serves both forms. A lease claim is one statement: it locks the claimable rows with
`SKIP LOCKED`, writes their lease and returns them. A claim of the service's own may leave the
rows to the crate's fetch, which reads them by a list of ids. A table on the database's clock
reads `statement_timestamp()`.

## MySQL and MariaDB

`mysql` serves MySQL 8.0.1 and MariaDB 10.6 or later, in both forms. Claims skip locked rows in
both forms, which those versions added: a subscription reads the server's version when it opens,
and an older server stops it with [`SqlxBrokerError::ServerTooOld`]. A claim's transaction runs
at READ COMMITTED, so a claim held for a handler locks no gaps between rows and holds back no
insert into its table. A server whose binary log records statements
(`binlog_format = STATEMENT`) refuses writes at that level; the row and mixed formats accept them.

A lease claim selects its rows, stamps each one with its lease and commits. The expiry is a whole
second, so a `DATETIME` column without fractions holds the token exactly. A dead letter into a
table is two statements in one transaction. A claim of the service's own comes with a [`Fetch`]
of its own, because the crate reads rows by a list of ids on Postgres alone; a subscription
without one stops at startup.

## SQLite

`sqlite` serves the lease form. SQLite locks the whole database for a writer, so no claim can hold
rows for a handler: its dialect implements no [`RowLock`](dialect::RowLock), and a subscription
to a table without `locked_until` does not compile. A claim is one `UPDATE .. RETURNING` that
leases its rows, and one writer at a time keeps two claims apart. The rows of one claim come in
no particular order. A claim of the service's own opens its transaction with `BEGIN IMMEDIATE`
and comes with a [`Fetch`] of its own.

SQLite keeps times as text and compares them as text. `chrono` times sort exactly. `time` values
sort right only across seconds, so with them a lease may end up to a second late, and a delayed
retry may come back up to a second early or late ([`QueueTime`]). The generated document
describes the server by its protocol alone, with no host.

## An `AnyPool`

`any` serves an `AnyPool`. The broker picks the built-in dialect of the database the pool
reaches when it connects, among the dialects whose features are on; another backend stops
`connect` with [`SqlxBrokerError::Backend`]. A row holds the types `sqlx::Any` carries: integers,
text and bytes, with no time and no JSON. An `AnyPool` therefore serves the row lock form on its
Postgres and MySQL backends, without `retry_after`, `processed_at` or `headers`. A lease table
is out of its reach, and on a SQLite backend a row lock table stops its subscription at startup.

# Startup checks and rows that do not decode

```no_run
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(retry_after, generated)]
    retry_after: DateTime<Utc>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Email {
    to: String,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    // A row that does not decode stays in the table and comes back every ten minutes, until a
    // release that reads it ships.
    let keep = FailurePolicy::RetryAfter(Duration::from_secs(600));
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send.on_failure(FailurePolicies::default().with_decode(keep)));
    })
}
# }
# fn main() {}
```

A mistake stops the service as early as it can be seen. A struct that cannot drive a queue does
not compile, and [`Inbox`] lists the errors. A subscription builds its statements when it opens
and prepares each one on the server: a table or a column the struct names and the database lacks
stops it, and [`SqlxBrokerError::Schema`] names the table and the statement. Preparing checks the
names, not the column types: the types are the service's to get right. A retry declaration the
table cannot carry stops it too ([`SqlxBrokerError::Declaration`]): `max_attempts(..)` needs an
`attempt` column. On MySQL and MariaDB the subscription also reads the server's version.

A claimed row whose columns do not decode into the struct reaches the subscription's
`on_failure(decode = ..)` policy, which settles it, and the subscription goes on with the next
row. The default policy drops the row: it is deleted, or marked where the table has
`processed_at`. Such a row still reports its attempt: the claim reads the `attempt` column alone,
as the struct reads it. So `max_attempts(..)` spends it like any other row, and a policy that
retries it keeps it only up to the cap. When the `attempt` column itself does not decode, the row
reports no attempt, and a policy that retries it keeps it in the queue until it decodes. A handler
that takes the bytes themselves, through a `Deserialized` type, receives an empty payload for such
a row. A row whose id does not decode fails the claim, and the error names the subscription and
the table; every claim that reaches the row fails the same way until the row is fixed.

# Batches

```no_run
# #[cfg(feature = "postgres")]
# mod demo {
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Email {
    to: String,
}

# async fn deliver(email: &Email) -> bool { !email.to.is_empty() }
// One claim is the batch, and each email settles its own row.
#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send_all(emails: &[Email]) -> Vec<HandlerOutcome> {
    let mut outcomes = Vec::with_capacity(emails.len());
    for email in emails {
        outcomes.push(if deliver(email).await {
            HandlerOutcome::ack()
        } else {
            HandlerOutcome::retry()
        });
    }
    outcomes
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send_all.batch(nonzero!(50)));
    })
}
# }
# fn main() {}
```

A batch is one claim: up to `batch(n)` rows of the queue. A row that does not decode is settled by
the decode policy, and the batch keeps its other rows. On SQLite the rows of a batch come in no
particular order.

In the row lock form the rows of a batch share the claim's transaction, which holds one
connection for the whole batch. Each delivery settles its own row in that transaction, and the
settlements take effect together, when the last delivery finishes. A settlement whose statement
fails rolls the whole batch back, its rows return, and the later settlements fail with
[`SqlxBrokerError::BatchRolledBack`]. A batch that holds a row whose statement always fails
therefore rolls back and returns all of its rows on every attempt, until the service's SQL or
schema is fixed.

In the lease form each delivery settles on its own, on a connection it takes for the settlement.

# Routes and by-name subscriptions

```no_run
# #[cfg(feature = "postgres")]
# mod demo {
use ruststream::OutgoingMessage;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Postgres};

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
pub struct Job {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for Job {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO jobs (name, payload) VALUES ($1, $2)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Deserialize)]
pub struct Signup {
    email: String,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "welcome")]
pub struct Welcome {
    email: String,
}

#[subscriber("signups", reply)]
async fn greet(signup: &Signup) -> Welcome {
    Welcome {
        email: signup.email.clone(),
    }
}

pub fn app(pool: PgPool) -> RustStream {
    // Both names lead into `jobs`: `signups` is read by name, and the replies become rows of the
    // `welcome` group.
    let broker = SqlxBroker::new(pool)
        .route::<Job>("signups")
        .route::<Job>("welcome");
    RustStream::new(AppInfo::new("signup", "0.1.0")).with_broker(broker, |b| {
        b.include(greet);
    })
}
# }
# fn main() {}
```

[`SqlxBroker::route`] leads a name into the table of a row type, through the row's [`Publish`]:
the service's own insert, which lays the name, the bytes and the headers out in its columns. A
route serves both directions. A publish to the name writes a row, and `#[subscriber("signups")]`
opens a subscription by that name on the same table. A name ending in `*` leads every name that
starts with what precedes it, and an exact name wins over a prefix.

A [`Repository`] names its table at compile time, so a publish through it is a static call.
[`Routed`], the broker's default for replies and `Out` slots, looks each message's name up: one
hash lookup for an exact name, the prefixes scanned only when none matches, then one dynamic call
into the row's `Publish`. That call's future stays in a 1024-byte slot, so a routed publish
allocates what a repository publish does; a row whose `Publish` future is larger is boxed, one
allocation per publish on its route. A name no route leads anywhere fails the publish with
[`SqlxBrokerError::NoRoute`].

A by-name subscription takes the broker's poll interval and lease. Where the route's row leaves
every event to the crate and holds column types the crate reads itself, listed on
[`NamedSubscriber`], the subscription reads the rows by those columns: no box and no dynamic call
per message, in either form, as through an [`InboxQueue`]. Any other row runs its own code, at one
boxed delivery and one boxed settlement future per message. By-name subscriptions run on a
dialect that implements [`ByName`] for its database, as every built-in dialect does, and refuse
`max_attempts(..)` and `dead_letter(..)` at startup: an [`InboxQueue`] takes those.

# Testing a service on the inbox

```no_run
# #[cfg(all(feature = "sqlite", feature = "chrono", feature = "testing"))]
# mod demo {
# use chrono::{DateTime, Utc};
# use ruststream::OutgoingMessage;
# use ruststream_sqlx::prelude::*;
# use serde::{Deserialize, Serialize};
# use sqlx::SqlitePool;
# #[derive(Inbox, sqlx::FromRow)]
# #[inbox(table = "thumbnail_jobs")]
# pub struct MakeThumbnail { #[field(id, generated)] id: i64, #[field(attempt, generated)] attempt: i16, #[field(locked_until)] locked_until: Option<DateTime<Utc>>, #[field(payload)] payload: Vec<u8> }
# impl Publish<Sqlite> for MakeThumbnail {
#     async fn publish(conn: &mut SqliteConnection, message: &OutgoingMessage<'_>) -> Result<(), sqlx::Error> {
#         sqlx::query("INSERT INTO thumbnail_jobs (payload) VALUES (?)").bind(message.payload()).execute(conn).await?;
#         Ok(())
#     }
# }
# #[derive(Serialize, Deserialize, Outgoing)]
# pub struct Image { path: String }
# #[subscriber(InboxQueue::<MakeThumbnail>::new("thumbnails"))]
# async fn thumbnail(_: &Image) -> HandlerOutcome { HandlerOutcome::ack() }
# pub fn app(pool: SqlitePool) -> RustStream {
#     RustStream::new(AppInfo::new("thumbnails", "0.1.0"))
#         .with_broker(SqlxBroker::new(pool).route::<MakeThumbnail>("thumbnails"), |b| { b.include(thumbnail); })
# }
use std::error::Error;

use ruststream::testing::TestApp;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Connection, Sqlite, SqliteConnection};

const SCHEMA: &str = "CREATE TABLE thumbnail_jobs (id INTEGER PRIMARY KEY, \
    attempt INTEGER NOT NULL DEFAULT 1, locked_until TEXT, payload BLOB NOT NULL)";

pub async fn a_thumbnail_is_made() -> Result<(), Box<dyn Error + Send + Sync>> {
    // An in-memory database that every connection naming it shares, alive while one of them
    // is open: the test holds one beside the pool.
    let options: SqliteConnectOptions =
        "sqlite:file:thumbnails?mode=memory&cache=shared".parse()?;
    let keeper = SqliteConnection::connect_with(&options).await?;
    let pool = SqlitePoolOptions::new().connect_with(options).await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;

    let tb = TestApp::start_live(app(pool)).await?;
    tb.broker::<SqlxBroker<Sqlite>>()
        .message(&Image { path: "cat.png".to_owned() })
        .to("thumbnails")
        .publish()
        .await?;
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("thumbnails")
        .assert_called_once();
    tb.shutdown().await?;
    keeper.close().await?;
    Ok(())
}
# }
# fn main() {}
```

A test runs the service against the database it runs on, because the service's SQL is part of
what the test checks. The message goes in through the route and the service's [`Publish`], as in
production. On Postgres, MySQL and MariaDB a test takes a database of its own on a server, with
the service's migrations applied. SQLite needs no server: a named in-memory database serves every
connection that names it and lives while one of them is open. The clock is real as well: a paused
tokio clock jumps to the next timer while a database reply is in flight, so a test starts with
`TestApp::start_live`, and `tb.advance(by)` lets that much real time pass.
