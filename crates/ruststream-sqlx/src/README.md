SQL databases for the [RustStream](https://github.com/powersemmi/ruststream) messaging framework,
through [`sqlx`](https://docs.rs/sqlx).

The crate serves task queues kept in the service's own tables, on Postgres, MySQL, MariaDB and
SQLite. A struct of the service's own describes a queue table: [`Inbox`] reads the table from
`#[inbox(..)]`, the column names from sqlx's attributes and the role of each column from
`#[field(..)]`. [`SqlxBroker`] serves the queues of the service's sqlx pool as a RustStream
broker: a subscription claims rows, a handler's outcome settles them, and a publish writes a row
through the service's own SQL.

The crate also serves a transactional outbox over any RustStream broker: what a service publishes
is recorded in its own table until a consumer has processed it, and what was not processed is
published again at startup. It is described in [the transactional outbox](#the-transactional-outbox).

The `inbox` feature turns the broker on, the `outbox` feature the outbox, and a service adds what
its database and its tables need:

- `postgres`, `mysql` (MySQL and MariaDB) and `sqlite`: a built-in dialect and its sqlx driver;
- `any`: an `AnyPool`, served by the dialect of the database it reaches;
- `chrono` or `time`: the time columns; `json`: the headers column;
- `testing`: the broker's in-process mode for `TestApp`, and the outbox's test switch;
  `asyncapi`: what a subscription adds to the generated document.

Where things are:

- [`Inbox`]: what a struct declares, the roles a column plays, the compile errors it meets.
- [`SqlxBroker`] and [`InboxQueue`]: the broker and its subscriptions, described below.
- [`Repository`] and [`Routed`]: publishing into tables.
- [`keys`]: what a handler reads off a delivery.
- [`InboxSettings::transactional`] and [`Tx`]: transactional mode, where a handler writes through
  its delivery's transaction.
- [`dialect`]: the SQL each database runs, and the traits a dialect of the service's own
  implements.
- [The transactional outbox](#the-transactional-outbox): `#[derive(Outbox)]`, `outbox!` and the
  registry's middlewares, under the `outbox` feature.

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
waits the poll interval, one second unless set. The bytes reach the codec lent from the row. A
table without a payload column hands its handler the row itself (see [Row mode](#row-mode)).

What a handler answers decides the row's fate:

- `ack()` deletes the row, or sets `processed_at` where the table has it;
- `retry()` returns the row to the queue at once, and its next delivery reads one more attempt;
- `retry_after(d)` hides the row until `retry_after` comes; a table without that column returns
  it at once, and the runtime logs a warning;
- `drop()` finishes the row as `ack()` does;
- `max_attempts(n)` and `dead_letter(..)` on the mount come together: `attempt` counts the
  deliveries, and at the cap the row moves to another group or into a table with the same
  columns. With `max_attempts(1)` every failure moves the row at once. A mount that declares one
  without the other stops at startup.

"Now" comes from [`SystemClock`] unless the struct names another source:
`#[inbox(clock = DatabaseClock)]` reads the database's clock, and a service's own [`Clock`] fits
there too. Hosts that bind "now" keep their clocks in step.


## An event of the service's own

```no_run
# #[cfg(feature = "postgres")]
# mod demo {
use ruststream_sqlx::Ack;
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::{PgConnection, PgPool, Postgres};

// email_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, payload BYTEA NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs", custom(ack))]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

// A sent email stays in its table, in the `sent` group, for an audit.
impl Ack<Postgres> for SendEmail {
    async fn ack(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE email_jobs SET name = 'sent' WHERE job_id = $1")
            .bind(id)
            .execute(conn)
            .await?;
        Ok(())
    }
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
    RustStream::new(AppInfo::new("mailer", "0.1.0"))
        .with_broker(SqlxBroker::new(pool), |b| {
            b.include(send);
        })
}
# }
# fn main() {}
```

An event listed in `custom(..)` runs the service's own SQL in place of the statement the dialect
builds, and every other event keeps the dialect's. The table and the broker stay as they were.
Each event is a trait the struct implements for its database: [`Claim`], [`Fetch`], [`Ack`],
[`Retry`], [`RetryAfter`], [`Discard`], [`DeadLetter`], [`Extend`], and [`Lock`] and [`Unlock`]
in the advisory lock form. A listed event without its impl does not compile, and the error names
the trait. A settlement of the service's own runs in the transaction the crate commits, so it
settles the row as the built-in one does. An acknowledgement takes the row out of what the claim
selects: a row it leaves claimable is delivered again.
