# Macro or manual

```no_run
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::spec::{Attempt, Payload, ProcessedAt, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::Deserialize;
use sqlx::PgPool;

// email_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT, retry_after TIMESTAMPTZ DEFAULT now(),
// attempt SMALLINT DEFAULT 1, processed_at TIMESTAMPTZ, payload BYTEA
#[derive(sqlx::FromRow)]
pub struct SendEmail {
    job_id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SendEmail {
    type Id = i64;
    type Table = InboxSpec<(
        RetryAfter<DateTime<Utc>>,
        Attempt,
        ProcessedAt<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("job_id").generated())
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after").generated())
        .attempt(Column::new("attempt").generated())
        .processed_at(Column::new("processed_at").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.job_id
    }
}

impl PayloadRow for SendEmail {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl AttemptRow for SendEmail {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
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
    HandlerOutcome::retry_after(Duration::from_secs(60 * attempt.unwrap_or(1)))
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send);
    })
}
# }
# fn main() {}
```

`#[derive(Inbox)]` and `#[derive(InboxHeaders)]` describe a table through a public API, and a
service may write that description by hand: for a declaration a reviewer reads as it is, or for a
struct that holds fewer fields than the table has columns. The table above is the one [the inbox
broker](#the-inbox-broker) derives. Both forms build the same description, the same statements and
the same per-message code.

A table described by hand implements [`InboxTable`]. `TABLE` holds its description, built by
[`InboxSpec`]: `new` takes the table's name and the id column, and each setter adds a setting.
Every setter is a `const fn`, so the description is a constant and costs nothing at run time.

`type Table` names the builder's final type. Stable Rust does not infer the type of an associated
constant, so the impl writes it once, and the compiler holds the chain to it: a chain that does
not match the type is a type mismatch. The type lists the settings the crate reads as types, in
the order the chain sets them:

- the form: [`Lease<Time>`](spec::Lease) or [`Advisory`](spec::Advisory);
- payload mode: [`Payload`](spec::Payload);
- the roles a delivery reads off a row: [`Key`](spec::Key), [`Attempt`](spec::Attempt),
  [`Headers`](spec::Headers), [`HeaderFields`](spec::HeaderFields);
- the time roles with their time type: [`RetryAfter<Time>`](spec::RetryAfter),
  [`ProcessedAt<Time>`](spec::ProcessedAt);
- [`Fifo`](spec::Fifo), the clock [`Clock<Source>`](spec::Clock) and the opening
  [`Opens<Level>`](spec::Opens);
- the events the service writes itself, the markers of [`spec::own`].

The time types in the chain come from this type. The column-only setters (`within`, `group`,
`priority`, `data`, `fetching`, `selecting_all`) leave the type as it is. A setting the chain
leaves out keeps its default: the row lock form, row mode, [`SystemClock`], the database's own
opening and the crate's events.

The struct holds only what the service reads. A column only the queue reads, such as `name` or
`processed_at` above, is named in the description and needs no field. A role a delivery reads off
the row is a small trait, implemented where the type names the role:

| In `type Table` | Trait | What it lends |
| --- | --- | --- |
| `Payload` | [`PayloadRow`] | the message bytes, and the column's type |
| `Key` | [`KeyRow`] | the partition key |
| `Attempt` | [`AttemptRow`] | the attempt count |
| `Headers` | [`HeaderRow`] | the field that holds the headers column |
| `HeaderFields` | [`HeaderFields`] | the header map, built with [`put_header`] |

A role in the type without its trait does not compile, and the error names the trait.

A table without `.payload(..)` is in row mode, and its struct says so with one more impl: `impl
Input for SendEmail { type Axis = SoloCarried<Self>; }` from `ruststream::runtime`. `Input` is the
core's trait, so the crate cannot write it for the service's type, and the derive writes that line
too. The struct derives `Clone`, and `.data(..)` names the columns of its own data, which the claim
reads beside the roles ([row mode](#row-mode) shows a whole table).

An event the service writes itself is a marker in the chain, `.own::<own::Ack>()`, with
`own::Ack` in the type, where the derive takes `custom(ack)`. The struct implements the event's
trait, and a named event without its impl does not compile at the mount site.

[`dialect::insert`] renders the built-in insert from the description, in a constant:

```
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
# use chrono::{DateTime, Utc};
# use ruststream_sqlx::dialect::Column;
# use ruststream_sqlx::spec::{Attempt, Payload, ProcessedAt, RetryAfter};
# use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use ruststream_sqlx::dialect::insert::{self, Sql};
use sqlx::PgConnection;
# #[derive(sqlx::FromRow)]
# pub struct SendEmail { job_id: i64, attempt: i16, payload: Vec<u8> }
# impl InboxTable for SendEmail {
#     type Id = i64;
#     type Table = InboxSpec<(RetryAfter<DateTime<Utc>>, Attempt, ProcessedAt<DateTime<Utc>>, Payload)>;
#     const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("job_id").generated())
#         .group(Column::new("name"))
#         .retry_after(Column::new("retry_after").generated())
#         .attempt(Column::new("attempt").generated())
#         .processed_at(Column::new("processed_at").generated())
#         .payload(Column::new("payload"));
#     fn id(&self) -> &i64 { &self.job_id }
# }
# impl PayloadRow for SendEmail { type Column = Vec<u8>; fn payload(&self) -> &[u8] { &self.payload } }
# impl AttemptRow for SendEmail { type Attempt = i16; fn attempt(&self) -> &i16 { &self.attempt } }

// INSERT INTO "email_jobs" ("name", "payload") VALUES ($1, $2)
const INSERT: Sql<128> = insert::postgres(&SendEmail::TABLE.spec());

/// Queues an email in the service's own transaction, so it commits with the order it belongs to.
pub async fn enqueue(tx: &mut PgConnection, email: &[u8]) -> Result<(), sqlx::Error> {
    sqlx::query(INSERT.as_str())
        .bind("emails")
        .bind(email)
        .execute(tx)
        .await?;
    Ok(())
}
# }
# fn main() {}
```

The insert names every column the database does not fill, and binds them in the description's
order: the roles, then the data columns. The derive's [`insert`](Insert::insert) takes its text
from the same writer. A capacity `N` the text outgrows is a build error that names the size to
raise it to.

## Where each rule is checked

The derive checks a table while it expands, and points at the field at fault. By hand, a rule is
held by a type where it can be, and otherwise by the evaluation of the constant while the service
builds:

| Rule | Derive | By hand |
| --- | --- | --- |
| an id and a table name | compile | the constructor's arguments |
| one form, one message mode, one clock, one opening; each typed role and each own event once | compile | the setter's bound, at compile time |
| a role in the type has its trait; an own event is implemented | compile | a trait bound, at compile time |
| the lease form on the host's clock; no FIFO group and no own claim in the advisory lock form; an own extend only in the lease form; own lock and unlock together, only in the advisory lock form | compile | a bound on `type Table`, at compile time |
| a column-only role set once; a column named once | compile | the constant's evaluation, at build time |
| an outbox name tracked once | compile (`outbox!`) | the evaluation of [`track`](outbox::Outbox::track), at build time |
| an outbox name registered once through `register("..")` | n/a | app construction |
| a lock key names a column the table has | compile | the statement's prepare, at startup |

A refusal at build time names the table and the rule, and points at the crate's check rather
than the service's line. That is the one place where the manual form reads worse than the derive.
