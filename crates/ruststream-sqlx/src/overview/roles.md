# Roles

```
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

// support_tickets: id BIGSERIAL PRIMARY KEY, queue TEXT NOT NULL, urgency SMALLINT NOT NULL,
// customer TEXT NOT NULL, retry_after TIMESTAMPTZ NOT NULL DEFAULT now(),
// attempt SMALLINT NOT NULL DEFAULT 1, processed_at TIMESTAMPTZ, payload BYTEA NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "support_tickets")]
pub struct Ticket {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    queue: String,
    #[field(priority)]
    urgency: i16,
    #[field(partition_key)]
    customer: String,
    #[field(retry_after, generated)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(processed_at, generated)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Question {
    text: String,
}

# async fn answer(_: &Question) -> bool { true }
#[subscriber(InboxQueue::<Ticket>::new("billing"))]
async fn triage(question: &Question) -> HandlerOutcome {
    if answer(question).await {
        return HandlerOutcome::ack();
    }
    HandlerOutcome::retry_after(Duration::from_secs(300))
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("support", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(triage.workers_by_key(nonzero!(4)))
            .max_attempts(nonzero!(5u32))
            .dead_letter("escalated");
    })
}
# }
# fn main() {}
```

Each role is a switch: a column that plays it turns on one behaviour of the queue, and a table
without it keeps the default. The subscription above reads the `billing` group, takes urgent
tickets first, keeps each customer's tickets in order on one worker, delays a failed ticket by
five minutes, and moves it to the `escalated` group at the fifth failure. An acknowledged ticket
stays in the table with its `processed_at` set.

| Role | By hand | With the role | Without it |
| --- | --- | --- | --- |
| `group` | `.group(..)` | a subscription reads the group its name selects; a dead letter moves the row to another group | a subscription reads the whole table; a dead letter moves the row into another table |
| `group, fifo = true` | `.fifo_group(..)` | one row of a group is in work at a time, in claim order ([FIFO groups](#fifo-groups)) | the rows of a group run side by side |
| `priority` | `.priority(..)` | a smaller value is claimed first | rows are claimed by `retry_after` where the table has it, then by id |
| `partition_key` | `.partition_key(..)` | the delivery's partition key: under `workers_by_key(n)` the rows of one key run in order | the delivery has no key |
| `retry_after` | `.retry_after(..)` | `retry_after(d)` hides the row for `d` | `retry_after(d)` returns the row at once, with a warning |
| `attempt` | `.attempt(..)` | `Ctx<keys::Attempt>` reads the count, and `max_attempts(n)` caps it | `Ctx<keys::Attempt>` reads `None`, and a mount with `max_attempts(n)` stops at startup |
| `processed_at` | `.processed_at(..)` | acknowledgement marks the row | acknowledgement deletes the row |
| `locked_until` | `.lease(..)` | the lease form ([the lease form](#the-lease-form)) | the row lock form, or the advisory lock form under `advisory_lock` |
| `headers` | `.headers(..)` | the delivery's headers, kept in one column | the delivery's headers are empty |
| `payload` | `.payload(..)` | payload mode: the handler takes the decoded payload | row mode: the handler takes the row ([Row mode](#row-mode)) |

`generated` beside a role marks a column the database fills in, and the derive's insert leaves it
out. Every role but `id` is optional, and each plays on one column at most.

By hand, each role is a setter of [`InboxSpec`]. A role a delivery reads off the row also names
its marker in `type Table` and implements its trait: `Payload` and [`PayloadRow`], `Key` and
[`KeyRow`], `Attempt` and [`AttemptRow`], `Headers` and [`HeaderRow`]. The roles only the queue
reads need no field in the struct:

```
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Key, Payload, ProcessedAt, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, KeyRow, PayloadRow};
# use std::time::Duration;
# use ruststream_sqlx::prelude::*;
# use serde::Deserialize;
# use sqlx::PgPool;

#[derive(sqlx::FromRow)]
pub struct Ticket {
    id: i64,
    customer: String,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for Ticket {
    type Id = i64;
    type Table = InboxSpec<(
        Key,
        RetryAfter<DateTime<Utc>>,
        Attempt,
        ProcessedAt<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("support_tickets", Column::new("id").generated())
        .group(Column::new("queue"))
        .priority(Column::new("urgency"))
        .partition_key(Column::new("customer"))
        .retry_after(Column::new("retry_after").generated())
        .attempt(Column::new("attempt").generated())
        .processed_at(Column::new("processed_at").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl KeyRow for Ticket {
    type Key = String;

    fn partition_key(&self) -> &String {
        &self.customer
    }
}

impl AttemptRow for Ticket {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for Ticket {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}
# #[derive(Deserialize)]
# pub struct Question { text: String }
# #[subscriber(InboxQueue::<Ticket>::new("billing"))]
# async fn triage(question: &Question) -> HandlerOutcome {
#     tracing::info!(text = %question.text, "triaging");
#     HandlerOutcome::retry_after(Duration::from_secs(300))
# }
# pub fn app(pool: PgPool) -> RustStream {
#     RustStream::new(AppInfo::new("support", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
#         b.include(triage.workers_by_key(nonzero!(4)))
#             .max_attempts(nonzero!(5u32))
#             .dead_letter("escalated");
#     })
# }
# }
# fn main() {}
```
