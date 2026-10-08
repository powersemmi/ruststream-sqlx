# FIFO groups

```no_run
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use chrono::{DateTime, Utc};
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

// sync_jobs: id BIGSERIAL PRIMARY KEY, target TEXT NOT NULL,
// retry_after TIMESTAMPTZ NOT NULL DEFAULT now(), payload BYTEA NOT NULL
// Each group holds the changes bound for one system.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "sync_jobs")]
pub struct SyncChange {
    #[field(id, generated)]
    id: i64,
    #[field(group, fifo = true)]
    target: String,
    #[field(retry_after, generated)]
    retry_after: DateTime<Utc>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Change {
    record: String,
}

# async fn push(change: &Change) -> bool { !change.record.is_empty() }
// The CRM receives its changes one at a time, in the order they were queued.
#[subscriber(InboxQueue::<SyncChange>::new("crm"))]
async fn sync(change: &Change) -> HandlerOutcome {
    if push(change).await {
        return HandlerOutcome::ack();
    }
    // At once, so the change keeps its place at the head of the group.
    HandlerOutcome::retry()
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("crm-sync", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(sync);
    })
}
# }
# fn main() {}
```

By hand, `.fifo_group(..)` names the group's column, and `Fifo` goes into the type:

```no_run
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Fifo, Payload, RetryAfter};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
# use ruststream_sqlx::prelude::*;
# use serde::Deserialize;
# use sqlx::PgPool;

#[derive(sqlx::FromRow)]
pub struct SyncChange {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for SyncChange {
    type Id = i64;
    type Table = InboxSpec<(Fifo, RetryAfter<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("sync_jobs", Column::new("id").generated())
        .fifo_group(Column::new("target"))
        .retry_after(Column::new("retry_after").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for SyncChange {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}
# #[derive(Deserialize)]
# pub struct Change { record: String }
# #[subscriber(InboxQueue::<SyncChange>::new("crm"))]
# async fn sync(change: &Change) -> HandlerOutcome {
#     let _ = &change.record;
#     HandlerOutcome::ack()
# }
# pub fn app(pool: PgPool) -> RustStream {
#     RustStream::new(AppInfo::new("crm-sync", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
#         b.include(sync);
#     })
# }
# }
# fn main() {}
```

`#[field(group, fifo = true)]` keeps each group of the table in order. A group has at most one
row in work at a time, however many brokers read it. Its rows go out in claim order: a smaller
`priority` first, then an earlier `retry_after`, then the smaller id, each where the table has the
column. The group's first unfinished row in that order is its head. A claim takes the head alone.
The rows behind the head wait while it is in work or not yet due.

`retry()` keeps the row at the head. `retry_after(d)` gives the row a later `retry_after`, and it
moves behind the rows that come due before it. On a table with `priority` it moves only among the
rows of its own priority. A head that keeps failing with `retry()` holds its group back.
`max_attempts(n)` with `dead_letter(..)` moves such a row out of the group after n attempts. In
the lease form a crash with a row in work holds its group until that row's lease runs out.

`workers(n)` gives no parallelism inside a group, so one worker serves a FIFO subscription. A
batch of a FIFO group holds one row, its head. Groups run side by side, each through a
subscription of its own.

FIFO groups serve the row lock and lease forms. In the advisory lock form a key on the group's field
keeps one row of the group in work, as `advisory_lock = "sync_jobs-{target}"` would here. `fifo =
true` beside `advisory_lock` does not compile, and the error says to put the group's field into the
key. By hand, `Fifo` beside `Advisory` in the type does not compile either.

