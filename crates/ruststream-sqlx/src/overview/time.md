# Time

```
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::DatabaseClock;
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

// invoice_jobs: id BIGSERIAL PRIMARY KEY, retry_after TIMESTAMPTZ NOT NULL DEFAULT now(),
// processed_at TIMESTAMPTZ, payload BYTEA NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "invoice_jobs", clock = DatabaseClock)]
pub struct SendInvoice {
    #[field(id, generated)]
    id: i64,
    #[field(retry_after, generated)]
    retry_after: DateTime<Utc>,
    #[field(processed_at, generated)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Invoice {
    number: String,
}

# async fn deliver(_: &Invoice) -> bool { true }
#[subscriber(InboxQueue::<SendInvoice>::new("invoices"))]
async fn send(invoice: &Invoice) -> HandlerOutcome {
    if deliver(invoice).await {
        return HandlerOutcome::ack();
    }
    // An hour by the database's clock, whichever host claimed the row.
    HandlerOutcome::retry_after(Duration::from_secs(3600))
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send);
    })
}
# }
# fn main() {}
```

A queue reads "now" when it claims a row, delays a retry and marks a row processed. By default
the host reads it: [`SystemClock`] takes the system clock, and the crate binds the time in the
column's own type, so sqlx encodes it as the service's own inserts do. Keep the clocks of the hosts
that serve one table in step, well within a lease.

`#[inbox(clock = DatabaseClock)]` makes the statements read the database's clock instead:
`statement_timestamp()` on Postgres, `UTC_TIMESTAMP(6)` on MySQL and MariaDB, and the current time
as text on SQLite. Every host then reads one clock. The lease form reads the host's clock, because
each settlement names the expiry its claim wrote: `locked_until` beside `DatabaseClock` does not
compile.

By hand, the table names its clock in its chain and in its type:

```
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Clock, Payload, ProcessedAt, RetryAfter};
use ruststream_sqlx::{DatabaseClock, InboxSpec, InboxTable, PayloadRow};
# use std::time::Duration;
# use ruststream_sqlx::prelude::*;
# use serde::Deserialize;
# use sqlx::PgPool;

#[derive(sqlx::FromRow)]
pub struct SendInvoice {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for SendInvoice {
    type Id = i64;
    type Table = InboxSpec<(
        Clock<DatabaseClock>,
        RetryAfter<DateTime<Utc>>,
        ProcessedAt<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("invoice_jobs", Column::new("id").generated())
        .clock::<DatabaseClock>()
        .retry_after(Column::new("retry_after").generated())
        .processed_at(Column::new("processed_at").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for SendInvoice {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}
# #[derive(Deserialize)]
# pub struct Invoice { number: String }
# #[subscriber(InboxQueue::<SendInvoice>::new("invoices"))]
# async fn send(invoice: &Invoice) -> HandlerOutcome {
#     tracing::info!(number = %invoice.number, "sending");
#     HandlerOutcome::retry_after(Duration::from_secs(3600))
# }
# pub fn app(pool: PgPool) -> RustStream {
#     RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
#         b.include(send);
#     })
# }
# }
# fn main() {}
```

A clock of the service's own implements [`Clock`](crate::Clock) and takes the same place,
`#[inbox(clock = ..)]` or `.clock::<..>()`; the example of [`SystemClock`] shows one.

A time column holds a [`QueueTime`]: `chrono::DateTime<Utc>` under the `chrono` feature, or
`time::OffsetDateTime` under `time`. SQLite keeps times as text, where `time` values sort right
only across seconds ([SQLite](#sqlite)).

A test reads the real clock too: it starts the service with `TestApp::start_live`, and
`tb.advance(by)` lets that much real time pass
([Testing a service on the inbox](#testing-a-service-on-the-inbox)).
