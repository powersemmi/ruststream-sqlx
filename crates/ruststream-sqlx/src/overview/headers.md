# The headers layout

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use ruststream_sqlx::Fetch;
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::{PgConnection, PgPool, Postgres};

// order_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT, attempt SMALLINT DEFAULT 1, tenant TEXT,
// trace TEXT NULL, order_id BIGINT, note TEXT NULL
// orders: id BIGINT PRIMARY KEY, customer TEXT, total BIGINT
/// The queue table: the columns that run the queue, and the headers `tenant`, `trace` and
/// `order_id`.
#[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
#[inbox(table = "order_jobs")]
pub struct OrderHeaders {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(attempt, generated)]
    attempt: i16,
    tenant: String,
    trace: Option<String>,
    order_id: i64,
}

/// A job and its note, read from the queue table by the default fetch.
#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
pub struct OrderJob {
    #[field(headers)]
    #[sqlx(flatten)]
    headers: OrderHeaders,
    note: Option<String>,
}

/// A job and the order it is for, read by the service's own fetch over a join.
#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
#[inbox(custom(fetch))]
pub struct OrderMail {
    #[field(headers)]
    #[sqlx(flatten)]
    headers: OrderHeaders,
    customer: String,
    total: i64,
}

impl Fetch<Postgres> for OrderMail {
    async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT j.job_id, j.name, j.attempt, j.tenant, j.trace, j.order_id, \
                    o.customer, o.total \
             FROM order_jobs j JOIN orders o ON o.id = j.order_id \
             WHERE j.job_id = ANY($1)",
        )
        .bind(ids)
        .fetch_all(conn)
        .await
    }
}

#[derive(Deserialize)]
pub struct Tenant {
    tenant: String,
    trace: Option<String>,
}

#[subscriber(InboxQueue::<OrderJob>::new("packing"))]
async fn pack(job: &OrderJob) -> HandlerOutcome {
    tracing::info!(attempt = job.headers.attempt, note = ?job.note, "packing");
    HandlerOutcome::ack()
}

// The header map is built from `OrderHeaders` on its first read, here by `Headers<Tenant>`.
#[subscriber(InboxQueue::<OrderMail>::new("receipts"))]
async fn send_receipt(mail: &OrderMail, Headers(tenant): Headers<Tenant>) -> HandlerOutcome {
    tracing::info!(
        tenant = %tenant.tenant,
        trace = ?tenant.trace,
        customer = %mail.customer,
        total = mail.total,
        "sending the receipt"
    );
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(pack);
        // A job whose order is gone fails to decode: the default policy drops it.
        b.include(send_receipt);
    })
}
# }
# fn main() {}
```

By hand, one description on the message holds the whole table. The headers struct is a plain sqlx
struct, `.data(..)` names the header columns, `.fetching(..)` the message's own, and
`.header_fields()` builds the header map from the fields through [`HeaderFields`]:

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use ruststream::HeaderMap;
use ruststream::runtime::{Input, SoloCarried};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::spec::{self, Attempt};
use ruststream_sqlx::{AttemptRow, HeaderFields, InboxSpec, InboxTable, put_header};
# use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderHeaders {
    job_id: i64,
    attempt: i16,
    tenant: String,
    trace: Option<String>,
    order_id: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderJob {
    #[sqlx(flatten)]
    headers: OrderHeaders,
    note: Option<String>,
}

impl InboxTable for OrderJob {
    type Id = i64;
    type Table = InboxSpec<(Attempt, spec::HeaderFields)>;
    const TABLE: Self::Table = InboxSpec::new("order_jobs", Column::new("job_id").generated())
        .group(Column::new("name"))
        .attempt(Column::new("attempt").generated())
        .data(&[Column::new("tenant"), Column::new("trace"), Column::new("order_id")])
        .fetching(&[Column::new("note")])
        .header_fields();

    fn id(&self) -> &i64 {
        &self.headers.job_id
    }
}

impl Input for OrderJob {
    type Axis = SoloCarried<Self>;
}

impl AttemptRow for OrderJob {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.headers.attempt
    }
}

impl HeaderFields for OrderJob {
    const NAMES: &'static [&'static str] = &["tenant", "trace", "order_id"];

    fn header_map(&self) -> HeaderMap {
        let mut headers = HeaderMap::with_capacity(Self::NAMES.len());
        put_header(&mut headers, "tenant", &self.headers.tenant);
        put_header(&mut headers, "trace", &self.headers.trace);
        put_header(&mut headers, "order_id", &self.headers.order_id);
        headers
    }
}
# #[subscriber(InboxQueue::<OrderJob>::new("packing"))]
# async fn pack(job: &OrderJob) -> HandlerOutcome {
#     tracing::info!(attempt = job.headers.attempt, note = ?job.note, "packing");
#     HandlerOutcome::ack()
# }
# pub fn app(pool: PgPool) -> RustStream {
#     RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
#         b.include(pack);
#     })
# }
# }
# fn main() {}
```

A message read by a fetch of its own adds `.own::<spec::own::Fetch>()` to the chain, with
`spec::own::Fetch` in the type, and implements [`Fetch`] as `OrderMail` does above.

In the headers layout of the derives, two structs describe one queue. The headers struct derives
[`InboxHeaders`](derive@InboxHeaders) and describes the queue table. The message struct derives
[`Inbox`](derive@Inbox) and is what a handler takes. It holds the headers struct in a field marked
`#[field(headers)]` and `#[sqlx(flatten)]`, beside data of its own. Without `#[sqlx(flatten)]`, a
`headers` field is a header column, as in a flat struct.

The headers layout is row mode. A handler takes the message struct itself, `&OrderJob`, as the
driver read it, with no codec. A batch handler takes `&[OrderJob]`, the rows of one claim. The
message struct derives `Clone` and does not derive `Deserialize`, as in [row mode](#row-mode).
Several message structs may flatten one headers struct, each read by its own subscriptions.

## Where the attributes go

The headers struct carries everything that describes the table: `#[inbox(table, schema,
advisory_lock, clock, isolation, mode)]` and the fields that play a role (`id`, `group`,
`attempt`, `locked_until` and the rest). So the headers struct selects the form: a
`#[field(locked_until)]` field gives the lease form, `advisory_lock = ".."` the advisory lock
form, with a key built from the headers struct's fields. The message's deliveries hold the lease
or the lock as a flat struct's do.

The message struct carries `#[inbox(custom(..))]` alone, and no role but `headers`. A role or a
table attribute on the message does not compile, and the error names the headers struct as the
place for it. A custom event the headers struct's form does not take does not compile either:
`claim` in the advisory lock form, `extend` outside the lease form, `lock` and `unlock` outside
the advisory lock form.

The headers struct gets the generated [`insert`](Insert::insert), which writes the table's columns.
The message struct gets none: the service writes the message's own data, or writes the whole row
through a [`Publish`] of its own. A table described by hand writes its insert itself, over the text
[`dialect::insert`] renders ([row mode](#row-mode) shows one).

## The default fetch

The claim selects the ids of the rows it takes, and a fetch reads their rows. A message struct
without `custom(fetch)` is read by the default fetch. It names the headers struct's columns and
the message's own fields, and reads them from the queue table. A subscription prepares it when it
opens: a message field whose column the table lacks stops the subscription at startup with
[`SqlxBrokerError::Schema`], which names the table and the statement.

## The header values

Every field of the headers struct without a role is a header of the delivery. Its name is the
column's name, as sqlx's `rename` and `rename_all` set it. Its value comes from [`HeaderField`]:

- strings and byte vectors give their bytes;
- integers and `bool` give their decimal text;
- `chrono::DateTime<Utc>` (feature `chrono`) and `time::OffsetDateTime` (feature `time`) give
  their RFC 3339 text;
- `Option<T>` gives its value, and `None` leaves the header out.

A service implements [`HeaderField`] for a type of its own. A field of a type without it does not
compile, and the error offers the impl or a role.

The header map is built on the first read of the delivery's headers, by middleware or an
extractor such as `Headers<T>`. A delivery whose headers nobody reads never builds it. The fields
that play a role are not headers: the handler reads them on the message.

A message published into the table through a [`Repository`] carries only the headers the headers
struct names. A publish with another header fails with [`SqlxBrokerError::Header`], so no header
is lost on the way into the table.

## A fetch over a join

A message whose data lives in other tables lists `fetch` in `#[inbox(custom(..))]` and implements
[`Fetch`] for its database. The fetch takes the claimed ids and returns the rows it found. Its
`SELECT` returns the headers struct's columns under their names, beside the message's own, and
the crate matches the rows to the claimed ids by the headers struct's `id` field. The fetch
selects every column of the headers struct, `locked_until` included in the lease form.

The fetch runs on the claim's connection: in the claim's transaction in the row lock form, and
once per row, with that row's id alone, in the advisory lock form. A
subscription to such a message does not check the message's own columns at startup, because the
fetch reads them from wherever they live.

## A claimed id without a row

A claimed id that the fetch returned no row for, such as a job whose order the join does not find,
settles by the subscription's decode policy, `on_failure(decode = ..)`, before the handler runs.
The default policy drops the row. The log names the id. A batch lends its handler the rows the
fetch found, and the policy settles the others. The headers of such a delivery are empty.
