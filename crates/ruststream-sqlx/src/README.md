SQL databases for the [RustStream](https://github.com/powersemmi/ruststream) messaging framework,
through [`sqlx`](https://docs.rs/sqlx).

The crate has two parts. The inbox is a task queue in the service's own table, on Postgres, MySQL,
MariaDB and SQLite: a struct describes the table, [`SqlxBroker`] serves it as a RustStream
broker, a subscription claims rows, and a handler's outcome settles them. The outbox records what
a service publishes on any RustStream broker until a consumer has processed it, and publishes the
rest again at startup.

# A service

A queue in a Postgres table, a handler that owns its backoff, and a route that writes a publish
to `emails` through the service's own `INSERT`:

```no_run
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgConnection, Postgres};

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

#[ruststream::app]
fn app() -> impl App {
    // The options read the `PG*` environment variables.
    let broker = SqlxBroker::from_connect_options(PgConnectOptions::new(), PgPoolOptions::new());
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        broker.route::<SendEmail>("emails"),
        |b| {
            b.include(send);
        },
    )
}
# }
# fn main() {}
```

[`SqlxBroker::from_connect_options`] only records the pool's settings, so the app's builder runs
before the runtime starts. The runtime connects the broker at startup, which builds the pool then,
and shuts it down when the service stops, which closes it. [`SqlxBroker::new`] takes a pool the
service shares with code of its own and leaves it open.

# Where things are

The [guides][guides] build a service on SQL databases, in this order:
[getting started][g-start], [enqueueing jobs][g-enqueueing], [retries and dead letters][g-retries],
[long jobs and the claim form][g-forms], [ordering][g-ordering],
[transactional handlers][g-transactional], [several instances][g-scaling],
[relaying events to another broker][g-relay], [the outbox][g-outbox], [testing][g-testing] and
[running in production][g-production]. The [architecture][architecture] pages show how a queue in
a SQL table works and where the crate sits between the database and the runtime, and the
[benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [SqlxBroker][c-broker]: [`SqlxBroker`], connected into [`ConnectedSqlxBroker`] and closed into
  [`ClosedSqlxBroker`].
- [Connections][c-connections]: what each message in work holds of the pool, and
  [`SqlxBroker::connection_limit`].
- [Startup][c-startup]: the statements a subscription prepares when it opens.
- [Shutdown][c-shutdown]: the advisory locks released, and a publisher kept past it.
- [Prelude][c-prelude]: [`prelude`], the one glob an inbox service imports.
- [Errors][c-errors]: [`SqlxBrokerError`].

## Tables

- [Inbox][c-tables]: [`Inbox`](derive@Inbox), a queue table described by a struct.
- [Roles][c-roles]: what each `#[field(..)]` role turns on, and the row traits by hand
  ([`PayloadRow`], [`KeyRow`], [`AttemptRow`], [`HeaderRow`]).
- [Columns][c-columns]: column names from sqlx's attributes.
- [InboxSpec][c-inbox-spec]: [`InboxTable`] and [`InboxSpec`], a table described by hand, with its
  settings in [`spec`].
- [Insert][c-insert]: [`Insert`], a task written in the service's own transaction, and
  [`dialect::insert`] by hand.
- [Events][c-events]: `custom(..)` and [`spec::own`], one event in the service's own SQL
  ([`Claim`], [`Fetch`], [`Ack`], [`Retry`], [`RetryAfter`], [`Discard`], [`DeadLetter`],
  [`Extend`]).
- [checked][c-checked]: `#[inbox(checked, db = ..)]`, the statements in sqlx's compile-time check.

## Messages

- [Payload][c-payload]: payload mode, the column's bytes lent to the codec.
- [Rows][c-rows]: row mode, a handler of the row itself, and [`RowBatch`].
- [Decoding][c-decoding]: a row that does not decode, settled by the decode policy.

## Headers

- [Headers][c-headers]: a `headers` column of JSON strings.
- [HeaderColumn][c-header-column]: [`HeaderColumn`], a headers column of the service's own type.
- [InboxHeaders][c-inbox-headers]: [`InboxHeaders`](derive@InboxHeaders), a message assembled
  from a headers struct.
- [HeaderField][c-header-field]: [`HeaderField`], the header value of a field, and
  [`HeaderFields`] with [`put_header`] by hand.
- [Fetch][c-fetch]: `custom(fetch)`, a message read over a join.

## Subscriptions

- [InboxQueue][c-subscriptions]: [`InboxQueue`], one queue of a table, opened as an
  [`InboxSubscriber`].
- [poll_interval][c-poll-interval]: [`InboxQueue::poll_interval`] and
  [`SqlxBroker::poll_interval`].
- [Workers][c-workers]: claims in flight for `workers(n)`, and `workers_by_key(n)`.
- [Batches][c-batches]: one claim as a batch.
- [FIFO][c-fifo]: `#[field(group, fifo = true)]`, one row of a group in work.
- [on_threads][c-on-threads]: [`InboxSettings::on_threads`] and [`InboxThreads`].
- [Names][c-names]: `#[subscriber("name")]`, a [`NamedSubscriber`] through a route.

## Forms

- [Forms][c-forms]: the row lock form, `FOR UPDATE SKIP LOCKED` held while the handler runs.
- [Lease][c-lease]: `#[field(locked_until)]`, [`InboxQueue::lease`] and [`SqlxBroker::lease`].
- [Advisory][c-advisory]: `advisory_lock = ".."`, a lock on each row's key.
- [Lock][c-lock]: `custom(lock, unlock)` with [`Lock`] and [`Unlock`].

## Settlement

- [Outcomes][c-settlement]: what `ack`, `retry` and `drop` do to a row.
- [retry_after][c-retry-after]: a row hidden until its `retry_after` comes.
- [dead_letter][c-dead-letter]: `max_attempts(n)` with `dead_letter(..)`.

## Transactions

- [Transactional][c-transactions]: [`InboxSettings::transactional`] and [`Tx`], the handler's
  writes committed with the settlement.
- [Isolation][c-isolation]: `isolation = ..` and SQLite's `mode = ..`.

## Context

- [Context][c-context]: [`keys`], what a handler reads through `Ctx<..>`.
- [Attempt][c-attempt]: [`keys::Attempt`].
- [Pool][c-pool]: [`keys::Pool`], writes that commit on their own.

## Time

- [Time][c-time]: [`SystemClock`], "now" from the host.
- [DatabaseClock][c-database-clock]: [`DatabaseClock`], "now" from the database.
- [Clock][c-clock]: [`Clock`], a clock of the service's own.
- [QueueTime][c-queue-time]: [`QueueTime`], the time types `chrono` and `time`.

## Publishing

- [Repository][c-publishing]: [`Publish`] and [`Repository`], a publish written into a table.
- [route][c-route]: [`SqlxBroker::route`] and [`Routed`].

## Wake

- [Wake][c-wake]: a publish that wakes the subscriptions of its table.
- [listen_notify][c-listen-notify]: [`SqlxBroker::listen_notify`] on Postgres.

## Databases

- [Databases][c-databases]: [`SqlxBroker::new`] and [`BuiltIn`], the dialect built in for the
  pool's database.
- [Postgres][c-postgres], [MySQL][c-mysql] and [SQLite][c-sqlite]: what each database serves.
- [AnyPool][c-any-pool]: an `AnyPool`, served by the dialect of the database it reaches.
- [Dialect][c-dialect]: [`SqlxBroker::with_dialect`] and [`dialect`], a dialect of the service's
  own.

## Tooling

- [AsyncAPI][c-asyncapi]: the [server][c-asyncapi-server] and the [channels][c-asyncapi-channels]
  the generated document names (feature `asyncapi`).
- [Testing][c-testing]: the in-process mode
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker through (feature `testing`), and [Live][c-live] on the real clock.

## Outbox

- [Transactional outbox][c-outbox]: [`outbox`](mod@outbox), what a service publishes tracked
  until a consumer processed it.
- [Record][c-record]: [`Outbox`](derive@Outbox) and [`OutboxTable`](outbox::OutboxTable), the
  outbox table.
- [Registry][c-registry]: [`outbox!`](macro@outbox), [`Outbox::track`](outbox::Outbox::track) and
  [`Outbox::register`](outbox::Outbox::register).
- [Tracking][c-tracking]: [`Outbox::layer`](outbox::Outbox::layer) and
  [`Outbox::publish_layer`](outbox::Outbox::publish_layer).
- [republish][c-republish]: [`Outbox::republish`](outbox::Outbox::republish) at startup.
- [wrap][c-wrap]: [`Outbox::wrap`](outbox::Outbox::wrap), a publisher outside the pipeline.
- [set_pool][c-set-pool]: [`Outbox::set_pool`](outbox::Outbox::set_pool) inside
  `#[ruststream::app]`.
- [Statements][c-statements]: `custom(..)`, the record's events in the service's own SQL.
- [Interplay][c-interplay]: the outbox beside the inbox.
- [Switch][c-switch]: `RUSTSTREAM_SQLX_OUTBOX=on` in test builds.
- [OutboxError][c-outbox-error]: [`OutboxError`](outbox::OutboxError).

# Cargo features

`inbox` turns the broker on and `outbox` the outbox, each independently of the other. `postgres`,
`mysql` (MySQL and MariaDB) and `sqlite` add a built-in dialect and its sqlx driver, and `any` an
`AnyPool` served by the dialect of the database it reaches. `chrono` or `time` add the time
columns, and `json` a headers column. `testing` adds the broker's in-process mode for `TestApp`
and the outbox's test switch, and `asyncapi` what a subscription adds to the generated document.

[guides]: https://powersemmi.github.io/ruststream-sqlx/latest/
[g-start]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/getting-started/
[g-enqueueing]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/enqueueing/
[g-retries]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/retries/
[g-forms]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/claim-forms/
[g-ordering]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/ordering/
[g-transactional]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/transactional/
[g-scaling]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/scaling/
[g-relay]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/relay/
[g-outbox]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/outbox/
[g-testing]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-sqlx/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-sqlx/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-sqlx/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-sqlx/latest/components/broker/
[c-connections]: https://powersemmi.github.io/ruststream-sqlx/latest/components/broker/connections/
[c-startup]: https://powersemmi.github.io/ruststream-sqlx/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-sqlx/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-sqlx/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-sqlx/latest/components/broker/errors/
[c-tables]: https://powersemmi.github.io/ruststream-sqlx/latest/components/tables/
[c-roles]: https://powersemmi.github.io/ruststream-sqlx/latest/components/tables/roles/
[c-columns]: https://powersemmi.github.io/ruststream-sqlx/latest/components/tables/columns/
[c-inbox-spec]: https://powersemmi.github.io/ruststream-sqlx/latest/components/tables/inbox-spec/
[c-insert]: https://powersemmi.github.io/ruststream-sqlx/latest/components/tables/insert/
[c-events]: https://powersemmi.github.io/ruststream-sqlx/latest/components/tables/events/
[c-checked]: https://powersemmi.github.io/ruststream-sqlx/latest/components/tables/checked/
[c-payload]: https://powersemmi.github.io/ruststream-sqlx/latest/components/messages/
[c-rows]: https://powersemmi.github.io/ruststream-sqlx/latest/components/messages/rows/
[c-decoding]: https://powersemmi.github.io/ruststream-sqlx/latest/components/messages/decoding/
[c-headers]: https://powersemmi.github.io/ruststream-sqlx/latest/components/headers/
[c-header-column]: https://powersemmi.github.io/ruststream-sqlx/latest/components/headers/header-column/
[c-inbox-headers]: https://powersemmi.github.io/ruststream-sqlx/latest/components/headers/inbox-headers/
[c-header-field]: https://powersemmi.github.io/ruststream-sqlx/latest/components/headers/header-field/
[c-fetch]: https://powersemmi.github.io/ruststream-sqlx/latest/components/headers/fetch/
[c-subscriptions]: https://powersemmi.github.io/ruststream-sqlx/latest/components/subscriptions/
[c-poll-interval]: https://powersemmi.github.io/ruststream-sqlx/latest/components/subscriptions/poll-interval/
[c-workers]: https://powersemmi.github.io/ruststream-sqlx/latest/components/subscriptions/workers/
[c-batches]: https://powersemmi.github.io/ruststream-sqlx/latest/components/subscriptions/batches/
[c-fifo]: https://powersemmi.github.io/ruststream-sqlx/latest/components/subscriptions/fifo/
[c-on-threads]: https://powersemmi.github.io/ruststream-sqlx/latest/components/subscriptions/on-threads/
[c-names]: https://powersemmi.github.io/ruststream-sqlx/latest/components/subscriptions/names/
[c-forms]: https://powersemmi.github.io/ruststream-sqlx/latest/components/forms/
[c-lease]: https://powersemmi.github.io/ruststream-sqlx/latest/components/forms/lease/
[c-advisory]: https://powersemmi.github.io/ruststream-sqlx/latest/components/forms/advisory/
[c-lock]: https://powersemmi.github.io/ruststream-sqlx/latest/components/forms/lock/
[c-settlement]: https://powersemmi.github.io/ruststream-sqlx/latest/components/settlement/
[c-retry-after]: https://powersemmi.github.io/ruststream-sqlx/latest/components/settlement/retry-after/
[c-dead-letter]: https://powersemmi.github.io/ruststream-sqlx/latest/components/settlement/dead-letter/
[c-transactions]: https://powersemmi.github.io/ruststream-sqlx/latest/components/transactions/
[c-isolation]: https://powersemmi.github.io/ruststream-sqlx/latest/components/transactions/isolation/
[c-context]: https://powersemmi.github.io/ruststream-sqlx/latest/components/context/
[c-attempt]: https://powersemmi.github.io/ruststream-sqlx/latest/components/context/attempt/
[c-pool]: https://powersemmi.github.io/ruststream-sqlx/latest/components/context/pool/
[c-time]: https://powersemmi.github.io/ruststream-sqlx/latest/components/time/
[c-database-clock]: https://powersemmi.github.io/ruststream-sqlx/latest/components/time/database-clock/
[c-clock]: https://powersemmi.github.io/ruststream-sqlx/latest/components/time/clock/
[c-queue-time]: https://powersemmi.github.io/ruststream-sqlx/latest/components/time/queue-time/
[c-publishing]: https://powersemmi.github.io/ruststream-sqlx/latest/components/publishing/
[c-route]: https://powersemmi.github.io/ruststream-sqlx/latest/components/publishing/route/
[c-wake]: https://powersemmi.github.io/ruststream-sqlx/latest/components/wake/
[c-listen-notify]: https://powersemmi.github.io/ruststream-sqlx/latest/components/wake/listen-notify/
[c-databases]: https://powersemmi.github.io/ruststream-sqlx/latest/components/databases/
[c-postgres]: https://powersemmi.github.io/ruststream-sqlx/latest/components/databases/postgres/
[c-mysql]: https://powersemmi.github.io/ruststream-sqlx/latest/components/databases/mysql/
[c-sqlite]: https://powersemmi.github.io/ruststream-sqlx/latest/components/databases/sqlite/
[c-any-pool]: https://powersemmi.github.io/ruststream-sqlx/latest/components/databases/any-pool/
[c-dialect]: https://powersemmi.github.io/ruststream-sqlx/latest/components/databases/dialect/
[c-asyncapi]: https://powersemmi.github.io/ruststream-sqlx/latest/components/asyncapi/
[c-asyncapi-server]: https://powersemmi.github.io/ruststream-sqlx/latest/components/asyncapi/server/
[c-asyncapi-channels]: https://powersemmi.github.io/ruststream-sqlx/latest/components/asyncapi/channels/
[c-testing]: https://powersemmi.github.io/ruststream-sqlx/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-sqlx/latest/components/testing/live/
[c-outbox]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/
[c-record]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/record/
[c-registry]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/registry/
[c-tracking]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/tracking/
[c-republish]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/republish/
[c-wrap]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/wrap/
[c-set-pool]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/set-pool/
[c-statements]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/statements/
[c-interplay]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/interplay/
[c-switch]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/switch/
[c-outbox-error]: https://powersemmi.github.io/ruststream-sqlx/latest/components/outbox/outbox-error/
