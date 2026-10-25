---
sidebar_position: 1
description: "Install ruststream-sqlx, describe a queue table, run a first subscriber on SQLite and test it without a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-sqlx`, creates a queue table and runs a first service on it, with SQLite and no server.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-sqlx = { version = "0.7", features = ["inbox", "sqlite", "chrono"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "sqlite", "derive", "chrono"] }
chrono = "0.4"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tracing = "0.1"

[dev-dependencies]
ruststream-sqlx = { version = "0.7", features = ["inbox", "sqlite", "chrono", "testing"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-sqlx` serves the queue, and `sqlx` is the client the service already uses for its database.
The `testing` feature adds the [in-process mode](../components/testing/index.md) that tests run on.

The crate has no default features. A service turns on what it uses:

| Feature | What it gives |
|---|---|
| `inbox` | the [task queue](../components/tables/index.md) in the service's tables, served by [`SqlxBroker`](../components/broker/index.md) |
| `outbox` | the [transactional outbox](../components/outbox/index.md) over any RustStream broker |
| `postgres`, `mysql`, `sqlite` | a built-in [dialect](../components/databases/index.md) and its sqlx driver; `mysql` serves MySQL and MariaDB |
| `any` | an [`AnyPool`](../components/databases/any-pool.md), served by the dialect of the database it reaches |
| `chrono`, `time` | the [time columns](../components/time/queue-time.md) in that crate's types |
| `json` | a JSON [headers column](../components/headers/index.md) |
| `testing` | the in-process mode behind `TestApp`, and the outbox's [test switch](../components/outbox/switch.md) |
| `asyncapi` | the tables in the generated [AsyncAPI document](../components/asyncapi/index.md) |

## The table

The queue is a table of the service's own, and the service creates it:

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/getting_started.rs#schema"
```

Each row is one job.
`id` names the row, `attempt` counts its deliveries, and `payload` holds the encoded message.
`locked_until` holds the [lease](../components/forms/lease.md) of a row in work: SQLite has no row locks, so a SQLite queue claims its rows under a lease.
[Long jobs, crashes and the claim form](claim-forms.md) compares the forms a table can take.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

[`#[derive(Inbox)]`](../components/tables/index.md) describes the table: `#[inbox(table = ..)]` names it, and each `#[field(..)]` gives a column its [role](../components/tables/roles.md).
`generated` marks a column the database fills.
In the manual form, [`InboxSpec`](../components/tables/inbox-spec.md) says the same.
[`InboxQueue::<MakeThumbnail>::new("thumbnails")`](../components/subscriptions/index.md) is the subscription: it claims rows of `thumbnail_jobs` and hands each payload to the codec.
What the handler returns settles the row: `ack()` deletes it, and `drop()` finishes it the same way without a thumbnail.
[`Ctx<keys::Attempt>`](../components/context/attempt.md) reads which delivery of the row this is.

`service` takes the database's URL.
[`SqlxBroker::from_url`](../components/broker/index.md) records it with the pool's options and connects nothing, so the app is built before the runtime starts.
`on_startup` creates the table before the subscription checks it.

A publish to `thumbnails` writes a row.
[`route`](../components/publishing/route.md) leads the name to `MakeThumbnail`, and the service's own [`Publish`](../components/publishing/index.md) statement inserts it.
A handler's reply or a test publishes the same way.
Code outside a handler, such as an HTTP endpoint, writes the row with the derive's [`insert`](../components/tables/insert.md), inside its own transaction: [Enqueueing jobs](enqueueing.md) shows it.

## Running the service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#main"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#main"
```

</TabItem>
</Tabs>

`#[ruststream::app]` generates `main`, which builds the app and runs it.
The broker builds its pool when the service starts, and closes it when the service stops.

```bash
cargo run
```

The service claims rows as they appear in `thumbnails.db`.
A row another program inserts is seen within the [poll interval](../components/subscriptions/poll-interval.md), one second unless set.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.

## The first test

The example file also holds two tests.
Each opens a SQLite database in memory and starts the same `service` on it with `TestApp::start`.
A test publishes an image through the route, as production does, and checks how `thumbnail` settled it and what is left in the table:

```bash
cargo test
```

[Testing a service on SQL](testing.md) walks through these tests.

## Moving to Postgres or MySQL

The same struct serves a `PgPool` or a `MySqlPool`: the broker takes the dialect of its pool.
Turn on `postgres` or `mysql` in both crates, build that pool, and write `Publish<Postgres>` or `Publish<MySql>` with the database's placeholders.
On a server, a table without `locked_until` claims its rows under a [row lock](../components/forms/index.md), held while the handler runs.
Index the claim order, `(group, priority, retry_after, id)` for the columns the table has; [Running in production](production.md) explains why.

## Coming from a hand-written queue

A queue on sqlx usually is a loop of `SELECT .. FOR UPDATE SKIP LOCKED` and a few columns of bookkeeping.
Here is where each part goes:

| Hand-written | ruststream-sqlx |
|---|---|
| the claim loop and its statement | an [`InboxQueue`](../components/subscriptions/index.md) subscription; the [dialect](../components/databases/index.md) builds the statement |
| `DELETE` or `UPDATE .. SET done` after the work | the handler's [outcome](../components/settlement/index.md) |
| an `attempts` column raised by hand | `#[field(attempt)]` and [`Ctx<keys::Attempt>`](../components/context/attempt.md) |
| a `run_at` column for later | `#[field(retry_after)]` and [`retry_after(d)`](../components/settlement/retry-after.md) ([Retries](retries.md)) |
| a `locked_until` column and a reaper | the [lease form](../components/forms/lease.md) ([Long jobs](claim-forms.md)) |
| a dead-letter table and its move | `max_attempts(n)` and [`dead_letter`](../components/settlement/dead-letter.md) |
| a job inserted with the data it serves | [`insert`](../components/tables/insert.md) in the same transaction ([Enqueueing jobs](enqueueing.md)) |
