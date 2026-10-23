---
title: Design
sidebar_position: 2
description: "Where ruststream-sqlx sits: between the RustStream runtime and the service's sqlx pool, with SQL built once per subscription and the outbox beside any broker."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the service's sqlx pool, and the database, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, workers, settlement, publish",
  "crate": "ruststream-sqlx",
  "subscriber": ["InboxSubscriber", "the claim loop"],
  "delivery": ["InboxDelivery", "settles its row"],
  "publishers": ["Repository, Routed", "insert rows"],
  "descriptor": ["InboxQueue<Row>", "descriptor"],
  "broker": ["SqlxBroker", "configuration"],
  "dialect": ["Dialect", "SQL per database"],
  "outbox": ["Outbox", "layers, any broker"],
  "sqlx": "sqlx",
  "pool": ["Pool<DB>", "the service's own"],
  "database": ["PostgreSQL, MySQL, MariaDB", "or SQLite"],
  "inProcess": ["Under the testing feature, TestApp connects in process:", "the same pool and database, on the test's clock"],
  "mounted": "mounted",
  "polls": "polls",
  "driver": "the driver"
};

# Design

Where `ruststream-sqlx` sits: between the RustStream runtime and the service's sqlx pool, with SQL
built once per subscription and the outbox beside any broker.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over [sqlx](https://docs.rs/sqlx/0.9/sqlx/). The
runtime above it is the same for every broker. Everything SQL-specific is in this crate: the
[`InboxQueue`](api:ruststream_sqlx::InboxQueue) descriptor, the claim loop, the three claim forms,
what an outcome writes, and the SQL of each database. The crate subscribes, by descriptor and by
name, takes batches, hands a row-mode handler the row itself, hands the partition key to keyed
dispatch, publishes into tables, and describes its server in the generated document. It has no
request-reply and no transactional publisher: [transactional mode](queues/transactions.md) is a
step of a subscription, where a handler writes through its delivery's transaction.

The broker takes the service's pool and never closes it. The pool stays the service's: its size,
its options and its migrations. [`SqlxBroker::new`](api:ruststream_sqlx::SqlxBroker::new) takes a
pool, and sqlx builds a pool only inside a Tokio runtime, so a service builds the pool in its
`main` and passes it to the function that builds the app.

## The SQL

A dialect builds every statement a subscription runs: PostgreSQL, MySQL and MariaDB, and SQLite are
built in, an `AnyPool` takes the one of the database it reaches when the broker connects, and a
service may bring a dialect of its own for another database. A subscription builds its statements
once, when it opens, from the table's description, a `const` value the derive or the hand-written
[`InboxSpec`](api:ruststream_sqlx::InboxSpec) produces. So a message costs the same with the
derive, the manual description or a dialect of the service's own. The startup then prepares each
statement on the server, so a missing table or column stops the service before the first message.

## Publishing

A retry stays in its row. The descriptor declares that the database moves a delivery itself, so a
`retry_after` is an `UPDATE` of the row's `retry_after` column, and no copy is ever published:
[Settlement](settlement.md) draws it. A publish writes a row through the row type's own
[`Publish`](api:ruststream_sqlx::Publish) impl. A [`Repository`](api:ruststream_sqlx::Repository)
names its table at compile time. [`Routed`](api:ruststream_sqlx::Routed), the connected broker's
default publisher and so the one replies take, looks the message's name up in the routes the broker
was built with. Both take the encoded bytes as a lent slice, so a reply's buffer is reused from one
message to the next.

What remains per message is stated by the crate and measured on
[Benchmarks](/architecture/benchmarks/):

- `Routed` makes one hash lookup and one dynamic call into the row's `Publish`. The call's future
  stays in a 1024-byte slot in place, and is boxed only when it is larger.
- A publish reads the broker's closed flag, one atomic load, and wakes the subscriptions of its
  table in the process, one atomic operation each.
- A handler that reads `Tx` or `Ctx<keys::Pool>` pays one reference-count increment per delivery.
- A by-name subscription over a row whose events are the service's own pays one boxed delivery and
  one boxed settlement future per message.

## In-process mode

Under the `testing` feature, `TestApp::start` connects the broker in process. Nothing below the
pool is replaced: the in-process broker reaches the database the test's pool names, runs the
service's SQL, its schema and its claim forms, and fails where the database fails. SQLite in
memory serves a test with no server. What changes is time. The broker reads "now" from the tokio
clock, anchored at the wall clock when it connected, so a test on a paused clock moves the queue's
time with `tb.advance(..)`: a `retry_after` row comes due when the test advances past it. Every
database call runs off that clock, so a paused clock does not jump to its next timer while a reply
from the database is on its way. The test clock stands in for
[`SystemClock`](api:ruststream_sqlx::SystemClock) alone: a table on `DatabaseClock` reads the
database's clock, and a clock of the service's own stays in force, as in production.

The harness waits on the database's work through the broker's books. It counts a message when it
lands in a queue a subscription of the test reads, and again when a retry returns its row, so
`tb.settle()` returns once every row that reached a queue was delivered and settled.

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one row on
[Path](path.md).
