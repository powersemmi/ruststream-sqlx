---
sidebar_position: 9
description: "Track what a service publishes on any RustStream broker until a consumer has processed it, and publish the rest again at startup, with the outbox plugin."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Tracking what you publish: the outbox plugin

This guide makes every message a service publishes reach a handler that processes it, on a broker that keeps nothing.

Some brokers deliver a message only to whoever listens at that moment, Redis Pub/Sub and NATS core among them, and a consumer may stop halfway through any message.
The [outbox](../components/outbox/index.md) records each message the service publishes under a registered name, marks the record once a consumer processed the message, and publishes the unprocessed records again at startup.
It is a plugin over any RustStream broker and needs no relay process.
Its `outbox` feature is independent of the inbox.

The example takes payments at checkout and ships the paid orders, over the in-memory broker.

## The table

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/outbox.rs#schema"
```

The table is the service's: it creates it, with its own migrations in production.
`processed_at` stays empty until a consumer processed the message.

## The record

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/outbox.rs#record"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/outbox.rs#record"
```

</TabItem>
</Tabs>

[`#[derive(Outbox)]`](../components/outbox/record.md) describes the record: `id`, `name` and `payload` are required, `headers` and `processed_at` optional.
The service writes the record with its own [`outbox::Publish`](../components/outbox/statements.md) and returns its id; the crate builds the other statements.
In the manual form, `OutboxSpec` says the same, and a `TrackedName` type names what is tracked.

## The handlers

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/outbox.rs#handlers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/outbox.rs#handlers"
```

</TabItem>
</Tabs>

The handlers know nothing of the outbox: the middlewares of the app do the tracking.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/outbox.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/outbox.rs#app"
```

</TabItem>
</Tabs>

[`outbox!`](../components/outbox/registry.md) registers `payments` with its record; a name registered twice does not compile.
`#[ruststream::app]` builds the app before the Tokio runtime exists, and sqlx builds a pool only inside one.
So the registry starts without a pool, and [`on_startup`](../components/outbox/set-pool.md) builds the pool and hands it over with `set_pool`; `after_shutdown` closes it.

A tracked message goes through four steps:

1. The [publish layer](../components/outbox/tracking.md) writes the record, then sends the message with the record's id in the `x-ruststream-outbox-id` header. A record that cannot be written fails the publish.
2. The subscription layer takes a delivery that carries an id into work by that id and runs the handler; a copy whose record is processed already is acknowledged without the handler.
3. The handler's acknowledgement or drop marks the record processed; a retry leaves it as it was.
4. At startup, [`republish`](../components/outbox/republish.md) sends every unprocessed record again, once the subscriptions are open.

The consumer marks the record in the producer's table, so producer and consumer share the registry and the database, as both handlers in one app do here.

## What the outbox guarantees

A message is delivered at least once.
The record is written before the message is sent, so a failed send, a consumer that stops mid-handler, or a handler that asks for the message again leaves the record for the next startup.
Two instances that start together may both send the same record again, so a consumer that must not process a message twice checks that itself.

The record is written on a connection of its own and commits at once.
It does not join a transaction of the service: a [transactional](transactional.md) inbox handler that is retried discards its own writes and keeps the record of what it published.
On SQLite such a handler publishes before its first write through its transaction, because the record's insert needs that transaction's write lock; a publish after that write fails with `OutboxError::WriteLockHeld` ([Interplay](../components/outbox/interplay.md)).
An event that must commit with the business rows is a row of the business transaction, relayed afterwards: [Relaying events to another broker](relay.md).

A publish outside a handler, an HTTP endpoint for one, takes the running app's publisher and [`wrap`](../components/outbox/wrap.md)s it in the registry, so it is tracked the same way.
The tracking costs a connection for each tracked publish, and two for each tracked delivery: one for the fetch, one for the settlement.
A message under a name the registry does not hold costs a comparison of names.
The repository's [`examples/outbox`](https://github.com/powersemmi/ruststream-sqlx/tree/main/examples/outbox) runs the same flow over Redis Pub/Sub, with migrations and a fetch of its own.

## Testing

A test build leaves the outbox off unless `RUSTSTREAM_SQLX_OUTBOX=on` turns it on ([Switch](../components/outbox/switch.md)), so a service's own tests run its handlers untracked.
The tests in the example file that check the tracking skip without the switch; one more test checks that the service runs untracked with it off:

```bash
cargo test
RUSTSTREAM_SQLX_OUTBOX=on cargo test
```

With the switch on, one test checks that a payment is recorded, sent with its record's id, and marked once shipped.
Another writes a record an earlier instance left unprocessed and finds it shipped after the startup.
Both run with [`TestApp::start`](../components/testing/index.md) on a SQLite database in memory, which the service's own pool opens by its URL.
