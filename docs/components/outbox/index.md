---
title: Transactional outbox
description: "The outbox records each message a service publishes under a registered name until a consumer processed it, and publishes the rest again at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transactional outbox

The outbox records each message a service publishes under a registered name until a consumer processed it, and publishes the rest again at startup.

The outbox is a plugin over any RustStream broker and needs no broker of its own.
The `outbox` feature turns it on, beside a driver feature: `postgres`, `mysql`, `sqlite` or `any`.
It is independent of the inbox, and a service may enable both.

A tracked message goes through four steps.
The publish middleware writes a [record](./record.md) of the message, then sends it with the record's id in the [`x-ruststream-outbox-id`](api:ruststream_sqlx::outbox::OUTBOX_ID_HEADER) header.
The subscription middleware takes a delivery that carries an id into work by that id, and runs the handler.
The handler's outcome then [settles the record](./tracking.md): an acknowledgement marks it processed.
At startup the [republish](./republish.md) sends every unprocessed record again.
A message under a name the [registry](./registry.md) does not hold passes both middlewares after a comparison of names.

A message is delivered at least once.
The record is written before the message is sent, so a send that fails, a consumer that stops before its handler finishes, or a handler that asks for the message again leaves the record unprocessed for the next startup.
A copy whose record is processed already is acknowledged without running the handler.
Two instances of a service that start together may both send the same record again, so a consumer that must not process a message twice checks that itself.

A message whose name is not registered costs one comparison per registered name: no allocation and no database call.
A tracked publish takes a connection for the record's insert and allocates the id header's value.
A tracked delivery takes a connection for the fetch, returns it before the handler runs, and takes one again for the settlement.
Under sqlx's default `test_before_acquire`, each acquire pings the connection first; a pool built with `test_before_acquire(false)` skips that round trip.
The default retry takes no connection, and the republish allocates its body once per startup.

The repository's [`examples/outbox`](https://github.com/powersemmi/ruststream-sqlx/tree/main/examples/outbox) runs the same flow over Redis Pub/Sub, with migrations and a fetch of its own.

## Example

The service answers a request on `checkout` with an `OrderPlaced` on `orders`, a registered name: the reply is recorded before it is sent, the acknowledgement of `fulfil` marks the record, and the startup sends what no consumer processed.
`RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example sqlx_outbox --features outbox,sqlite,testing` starts it; the [switch](./switch.md) is there because `testing` is among the example's features.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox.rs#service"
```

</TabItem>
</Tabs>
