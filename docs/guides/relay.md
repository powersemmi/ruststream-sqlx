---
sidebar_position: 8
description: "Commit a business row and its event in one transaction, then relay the event to Kafka, NATS or any other broker with an inbox subscription."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Relaying events to another broker

This guide writes an event in the same transaction as the data it describes, then publishes it to another broker.

A service that saves an order and then publishes "order placed" to Kafka has two writes that can part.
The order can commit while the publish fails, or the publish can go out for an order that rolled back.
The [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) joins them: the event is a row written in the order's transaction, and a relay publishes the rows that committed.
With this crate the event table is a queue table, and the relay is an ordinary subscription.

The example is a shop.
A checkout places the order and writes its event; the relay publishes each event to a bus, the in-memory broker here.

## The order and its event

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/relay.rs#schema"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/relay.rs#write"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/relay.rs#write"
```

</TabItem>
</Tabs>

`checkout` runs in [transactional mode](transactional.md): it writes the order and the event through its delivery's transaction.
Its acknowledgement commits both with the checkout's settlement.
An order whose insert fails rolls its event back, and nothing is published for it.
Code outside a handler, an HTTP endpoint for one, writes the same two rows in a sqlx transaction of its own, as [Enqueueing jobs](enqueueing.md) does.
The event row wakes no subscription in that case either: the relay finds it within its [poll interval](../components/subscriptions/poll-interval.md).

## The relay

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/relay.rs#relay"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/relay.rs#relay"
```

</TabItem>
</Tabs>

`relay` reads `order_events` and answers each event with itself; the reply type names its destination, `orders`.
The reply leaves through the bus: [`bindable()`](api:ruststream::Broker::bindable) wraps the target broker, `bind(MemoryPublish)` mints a publish token for it, and `.out_reply(to_bus)` sends the replies of `relay` through that token.
A Kafka or NATS service puts its own broker and publish policy in the same two lines.
The reply is published before the event's row settles, and the acknowledgement deletes the row.

The relay is at least once.
A reply whose publish fails returns the event to the queue, and a crash after the publish and before the acknowledgement publishes it again.
A consumer that must not act twice keeps the event's identity, the order here, and passes over one it has seen.

## Order per aggregate

`aggregate` is the event's [partition key](../components/subscriptions/workers.md), the order's id.
`workers_by_key(4)` relays four orders at once and keeps the events of one order in the order they were written.
That order holds within one relay subscription.
A relay on several instances whose events must stay in order per aggregate across all of them keeps them in a [FIFO group](ordering.md) or in the advisory lock form.

The relay needs no "sent" flag and no polling of its own: the claim is the queue's, the delay after a failed publish is a [retry](retries.md), and a cap with a dead-letter group parks an event the target keeps refusing.
[Tracking what you publish](outbox.md) is the other shape: the outbox plugin records what a handler publishes and marks it once a consumer processed it, but its record commits on its own, outside the business transaction.

## Testing

The tests in the example file publish checkouts through the harness and read what reached the bus with `tb.broker::<MemoryBroker>().published(..)`.
One order reaches the bus once, and its event row is gone.
The same order checked out twice publishes one event: the second checkout fails, and its event rolled back.
The event rows wake no subscription, so the tests run on the real clock with [`TestApp::start_live`](../components/testing/live.md) past the poll interval.
