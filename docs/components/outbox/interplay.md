---
sidebar_position: 8
title: Interplay
description: "The outbox wraps inbox handlers as any other; a record written from a transactional handler commits on its own, whatever the delivery's outcome."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Interplay

The outbox wraps inbox handlers as any other; a record written from a [transactional](../transactions/index.md) handler commits on its own, whatever the delivery's outcome.

The two middlewares wrap the handlers of an inbox subscription as they wrap the handlers of any broker, and a tracked name may lead into an inbox table.
A tracked publish writes its record on a connection of its own, never in the delivery's transaction.
So a handler in transactional mode commits its own writes with the delivery, while the record of what it published commits at once: a delivery that is dropped or retried discards the handler's writes and leaves the record.
The guarantee stays the outbox's own: the message is delivered at least once, and a consumer that must not process it twice checks that itself.

A message tracked into an inbox table carries its record id only where the table keeps headers: a [headers column](../headers/index.md) that the table's `Publish` writes from the message's headers.
The consumer's subscription then reads the id back, and its acknowledgement marks the record.
A table without a headers column refuses a publish that carries a header, the id among them.

SQLite takes one writer at a time, and a transaction holds the write lock from its first write until it ends.
A handler there publishes its tracked messages before its first write through the delivery's transaction: a record's insert on its own connection waits for that transaction otherwise, and the transaction waits for the handler.
A record written or marked from a subscription on dedicated threads opens its connection on the app's runtime, so the pool outlives the threads.

## Example

`take` reads an order from `order_jobs` in transactional mode: it sends a tracked `Receipt` into `receipt_jobs`, then writes the order through the delivery's transaction.
A dropped order leaves no row in `orders`, and its receipt is still recorded, delivered and marked.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_interplay.rs#interplay"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_interplay.rs#interplay"
```

</TabItem>
</Tabs>
