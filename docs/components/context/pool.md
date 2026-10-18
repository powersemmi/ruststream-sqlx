---
sidebar_position: 2
title: Pool
description: "keys::Pool gives a handler the service's pool, whose writes commit on their own whatever the outcome; on dedicated threads it is the thread's pool."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Pool

[`keys::Pool`](api:ruststream_sqlx::keys::Pool) gives a handler the service's pool, whose writes commit on their own whatever the outcome; on dedicated threads it is the thread's pool.

`Ctx(pool): Ctx<keys::Pool<Sqlite>>` binds a `Pool<Sqlite>`, a handle the read clones; every delivery of an `InboxQueue` subscription lends it.
A write through it commits on its own: a retry, a drop or a rolled-back [transaction](../transactions/index.md) leaves it in place.
That makes it the place for what must outlive the outcome: a delivery log, an error note, a counter.
Each query takes a connection of the pool for its run, on top of the ones the subscription holds, so the pool is sized for the handlers' own queries too ([Connections](../broker/connections.md)).
On SQLite a delivery's transaction holds the database's write lock from its first write, so a transactional handler writes through the pool before it writes through `tx`.
On [dedicated threads](../subscriptions/on-threads.md) the key yields the thread's own pool, built from the service pool's options, whose connections open on the thread and close with it.
After `keys::Tx` the key still reads the pool, and `keys::Attempt` may follow it ([Context](./index.md)).

## Example

Every attempt to send a text message writes a line to the delivery log through the pool, and the line stays when the gateway is busy and the handler retries.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_context_pool.rs#pool"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_context_pool.rs#pool"
```

</TabItem>
</Tabs>
