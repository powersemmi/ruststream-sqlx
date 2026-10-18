---
title: Wake
description: "A publish through the broker wakes the subscriptions of its table and group in the same process at once; a row written by other means waits the poll."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Wake

A publish through the broker wakes the subscriptions of its table and group in the same process at once; a row written by other means waits the poll.

A subscription claims rows as long as its claims come back full.
After a claim that found fewer rows than it asked for, it waits the [poll interval](../subscriptions/poll-interval.md): one second unless the broker or the subscription sets another.
Two things end the wait early: a publish from the same process, and a [Postgres notification](./listen-notify.md).

Every publish through the broker wakes: a [`Repository`](../publishing/index.md), a [route](../publishing/route.md), a reply or an `Out` slot published through either of them, and a test's input through the harness.
In a table with groups, a publish wakes the subscriptions of its own group, and the others keep their wait.
In a table without groups, it wakes every subscription of the table.
A subscription busy with a claim keeps the wake-up, so its next wait returns at once.

A row the service writes itself wakes nobody: an [`insert`](../tables/insert.md), a statement of the service's own, a write through a handler's `Tx`.
Such a row waits the poll interval, or a notification the service sends.

The wake-up costs a publish one atomic operation per subscription it wakes.
A subscription pays nothing per message for it.
A long poll interval therefore costs a service that publishes its own work no latency: its subscriptions poll only for rows other programs write.

## Example

The broker polls an empty queue once an hour.
The welcome a signup's handler replies with still reaches its subscription at once, because the reply's row wakes it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_wake.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_wake.rs#service"
```

</TabItem>
</Tabs>
