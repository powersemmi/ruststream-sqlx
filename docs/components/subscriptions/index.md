---
title: InboxQueue
description: "InboxQueue reads one queue of a table: its name selects a group where the table has a group column and addresses the whole table where it has none."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# InboxQueue

[`InboxQueue`](api:ruststream_sqlx::InboxQueue) reads one queue of a table: its name selects a group where the table has a group column and addresses the whole table where it has none.

[`InboxQueue::<Row>::new(name)`](api:ruststream_sqlx::InboxQueue::new) takes the struct that describes the [table](../tables/index.md) and the queue's name.
The descriptor goes where a name goes, as the first argument of `#[subscriber(..)]`; the manual form mounts it on `subscriber(name, body)` with `map_source`.
Two settings belong to the descriptor, and both default to the broker's: the [poll interval](./poll-interval.md) and, on a lease table, the [lease](../forms/lease.md).
The mount site adds the rest: [workers](./workers.md), [batches](./batches.md), [transactional mode](../transactions/index.md), [dedicated threads](./on-threads.md) and a [retry cap](../settlement/dead-letter.md).
A subscription claims rows when the runtime polls its stream, so it takes rows as fast as its handlers settle them.
Every claimed row is delivered at least once: the row of a process that crashed returns to the queue by the rules of its table's [form](../forms/index.md).
A broker serves one subscription per queue, and one per table without a group column; a second one stops the service at [startup](../broker/startup.md).
A retry never copies a row: the claim's own statements move it, so the mount site names no retry publisher.
A plain name in `#[subscriber("emails")]` reads the table its [route](../publishing/route.md) leads to instead, as [Names](./names.md) shows.

## Example

Two subscriptions read the two groups of one table, and a notice published to `sms` reaches the handler of `sms` alone.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_subscriptions_queue.rs#queue"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_subscriptions_queue.rs#queue"
```

</TabItem>
</Tabs>
