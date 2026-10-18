---
sidebar_position: 4
title: FIFO
description: "#[field(group, fifo = true)] keeps each group in order: one row of a group in work at a time across every broker, the head first."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FIFO

`#[field(group, fifo = true)]` keeps each group in order: one row of a group in work at a time across every broker, the head first.

A FIFO group has at most one row in work, however many brokers and processes read it.
Its rows go out in claim order: a smaller `priority` first, then an earlier `retry_after`, then the smaller id, each where the table has the column.
The group's first unfinished row in that order is its head, and a claim takes the head alone.
The rows behind the head wait while it is in work or not yet due.
[`retry()`](../settlement/index.md) keeps a row at the head, so a change that failed goes again before the ones behind it.
[`retry_after(d)`](../settlement/retry-after.md) gives the row a later `retry_after`, and it moves behind the rows due before it; on a table with `priority` it moves only among the rows of its own priority.
A head that keeps failing holds its group back, so a FIFO table pairs with a [retry cap](../settlement/dead-letter.md) that moves such a row out of the group.
In the [lease form](../forms/lease.md) a crash with a row in work holds its group until that row's lease runs out.
`workers(n)` gives no parallelism inside a group, so one worker serves a FIFO subscription, and a batch of a FIFO group holds one row.
Groups run side by side, each through a subscription of its own.
FIFO groups serve the row lock and the lease forms.
In the [advisory lock form](../forms/advisory.md) a key on the group's field keeps one row of the group in work, and `fifo = true` beside `advisory_lock` does not compile.
By hand, `.fifo_group(..)` names the group's column, and `Fifo` goes into the description's type.

## Example

The CRM receives its changes one at a time, in the order they were queued, and a change that timed out goes again before the next one.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_subscriptions_fifo.rs#fifo"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_subscriptions_fifo.rs#fifo"
```

</TabItem>
</Tabs>
