---
sidebar_position: 1
title: poll_interval
description: "A subscription claims while its claims come back full, then waits the poll interval, one second unless the broker or the queue sets another."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# poll_interval

A subscription claims while its claims come back full, then waits the poll interval, one second unless the broker or the queue sets another.

[`InboxQueue::poll_interval`](api:ruststream_sqlx::InboxQueue::poll_interval) sets the wait of one subscription, and [`SqlxBroker::poll_interval`](api:ruststream_sqlx::SqlxBroker::poll_interval) the wait of every subscription of the broker that sets none.
A claim that found as many rows as it asked for is followed by the next one at once, so a backlog drains without a pause.
A claim that found fewer waits the interval before the next one.
A [subscription by name](./names.md) takes the broker's interval.
A publish through the broker into the subscription's table and group ends the wait at once, as [Wake](../wake/index.md) describes.
So the interval bounds how long a row written by other means waits for its claim: a row another service inserts, a trigger writes, or the service writes with [`Insert`](../tables/insert.md).
On Postgres [`listen_notify`](../wake/listen-notify.md) wakes a subscription for the rows of other processes too.
A claim that fails is reported as an error of the subscription's stream, and the next claim waits one second.
A shorter interval finds outside rows sooner, at the price of one more claim statement per interval and subscription.

## Example

The warehouse writes exports into the table with its own SQL, so the queue claims five times a second; every other subscription of the broker waits five seconds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_subscriptions_poll.rs#poll_interval"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_subscriptions_poll.rs#poll_interval"
```

</TabItem>
</Tabs>
