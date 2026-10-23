---
sidebar_position: 1
title: route
description: "SqlxBroker::route leads a name into a row type's table in both directions; Routed, the default for replies and slots, looks each name up."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# route

[`SqlxBroker::route`](api:ruststream_sqlx::SqlxBroker::route) leads a name into a row type's table in both directions; [`Routed`](api:ruststream_sqlx::Routed), the default for replies and slots, looks each name up.

`.route::<Job>("orders")` on the broker serves two directions.
A publish to `orders` writes a row of `Job`'s table through the row's [`Publish`](./index.md).
`#[subscriber("orders")]` opens a [subscription by that name](../subscriptions/names.md) on the same table.
A name ending in `*` leads every name that starts with what precedes it: `reports.*` takes `reports.daily` and `reports.weekly`.
An exact name wins over a prefix, and a longer prefix over a shorter one.
A name routed twice leads to the last row type.

`Routed` is the broker's default publish policy.
A reply or an `Out` slot that names no policy of its own publishes through it, and so does a test's input through the harness.
A publish looks its name up in the route table: one hash lookup for an exact name, the prefixes scanned only when none matches, then one dynamic call into the row's `Publish`.
That call's future stays in place in 1024 bytes, so a routed publish allocates what a [repository](./index.md) publish does.
A row whose `Publish` future is larger is boxed: one allocation per publish on its route.
A name no route leads anywhere fails the publish with [`SqlxBrokerError::NoRoute`](../broker/errors.md), which names the route to add.

A route asks of the broker's dialect what an [`InboxQueue`](../subscriptions/index.md) of the row asks: the table's [form](../forms/index.md) and the isolation level or SQLite mode the struct declares.
A routed publish [wakes](../wake/index.md) the subscriptions of its table and group in the same process, as a repository publish does.

## Example

The prefix route `reports.*` leads the daily report a client publishes and the weekly report the service replies with into one table.
The reply names no policy, so it goes through the routes.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_publishing_route.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_publishing_route.rs#service"
```

</TabItem>
</Tabs>
