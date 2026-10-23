---
sidebar_position: 6
title: Names
description: "A subscription by name, #[subscriber(\"emails\")], reads the table its route leads to; it takes the broker's settings and no retry cap."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Names

A subscription by name, `#[subscriber("emails")]`, reads the table its route leads to; it takes the broker's settings and no retry cap.

A [route](../publishing/route.md), `SqlxBroker::route::<Row>("emails")`, leads a name into the table of a row type in both directions.
A publish to the name writes a row through the row's `Publish`, and a subscription by the name reads that table.
On a table with a group column the name selects its group, as an [`InboxQueue`](./index.md) of the same name would.
A name without a route stops the service at startup.
A route leads only into a table in payload mode, so a subscription by name reads payload tables.
It takes the broker's [poll interval](./poll-interval.md) and [lease](../forms/lease.md), and refuses `max_attempts(..)` and `dead_letter(..)` at startup: an `InboxQueue` takes those.
Where the route's row leaves every event to the crate and holds column types the crate reads itself, the subscription reads the rows by those columns, with no box and no dynamic call per message, in every form.
Any other row runs its own code at one boxed delivery and one boxed settlement per message ([`NamedSubscriber`](api:ruststream_sqlx::NamedSubscriber)).
A subscription by name needs a dialect that implements [`ByName`](api:ruststream_sqlx::ByName) for its database, as every built-in dialect does.

## Example

Both names lead into one table: a signup read by name is answered by a welcome, which becomes a row of the `welcome` group that a second subscription by name reads.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_subscriptions_names.rs#names"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_subscriptions_names.rs#names"
```

</TabItem>
</Tabs>
