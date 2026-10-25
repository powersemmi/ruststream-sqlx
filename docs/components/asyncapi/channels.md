---
sidebar_position: 2
title: Channels
description: "Each InboxQueue channel carries x-ruststream-sqlx: the table it reads, qualified with its schema, and the group its name selects where the table has groups."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

Each [`InboxQueue`](../subscriptions/index.md) channel carries `x-ruststream-sqlx`: the table it reads, qualified with its schema, and the group its name selects where the table has groups.

The extension holds two fields.
`table` is the table's name, with its schema in front where the description names one (`app.email_jobs`).
`group` is the subscription's name, present only when the table has a [group](../tables/roles.md) column: in a table without groups the name addresses the whole table.
A channel the service publishes to and a [subscription by name](../subscriptions/names.md) carry no extension: a name leads into a table through a [route](../publishing/route.md), which the channel does not describe.

## Example

The channel `emails` reports `{ "table": "app.email_jobs", "group": "emails" }`, and the channel `reports` reports `{ "table": "reports" }`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
