---
sidebar_position: 4
title: Prelude
description: "One glob, ruststream_sqlx::prelude, brings the core prelude, the derives, the broker, InboxQueue, its steps, the publish policies and the keys."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_sqlx::prelude`](api:ruststream_sqlx::prelude), brings the core prelude, the derives, the broker, `InboxQueue`, its steps, the publish policies and the keys.

`use ruststream_sqlx::prelude::*;` is the one import a service on the inbox writes.
It carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds the derives [`Inbox`](../tables/index.md) and [`InboxHeaders`](../headers/inbox-headers.md), the broker [`SqlxBroker`](./index.md), the descriptor [`InboxQueue`](../subscriptions/index.md), the mount steps of [`InboxSettings`](../transactions/index.md) and [`InboxThreads`](../subscriptions/on-threads.md), the traits [`Insert`](../tables/insert.md) and [`Publish`](../publishing/index.md), the publish policies [`Repository`](../publishing/index.md) and [`Routed`](../publishing/route.md), and the [`keys`](../context/index.md) a handler reads.

`Publish` here is the trait a table implements to receive a publish, not a policy.
Another broker's prelude may name its publish policy `Publish`, so a file that imports both globs names one of them by its path.

The rest is imported by path: the event traits such as [`Ack`](../tables/events.md) and [`Fetch`](../headers/fetch.md), [`DatabaseClock`](../time/database-clock.md), [`HeaderColumn`](../headers/header-column.md), the [`dialect`](api:ruststream_sqlx_dialect) and [`spec`](api:ruststream_sqlx::spec) modules a table described by hand uses, and everything of the [outbox](../outbox/index.md).

## Example

The descriptor with its poll interval, the attempt key, the derive, the routes and the `Routed` policy of an `Out` slot all come from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_broker_prelude.rs#imports"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_broker_prelude.rs#imports"
```

</TabItem>
</Tabs>
