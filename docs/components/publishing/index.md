---
title: Repository
description: "A publish writes a row through the row's Publish, the service's own INSERT; Repository names the table at compile time, a static call per publish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Repository

A publish writes a row through the row's [`Publish`](api:ruststream_sqlx::Publish), the service's own `INSERT`; [`Repository`](api:ruststream_sqlx::Repository) names the table at compile time, a static call per publish.

`Publish<DB>` is the statement of a queue table that takes a published message.
Its `publish(conn, message)` receives the message's name, its encoded bytes and its headers, and lays them out in the table's columns.
In a table with a [group](../tables/roles.md) column, the name usually goes there, so a subscription of that group reads the row.
The trait has no default: a row without it is not a publisher, and a mount that names it does not compile.

`Repository::<Row>::default()` is the publish policy of `Row`'s table.
A mount site names it wherever a registration publishes: `.out_reply(Repository::<Job>::default())` for a reply, `.out(Slot, Repository::<Job>::default())` for an `Out` slot.
The runtime pairs it with the connected broker at startup into a [`RepositoryPublisher`](api:ruststream_sqlx::RepositoryPublisher).
A publisher outside a handler, such as an HTTP endpoint's, comes from `SqlxBroker::new(pool).bindable()`, `bind(Repository::<Job>::default())` and `running.publisher(..)`.

Each publish takes a connection of the pool, runs the row's `Publish` on it and commits on its own.
A handler's [transaction](../transactions/index.md) does not cover it: a row that must commit with the handler's writes goes in through [`Insert`](../tables/insert.md) on the handler's `Tx`.
A message whose header the table cannot hold byte for byte is refused before the statement runs, with [`SqlxBrokerError::Header`](../broker/errors.md); a [headers column](../headers/index.md) holds them.
A failed statement returns `SqlxBrokerError::Publish`, which names the message's name, the table and the row type.
After shutdown a publisher answers `SqlxBrokerError::Closed`.

A publish through a repository [wakes](../wake/index.md) the subscriptions of its table and group in the same process at once.
A name the service does not know at compile time goes through a [route](./route.md) instead: the broker's default publisher looks the name up.

## Example

An order's confirmation becomes a row of `jobs` in the group `emails`, and the second subscription of the service reads it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_publishing_repository.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_publishing_repository.rs#service"
```

</TabItem>
</Tabs>
