---
sidebar_position: 4
title: AnyPool
description: "With any, an AnyPool is served by the built-in dialect of the database it reaches at connect; its rows hold integers, text and bytes only."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AnyPool

With the `any` feature, an `AnyPool` is served by the built-in dialect of the database it reaches at connect; its rows hold integers, text and bytes only.

An `AnyPool` names its database by the scheme of its URL, so the service picks the database in its configuration and builds one binary for all of them.
`sqlx::any::install_default_drivers()` registers the drivers before the pool connects.
The broker picks the dialect when it [connects](../broker/startup.md), among the dialects whose features are on; another backend stops `connect` with [`SqlxBrokerError::Backend`](../broker/errors.md).

A row holds the types `sqlx::Any` carries: integers, text and bytes, with no time and no JSON.
A table on an `AnyPool` therefore has no `locked_until`, `retry_after`, `processed_at` or `headers` column, and the [lease form](../forms/lease.md) is out of its reach.
It serves the row lock form on its Postgres and MySQL backends and the [advisory lock form](../forms/advisory.md) on all three.
A row lock table on a SQLite backend stops its subscription at startup, because the database is known only then.
For the same reason a table's [isolation level](../transactions/isolation.md) compiles at every level, and the backend's dialect refuses one its database does not open when the subscription starts, with `SqlxBrokerError::Dialect`.

One `Publish` serves every backend when it writes the row through the derive's [`Insert`](../tables/insert.md) on the connection the pool hands out.
The [AsyncAPI document](../asyncapi/server.md) describes the server by the URL's scheme, never its credentials.
A test addresses the broker as `tb.broker::<SqlxBroker<Any>>()`.

## Example

An email queue in the advisory lock form, which every backend serves; the test runs it on SQLite in memory.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_databases_any.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_databases_any.rs#service"
```

</TabItem>
</Tabs>
