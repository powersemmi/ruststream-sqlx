---
sidebar_position: 2
title: Registry
description: "outbox! registers record types under the names they track and returns the registry; a name written twice does not compile."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Registry

[`outbox!`](api:ruststream_sqlx::outbox) registers record types under the names they track and returns the registry; a name written twice does not compile.

The registry is an [`Outbox`](api:ruststream_sqlx::outbox::Outbox): the pool its records live in and the list of names it tracks, each with its record type.
`outbox! { pool: pool, "orders" => OrderOutbox }` builds one on a pool, and without `pool:` it starts without one, for the [`set_pool`](./set-pool.md) shape.
Each registration builds the record's default statements once, from its table's description and the database's built-in dialect.
The registry is cheap to clone, and every clone reads the same pool and the same names.
It hands out the [two middlewares](./tracking.md), the [republish](./republish.md) and [`wrap`](./wrap.md).

One record type may track several names, so one table serves them all.
Each registration republishes only the records of its own name.
A name has one record type.

By hand, [`Outbox::new`](api:ruststream_sqlx::outbox::Outbox::new) takes the pool and [`Outbox::deferred`](api:ruststream_sqlx::outbox::Outbox::deferred) starts without one.
[`track::<Record, Name>()`](api:ruststream_sqlx::outbox::Outbox::track) registers a record type under a name that is a type, a [`TrackedName`](api:ruststream_sqlx::outbox::TrackedName), and a name tracked twice stops the build.
[`register::<Record>(name)`](api:ruststream_sqlx::outbox::Outbox::register) takes the name as a `&'static str`, for a name the service computes, and panics on a repeated name when the app is built.
The registry's type lists every record type it holds, so a handle carries no lookup table and an unregistered name costs only the comparisons.

## Example

`OrderOutbox` tracks both `orders` and `refunds`: the replies of `place` and `cancel` are recorded in the one `outbox` table, each under its own name.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_registry.rs#registry"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_registry.rs#registry"
```

</TabItem>
</Tabs>
