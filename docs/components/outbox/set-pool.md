---
sidebar_position: 6
title: set_pool
description: "Inside #[ruststream::app] the registry starts without a pool, and on_startup builds one and hands it over with set_pool; the service closes it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# set_pool

Inside `#[ruststream::app]` the registry starts without a pool, and `on_startup` builds one and hands it over with [`set_pool`](api:ruststream_sqlx::outbox::Outbox::set_pool); the service closes it.

`#[ruststream::app]` builds the app before the Tokio runtime exists, and sqlx builds a pool only inside one.
So the registry starts without a pool: `outbox!` without `pool:`, or [`Outbox::deferred`](api:ruststream_sqlx::outbox::Outbox::deferred) by hand.
`on_startup` runs inside the runtime, builds the pool and hands it to a clone of the registry with `set_pool`.
Every handle the registry gave out, the middlewares and the republish among them, reads that one pool.
An app built inside a runtime passes the pool directly instead: `outbox! { pool: pool, .. }` or [`Outbox::new`](api:ruststream_sqlx::outbox::Outbox::new).

`set_pool` takes one pool, once: a second call, or a call on a registry built with a pool, returns [`PoolAlreadySet`](./outbox-error.md).
Until the pool is set, a tracked publish fails with `OutboxError::NoPool`, which names the name, and a tracked delivery is retried without its handler.
The value `on_startup` returns becomes the app's state, and `after_shutdown` receives it: the service closes the pool there, once the handlers have finished and the brokers have stopped.

## Example

`service` builds the app from a database URL, and `#[ruststream::app]` runs it on the service's own file.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_pool.rs#pool"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_pool.rs#pool"
```

</TabItem>
</Tabs>
