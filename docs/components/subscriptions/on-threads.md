---
sidebar_position: 5
title: on_threads
description: "on_threads runs an InboxQueue subscription's handlers on dedicated threads, each with a pool of its own, counted against connection_limit."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# on_threads

[`on_threads`](api:ruststream_sqlx::InboxSettings::on_threads) runs an `InboxQueue` subscription's handlers on dedicated threads, each with a pool of its own, counted against [`connection_limit`](api:ruststream_sqlx::SqlxBroker::connection_limit).

It is the inbox's spelling of the framework's `threads(n)`, for handlers that compute: it fills the position `workers(n)` fills, so the attribute names neither.
[`InboxThreads::new(n)`](api:ruststream_sqlx::InboxThreads::new) names the threads, [`by_key`](api:ruststream_sqlx::InboxThreads::by_key) sends the rows of one [partition key](./workers.md) to one thread, and [`connections`](api:ruststream_sqlx::InboxThreads::connections) sets how many connections each thread opens, one unless set.
The subscription claims on the app's runtime and runs each handler on one of its threads.
Claims, settlements, transactions and publishes take the service's pool, and their connections open on the app's runtime.
A handler's [`Ctx<keys::Pool<DB>>`](../context/pool.md) is the thread's own pool, with the service pool's options; its connections open on the thread and close when the thread ends.
The service therefore opens its pool's connections and, for each such subscription, threads times connections.
`connection_limit(n)` on the broker holds that sum to what the database allows: a subscription that would pass it stops the service at startup with [`SqlxBrokerError::ConnectionLimit`](api:ruststream_sqlx::SqlxBrokerError::ConnectionLimit).
The framework's own `threads(n)` gives its threads pools of one connection too, and the limit does not count them.
In the row lock form each delivery a thread holds keeps a connection of the service's pool, so the pool's bound applies as with `workers(n)` ([Connections](../broker/connections.md)).
A [subscription by name](./names.md) takes the framework's `threads(n)`.

## Example

Four threads scan uploads, each writing its verdict through a pool of its own, and the broker holds the service to twelve connections.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_subscriptions_threads.rs#on_threads"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_subscriptions_threads.rs#on_threads"
```

</TabItem>
</Tabs>
