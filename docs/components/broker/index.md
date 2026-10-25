---
title: SqlxBroker
description: "SqlxBroker serves the queues in the tables of the service's database through a sqlx pool it builds at connect, or the service's own; connect checks one connection and picks the dialect."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SqlxBroker

[`SqlxBroker`](api:ruststream_sqlx::SqlxBroker) serves the queues in the tables of the service's database through a sqlx pool; it builds the pool when it connects, or takes the service's own.

[`SqlxBroker::from_url`](api:ruststream_sqlx::SqlxBroker::from_url) records the database's URL and the pool's options, and [`from_connect_options`](api:ruststream_sqlx::SqlxBroker::from_connect_options) records the driver's connect options in place of the URL.
Neither does I/O, so the service builds the broker in its `#[ruststream::app]` builder, before the runtime starts.
`connect` builds the pool, and `shutdown` closes it.
[`SqlxBroker::new`](api:ruststream_sqlx::SqlxBroker::new) takes a `Pool<DB>` the service owns and shares with the rest of its code; the broker never closes that pool.
sqlx builds a pool only inside a Tokio runtime, so a service that owns its pool builds it in its own `main`.
The database is one a built-in dialect serves: Postgres, MySQL, MariaDB or SQLite.
The settings that follow the constructor are builder steps: [routes](../publishing/route.md) that lead publishes into tables, the [poll interval](../subscriptions/poll-interval.md) and the [lease](../forms/lease.md) every subscription takes unless it names its own, and the [connection limit](../subscriptions/on-threads.md) of dedicated threads.

At startup the runtime connects the broker and receives a [`ConnectedSqlxBroker`](api:ruststream_sqlx::ConnectedSqlxBroker).
The connect takes one connection from the pool, so a database out of reach fails startup with [`SqlxBrokerError::Connect`](./errors.md), and a URL the driver cannot read with `SqlxBrokerError::Url`.
It also picks the dialect from that connection, which matters for an [`AnyPool`](../databases/any-pool.md).
Every subscription then reads one queue through an [`InboxQueue`](../subscriptions/index.md) descriptor, and the [startup checks](./startup.md) run as it opens.
At [shutdown](./shutdown.md) the runtime turns the connected broker into a `ClosedSqlxBroker`.

A message in work holds connections of the pool by the [form](../forms/index.md) its table claims rows in, and a publish takes one more; [Connections](./connections.md) shows how to size the pool.

## Example

The service serves the queue `thumbnails` from a SQLite file: the broker builds its pool from the file's URL, and `on_startup` creates the table before the subscription checks it.
A test hands `service` a database of its own.
`cargo run -p ruststream-sqlx --example sqlx_broker --features inbox,sqlite,chrono,testing` starts it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_broker.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_broker.rs#service"
```

</TabItem>
</Tabs>
