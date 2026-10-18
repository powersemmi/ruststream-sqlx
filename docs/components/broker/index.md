---
title: SqlxBroker
description: "SqlxBroker serves the queues in the tables of the service's sqlx pool; new records the pool, and connect checks one connection and picks the dialect."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SqlxBroker

[`SqlxBroker`](api:ruststream_sqlx::SqlxBroker) serves the queues in the tables of the service's sqlx pool; `new` records the pool, and `connect` checks one connection and picks the dialect.

[`SqlxBroker::new`](api:ruststream_sqlx::SqlxBroker::new) takes a `Pool<DB>` of a database a built-in dialect serves: Postgres, MySQL, MariaDB or SQLite.
It does no I/O, and the broker never closes the pool: the pool stays the service's.
sqlx builds a pool only inside a Tokio runtime, so a service on the inbox builds its pool in `#[tokio::main]` and hands it to the function that builds the app.
The settings that follow `new` are builder steps: [routes](../publishing/route.md) that lead publishes into tables, the [poll interval](../subscriptions/poll-interval.md) and the [lease](../forms/lease.md) every subscription takes unless it names its own, and the [connection limit](../subscriptions/on-threads.md) of dedicated threads.

At startup the runtime connects the broker and receives a [`ConnectedSqlxBroker`](api:ruststream_sqlx::ConnectedSqlxBroker).
The connect takes one connection from the pool, so a database out of reach fails startup with [`SqlxBrokerError::Connect`](./errors.md).
It also picks the dialect from that connection, which matters for an [`AnyPool`](../databases/any-pool.md).
Every subscription then reads one queue through an [`InboxQueue`](../subscriptions/index.md) descriptor, and the [startup checks](./startup.md) run as it opens.
At [shutdown](./shutdown.md) the runtime turns the connected broker into a `ClosedSqlxBroker`.

A message in work holds connections of the pool by the [form](../forms/index.md) its table claims rows in, and a publish takes one more; [Connections](./connections.md) shows how to size the pool.

## Example

The service serves the queue `thumbnails` from a SQLite file: `main` builds the pool, creates the table and runs the app.
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
