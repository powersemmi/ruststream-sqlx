---
sidebar_position: 3
title: Shutdown
description: "Shutdown returns ClosedSqlxBroker, releases every advisory lock in work, and a publisher kept past it reports SqlxBrokerError::Closed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown returns [`ClosedSqlxBroker`](api:ruststream_sqlx::ClosedSqlxBroker), releases every advisory lock in work, and a publisher kept past it reports [`SqlxBrokerError::Closed`](api:ruststream_sqlx::SqlxBrokerError::Closed).

At shutdown the runtime first stops the subscriptions and lets the handlers in work finish, up to its shutdown timeout.
Then it shuts the broker down: the claim loops stop, and so does the extension of the leases.
A claim the runtime cut short as it stopped a subscription may still run on the server; shutdown waits until the server has ended it and rolled its transaction back, so the stopped service holds no lock on the table.
A delivery in work in the [lease form](../forms/lease.md) keeps its lease, which runs out unless the delivery settles first; its row then returns to the queue.
In the [advisory lock form](../forms/advisory.md) the broker releases the lock of every delivery in work and waits for the settlements in flight, so no lock of the broker is left.

A pool the broker built ([`from_url`](api:ruststream_sqlx::SqlxBroker::from_url)) closes last: it lends no new connection, and a connection a delivery in work still holds closes when it comes back.
A settlement after the shutdown that needs a new connection, as in the lease form, then fails, and its row returns when its lease runs out.
The service's own pool ([`new`](api:ruststream_sqlx::SqlxBroker::new)) stays open, and the service closes it after `run` returns.
A handle taken before shutdown, such as a [repository](../publishing/index.md) publisher, refuses every publish afterwards with `SqlxBrokerError::Closed`, and never writes a row past the shutdown.
A tool that drives the broker by hand receives the `ClosedSqlxBroker`, whose [`locks_released`](api:ruststream_sqlx::ClosedSqlxBroker::locks_released) and [`connections_closed`](api:ruststream_sqlx::ClosedSqlxBroker::connections_closed) count what the shutdown did to the advisory locks.

## Example

`run` returns once the handlers have drained and the broker has shut down and closed the pool it built.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_broker_shutdown.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_broker_shutdown.rs#service"
```

</TabItem>
</Tabs>
