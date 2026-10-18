---
sidebar_position: 1
title: listen_notify
description: "On Postgres, listen_notify wakes subscriptions on pg_notify from any process: each publish announces its row, and a trigger can announce others."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# listen_notify

On Postgres, [`listen_notify`](api:ruststream_sqlx::SqlxBroker::listen_notify) wakes subscriptions on `pg_notify` from any process: each publish announces its row, and a trigger can announce others.

The broker listens on one connection of the pool, taken at connect and kept until shutdown.
Each subscription listens on its table's channel before its first claim.
The channel is the table's name, qualified with its schema where the table has one (`app.jobs`).
The payload is the group, and a notification with an empty payload wakes every subscription of the table.

Each publish of the broker announces its row: `SELECT pg_notify(channel, group)` runs on the connection that wrote the row, right after the write.
A subscription in another process therefore claims the row once it is visible.
A notification that fails is logged as a warning, and the publish still succeeds: the row is written, and the poll interval reaches it.
A writer outside RustStream sends the same notification itself, in its transaction or from a trigger.

Postgres delivers a notification when its transaction commits ([`NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html)).
A notification sent while the listening connection was lost is gone: the broker connects again and wakes every subscription once.
The [poll interval](../subscriptions/poll-interval.md) stays the floor under every wake-up.

Notifications have a price, and the switch is off unless set:

- the broker keeps one connection of the pool for its whole life, so the pool needs [one more](../broker/connections.md);
- each publish runs one more statement, `pg_notify`, on its connection;
- a transaction that notifies takes one lock, global to the server, at commit, so commits of notifying transactions run one after another, which limits many concurrent writers.

Postgres keeps channel names up to 63 bytes.
A subscription to a table whose qualified name is longer stops at [startup](../broker/startup.md) with `SqlxBrokerError::Declaration`, which names the table.
On MySQL, MariaDB and SQLite `listen_notify` does not compile, and the error names the fix: [`Notifies`](api:ruststream_sqlx::Notifies) is Postgres's alone.

## Example

Another program inserts signups, and a trigger announces each row with its group.
The subscription claims the row as soon as the insert commits, though its poll waits a minute.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_wake_notify.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_wake_notify.rs#service"
```

</TabItem>
</Tabs>
