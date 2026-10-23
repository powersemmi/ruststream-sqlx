---
sidebar_position: 2
title: Startup
description: "A subscription builds and prepares its statements when it opens, so a missing table or column, a retry the table cannot carry or a second reader stops startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

A subscription builds and prepares its statements when it opens, so a missing table or column, a retry the table cannot carry or a second reader stops startup.

A struct that cannot drive a queue does not compile at all; the [derive](../tables/index.md) lists those errors.
What only the database can tell is checked at startup.
Each subscription builds its statements with the broker's dialect and prepares each one on the server.
A table or a column the description names and the database lacks fails startup with [`SqlxBrokerError::Schema`](./errors.md), which names the subscription, the table and the statement.
Preparing checks names, not column types: a row whose columns do not decode reaches the [decode policy](../messages/decoding.md) instead.

The other refusals at startup:

| Error | When |
|---|---|
| `Declaration` | the mount declares what the table cannot carry: `max_attempts(n)` without [`dead_letter(..)`](../settlement/dead-letter.md) or the reverse, a cap on a table without an `attempt` column, or [transactional mode](../transactions/isolation.md) in the lease form at a strict isolation level on Postgres |
| `Dialect` | the dialect cannot build a statement the table needs |
| `ServerTooOld` | a [MySQL](../databases/mysql.md) older than 8.0.1 or a MariaDB older than 10.6, which lack `SKIP LOCKED` |
| `AlreadySubscribed` | a second subscription to one queue on the same broker; a table without a `group` column is one queue |
| `Connect`, `Backend` | the broker could not take a connection, or an `AnyPool` reached a database whose dialect feature is off |

The service runs its migrations before the app starts, and the checks then see the schema the release reads.

## Example

A release adds an `attempt` column to the queue; `main` applies the schema before the app starts, and a table without the column stops startup with `SqlxBrokerError::Schema`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_broker_startup.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_broker_startup.rs#service"
```

</TabItem>
</Tabs>
