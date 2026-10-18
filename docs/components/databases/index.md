---
title: Databases
description: "SqlxBroker::new takes the dialect built in for its pool's database; statements are built once when a subscription opens, the insert at compile time."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Databases

[`SqlxBroker::new`](api:ruststream_sqlx::SqlxBroker::new) takes the dialect built in for its pool's database; statements are built once when a subscription opens, the insert at compile time.

A dialect builds the SQL of a broker's tables from their descriptions.
The built-in one, [`BuiltIn<DB>`](api:ruststream_sqlx::BuiltIn), is the broker's default second type parameter: `SqlxBroker<Sqlite>` and `SqlxBroker<Sqlite, BuiltIn<Sqlite>>` name the same type.
Each database has a feature that adds its dialect and its sqlx driver: [`postgres`](./postgres.md), [`mysql`](./mysql.md) for MySQL and MariaDB, and [`sqlite`](./sqlite.md).
[`any`](./any-pool.md) serves an `AnyPool` by the built-in dialect of the database it reaches.
A database without a built-in dialect is served by a [dialect of the service's own](./dialect.md).

One struct describes a queue table on every database.
The broker takes the dialect of its pool, which builds the claims and settlements of each subscription once, when it opens, so a message costs the same on every database.
The derive renders the insert of every enabled dialect at compile time.
The service writes its [`Publish`](../publishing/index.md) per database, because that statement is its own SQL.

What each database serves:

| | Postgres | MySQL, MariaDB | SQLite | `AnyPool` |
|---|---|---|---|---|
| [Row lock form](../forms/index.md) | yes | yes | no | Postgres and MySQL backends |
| [Lease form](../forms/lease.md) | yes | yes | yes | no |
| [Advisory lock form](../forms/advisory.md) | yes | yes | in the process | yes |
| [Isolation levels](../transactions/isolation.md) | three | four | three modes | the backend's |
| [`listen_notify`](../wake/listen-notify.md) | yes | no | no | no |
| [Database clock](../time/database-clock.md) | `statement_timestamp()` | `UTC_TIMESTAMP(6)` | `strftime(..)`, as text | no time columns |

The connections a subscription holds depend on its form and on transactional mode, alike on every database: [Connections](../broker/connections.md) shows how to size the pool.

## Example

The broker's type names the built-in SQLite dialect, the default of `SqlxBroker::new`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_databases.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_databases.rs#service"
```

</TabItem>
</Tabs>
