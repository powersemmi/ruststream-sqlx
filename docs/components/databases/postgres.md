---
sidebar_position: 1
title: Postgres
description: "On Postgres every form runs: SKIP LOCKED claims, a one-statement lease claim, advisory locks on 64-bit hashes, statement_timestamp and LISTEN/NOTIFY."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Postgres

On Postgres every form runs: `SKIP LOCKED` claims, a one-statement lease claim, advisory locks on 64-bit hashes, `statement_timestamp` and `LISTEN/NOTIFY`.

The `postgres` feature adds the dialect and sqlx's Postgres driver.
The row lock and lease claims pass over rows other claims hold with [`FOR UPDATE SKIP LOCKED`](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE), which Postgres 9.5 added.
A lease claim is one statement: it locks the claimable rows, writes their lease and returns them.
A claim of the service's own may leave the rows to the crate's fetch, which reads them by a list of ids.
A table on the [database's clock](../time/database-clock.md) reads `statement_timestamp()`.

The [advisory lock form](../forms/advisory.md) and the claims of [FIFO groups](../subscriptions/fifo.md) lock 64-bit hashes made by `hashtextextended`, which Postgres 11 added.
An older server refuses their statements, so their subscriptions stop when they open.
An advisory lock belongs to the session that took it, so the advisory lock form needs a direct connection or `PgBouncer` in session pooling.

A table that declares no [level](../transactions/isolation.md) opens its transactions with a plain `BEGIN`, at the server's default.
A row lock table works at `read_committed`; the stricter levels fail claims with serialization errors when claims and settlements of its rows run at once.
Postgres runs READ UNCOMMITTED as READ COMMITTED, so its dialect does not open it, and a table that declares it does not compile.
With [`listen_notify`](../wake/listen-notify.md) a subscription wakes on rows other processes write and announce.
The [AsyncAPI document](../asyncapi/server.md) names the server by the pool's host and port, never its credentials.

## Example

Invoices in the row lock form: each claim holds its row in a transaction while the handler runs, and four claims run at once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_databases_postgres.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_databases_postgres.rs#service"
```

</TabItem>
</Tabs>
