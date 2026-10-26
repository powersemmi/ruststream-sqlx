---
sidebar_position: 1
title: Connections
description: "Each message in work holds pool connections by its form; workers(n) claims in parallel but always leaves the pool one connection for handlers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connections

Each message in work holds pool connections by its [form](../forms/index.md); [`workers(n)`](../subscriptions/workers.md) claims in parallel but always leaves the pool one connection for handlers.

A subscription mounted with `workers(n)` keeps a claim in flight for each free worker.
It never holds more than the pool's size less one connection, so the pool always keeps one for the handlers' own queries, their publishes and the settlements that take one.
A second claim starts only while the pool has a connection to spare, so subscriptions that share a pool leave each other room, and a pool short of room runs fewer deliveries at once.
A subscription keeps one claim in flight on SQLite, which takes one writer at a time, on a table whose [groups keep their order](../subscriptions/fifo.md), and with a `partition_key` column under `workers_by_key(n)`.
On SQLite that one claim takes a row for every worker in the lease form; in transactional mode each of those rows holds its transaction's connection while it waits for a worker, within the connections the subscription may hold.

What a delivery holds depends on the form:

| Form | Connections |
|---|---|
| row lock | one per delivery in work and one per claim in flight, for the whole handler |
| lease | one for each claim's and each settlement's statement, and one per subscription each half lease to extend the leases |
| advisory lock | one per delivery in work until it settles, in a batch too |
| [transactional mode](../transactions/index.md), any form | one per delivery in work, for the transaction its handler writes through |

A publish takes a connection of its own for its insert, and a handler that reads [`keys::Pool`](../context/pool.md) takes one for each query.
So a pool is sized for every subscription: n for each `workers(n)`, one more for the pool, and the connections the handlers take for their own work.
In the lease form a pool of n + 1 lets every free worker claim at once.
Under sqlx's default `test_before_acquire`, each acquire pings the connection first, a round trip a pool built with `test_before_acquire(false)` skips.
Subscriptions on [dedicated threads](../subscriptions/on-threads.md) open connections of their own on top of the pool.

## Example

Four workers resize images, and each handler records its image through the pool; the pool has room for the four workers, the connection the subscription leaves free and the handlers' writes.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_broker_connections.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_broker_connections.rs#service"
```

</TabItem>
</Tabs>
