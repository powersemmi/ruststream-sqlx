---
sidebar_position: 2
title: Workers
description: "workers(n) keeps a claim in flight for each free worker, bounded by the pool; with a partition_key column workers_by_key(n) keeps each key in order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Workers

[`workers(n)`](api:ruststream::runtime::SubscriberSettings::workers) keeps a claim in flight for each free worker, bounded by the pool; with a `partition_key` column [`workers_by_key(n)`](api:ruststream::runtime::SubscriberSettings::workers_by_key) keeps each key in order.

A subscription with `n` workers runs up to `n` deliveries at once.
It keeps a claim in flight for each free worker, so its workers take rows at once instead of one after another.
It never holds more than the pool's size less one connection, so the pool keeps one for the handlers' own queries, their publishes and the settlements that take one ([Connections](../broker/connections.md)).
A subscription keeps one claim in flight where claims that run at once would cost more than they give.
On SQLite, which takes one writer at a time, they only collide.
On a table whose [groups keep their order](./fifo.md), or with a `partition_key` column, they could finish out of claim order.
On MySQL and MariaDB a lease claim locks the rows it reads, leases in work included, until it commits.
One claim in flight still feeds every worker: the next claim starts as soon as the last one returns.
On SQLite a [lease](../forms/lease.md) claim takes a row for every worker in one statement.
The rows past the free workers wait in the subscription for the next free worker, and their leases are extended while they wait.
Two claims never take the same row.

How many workers a SQLite queue uses depends on what its handlers write.
In the lease and advisory lock forms the throughput grows with the workers.
In [transactional mode](../transactions/index.md) a handler's first write takes the database's write lock, and the lock stays until the delivery settles.
Every other writer then waits in SQLite's busy handler, which sleeps a millisecond or more instead of queueing it.
A transactional subscription on SQLite whose handlers write gains little from a second worker in the lease form, and loses most of its throughput to these waits in the advisory lock form: it runs one worker there.

A `#[field(partition_key)]` column holds each row's key: text or bytes, optional or not ([`KeyColumn`](api:ruststream_sqlx::KeyColumn)).
`workers_by_key(n)` sends the deliveries of one key to one worker, so the rows of a key are handled in the order they were claimed, and different keys run side by side.
By hand, `.partition_key(..)` names the column, `Key` goes into the description's type, and the row reads its key through [`KeyRow`](api:ruststream_sqlx::KeyRow).

## Example

Images resize on four workers in any order, and the ledger's postings of one account stay in order on one of four workers.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_subscriptions_workers.rs#workers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_subscriptions_workers.rs#workers"
```

</TabItem>
</Tabs>
