---
sidebar_position: 3
title: SQLite
description: "sqlite serves the lease and advisory lock forms in the service's process: one writer at a time, times as text, and no server to run in tests."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SQLite

The `sqlite` feature serves the lease and advisory lock forms in the service's process: one writer at a time, times as text, and no server to run in tests.

SQLite locks the whole database for a writer, so no claim can hold rows for a handler.
Its dialect implements no row lock form, and a subscription to a table without `locked_until` or `advisory_lock` does not compile.
A [lease](../forms/lease.md) claim is one `UPDATE .. RETURNING` that leases its rows, and one writer at a time keeps two claims apart.
The rows of one lease claim come in no particular order.
A claim of the service's own opens its transaction with `BEGIN IMMEDIATE` and comes with a [`Fetch`](../headers/fetch.md) of its own.

The [advisory lock form](../forms/advisory.md) keeps its keys in the process, beside the pool.
One process therefore serves a database file: two processes on one file may deliver a row twice.
A table in that form needs no time column, so it needs neither `chrono` nor `time`.
A transaction the crate opens takes the [mode](../transactions/isolation.md) the table declares: `deferred`, `immediate` or `exclusive`.

SQLite keeps times as text and compares them as text.
`chrono` times sort exactly.
`time` values sort right only across seconds, so with them a lease may end up to a second late, and a delayed retry may come back up to a second early or late ([QueueTime](../time/queue-time.md)).
The [AsyncAPI document](../asyncapi/server.md) describes the server by its protocol alone, with no host.

An in-memory database named in a shared-cache URL, `sqlite:file:<name>?mode=memory&cache=shared`, serves every connection that names it and lives while one of them is open.
A [test](../testing/index.md) runs the service on one with no server.

## Example

Thumbnails in the advisory lock form: two workers each hold a row's key in the process while their handler runs.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_databases_sqlite.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_databases_sqlite.rs#service"
```

</TabItem>
</Tabs>
