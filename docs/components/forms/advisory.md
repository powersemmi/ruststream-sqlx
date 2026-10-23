---
sidebar_position: 2
title: Advisory
description: "advisory_lock = \"..\" holds each row by a lock on its key: rows that share a key go into work one at a time, and a crashed session frees its rows."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Advisory

`advisory_lock = ".."` holds each row by a lock on its key: rows that share a key go into work one at a time, and a crashed session frees its rows.

`advisory_lock = "video_jobs-{channel}"` in `#[inbox(..)]` names each row's lock key: a placeholder names a field, the key reads its column, and the database renders the key as text.
A key on the row's id holds each row alone; a key on another column keeps the rows that share it in work one at a time.
A key belongs to the database, not to its table, so a key that starts with its table's name keeps two tables apart.
A claim selects as many due rows as it may take, in claim order, each with its key.
It locks each key on a connection of its own, without waiting, and passes over a key held elsewhere.
Then it takes the row while the row is still claimable: the take counts the attempt and reads the row, and it commits, so a crash spends an attempt.
The connection that holds the delivery keeps the lock, and no transaction stays open while the handler runs.
A settlement runs its statement on that connection, where it commits on its own; then the lock is released and the connection goes back to the pool.
[`retry()`](../settlement/index.md) runs no statement: the claim counted the attempt, and the release returns the row.
A delivery dropped unsettled releases its lock and closes its connection within five seconds, and its row returns.
`shutdown` releases the lock of every delivery in work and waits for the settlements in flight ([Shutdown](../broker/shutdown.md)).
Each delivery in work holds a connection of its own until it settles, and a batch holds one row per key ([Connections](../broker/connections.md)).
Each database keeps the locks in its own way:

- [Postgres](../databases/postgres.md) locks a 64-bit hash of the key, and a lock lives in the session that took it, so the form needs a direct connection or `PgBouncer` in session pooling.
- [MySQL and MariaDB](../databases/mysql.md) lock a name with `GET_LOCK`: the database's name, a dot and the key, or its SHA-256 past 64 characters.
- [SQLite](../databases/sqlite.md) has no locks a session holds, so the process keeps the keys in work, and one process serves a database file.
  A claim there reads the first due rows alone and takes nothing while they are in work, so a subscription has one delivery in work at a time, and with `workers(n)` each next row waits up to a poll interval.

By hand, `.advisory(..)` takes the key as its parts, `KeyPart::Literal` text and `KeyPart::Column` columns, and `Advisory` goes into the description's type.

## Example

A video takes minutes to encode, and the videos of one channel go into work one at a time; an encode that fails returns its row with the release of its key.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_forms_advisory.rs#advisory"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_forms_advisory.rs#advisory"
```

</TabItem>
</Tabs>
