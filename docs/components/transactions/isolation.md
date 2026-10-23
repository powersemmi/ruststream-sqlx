---
sidebar_position: 1
title: Isolation
description: "isolation = <level> opens a table's transactions at an isolation level, mode = <mode> at a SQLite mode; a level the dialect does not open does not compile."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Isolation

`isolation = <level>` opens a table's transactions at an isolation level, `mode = <mode>` at a SQLite mode; a level the dialect does not open does not compile.

`isolation` in `#[inbox(..)]` takes `read_uncommitted`, `read_committed`, `repeatable_read` or `serializable`.
A SQLite table declares `mode` instead: `deferred`, `immediate` or `exclusive`.
SQLite runs every transaction serializable, and its mode decides when a transaction takes the write lock ([`Mode`](api:ruststream_sqlx_dialect::Mode)).
A table declares one of the two at most.
The declaration governs the transactions the crate opens for a delivery's work: in the [row lock form](../forms/index.md) the claim's transaction, and in [transactional mode](./index.md) the one the handler writes through, in every form.
A table that declares neither opens them with a plain `BEGIN` on Postgres and SQLite, at the database's default, and at READ COMMITTED on MySQL and MariaDB.
Each database opens the levels it keeps:

| Database | Opens |
|---|---|
| Postgres | `read_committed`, `repeatable_read`, `serializable`; it runs READ UNCOMMITTED as READ COMMITTED, so its dialect does not open it |
| MySQL and MariaDB | all four levels |
| SQLite | the three modes |

A subscription to a table at a level its broker's dialect does not open does not compile, and neither does a route into that table; the error names the dialect and the level.
An `AnyPool` names its database only when the broker connects, so there such a subscription stops at startup with [`SqlxBrokerError::Dialect`](api:ruststream_sqlx::SqlxBrokerError::Dialect).
On Postgres a row lock table at `repeatable_read` or `serializable` fails claims with serialization errors when claims and settlements of its rows run at once, so `read_committed` is the practical level there.
A lease table on Postgres at a strict level refuses transactional mode at startup: its transaction would not see the lease extended after its first statement.
On MySQL and MariaDB a row lock claim at `repeatable_read` or `serializable` locks every row it reads until its settlement; an index on the group's column keeps a claim to its own group.
By hand, `.opens::<Level>()` takes a marker of [`dialect::level`](api:ruststream_sqlx_dialect::level), and `Opens<Level>` goes into the description's type.

## Example

A withdrawal reads the balance and writes it back in one transaction; `mode = immediate` takes the write lock when the transaction begins, so no other writer changes the balance between the read and the write.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_transactions_isolation.rs#isolation"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_transactions_isolation.rs#isolation"
```

</TabItem>
</Tabs>
