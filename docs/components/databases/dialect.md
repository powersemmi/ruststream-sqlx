---
sidebar_position: 5
title: Dialect
description: "SqlxBroker::with_dialect serves a database with a dialect of the service's own: Dialect, then the trait of each form it builds, and ByName for names."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dialect

[`SqlxBroker::with_dialect`](api:ruststream_sqlx::SqlxBroker::with_dialect) serves a database with a dialect of the service's own: [`Dialect`](api:ruststream_sqlx_dialect::Dialect), then the trait of each form it builds, and [`ByName`](api:ruststream_sqlx::ByName) for names.

A dialect of the service's own serves a database without a built-in dialect, such as CockroachDB through sqlx's Postgres driver.
Its type is the broker's second type parameter, `SqlxBroker<Postgres, Cockroach>`, and each thing its tables can do is a trait it implements:

- `Dialect`: names, placeholders, the statement that opens a transaction at a table's level, the savepoint a [transactional](../transactions/index.md) handler's writes start after, and the statements every form runs to settle a row, move a dead letter, fetch rows and insert one;
- [`RowLock`](api:ruststream_sqlx_dialect::RowLock): the row lock form and its claim;
- [`Lease`](api:ruststream_sqlx_dialect::Lease): the lease form, its claim, the extension of a lease in work and the stamp of a row a claim only selected;
- [`Advisory`](api:ruststream_sqlx_dialect::Advisory): the advisory lock form, its claim of candidates with their keys, the lock and the release of a key, and the take of a row whose key the session holds;
- [`Opens<Level>`](api:ruststream_sqlx_dialect::Opens): an [isolation level](../transactions/isolation.md) or SQLite mode a table may declare;
- `ByName<DB>`: [subscriptions by name](../subscriptions/names.md), the JSON headers and the times their rows hold.

A table in a form whose trait the dialect lacks does not compile, and neither does a by-name mount on a dialect without `ByName`; the error names the trait.
A dialect refuses with a [`StatementError`](api:ruststream_sqlx_dialect::StatementError) what it does not build, such as FIFO groups, and the subscription of such a table stops at [startup](../broker/startup.md).
The provided methods of `Dialect` and `Lease` state what their defaults mean for a database without the feature.
The statements are built once, when a subscription opens, so a message costs the same as through a built-in dialect.

A dialect may wrap a built-in one, such as [`dialect::Sqlite`](api:ruststream_sqlx_dialect::Sqlite), and write one statement its own way.
It then forwards every other method, the provided methods the built-in dialect overrides included, or the wrapped statements change.
One statement written the service's own way is usually an [event of the table](../tables/events.md) instead, which needs no dialect.
The crate registers the brokers of its built-in dialects with the test harness; a test of a broker on a dialect of the service's own registers its type once, with `ruststream::register_testable_broker!(SqlxBroker<Sqlite, Audited>)`, and addresses it by that type, `tb.broker::<SqlxBroker<Sqlite, Audited>>()`.

## Example

`Audited` wraps the SQLite dialect and acknowledges a row by moving it to the group `acknowledged`, where it stays for an audit.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_databases_dialect.rs#dialect"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_databases_dialect.rs#dialect"
```

</TabItem>
</Tabs>
