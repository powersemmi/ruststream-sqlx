---
title: Inbox
description: "#[derive(Inbox)] describes a queue table with an ordinary sqlx struct: the table in #[inbox(..)], column roles in #[field(..)], names from sqlx."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Inbox

[`#[derive(Inbox)]`](api:ruststream_sqlx::Inbox) describes a queue table with an ordinary sqlx struct: the table in `#[inbox(..)]`, column roles in `#[field(..)]`, names from sqlx.

The struct derives `sqlx::FromRow` beside `Inbox`, and the queue is a table of the service's own schema.
`#[inbox(table = "..")]` names the table and is required; `schema = ".."` places it in a schema, and neither holds a dot.
`#[field(id)]` marks the row's identity, the one role every table has; its type is `Clone`.
The other roles, such as `group`, `attempt` and `locked_until`, each turn on one behaviour of the queue ([Roles](./roles.md)).
sqlx's own attributes name the [columns](./columns.md).
A struct with a `#[field(payload)]` field is in [payload mode](../messages/index.md), and one without it is in [row mode](../messages/rows.md).

The rest of `#[inbox(..)]` selects how the table is served:

| Attribute | What it does |
|---|---|
| `advisory_lock = "jobs-{id}"` | the [advisory lock form](../forms/advisory.md), with a key built from the fields in braces |
| `isolation = ..`, `mode = ..` | the [isolation level](../transactions/isolation.md) or the SQLite mode the table's transactions open at |
| `clock = ..` | where ["now" comes from](../time/index.md) |
| `custom(..)` | the [events](./events.md) the service writes itself |
| `checked, db = ..` | the statements in sqlx's [compile-time check](./checked.md) |

A generic struct keeps its parameters: the impl asks `Send + Sync + 'static` of the struct and `Clone + Debug + Send + Sync + 'static` of the `id` field's type.
A struct that cannot drive a queue does not compile, and the error points at the field or the name: no `id`, a role played twice, a column named twice, a dot in `table`, `isolation` beside `mode`, and the form rules of [Events](./events.md).
The derive writes the [`InboxTable`](./inbox-spec.md) impl a table described by hand writes, and the traits its roles need.

## Example

One table holds the notifications of every channel: its `channel` column is the group, and each subscription reads the group its name selects.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_tables_inbox.rs#table"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_tables_inbox.rs#table"
```

</TabItem>
</Tabs>
