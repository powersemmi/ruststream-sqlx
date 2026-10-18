---
sidebar_position: 3
title: InboxSpec
description: "A table described by hand implements InboxTable: a const InboxSpec chain, its settings as types in type Table, and the same statements as the derive."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# InboxSpec

A table described by hand implements [`InboxTable`](api:ruststream_sqlx::InboxTable): a `const` [`InboxSpec`](api:ruststream_sqlx::InboxSpec) chain, its settings as types in `type Table`, and the same statements as the derive.

The derive and the description by hand are two spellings of one public API, and they build the same statements and the same per-message code.
A service writes the description by hand for a declaration a reviewer reads as it is, or for a struct that holds fewer fields than the table has columns.
`TABLE` holds the description: `InboxSpec::new` takes the table's name and the id column, and each setter adds a setting.
Every setter is a `const fn`, so the description is a constant and costs nothing at run time.
`type Id` names the id's type, and `fn id` lends it off the row.

`type Table` names the chain's final type, because stable Rust does not infer the type of an associated constant; a chain that does not match it is a type mismatch.
The type lists the settings the crate reads, in the order the chain sets them:

- the form: [`Lease<Time>`](api:ruststream_sqlx::spec::Lease) or [`Advisory`](api:ruststream_sqlx::spec::Advisory);
- payload mode: [`Payload`](api:ruststream_sqlx::spec::Payload);
- the roles a delivery reads off a row: `Key`, `Attempt`, `Headers`, `HeaderFields`;
- the time roles with their time type: `RetryAfter<Time>`, `ProcessedAt<Time>`;
- `Fifo`, the clock `Clock<Source>` and the opening `Opens<Level>`;
- the events the service writes itself, the markers of [`spec::own`](api:ruststream_sqlx::spec::own).

The column-only setters, `within`, `group`, `priority`, `data`, `fetching` and `selecting_all`, leave the type as it is.
A setting the chain leaves out keeps its default: the row lock form, row mode, the host's clock, the database's own opening and the crate's events.
Each role a delivery reads names a [small trait](./roles.md) the struct implements.

| Rule | Derive | By hand |
|---|---|---|
| an id and a table name | compile | the constructor's arguments |
| one form, one message mode, one clock, one opening; each typed role and each own event once | compile | the setter's bound, at compile time |
| a role in the type has its trait; an own event is implemented | compile | a trait bound, at compile time |
| the form rules of [Events](./events.md) | compile | a bound on `type Table`, at compile time |
| a column-only role set once; a column named once | compile | the constant's evaluation, at build time |
| a lock key names a column the table has | compile | the statement's prepare, at startup |

A refusal at build time names the table and the rule, and points at the crate's check rather than the service's line.

## Example

One table, `email_jobs`, described twice: `Welcome` with the derive and `Reminder` by hand, each serving its own group.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_tables_spec.rs#table"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_tables_spec.rs#table"
```

</TabItem>
</Tabs>
