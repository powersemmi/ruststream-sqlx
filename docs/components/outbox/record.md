---
sidebar_position: 1
title: Record
description: "#[derive(Outbox)] describes the outbox table: id, name and payload are required, headers and processed_at optional; the service writes the record."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Record

[`#[derive(Outbox)]`](api:ruststream_sqlx::Outbox) describes the outbox table: `id`, `name` and `payload` are required, `headers` and `processed_at` optional; the service writes the record.

`#[outbox(table = "..")]` names the table and is required; `schema = ".."` places it in a schema.
`#[field(..)]` gives a field one role:

- `id`: the record's identity. A tracked message carries it in the id header through its `Display` and `FromStr`, so an integer and a UUID both fit.
- `name`: the name the message was published under.
- `payload`: the published bytes.
- `headers`: the published headers, a [`HeaderColumn`](api:ruststream_sqlx::HeaderColumn) type such as `Option<Json<BTreeMap<String, String>>>` (feature `json`). A republish sends them again.
- `processed_at`: the mark of a processed record. The outbox writes it from the database's clock, so any time type fits. A record without it is deleted once processed.

Every other field is a plain column sqlx reads, and sqlx's `rename`, `rename_all` and `skip` name the columns.

The service writes the record itself, in [`outbox::Publish`](api:ruststream_sqlx::outbox::Publish), because its statement decides how the name, the payload and the headers are laid out.
`publish` runs on a connection of its own and returns the new record's id.
The registry builds the other events from the description and the database's built-in dialect when the record type is registered; [Statements](./statements.md) replaces them with the service's own.
The crate implements [`OutboxRow`](api:ruststream_sqlx::outbox::OutboxRow) for every such record, and a service names it only as a bound.

A record described by hand implements [`OutboxTable`](api:ruststream_sqlx::outbox::spec::OutboxTable) with an [`OutboxSpec`](api:ruststream_sqlx::outbox::spec::OutboxSpec) chain.
`OutboxSpec::new` takes the table and its three required columns, and `type Table` lists the typed settings in the order the chain sets them: [`Headers`](api:ruststream_sqlx::outbox::spec::Headers), [`ProcessedAt`](api:ruststream_sqlx::outbox::spec::ProcessedAt) and the events of [`spec::own`](api:ruststream_sqlx::outbox::spec::own).
A record with `Headers` implements [`HeaderRow`](api:ruststream_sqlx::HeaderRow) for the field that holds the column.
Both forms describe one table, and the registry builds the same statements from either.

A struct the outbox cannot read does not compile, and the error points at the struct, the field or the word that causes it: no `id`, `name` or `payload`, a role played twice, a column named twice, an unknown role or event, a dot in `table` or `schema`.
A table whose identifier is longer than the database takes panics when the record type is registered.

## Example

`OrderOutbox` keeps every role, and its `Publish` stores the headers the message carried; the service stamps each order with its tenant on the way out.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_record.rs#record"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_record.rs#record"
```

</TabItem>
</Tabs>
