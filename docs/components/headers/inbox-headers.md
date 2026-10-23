---
sidebar_position: 2
title: InboxHeaders
description: "In the headers layout a headers struct describes the queue table and its header fields, and the message struct that flattens it is what a handler takes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# InboxHeaders

In the headers layout a headers struct describes the queue table and its header fields, and the message struct that flattens it is what a handler takes.

Two structs describe one queue.
The headers struct derives [`InboxHeaders`](api:ruststream_sqlx::InboxHeaders) and describes the table: `#[inbox(table, schema, advisory_lock, clock, isolation, mode)]` and the fields that play a role, so it also selects the [form](../forms/index.md).
Its other fields are the delivery's [header values](./header-field.md).
The message struct derives [`Inbox`](../tables/index.md) and holds the headers struct in a field marked `#[field(headers)]` and `#[sqlx(flatten)]`, beside data of its own.
It takes `#[inbox(custom(..))]` alone, and a role or a table attribute on it does not compile, with an error that names the headers struct as the place for it.

The headers layout is [row mode](../messages/rows.md): a handler takes the message struct itself, or a slice of them in a batch.
Several message structs may flatten one headers struct, each read by its own subscriptions.
The claim selects the ids of the rows it takes, and the default fetch reads their rows from the queue table, the headers struct's columns and the message's own fields.
A subscription prepares that fetch when it opens, so a message field whose column the table lacks stops [startup](../broker/startup.md) with `SqlxBrokerError::Schema`.
A message whose data lives in other tables reads it with a [fetch of its own](./fetch.md).

The headers struct gets the generated [insert](../tables/insert.md), and the message struct gets none.
By hand, one description on the message holds the whole table: `.data(..)` names the header columns, `.fetching(..)` the message's own, and `.header_fields()` builds the header map through [`HeaderFields`](api:ruststream_sqlx::HeaderFields) and [`put_header`](api:ruststream_sqlx::put_header).

## Example

A packing job: `OrderHeaders` describes `order_jobs` and its headers `tenant`, `trace` and `order_id`, and `OrderJob` adds the job's note.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_headers_layout.rs#layout"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_headers_layout.rs#layout"
```

</TabItem>
</Tabs>
