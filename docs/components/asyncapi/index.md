---
title: AsyncAPI
description: "With asyncapi the document names the database server and, on each subscription's channel, the table it reads and its group; rows get their schema."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AsyncAPI

With the `asyncapi` feature the document names the database server and, on each subscription's channel, the table it reads and its group; rows get their schema.

The core builds the [AsyncAPI document](api:ruststream::asyncapi) from the app, before anything connects.
This crate adds what it knows about the parts the core describes.
A [server](./server.md) reports the database the pool reaches: its host and port and the protocol, never a credential.
A [channel](./channels.md) of an `InboxQueue` subscription reports the table it reads under the extension `x-sqlx`, with the group its name selects.
Everything comes from the service's own declarations: the pool's options, the table's description and the subscription's name.

A message a handler decodes is documented by its type's `JsonSchema`, as on every broker.
A handler in [row mode](../messages/rows.md) takes the row itself, so the row's `JsonSchema` documents the message: the derive names `schemars` through the core, `#[schemars(crate = "ruststream::schemars")]`.
A pool built with `connect_lazy` builds the document with no database in reach, inside a Tokio runtime.

## Example

The dependency line:

```toml
ruststream-sqlx = { version = "0.7", features = ["inbox", "postgres", "asyncapi"] }
```

The service registers its broker under the label `postgres`, the key of its server in the document.
The tests build the document with [`build_spec`](api:ruststream::asyncapi::build_spec) and read it as JSON.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_asyncapi.rs#app"
```

</TabItem>
</Tabs>

The mail handler takes the row of `mail_jobs` itself, and the message it reads is documented by the row's columns.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_asyncapi.rs#rows"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_asyncapi.rs#rows"
```

</TabItem>
</Tabs>
