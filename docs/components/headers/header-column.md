---
sidebar_position: 1
title: HeaderColumn
description: "HeaderColumn lets a headers column hold a type of the service's own; it moves the map in and out and names a header it cannot keep byte for byte."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# HeaderColumn

[`HeaderColumn`](api:ruststream_sqlx::HeaderColumn) lets a headers column hold a type of the service's own; it moves the map in and out and names a header it cannot keep byte for byte.

The trait has three methods.
`take_headers` moves the headers the column holds into a header map, once per delivery as its row is claimed, so a column that owns its strings hands them over without a copy.
`from_headers` builds the column value that holds a message's headers, for the service's `Publish`.
`unfit` names the first header the column cannot hold byte for byte, and the broker refuses a publish that carries it with [`SqlxBrokerError::Header`](../broker/errors.md).

The crate implements it for `Json<BTreeMap<String, String>>` under the `json` feature, and for `Option<T>` of any implementation, where `None` holds no headers.
The trait is shared by the inbox and the [outbox](../outbox/record.md): one column type serves a queue table and an outbox table alike.
The type is also a sqlx type of its own, so the driver reads and writes the column through it.

## Example

`Lines` keeps the headers one per line, `name: value`, in a `TEXT` column, and refuses a value that is not text on one line.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_headers_column.rs#column"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_headers_column.rs#column"
```

</TabItem>
</Tabs>
