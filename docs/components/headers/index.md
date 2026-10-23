---
title: Headers
description: "A headers column keeps a delivery's headers as a JSON object of strings; a publish with a header the column cannot hold is refused."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

A headers column keeps a delivery's headers as a JSON object of strings; a publish with a header the column cannot hold is refused.

`#[field(headers)]` marks the column, of type `Option<Json<BTreeMap<String, String>>>` (feature `json`); by hand `.headers(..)` with [`HeaderRow`](api:ruststream_sqlx::HeaderRow), which lends the field.
A claim moves the column's object into the delivery's header map, so middleware, `ctx.headers()` and the `Headers<T>` extractor read it as on any broker.
In [row mode](../messages/rows.md) the row a handler reads holds the column empty.

The service's [`Publish`](../publishing/index.md) writes the column, with [`HeaderColumn::from_headers`](api:ruststream_sqlx::HeaderColumn::from_headers) over the message's headers.
A JSON string holds UTF-8 text only, so a header value that is not UTF-8 would come back changed.
Such a publish fails with [`SqlxBrokerError::Header`](../broker/errors.md) before the service's `Publish` runs, and a header never reaches the table changed.
A table without a headers column holds no header at all, and refuses a publish that carries one.
A column of the service's own type implements [`HeaderColumn`](./header-column.md), and a table whose headers are columns of their own takes the [headers layout](./inbox-headers.md).

## Example

The tenant of an export rides the headers column: the handler reads it with `Headers<Tenant>` and drops an export for a tenant the service does not serve.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_headers.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_headers.rs#headers"
```

</TabItem>
</Tabs>
