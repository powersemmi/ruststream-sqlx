---
sidebar_position: 3
title: HeaderField
description: "Each field of a headers struct without a role is a header: strings and bytes as they are, numbers in decimal, times in RFC 3339, None left out."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# HeaderField

Each field of a headers struct without a role is a header: strings and bytes as they are, numbers in decimal, times in RFC 3339, `None` left out.

The header's name is the field's column name, as sqlx's `rename` and `rename_all` set it.
Its value comes from [`HeaderField`](api:ruststream_sqlx::HeaderField):

| Field type | Header value |
|---|---|
| `String`, `Vec<u8>` | its bytes |
| integers, `bool` | its decimal text, `true` or `false` |
| `chrono::DateTime<Utc>` (feature `chrono`), `time::OffsetDateTime` (feature `time`) | its RFC 3339 text |
| `Option<T>` | the value of `T`; `None` leaves the header out |

A service implements `HeaderField` for a type of its own.
A field of a type without it does not compile, and the error offers the impl or a role.
The fields that play a role are not headers: the handler reads them on the message.

The header map is built on the first read of the delivery's headers, by middleware or an extractor such as `Headers<T>`, and a delivery whose headers nobody reads never builds it.
A message published into the table through a [repository](../publishing/index.md) carries only the headers the headers struct names, and a publish with another header fails with [`SqlxBrokerError::Header`](../broker/errors.md), so no header is lost on the way into the table.
By hand, [`HeaderFields`](api:ruststream_sqlx::HeaderFields) builds the map with [`put_header`](api:ruststream_sqlx::put_header), which writes each value through its `HeaderField`.

## Example

A shipment's headers are a carrier of the service's own type, an order number, a flag, a time and an optional trace; the handler writes them to an audit table as the header map holds them.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_headers_field.rs#fields"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_headers_field.rs#fields"
```

</TabItem>
</Tabs>
