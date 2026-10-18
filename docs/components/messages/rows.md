---
sidebar_position: 1
title: Rows
description: "A table without a payload column is in row mode: a handler takes the struct itself as the driver read it, one row or a slice per claim."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Rows

A table without a payload column is in row mode: a handler takes the struct itself as the driver read it, one row or a slice per claim.

A struct without a `#[field(payload)]` field, or a description without `.payload(..)`, is in row mode.
Its handler takes `&Row`, lent by the delivery with no codec and no copy.
A [batch](../subscriptions/batches.md) handler takes `&[Row]`, the rows of one claim in claim order, as a [`RowBatch`](api:ruststream_sqlx::RowBatch).
The struct derives `Clone`, because the test harness keeps a copy of each value, and does not derive `Deserialize`: a type that deserializes rides the codec.
Either mistake is a compile error.

Every form serves row mode, and so does [transactional mode](../transactions/index.md), where the handler takes `&Row` beside `Ctx<keys::Tx<DB>>`.
A handler of one row mounts in every form the core offers, a reply and `Out` slots included; a batch handler of rows mounts in the plain form.
The column types are the service's to get right, and a row the driver cannot read settles by the [decode policy](./decoding.md).
A nullable column takes `Option<T>`.
The `headers` column becomes the delivery's [headers](../headers/index.md), and the row the handler reads holds that column empty.

By hand, the description leaves `.payload(..)` out, names its own columns with `.data(..)`, and the struct writes one line of the core's: `impl Input for Row { type Axis = SoloCarried<Self>; }`.
A row-mode task is written with [`insert`](../tables/insert.md) or a [`Publish`](../publishing/index.md) of the service's own.
In a test, `with_value(&row)` compares the row a handler took, and `received_values::<Row>()` returns every row the subscription lent.

## Example

The mail queue has no payload column: the handler takes the `Mail` row, its recipient and subject read as columns.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_messages_rows.rs#rows"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_messages_rows.rs#rows"
```

</TabItem>
</Tabs>
