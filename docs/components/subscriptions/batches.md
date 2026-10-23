---
sidebar_position: 3
title: Batches
description: "A slice handler gets one claim of up to batch(n) rows; each row settles on its own, and in the row lock form the settlements commit together."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler gets one claim of up to `batch(n)` rows; each row settles on its own, and in the row lock form the settlements commit together.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is the most rows one claim takes for it.
A claim takes what is due at once and waits for nothing, so a quiet queue yields batches smaller than `n`.
A `Vec<HandlerOutcome>` settles row `i` with outcome `i`, and a single outcome settles them all.
A row that does not decode is settled by the [decode policy](../messages/decoding.md), and the batch keeps its other rows.
On SQLite the rows of a batch come in no particular order.
Each form settles a batch its own way:

| Form | A batch |
|---|---|
| [row lock](../forms/index.md) | shares the claim's transaction and one connection; each row settles in it, and the settlements take effect together when the last delivery finishes |
| [lease](../forms/lease.md) | settles each row on its own, on a connection taken for the statement |
| [advisory lock](../forms/advisory.md) | holds a connection per row and settles each row on it; it holds one row per key and ends where the pool has no connection to spare |

In the row lock form a settlement whose statement fails rolls the whole batch back, its rows return, and the later settlements fail with [`SqlxBrokerError::BatchRolledBack`](api:ruststream_sqlx::SqlxBrokerError::BatchRolledBack).
A table in row mode hands a batch handler `&[Row]`, the rows themselves ([Rows](../messages/rows.md)).
A batch handler runs outside [transactional mode](../transactions/index.md): a batch mounted with `.transactional()` does not compile.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` acknowledges the batch and `Err(outcomes)` settles it row by row.

## Example

One claim takes up to fifty emails, and an address that cannot receive mail drops its own row alone.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_subscriptions_batches.rs#batches"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_subscriptions_batches.rs#batches"
```

</TabItem>
</Tabs>
