---
sidebar_position: 4
title: Fetch
description: "custom(fetch) reads the claimed rows with the service's own SELECT, such as a join; a claimed id it returns no row for settles by the decode policy."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Fetch

`custom(fetch)` reads the claimed rows with the service's own `SELECT`, such as a join; a claimed id it returns no row for settles by the decode policy.

A message whose data lives in other tables lists `fetch` in `#[inbox(custom(..))]` and implements [`Fetch`](api:ruststream_sqlx::Fetch) for its database; by hand the chain adds `.own::<own::Fetch>()`, with `own::Fetch` in its type.
The fetch takes the claimed ids and returns the rows it found.
Its `SELECT` returns the headers struct's columns under their names, `locked_until` included in the lease form, beside the message's own.
The crate matches the rows to the claimed ids by the headers struct's `id` field, and an empty claim does not call the fetch.

The fetch runs on the claim's connection: in the claim's transaction in the row lock form, and once per row, with that row's id alone, in the advisory lock form.
A subscription to such a message does not check the message's own columns at startup, because the fetch reads them from wherever they live.

A claimed id that the fetch returned no row for, such as a job whose order the join does not find, settles by the subscription's [decode policy](../messages/decoding.md) before the handler runs.
The default policy drops the row, and the log names the id.
A batch lends its handler the rows the fetch found, and the policy settles the others.

## Example

Each claimed job is read joined to its order; SQLite takes the claimed ids as a JSON array, read back by `json_each`.
A job whose order is gone joins no row, and the default policy drops it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_headers_fetch.rs#fetch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_headers_fetch.rs#fetch"
```

</TabItem>
</Tabs>
