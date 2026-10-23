---
sidebar_position: 5
title: Events
description: "custom(..) hands one event of a table to the service's own SQL, Claim to Extend, and every other event keeps the statement the dialect builds."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Events

`custom(..)` hands one event of a table to the service's own SQL, `Claim` to `Extend`, and every other event keeps the statement the dialect builds.

An event the table names, in `#[inbox(custom(..))]` on the derive or with `.own::<own::X>()` by hand, runs the service's own code in place of the dialect's statement.
Each event is a trait the struct implements for its database:

| Event | Trait | What it does |
|---|---|---|
| `claim` | [`Claim`](api:ruststream_sqlx::Claim) | selects and takes the ids of the next rows |
| `fetch` | [`Fetch`](../headers/fetch.md) | reads the claimed rows, such as over a join |
| `ack` | [`Ack`](api:ruststream_sqlx::Ack) | finishes a row its handler acknowledged |
| `retry` | [`Retry`](api:ruststream_sqlx::Retry) | returns a row to the queue at once |
| `retry_after` | [`RetryAfter`](api:ruststream_sqlx::RetryAfter) | hides a row until a time |
| `discard` | [`Discard`](api:ruststream_sqlx::Discard) | finishes a row its handler dropped |
| `dead_letter` | [`DeadLetter`](api:ruststream_sqlx::DeadLetter) | moves a row past its cap |
| `extend` | [`Extend`](api:ruststream_sqlx::Extend) | extends the leases in work |
| `lock`, `unlock` | [`Lock`, `Unlock`](../forms/lock.md) | take and release the key of a row |

A named event without its impl does not compile, and the error names the trait.
The table and the broker stay as they were, and every event the table does not name keeps the dialect's statement.

A settlement of the service's own runs in the transaction the crate commits, so it settles the row as the built-in one does.
An acknowledgement takes the row out of what the claim selects: a row it leaves claimable is delivered again.
The form limits the events: the advisory lock form selects its candidates by key itself and takes no `claim`, `extend` belongs to the lease form, and `lock` and `unlock` come together in the advisory lock form.

## Example

A sent email stays in its table for an audit: the acknowledgement moves it to the `sent` group instead of deleting it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_tables_events.rs#events"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_tables_events.rs#events"
```

</TabItem>
</Tabs>
