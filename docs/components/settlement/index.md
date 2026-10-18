---
title: Settlement
description: "A handler's outcome is a statement on its row: ack deletes it or sets processed_at, retry returns it at once with one more attempt, drop finishes it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Settlement

A handler's outcome is a statement on its row: `ack` deletes it or sets `processed_at`, `retry` returns it at once with one more attempt, `drop` finishes it.

What a handler answers decides the row's fate:

| Outcome | The row |
|---|---|
| [`ack()`](api:ruststream::runtime::HandlerOutcome::ack) | is deleted, or marked with the current time where the table has a `processed_at` column |
| [`retry()`](api:ruststream::runtime::HandlerOutcome::retry) | returns to the queue at once, and its next delivery reads one more [attempt](../context/attempt.md) |
| [`retry_after(d)`](./retry-after.md) | stays out of the queue until its `retry_after` comes |
| [`drop()`](api:ruststream::runtime::HandlerOutcome::drop) | is finished as an acknowledgement finishes it |

A row marked with `processed_at` stays in the table, out of every claim, for an audit or a later cleanup.
The statement runs where the [form](../forms/index.md) holds the row: in the claim's transaction, by the delivery's lease, or on the session that holds its key.
It takes effect together with the end of that hold, so a row and its outcome never part.
A settlement whose statement fails reports itself, and the row returns as it would after a crash.
`max_attempts(n)` with [`dead_letter(..)`](./dead-letter.md) caps the retries and moves a spent row aside.
A table may run its own SQL for an outcome, an acknowledgement that moves the row to a `sent` group for one ([Events](../tables/events.md)); it runs in the transaction the crate commits, as the built-in statement does.
An acknowledgement takes the row out of what the claim selects, so a statement of one's own that leaves the row claimable delivers it again.

## Example

The mail server greylists a first delivery, so the handler retries it at once; an address that cannot receive mail is dropped, and both end marked in `processed_at`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_settlement_outcomes.rs#outcomes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_settlement_outcomes.rs#outcomes"
```

</TabItem>
</Tabs>
