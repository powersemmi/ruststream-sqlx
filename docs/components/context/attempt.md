---
sidebar_position: 1
title: Attempt
description: "keys::Attempt reads the delivery's attempt, 1 for the first: the attempt column as it stood before the claim, None where the table has none."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Attempt

[`keys::Attempt`](api:ruststream_sqlx::keys::Attempt) reads the delivery's attempt, 1 for the first: the attempt column as it stood before the claim, `None` where the table has none.

A `#[field(attempt)]` column counts the deliveries of a row, so it starts at 1, usually as the column's default.
A retry counts the attempt in the row lock form; the claim counts it in the lease and advisory lock forms, so a crash with the row in work spends one too.
Either way a delivery reports the attempt as it stood before its claim, and the first delivery reads 1 in every form.
The field is an `i16`, `i32`, `i64`, `u16`, `u32` or `u64` ([`AttemptColumn`](api:ruststream_sqlx::AttemptColumn)), and a negative value reads as 0.
A column of another type converts into the field through `#[sqlx(try_from = "..")]`, by hand through `.attempt_from::<T>(..)`.
A row that does not decode still reports its attempt, read alone, so a [retry cap](../settlement/dead-letter.md) counts it.
The backoff is the handler's: it reads the attempt and answers with [`retry_after`](../settlement/retry-after.md) of the delay it wants.
By hand, `.attempt(..)` names the column, `Attempt` goes into the description's type, and the row reads its attempt through [`AttemptRow`](api:ruststream_sqlx::AttemptRow).

## Example

A supplier's API answers from the third attempt on, and the handler waits twice as long before each new one: one second, then two.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_context_attempt.rs#attempt"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_context_attempt.rs#attempt"
```

</TabItem>
</Tabs>
