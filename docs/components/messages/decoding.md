---
sidebar_position: 2
title: Decoding
description: "A claimed row that does not decode is settled by the subscription's decode policy before the handler runs, and still spends its attempt."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Decoding

A claimed row that does not decode is settled by the subscription's decode policy before the handler runs, and still spends its attempt.

A row decodes in two steps: the driver reads its columns into the struct, and in payload mode the codec reads the payload into the handler's type.
A failure at either step reaches the subscription's `on_failure(decode = ..)` policy, set with [`FailurePolicies::with_decode`](api:ruststream::runtime::FailurePolicies::with_decode).
The policy settles the row, and the subscription goes on with the next one.
The default policy drops the row: it is deleted, or marked where the table has `processed_at`.
A policy that retries it keeps the row in the table, after a delay with `FailurePolicy::RetryAfter`.

Such a row still reports its [attempt](../context/attempt.md): the claim reads the `attempt` column alone, so `max_attempts(n)` spends it like any other row, and a retrying policy keeps it only up to the cap.
When the `attempt` column itself does not decode, the row reports no attempt.
A claimed id that the service's own [fetch](../headers/fetch.md) returned no row for settles the same way.
A row whose id does not decode fails the claim, and every claim that reaches it fails the same way until the row is fixed; the error names the subscription and the table.

## Example

A row that does not decode stays in the table and comes back every ten minutes, until a release that reads it ships.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_messages_decoding.rs#policy"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_messages_decoding.rs#policy"
```

</TabItem>
</Tabs>
