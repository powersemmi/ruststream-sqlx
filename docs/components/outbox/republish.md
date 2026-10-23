---
sidebar_position: 4
title: republish
description: "b.after_startup(policy, tracking.republish()) sends every unprocessed record again once the subscriptions are open; republish_names picks names."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# republish

`b.after_startup(policy, tracking.republish())` sends every unprocessed record again once the subscriptions are open; [`republish_names`](api:ruststream_sqlx::outbox::Outbox::republish_names) picks names.

[`Outbox::republish`](api:ruststream_sqlx::outbox::Outbox::republish) returns a [`Republishing`](api:ruststream_sqlx::outbox::Republishing), the hook `after_startup` runs with the broker's live publisher of the policy it names.
For each registered name it reads the unprocessed records through the record's `Recover` event and publishes each one with its id, its payload and the headers its record kept.
A consumer then takes each copy as any tracked delivery, so a copy whose record a consumer processed in the meantime is acknowledged without its handler.
The hook runs once the subscriptions are open, so a consumer in the same service receives what its own service sent before a restart.

[`republish_names`](api:ruststream_sqlx::outbox::Outbox::republish_names) republishes only the names it lists, for a service whose names live on several brokers: each broker's `after_startup` republishes the names that broker carries.
A name it lists that the registry does not hold panics when the app is built.

A record the republish cannot read fails startup with [`OutboxError::Recover`](./outbox-error.md), and one it cannot send with `OutboxError::Republish`.
A service whose records stay with a [selection of its own](./statements.md) writes `Recover` itself.
The republish allocates its body once per startup.

## Example

The broker's `after_startup` republishes the unprocessed records of `orders` through its `Publish` policy; `fulfil` consumes them and marks each record.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_republish.rs#republish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_republish.rs#republish"
```

</TabItem>
</Tabs>
