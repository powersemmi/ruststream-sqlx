---
sidebar_position: 10
title: OutboxError
description: "OutboxError names what the outbox could not do: record a publish, read the unprocessed records, or republish one, and a publish with no pool yet."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# OutboxError

[`OutboxError`](api:ruststream_sqlx::outbox::OutboxError) names what the outbox could not do: record a publish, read the unprocessed records, or republish one, and a publish with no pool yet.

Each variant names the registered name, and the record type where one is involved:

| Variant | When | What follows |
| --- | --- | --- |
| `NoPool` | a message under a registered name was published before the registry had a pool | the message is not sent |
| `Record` | the record's `Publish` failed; its source is the database's error | the message is not sent |
| `Recover` | the startup republish could not read a name's unprocessed records | startup fails |
| `Republish` | the startup republish could not send a record; it names the record's id | startup fails |

A publish through the publish middleware fails with `NoPool` or `Record`, as the error of the publisher a handler holds.
A publish through [`wrap`](./wrap.md) fails with [`TrackedPublishError`](api:ruststream_sqlx::outbox::TrackedPublishError): `Outbox` carries the `OutboxError`, and `Publish` the wrapped publisher's own error after the record was written, which leaves the record for the next startup.
The publish builder wraps either in its own `PublishError::Publish`.
[`PoolAlreadySet`](api:ruststream_sqlx::outbox::PoolAlreadySet) is what a second [`set_pool`](./set-pool.md) returns.

## Example

An HTTP endpoint answers each order by what its publish through `wrap` returned: `202` once the record is written, `503` when it is not, so the client sends the order again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_errors.rs#errors"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_errors.rs#errors"
```

</TabItem>
</Tabs>
