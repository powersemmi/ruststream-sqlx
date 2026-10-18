---
title: Payload
description: "A table with a payload column is in payload mode: the claim lends the column's bytes to the codec, and the handler takes the decoded message."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Payload

A table with a payload column is in payload mode: the claim lends the column's bytes to the codec, and the handler takes the decoded message.

`#[field(payload)]` marks the column, by hand `.payload(..)` with [`PayloadRow`](api:ruststream_sqlx::PayloadRow), which names the column's type and lends its bytes.
A claim reads the row, and the delivery lends those bytes to the codec as they are, with no copy.
The codec is the mount's: JSON by default, or any codec of the core the router or the scope names.
The bytes are the service's own: binary and empty payloads reach the handler byte for byte, and a publish through a [route](../publishing/route.md) writes them as the codec encoded them.

A row whose bytes do not decode into the handler's type is settled by the subscription's [decode policy](./decoding.md) before the handler runs.
A handler that takes a row instead of a message mounts only on a table in [row mode](./rows.md).

## Example

A signup queue in payload mode: the handler takes the decoded `Signup`, and the acknowledged row keeps the bytes it was decoded from.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_messages_payload.rs#payload"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_messages_payload.rs#payload"
```

</TabItem>
</Tabs>
