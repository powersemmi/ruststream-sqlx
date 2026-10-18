---
sidebar_position: 5
title: wrap
description: "Outbox::wrap gives a publisher outside the publish pipeline the same tracking, for an HTTP endpoint or an after_startup hook."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# wrap

[`Outbox::wrap`](api:ruststream_sqlx::outbox::Outbox::wrap) gives a publisher outside the publish pipeline the same tracking, for an HTTP endpoint or an `after_startup` hook.

The publish pipeline reaches what handlers publish.
A publisher from `RunningApp::publisher`, or the one an `after_startup` hook receives, runs outside it, so the publish middleware never sees its messages.
`wrap` takes such a publisher and returns a [`TrackedPublisher`](api:ruststream_sqlx::outbox::TrackedPublisher), a publisher like any other: a message under a registered name is recorded first, then sent with the record's id.
A message under any other name goes straight to the wrapped publisher.

A publish through it fails with [`TrackedPublishError`](./outbox-error.md): `Outbox` when the record was not written and the message was not sent, `Publish` when the wrapped publisher failed after the record was written.
In the second case the record stays unprocessed, and the next startup sends it.
The wrapper borrows the payload for the record in whatever form the publisher takes it, so the message is not copied.

## Example

The service starts its app by hand, takes the live publisher of a bound `Publish` policy, as an HTTP server would, and publishes each order through the wrapper.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_wrap.rs#wrap"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_wrap.rs#wrap"
```

</TabItem>
</Tabs>
