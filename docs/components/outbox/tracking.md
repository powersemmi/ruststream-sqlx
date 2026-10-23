---
sidebar_position: 3
title: Tracking
description: "Outbox::layer and publish_layer track messages: the publish records then sends, and the delivery's outcome settles its record by its id."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Tracking

[`Outbox::layer`](api:ruststream_sqlx::outbox::Outbox::layer) and [`publish_layer`](api:ruststream_sqlx::outbox::Outbox::publish_layer) track messages: the publish records then sends, and the delivery's outcome settles its record by its id.

`publish_layer` returns a [`TrackingPublishLayer`](api:ruststream_sqlx::outbox::TrackingPublishLayer), mounted with `RustStream::publish_layer`.
It reaches everything a handler publishes, replies and `Out` slots alike.
A message under a registered name is recorded through the record's `Publish`, then sent with the record's id in [`OUTBOX_ID_HEADER`](api:ruststream_sqlx::outbox::OUTBOX_ID_HEADER).
A record that fails is not sent, and the publish fails with [`OutboxError::Record`](./outbox-error.md).

`layer` returns a [`TrackingLayer`](api:ruststream_sqlx::outbox::TrackingLayer), mounted with `RustStream::layer`.
A delivery under a registered name that carries an id is taken into work by the record's fetch, on a connection the layer returns before the handler runs, so the handler can publish tracked messages of its own.
The handler's outcome then settles the record:

| Outcome | Event | Default statement |
| --- | --- | --- |
| acknowledged | [`outbox::Ack`](api:ruststream_sqlx::outbox::Ack) | marks the record processed |
| asked again (`retry`, `retry_after`) | [`outbox::Retry`](api:ruststream_sqlx::outbox::Retry) | none: the record waits for the next startup |
| dropped | [`outbox::Discard`](api:ruststream_sqlx::outbox::Discard) | marks the record processed |

A record without `processed_at` is deleted where the default marks it.
A record the fetch finds processed already acknowledges the delivery without running the handler, which is how a copy sent twice reaches its consumer once.
A fetch that fails retries the delivery without the handler.
A settlement that fails logs a warning and keeps the handler's outcome, and the record stays for the next startup.
A delivery without the header, with a header that is not an id, or under a name the registry does not hold runs untracked.

Both layers pass a message whose name is not registered after one comparison per registered name.

## Example

`fulfil` acknowledges an order in stock, whose record is marked, and asks again for one out of stock, whose record waits; the subscription takes one attempt, so the broker does not deliver that order again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_tracking.rs#tracking"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_tracking.rs#tracking"
```

</TabItem>
</Tabs>
