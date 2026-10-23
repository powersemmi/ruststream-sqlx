---
sidebar_position: 7
title: Statements
description: "custom(fetch, ack, retry, discard, recover) replaces a record's default statement with the service's own, for a status of its own or a saga."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Statements

`#[outbox(custom(fetch, ack, retry, discard, recover))]` replaces a record's default statement with the service's own, for a status of its own or a saga.

Each event is a trait of its own, and the registry runs the record's impl in place of the default:

| Event | Trait | Default |
| --- | --- | --- |
| a delivery takes the record into work | [`Fetch`](api:ruststream_sqlx::outbox::Fetch) | reads the record while it is unprocessed |
| the handler acknowledged | [`Ack`](api:ruststream_sqlx::outbox::Ack) | marks the record processed |
| the handler asked again | [`Retry`](api:ruststream_sqlx::outbox::Retry) | runs nothing |
| the handler dropped the message | [`Discard`](api:ruststream_sqlx::outbox::Discard) | marks the record processed |
| the startup republish | [`Recover`](api:ruststream_sqlx::outbox::Recover) | reads the unprocessed records of one name |

`Fetch` returns `Option<Self>`: `None` means the record is processed or taken already, and the delivery is acknowledged without its handler.
So a fetch of the service's own decides what "taken" means, a stamp, a status column or a lease, and the other events release or finish it.
[`Publish`](api:ruststream_sqlx::outbox::Publish) has no default: it is always the service's own and is never listed.
A default `Retry` takes no connection; a `Retry` of the service's own takes one, as the other events do.

By hand, the record's `OutboxSpec` names each event with `.own::<own::Fetch>()` and the others, and its `type Table` lists the same markers of [`outbox::spec::own`](api:ruststream_sqlx::outbox::spec::own).
A record type that names an event and lacks its trait does not register, and the error names the missing trait.

## Example

`OrderOutbox` stamps `taken_at` when a consumer takes the record, and its retry releases the record and counts the attempt; the acknowledgement keeps the default mark.
The repository's [`examples/outbox`](https://github.com/powersemmi/ruststream-sqlx/tree/main/examples/outbox) takes its records with such a fetch on Postgres.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_statements.rs#statements"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_statements.rs#statements"
```

</TabItem>
</Tabs>
