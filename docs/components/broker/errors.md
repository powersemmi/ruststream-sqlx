---
sidebar_position: 5
title: Errors
description: "SqlxBrokerError is the inbox's one error type: connecting, statements, the schema, declarations, leases, transactions, publishes, and a closed broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`SqlxBrokerError`](api:ruststream_sqlx::SqlxBrokerError) is the inbox's one error type: connecting, statements, the schema, declarations, leases, transactions, publishes, and a closed broker.

Every variant about a subscription names the subscription, the table and the row type, so one line of a log says where to look.
The database's own error is its `source`.

| Variant | When |
|---|---|
| `Connect` | the connect could not take a connection from the pool |
| `Url` | the connect could not read the URL of a broker made with `from_url` |
| `Backend` | an `AnyPool` reached a database whose dialect's feature is off |
| `Schema` | a statement failed to prepare at [startup](./startup.md): the table lacks a table or a column the description names |
| `Dialect` | the dialect could not build a statement for the table |
| `ServerTooOld` | the server is older than the statements of the table's form need |
| `Declaration` | the mount declared what the table cannot carry |
| `ConnectionLimit` | a subscription on [dedicated threads](../subscriptions/on-threads.md) would pass the broker's connection limit |
| `AlreadySubscribed` | a second subscription to one queue on the same broker |
| `Sqlx` | a statement failed while the subscription ran |
| `BatchRolledBack` | an earlier settlement of a [batch](../subscriptions/batches.md) failed, so the batch rolls back |
| `LeaseLost` | in the [lease form](../forms/lease.md), the lease ran out and another claim took the row, so the settlement took no effect |
| `TransactionHeld` | in [transactional mode](../transactions/index.md), the handler kept its transaction past its return |
| `Listen` | [`listen_notify`](../wake/listen-notify.md) could not listen on the table's channel |
| `Publish` | the service's [`Publish`](../publishing/index.md) failed |
| `Header` | the publish carries a header the table cannot [hold](../headers/index.md) byte for byte |
| `NoRoute` | no [route](../publishing/route.md) leads the name to a table |
| `Closed` | the broker was shut down, and a handle outlived it |

A service meets the error in three places.
A failed startup carries it as the source of the runtime's error, and the service exits before it reads a row.
A publish from a handler returns it inside the core's `PublishError`, and the handler decides how its delivery settles.
A settlement that fails, such as `LeaseLost`, goes to the log with the row's id.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

An invoice the database refuses to write is logged with its cause, and the payment waits a minute before it is billed again.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
