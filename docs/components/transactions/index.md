---
title: Transactional
description: ".transactional() lends the handler its delivery's transaction: acknowledgement commits the handler's writes with the row's settlement, or neither."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transactional

[`.transactional()`](api:ruststream_sqlx::InboxSettings::transactional) lends the handler its delivery's transaction: acknowledgement commits the handler's writes with the row's settlement, or neither.

The handler takes the transaction as its first `Ctx` parameter, `Ctx(mut tx): Ctx<keys::Tx<DB>>`, and runs its statements through `&mut *tx`.
A handler that takes [`keys::Tx`](api:ruststream_sqlx::keys::Tx) on a subscription without the step does not compile, and the error names the step.
Acknowledgement commits the handler's writes together with the row's settlement.
Every other outcome rolls them back first, then settles the row as it would without the step; a retry still counts the attempt.
A handler that panics while it holds `tx` commits none of its writes, even where its panic policy acknowledges the delivery.
A delivery dropped unsettled closes its connection, and the server rolls its transaction back.
A task the handler writes through `tx` into a queue table, with [`Insert`](../tables/insert.md) or its own SQL, commits with the acknowledgement too.
A write through [`keys::Pool`](../context/pool.md) commits on its own, whatever the outcome, and so does a publish through the broker.
What the transaction is depends on the [form](../forms/index.md):

| Form | The handler's transaction |
|---|---|
| row lock | the claim's transaction, with a savepoint right after the claim that a settlement other than acknowledgement rolls back to |
| lease | a transaction of the delivery's own, opened once the claim committed; a lost lease rolls it back and the settlement fails with [`LeaseLost`](api:ruststream_sqlx::SqlxBrokerError::LeaseLost) |
| advisory lock | a transaction on the session that holds the row's key, ended before the key's release |

Each of these transactions opens at the table's [isolation level or SQLite mode](./isolation.md).
On SQLite a delivery's transaction takes the database's write lock at its first write and holds it until the delivery settles, keeping every other writer out, claims included.
Each delivery in work holds a connection for its transaction, so a handler that takes a second one, through `keys::Pool` or a publish, needs a pool larger than its `workers(n)` ([Connections](../broker/connections.md)).
Transactional mode serves single deliveries: a batch handler mounted with `.transactional()` does not compile.
The transaction goes back to the delivery when the handler's [`Tx`](api:ruststream_sqlx::Tx) drops; a settlement that finds it still out, in a task the handler moved it into, fails with [`TransactionHeld`](api:ruststream_sqlx::SqlxBrokerError::TransactionHeld).

## Example

A signup opens its account and queues its welcome email through the delivery's transaction; when the CRM times out, the retry discards both, and the next attempt commits them.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_transactions.rs#transactional"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_transactions.rs#transactional"
```

</TabItem>
</Tabs>
