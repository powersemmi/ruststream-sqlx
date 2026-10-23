---
title: Context
description: "A handler reads the inbox's context through Ctx<keys::..>: the transaction, the pool and the attempt, in that order, leaving out what it does not need."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Context

A handler reads the inbox's context through `Ctx<keys::..>`: the transaction, the pool and the attempt, in that order, leaving out what it does not need.

The [`keys`](api:ruststream_sqlx::keys) module holds three keys, each read as a handler parameter:

| Key | Value | What it is |
|---|---|---|
| [`keys::Tx<DB>`](api:ruststream_sqlx::keys::Tx) | [`Tx<DB>`](api:ruststream_sqlx::Tx) | the delivery's transaction, in [transactional mode](../transactions/index.md) |
| [`keys::Pool<DB>`](./pool.md) | `Pool<DB>` | the service's pool, whose writes commit on their own |
| [`keys::Attempt`](./attempt.md) | `Option<u64>` | the delivery's attempt, 1 for the first, `None` where the table counts none |

A handler's first `Ctx` names the context the delivery builds for it, and decides which keys compile after it.
After `keys::Tx` come the other two, after `keys::Pool` only `keys::Attempt`, and nothing after `keys::Attempt`.
So the order `Tx`, `Pool`, `Attempt` fits every handler, with the keys it does not need left out, and a key in another place does not compile.
A key's type names its database, as in `keys::Pool<Sqlite>`.
A delivery builds the context only for a handler that reads it: a pool handle and the attempt are copied, and the transaction is lent.
The framework's extractors stand beside these keys: [`Headers<T>`](api:ruststream::runtime::Headers), `State<T>`, and a `&mut Context` parameter whose `headers()` read the delivery's headers.
In the manual form the body resolves a key with `Ctx::<keys::Attempt>::from_context(ctx)`, its `Handle` impl naming the context: `keys::InboxContext` for the attempt, `keys::PoolContext<DB>` after the pool, `keys::TxContext<DB>` after the transaction.

## Example

A transactional signup handler logs every attempt through the pool and opens the account through the delivery's transaction; a retry keeps the log and discards the account.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_context.rs#context"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_context.rs#context"
```

</TabItem>
</Tabs>
