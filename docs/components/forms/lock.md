---
sidebar_position: 3
title: Lock
description: "custom(lock, unlock) hands the lock and the release of each key to the service's SQL, so a queue shares a lock another system already takes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Lock

`custom(lock, unlock)` hands the lock and the release of each key to the service's SQL, so a queue shares a lock another system already takes.

The struct implements [`Lock`](api:ruststream_sqlx::Lock) and [`Unlock`](api:ruststream_sqlx::Unlock) for its database.
`lock` tries the key without waiting and answers whether it took it.
`unlock` answers whether the session held the key; an answer of `false`, or an error, closes the connection, which ends the session and its locks.
The two come together, and only in the [advisory lock form](./advisory.md); a description that breaks either rule does not compile.
The dialect still selects the candidates and takes each row, so the claim keeps its order and counts the attempt as before.
The process keeps no registry of keys for such a table, on SQLite too: the service's lock decides.
A claim reads the first due rows alone and takes nothing while their keys are held, so the next row waits up to a poll interval.
On Postgres a lock of the service's own is the place for `pg_try_advisory_lock` on a key another system computes; on SQLite it may be a table of the keys in work, as here.
By hand, the chain adds `.own::<own::Lock>()` and `.own::<own::Unlock>()`, and the description's type lists `own::Lock` and `own::Unlock` beside `Advisory`.
The other events of a table are on [Events](../tables/events.md).

## Example

Billing holds an account's key in `held_keys` while it changes the account, and a payout of that account waits until billing releases the key.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_forms_lock.rs#lock"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_forms_lock.rs#lock"
```

</TabItem>
</Tabs>
