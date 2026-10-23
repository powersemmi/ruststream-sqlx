---
sidebar_position: 5
description: "Keep jobs in order: a FIFO group that one instance at a time works on, and a partition key that keeps each account in order across parallel workers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Ordering: FIFO groups and partition keys

This guide keeps jobs in order: all the jobs of a group, or the jobs of each key while other keys run in parallel.

`SKIP LOCKED` makes claims fast because they pass over rows in work.
It also lets the next row go out while an earlier one is still in work or waiting for its retry.
A queue whose jobs must apply in order says so in its table.

The example is an accounts service.
It sends contact changes to a CRM, which must receive them in the order they happened, and applies ledger postings, which must keep their order within an account.

## The tables

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/ordering.rs#schema"
```

The index on `crm_jobs` follows the claim order, the group first.
A claim of a FIFO group reads the group's unfinished rows, and the index keeps that read to them ([Running in production](production.md)).

## A FIFO group

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#fifo"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#fifo"
```

</TabItem>
</Tabs>

`#[field(group, fifo = true)]` makes each group of `target` a [FIFO group](../components/subscriptions/fifo.md).
A FIFO group has at most one row in work, however many workers, subscriptions and processes read it.
Its first unfinished row in claim order is its head, and a claim takes the head alone.
The rows behind it wait while the head is in work, under a lease of another instance included.
`retry()` keeps the change at the head, so it goes again before the ones behind it.
`retry_after(d)` would move it behind the rows due before it.

A head that keeps failing holds its whole group.
The mount caps it:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#app"
```

</TabItem>
</Tabs>

After three refusals the change moves into the `crm.parked` group, and the next change of `crm` becomes the head.
A parked change waits there for a person; the changes behind it have gone out without it, and that is the price of freeing the group.

Groups run side by side, each through a subscription of its own, so a group suits a few streams known in advance: one per downstream system, one per tenant.
`workers(n)` gives a FIFO subscription no parallelism, and one worker serves it.

## A key per account

A ledger has many accounts, too many for a subscription each.
The order that matters is within one account, and accounts may run in parallel.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#keyed"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#keyed"
```

</TabItem>
</Tabs>

`#[field(partition_key)]` gives each row the account as its key.
The app mounts `post` with [`workers_by_key(4)`](../components/subscriptions/workers.md): four workers, and the postings of one account on one of them, in the order they were claimed.
The subscription keeps one claim in flight on such a table, so the claims do not overtake each other.
That order holds inside one subscription.
Postings of one account that must stay in order across several instances go into a FIFO group, or into the [advisory lock form](../components/forms/advisory.md) with a key on the account, which keeps the rows of one key in work one at a time across every process on a server.

## Testing

The tests in the example file run the service on SQLite in memory.
The first publishes a change the CRM refuses and one it takes: the first goes three times and is parked, then the second goes out.
The second writes the head of `crm` as leased by another instance and finds that the change behind it waits.
The third writes postings of two accounts, interleaved, and finds each account's postings applied in order.
The rows of the last two tests wake no subscription, so they run on the real clock with [`TestApp::start_live`](../components/testing/live.md) past the poll interval.
