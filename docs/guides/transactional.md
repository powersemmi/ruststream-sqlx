---
sidebar_position: 6
description: "Commit a handler's writes together with its job's settlement in one transaction, chain a follow-up job in it, and keep effects outside the database idempotent."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Exactly once inside the database: transactional handlers

This guide commits a handler's writes together with its job's settlement, so a job whose effects are rows in the same database takes effect exactly once.

A queue delivers each job at least once.
A handler that writes its result and then crashes before the job settles runs again, and writes the result twice.
When the result is rows in the database that holds the queue, one transaction removes that duplicate: the writes and the settlement commit together, or neither does.

The example is a refunds service.
A refund writes its record and a receipt job for the mailer, and calls the card processor.

## The tables

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/transactional.rs#schema"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/transactional.rs#tables"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/transactional.rs#tables"
```

</TabItem>
</Tabs>

`refund_jobs` is the queue this service reads.
`receipt_jobs` is the mailer's queue: this service only writes into it, with the derive's [`insert`](../components/tables/insert.md); in the manual form, with a statement of its own.

## The handler

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/transactional.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/transactional.rs#handler"
```

</TabItem>
</Tabs>

[`.transactional()`](../components/transactions/index.md) at the mount lends the handler its delivery's transaction.
The handler takes it as its first `Ctx` parameter, `Ctx<keys::Tx<Sqlite>>`, and writes through `&mut *tx`.
A handler that takes `keys::Tx` on a subscription without the step does not compile.

The acknowledgement commits the record, the receipt job and the refund row's settlement in one transaction.
Every other outcome rolls the writes back first, then settles the row; a retry still counts the attempt.
A panic commits nothing.
A second refund of the same order fails on the primary key of `refunds` and is dropped, so a duplicate that reaches the queue changes nothing.

The receipt job is a chain: the mailer sees it only once the refund committed, and never for a refund that rolled back.
A write through [`Ctx<keys::Pool>`](../components/context/pool.md) commits on its own, whatever the outcome, and so does a publish through the broker; neither joins the transaction.

## What stays outside

The card processor is outside the database, so no transaction covers it.
A retry after the processor refunded the card calls it again, so the call carries an idempotency key, the order here, and the processor refunds once.
Every effect outside the database stays at least once in the same way: an email, an HTTP call, a publish to another broker.
[Relaying events to another broker](relay.md) and [the outbox](outbox.md) deal with publishes.

On SQLite a delivery's transaction takes the database's write lock at its first write and holds it until the delivery settles, keeping every other writer out, claims included.
Here the processor's call runs after the two inserts, so the database stays locked for other writers while it runs.
A server locks only the rows the transaction wrote.
A handler that publishes a message the [outbox](outbox.md) tracks publishes it before its first write through `tx` on SQLite: the record is written on a connection of its own, which waits for the delivery's write lock otherwise.

## Isolation

Each delivery's transaction opens at the table's [isolation level](../components/transactions/isolation.md), `#[inbox(isolation = read_committed)]` on a server, or at its SQLite mode, `#[inbox(mode = immediate)]`.
A table that declares neither takes the database's default, and READ COMMITTED on MySQL and MariaDB.
Each delivery in work holds a connection for its transaction, so a handler that takes a second one, through `keys::Pool` or a publish, needs a pool larger than its `workers(n)`.
Transactional mode serves single deliveries; a batch handler mounted with it does not compile.
On MySQL and MariaDB, a handler that cuts a statement through `tx` short, as a `select!` around a query does, can confuse the settlement that follows, a bug of sqlx-mysql 0.9.0: let each statement finish.

## Testing

The tests in the example file run the service with [`TestApp::start`](../components/testing/index.md) on SQLite in memory and read the tables after each publish.
A refund commits one record and one receipt.
A refund whose first call to the processor times out is retried, and the second attempt leaves one record and one receipt, not two.
The same refund published twice is dropped the second time.
