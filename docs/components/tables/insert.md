---
sidebar_position: 4
title: Insert
description: "Insert writes a task with the derive's generated insert, inside the service's own transaction, so the task commits with the data it serves."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Insert

[`Insert`](api:ruststream_sqlx::Insert) writes a task with the derive's generated insert, inside the service's own transaction, so the task commits with the data it serves.

The derive implements `Insert` for the connection of each enabled built-in driver: `PgConnection`, `MySqlConnection` and `SqliteConnection`.
`insert(&self, conn)` writes every field of the struct but the `generated` ones: the roles, then the data columns, in the order the description lists them.
A delayed task is a row whose `retry_after` lies ahead.
The statement is built at compile time, and a call binds the fields and runs it on the connection it is given: a transaction of the service's own, or a handler's [transaction](../transactions/index.md).
A [headers struct](../headers/inbox-headers.md) gets the insert, and the message that flattens it gets none.

A table described by hand writes its insert over the text [`dialect::insert`](api:ruststream_sqlx_dialect::insert) renders from its description, in a constant of type `Sql<N>`.
The text names every column the database does not fill, in the description's order, and the impl binds them in that order.
A capacity `N` the text outgrows is a build error that names the size to raise it to.

A row written this way wakes no subscription in the process: the subscription finds it on its next claim, at most a [poll interval](../subscriptions/poll-interval.md) later.
A publish through a [route](../publishing/route.md) or a [repository](../publishing/index.md) [wakes](../wake/index.md) the subscriptions of its table at once.

## Example

An order and the task that sends its receipt go into one transaction: both commit, or neither does.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_tables_insert.rs#insert"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_tables_insert.rs#insert"
```

</TabItem>
</Tabs>
