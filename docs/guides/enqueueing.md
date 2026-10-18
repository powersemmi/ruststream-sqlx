---
sidebar_position: 2
description: "Write a job in the same transaction as the data it serves, schedule one for later, claim urgent jobs first and keep finished jobs for an audit."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Enqueueing jobs

This guide writes jobs into a queue table: in the same transaction as the data they serve, for a later time, and by priority.

A queue in the service's database has one advantage over a broker of its own: a job commits with the data it serves.
An order that rolls back leaves no confirmation email behind, and an email never goes out for an order that was not saved.

The example is a shop.
A checkout subscription places each order, and an email subscription sends its confirmation at once and a request for a review three days later.

## The table

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/enqueueing.rs#schema"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/enqueueing.rs#table"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/enqueueing.rs#table"
```

</TabItem>
</Tabs>

Each column turns on one behaviour of the queue ([Roles](../components/tables/roles.md)).
`retry_after` holds the time a row becomes due: a claim takes only rows whose time has come.
`priority` orders the claims: a smaller value goes first, then the earlier `retry_after`, then the smaller id.
`processed_at` keeps a finished row: an acknowledgement writes the time into it instead of deleting the row.
`generated` marks the columns the database fills, so the insert leaves them out.
In the manual form, the service writes that insert itself, as an [`Insert`](../components/tables/insert.md) impl.

## A job in the order's transaction

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/enqueueing.rs#place_order"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/enqueueing.rs#place_order"
```

</TabItem>
</Tabs>

`place_order` opens one sqlx transaction and writes three rows in it: the order, the confirmation and the reminder.
The derive's [`insert`](../components/tables/insert.md) takes any connection of the table's database, so `&mut *tx` puts the jobs in the order's transaction.
They commit together, or roll back together: an order whose insert fails leaves no email in the table.
An HTTP endpoint that places orders calls the same function with the service's pool.

The confirmation is due now and urgent.
The reminder is due in three days, and nothing claims it until then.
A job that recurs is the same move: its handler inserts the next row with a later `retry_after`.
A job that must exist once is the service's unique index and an `INSERT .. ON CONFLICT DO NOTHING` in its own statement.

## The subscription

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/enqueueing.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/enqueueing.rs#service"
```

</TabItem>
</Tabs>

`checkout` takes the [pool](../components/context/pool.md) from its context and places the order; an order that fails is dropped with a warning.
Its own transaction is separate from the claim of its checkout row: a handler whose writes must commit with its row's settlement runs in [transactional mode](transactional.md).
`send` reads every row of `email_jobs` in claim order.
Its acknowledgement marks the row with `processed_at`, so the sent emails stay in the table for an audit, and a claim passes over them.
A table without that column loses the row on acknowledgement.

## When a job is seen

A publish through the broker [wakes](../components/wake/index.md) the subscriptions of its table in the same process at once: a [route](../components/publishing/route.md), a [`Repository`](../components/publishing/index.md), a reply or an `Out` slot.
The checkout published by the front end wakes `checkout` that way.
A row the service writes with its own SQL, as `place_order` does, wakes nobody.
The subscription finds it after its [poll interval](../components/subscriptions/poll-interval.md), one second unless set.
The broker's publish does not join the service's transaction, so a job that must commit with the order goes in through `insert` and waits that interval.
On Postgres, a [notification](../components/wake/listen-notify.md) from the service or a trigger ends the wait early.

## Testing

The tests in the example file publish checkouts through the harness and check the table and the handlers.
An email row reaches its subscription on the next poll, so the tests that wait for an email start the app with [`TestApp::start_live`](../components/testing/live.md) and let two seconds pass with `tb.advance`.
One test checks that the confirmation went out and the reminder still waits.
One places the same order twice and finds only the first order's emails.
One writes a routine and an urgent email before the service starts and checks that the urgent one was sent first.
[Testing a service on SQL](testing.md) covers the harness.
