---
sidebar_position: 7
description: "Run several instances on one queue table: no job taken twice, how fast a new row is seen, notifications on Postgres, parallel workers and the pool they need."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Several instances and waking workers

This guide runs a queue on several instances of a service and several workers in each, and sets how fast they see a new job.

A queue table is shared by every instance that reads it.
Nothing coordinates the instances but the database: each claims rows, and the [form](../components/forms/index.md) of the table keeps any row in one claim at a time.
Two claimers never take the same row, in any form and on any database.

The example renders invoices.
The shop writes the invoice jobs with its own SQL, and the rendering service runs as many instances as the load asks for.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/scaling.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/scaling.rs#service"
```

</TabItem>
</Tabs>

[`workers(4)`](../components/subscriptions/workers.md) runs up to four invoices at once in each instance.
The subscription keeps a claim in flight for each free worker, so its workers take rows at once instead of one after another.
On SQLite, which takes one writer at a time, it keeps one claim in flight, and each claim still feeds the next free worker.
A second instance on the same table adds four more workers, and no invoice goes to both.

## How fast a new job is seen

A subscription claims while its claims come back full, so a backlog drains without a pause.
After a claim that found fewer rows than it asked for, it waits the [poll interval](../components/subscriptions/poll-interval.md), one second unless set.
Three things bound how long a new row waits:

- A publish through the broker [wakes](../components/wake/index.md) the subscriptions of its table in the same process at once, at the cost of one atomic operation per woken subscription.
- A row written by other means waits the poll interval: another program's insert, a trigger, an [`insert`](../components/tables/insert.md) of the service, a publish from another instance.
  `poll_interval(250 ms)` here bounds that wait to a quarter of a second, at the price of four claim statements a second per subscription on an idle table.
- On Postgres, a notification ends the wait.

## Notifications on Postgres

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/scaling.rs#notify"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/scaling.rs#notify"
```

</TabItem>
</Tabs>

[`listen_notify()`](../components/wake/listen-notify.md) listens on one connection of the pool, and each subscription listens on its table's channel.
Each publish of the broker runs `pg_notify(channel, group)` right after its write, so a subscription in another instance claims the row once it is visible.
A writer outside RustStream sends the same notification itself; the trigger above does it for every insert, and its empty payload wakes every subscription of the table.
With notifications the poll interval becomes a floor for what a notification missed, a minute here.

Notifications have a price, and the switch is off unless set.
The broker keeps one connection of the pool for its whole life.
Each publish runs one more statement.
A transaction that notifies takes a lock global to the server at commit, so commits of notifying transactions run one after another; a service with many concurrent writers measures that before it turns notifications on ([NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)).

## Workers and the pool

A subscription never holds more than the pool's size less one connection, so the pool always keeps one for the handlers, their publishes and the settlements.
A pool too small for its workers runs fewer deliveries at once; it does not fail.
In the lease form a pool of `n + 1` lets every free worker of `workers(n)` claim at once; the row lock and advisory lock forms also hold a connection for each job in work ([Connections](../components/broker/connections.md)).
[Running in production](production.md) sizes the pool for a whole service.

## Clocks

Each instance reads "now" from its host's clock to claim, delay and lease rows ([Time](../components/time/index.md)).
The hosts that serve one table keep their clocks in step, well within a lease, as NTP does.
Where they cannot, a table on [`DatabaseClock`](../components/time/database-clock.md) reads the database's clock in every statement; the lease form keeps the host's clock, so such a table takes the row lock or the advisory lock form.

## Testing

The tests in the example file run the service on SQLite in memory.
Two instances, each with a pool of its own, start on one table that holds twenty invoices, and between them render each invoice once.
A row another program writes is seen within the quarter second of the poll interval.
A publish through the broker reaches its own instance at once.
Those rows wake no subscription, so the tests that wait for them run on the real clock with [`TestApp::start_live`](../components/testing/live.md).
A last test runs the Postgres service against a database of its own and finds an insert claimed long before the minute's poll; it reads the server's address from `POSTGRES_TEST_URL` and skips without it.
