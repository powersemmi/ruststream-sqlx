---
sidebar_position: 11
description: "Run a queue in production: its schema and index, startup and compile-time checks, the pool, database versions, poolers, shutdown and the AsyncAPI document."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running in production

This guide takes a queue to production on Postgres: the schema and its index, the checks that catch a wrong schema, the pool, the database versions and poolers, and shutdown.

The service is a mailer with eight workers, a cap of five attempts and a dead-letter group.

## The schema

{/* single-form: the migration is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/production.rs#migration"
```

The crate creates no table: the schema is the service's, and its migrations create it, as `sqlx::migrate!` runs them in [`examples/outbox`](https://github.com/powersemmi/ruststream-sqlx/tree/main/examples/outbox).
The index follows the claim order, `(group, priority, retry_after, id)` for the columns the table has, `(name, job_id)` here.
A claim locks every row of its group it reads before it sorts them, so without that index a claim held for a handler keeps the rest of its group from other claims.
In a table with [FIFO groups](ordering.md) the same index keeps the claim's read of the group to the group's own rows.

## Checked statements

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#table"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#table"
```

</TabItem>
</Tabs>

[`checked, db = postgres`](../components/tables/checked.md) hands every statement the struct determines to sqlx's compile-time check: the claim, the settlements, the dead letter of a group and the insert.
A column the struct names and the table lacks fails the build.
The build checks against `DATABASE_URL`, or against the `.sqlx` data that `cargo sqlx prepare` writes, committed beside the crate; `cargo sqlx prepare --check` in CI fails a build whose data is stale.
The subscription runs the same statements it runs without `checked`, so a message costs the same.
The check is the derive's: a table described by hand keeps the startup check alone.

Every subscription also [prepares its statements at startup](../components/broker/startup.md).
A table or column the database lacks stops the service with `SqlxBrokerError::Schema`, which names the subscription, the table and the statement.
A mount that declares a cap without its dead letter, or the reverse, stops it too.
So `main` runs the migrations before the app, and the checks see the schema the release reads.

## The pool

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

The table names no form, so it claims by [row lock](../components/forms/index.md): each email in work holds its row's lock and a connection until it settles.
A subscription never holds more than the pool's size less one, and leaves that one to the handlers' own queries, their publishes and the settlements.
So the pool is sized for every subscription of the service: `n` for each `workers(n)`, one more, and the connections the handlers take for their own work ([Connections](../components/broker/connections.md)).
A [lease](claim-forms.md) table holds a connection only for each statement, and suits a pool that cannot spare one per job.
On Postgres, [notifications](scaling.md) keep one more connection for the broker's life.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#main"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#main"
```

</TabItem>
</Tabs>

## Databases and poolers

| Database | Needs |
|---|---|
| [Postgres](../components/databases/postgres.md) | 9.5 for `SKIP LOCKED`; 11 for the advisory lock form and FIFO groups, which lock 64-bit hashes |
| [MySQL, MariaDB](../components/databases/mysql.md) | MySQL 8.0.1 or MariaDB 10.6, read at startup; an older server stops it with `ServerTooOld` |
| [SQLite](../components/databases/sqlite.md) | the lease or the advisory lock form; one process per database file in the advisory form |

On MySQL and MariaDB a claim runs at READ COMMITTED unless a row lock table declares another level.
A server that logs statements, `binlog_format = STATEMENT`, refuses writes at that level; the row and mixed formats accept them.
On Postgres `read_committed` is the practical [isolation level](../components/transactions/isolation.md) of a row lock table: a stricter level fails claims with serialization errors when claims and settlements run at once.
The advisory lock form keeps its lock in the database session, so it needs a direct connection or PgBouncer in session pooling; transaction pooling hands the session to another client between statements.
A broker with [`listen_notify`](../components/wake/listen-notify.md) listens on a session too.

## Shutdown

At shutdown the runtime stops the subscriptions, lets the handlers in work finish within its shutdown timeout, then [shuts the broker down](../components/broker/shutdown.md).
A row lock delivery holds its transaction until it settles; one still unsettled when the process exits rolls back with its connection, and its row returns.
A lease delivery keeps its lease and settles if it can, or its row returns when the lease runs out.
The broker releases the advisory locks of the deliveries in work.
The pool stays the service's, and `main` closes it after `run` returns.

## The AsyncAPI document

With the `asyncapi` feature, the generated [AsyncAPI document](../components/asyncapi/index.md) describes the server and each subscription's table and group.
The label of `with_broker_labeled` names the server, and the server entry holds the pool's host and port, never its user or password: the document is published and shared.

## Testing

One test in the example file builds the document from a lazy pool on a URL with a password, with no I/O, and finds the host and port in it and the password nowhere.
Two more run against a Postgres database of their own: a database without the table stops the startup with an error that names it, and the checked statements serve the queue.
They read the server's address from `POSTGRES_TEST_URL` and skip without it ([Testing a service on SQL](testing.md)).
