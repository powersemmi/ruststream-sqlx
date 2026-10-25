---
title: Lifecycle
sidebar_position: 3
description: "SqlxBroker::new records configuration, connect resolves the dialect, each subscription prepares its SQL at startup, and shutdown stops the claims."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's states as types, and what each transition does on the database",
  "text": "The diagram as text",
  "states": "Broker",
  "database": "On the database",
  "unconnected": ["SqlxBroker", "::new(pool)"],
  "connected": ["ConnectedSqlx", "Broker"],
  "closed": ["ClosedSqlxBroker", "locks_released,", "connections_closed"],
  "config": ["The pool, routes, poll", "interval, lease: no I/O"],
  "connection": ["One connection taken,", "the dialect resolved;", "one held for LISTEN", "under listen_notify"],
  "subscriptions": ["Each subscription:", "declaration checked,", "statements built,", "server version read,", "each one prepared"],
  "running": ["Claim loops run;", "a keeper extends the", "leases each half lease"],
  "stop": ["Claims and keepers", "stop; advisory locks", "released; the listener", "closed; the pool stays", "the service's"],
  "afterShutdown": ["A publisher that outlives the broker answers", "SqlxBrokerError::Closed, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`SqlxBroker::new`](api:ruststream_sqlx::SqlxBroker::new) records configuration, connect resolves
the dialect, each subscription prepares its SQL at startup, and shutdown stops the claims.

<Diagram name="lifecycle" words={words} />

`new`, `route`, `poll_interval`, `lease`, `connection_limit` and `listen_notify` do no I/O, so the
service's builder stays synchronous. The pool they record may connect lazily: sqlx builds it inside
a Tokio runtime and opens its connections on first use.

Connect takes one connection of the pool, which checks that the database is reachable: a database
out of reach fails the start with
[`SqlxBrokerError::Connect`](api:ruststream_sqlx::SqlxBrokerError::Connect). On that connection the
broker resolves its dialect, which matters for an `AnyPool`: it learns there which database it
reaches. Then it gives the connection back. Under `listen_notify` it takes one more, which it keeps
until shutdown to listen for notifications.

Each subscription opens in order. Its declaration is checked first: `max_attempts` without
`dead_letter`, or the other way round, and a cap on a table without an `attempt` column, stop it
with [`SqlxBrokerError::Declaration`](api:ruststream_sqlx::SqlxBrokerError::Declaration). Its
statements are built from the table's description, and a second subscription to the same queue on
this broker is refused. Then, on one connection, the dialect reads the server's version where it
has a floor, as MySQL and MariaDB do, and every statement is prepared on the server: a table or a
column the database lacks stops the service at startup with
[`SqlxBrokerError::Schema`](api:ruststream_sqlx::SqlxBrokerError::Schema), naming the table and the
statement. Last, under `listen_notify`, the subscription listens on its table's channel. Every
error at this point is an error of startup, before a message is claimed.

While the service runs, each subscription claims as [Waking](queues/waking.md) describes, and in
the lease form a task of its own extends every lease in work each half lease.

Shutdown sets the broker's closed flag, which refuses every publisher handed out before, since the
pool stays open for the service. It stops the claim loops and the lease keepers. A delivery in
work in the row lock or lease form settles as before: its transaction or its lease still holds the
row, and a lease is no longer extended. In the advisory lock form shutdown releases the lock of
every delivery in work, and a delivery whose lock it released settles no more: its settlement
fails with `Closed`, and its row is back in the queue. A claim the runtime cut short when it
stopped a subscription may still run on the server; its connection waits for the server to end the
claim's statement and roll its transaction back, and then closes. Shutdown waits for those
connections and for every session still closing, closes the listening connection, and returns
[`ClosedSqlxBroker`](api:ruststream_sqlx::ClosedSqlxBroker), which counts the locks released with
their connections back in the pool and the connections closed instead.

Shutdown does not return claimed rows by itself. Each form returns them in its own way: the row
lock form when its transaction rolls back or its connection closes, the lease form when a dropped
delivery releases its lease or the lease runs out, the advisory lock form when its lock is
released. In a service the runtime drains the handlers before it shuts the broker down, and a
handler aborted at the drain timeout drops its delivery.
