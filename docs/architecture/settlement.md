---
title: Settlement
sidebar_position: 5
description: "What ack, drop, retry and retry_after write into a queue table in each claim form, and how the attempt count and the dead letter turn a retry into a move."
---

import {Diagram} from '@site/src/components/Diagrams';

export const settlement = {
  "title": "A settlement: the outcome picks a statement, transactional mode rolls the handler's writes back unless it acknowledges, and the statement runs where the form holds the row",
  "text": "The diagram as text",
  "outcome": ["The handler's outcome: ack, drop, retry(),", "retry_after(d), or the dead letter at the cap"],
  "rollback": ["In transactional mode, any outcome but ack first", "rolls back what the handler wrote"],
  "statement": ["The table's statement for the outcome, its own", "or the dialect's: an UPDATE, a DELETE, or a", "move to the dead letter"],
  "rowLock": ["Row lock:", "in the claim's", "transaction,", "then COMMIT"],
  "lease": ["Lease: only", "while the row", "holds this", "delivery's", "lease"],
  "advisory": ["Advisory lock:", "on the key's", "connection,", "then unlock"],
  "dropped": ["Dropped unsettled: the transaction rolls back, the lease", "is released, or the lock goes with its connection"]
};

export const retries = {
  "title": "A retry on a queue table: the row counts its attempts, and the cap decides between returning the row and its dead letter",
  "text": "The diagram as text",
  "row": ["A row in work, its attempt column", "counting the deliveries"],
  "outcome": ["The handler returns retry() or retry_after(d)"],
  "cap": ["max_attempts: has the attempt reached the cap?"],
  "returns": ["Below the cap: the row", "returns with one more", "attempt, at once or", "after retry_after"],
  "deadLetter": ["At the cap: the row", "moves to its", "dead-letter group", "or table"],
  "restart": ["The delay is a column of the row: it outlives a restart,", "and no copy of the message is ever published"],
  "handled": "handled",
  "retries": "asks again",
  "below": "below",
  "atCap": "at the cap"
};

# Settlement

What `ack`, `drop`, `retry` and `retry_after` write into a queue table in each claim form, and how
the attempt count and the dead letter turn a retry into a move.

<Diagram name="settlement" words={settlement} />

| Outcome | Row lock | Lease | Advisory lock |
|---|---|---|---|
| `ack`, `drop` | `DELETE`, or `processed_at` set, then `COMMIT` | the same, `WHERE locked_until` is the delivery's token | the same, on the delivery's connection, then unlock |
| `retry()` | `attempt = attempt + 1`, then `COMMIT`; without an `attempt` column a rollback | `locked_until = NULL` | no statement: the unlock returns the row |
| `retry_after(d)` | `retry_after` set later, `attempt + 1` | `retry_after` set later, `locked_until = NULL` | `retry_after` set later, then unlock |
| the cap reached | the row's group set to the dead-letter group, or the row moved into the dead-letter table | the same, the lease cleared | the same, then unlock |
| dropped unsettled | rolled back | the lease released at once | unlocked, its connection closed |

The form decides what holds the row, so it decides where the statement runs. In the row lock form
the statement runs in the claim's transaction, and its commit is the settlement: the row and its
outcome change together. In the lease form every statement carries the delivery's token, the
expiry its claim wrote, and touches the row only while the row still holds it. A settlement that
finds another token fails with
[`SqlxBrokerError::LeaseLost`](api:ruststream_sqlx::SqlxBrokerError::LeaseLost), and the row
belongs to the claim that took it since. In the advisory lock form the statement commits on its
own on the connection that holds the key, and the unlock follows. A statement that fails there
still releases the lock, and the row returns as it was.

`drop` finishes the row as `ack` does: a poison message leaves the queue. A table without a
`retry_after` column takes `retry_after(d)` as `retry()`, with a warning in the log. An event of
the service's own, named with `custom(..)`, runs its SQL in place of the dialect's statement, in the
same transaction or on the same connection.

## Retries

<Diagram name="retries" words={retries} />

A queue table counts its own deliveries: the `attempt` column. The row lock form counts it in the
statement that returns the row, the lease form at the claim and the advisory lock form at the
take. A handler reads the count through `Ctx<keys::Attempt>`, so it computes its own backoff: the
crate keeps no backoff policy.

The cap and its destination come together at the mount site: `max_attempts(n)` and
`dead_letter(..)`. A retry or a `retry_after` of a row whose attempt has reached the cap moves the
row instead, with a warning in the log. In a table with groups the dead letter is a group, and the
move is one `UPDATE` of the row's group. In a table without groups it is another table with the
same columns: on PostgreSQL one statement, `WITH moved AS (DELETE .. RETURNING ..) INSERT ..
SELECT`; on MySQL, MariaDB and SQLite a copy, then a delete, in one transaction. `max_attempts(1)`
moves the row at its first failure.

A delay never leaves the database. The descriptor declares that the database moves a delivery
itself, so the runtime's deferred copy, which other brokers publish for a `retry_after`, has no use
here: the row waits in the queue with its `retry_after` ahead, and survives a restart of the
service.

The outbox settles its records by the handler's outcome as well, on the consumer's side:
acknowledgement and drop mark the record, a retry leaves it for the next startup, as
[Outbox](outbox.md) draws.
