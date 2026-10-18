---
title: Transactions
sidebar_position: 2
description: "The transactions the crate opens for a delivery: the row lock form's claim, and in transactional mode the one a handler writes through, at the table's level."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "In transactional mode, the transaction a handler writes through in each form, and what commits outside it",
  "text": "The diagram as text",
  "rowLock": "Row lock",
  "lease": "Lease",
  "advisory": "Advisory lock",
  "rowLockTx": "Claim's tx",
  "leaseTx": "Delivery's tx",
  "advisoryTx": "Delivery's tx",
  "rowLockClaim": ["BEGIN, claim", "FOR UPDATE", "SKIP LOCKED"],
  "rowLockSavepoint": ["SAVEPOINT", "right after", "the claim"],
  "rowLockWrites": ["Handler", "writes", "through Tx"],
  "rowLockOutcome": ["ack: COMMIT", "with the", "settlement;", "else ROLLBACK", "TO SAVEPOINT"],
  "leaseClaim": ["Claim stamps", "the lease,", "COMMIT"],
  "leaseBegin": ["BEGIN at the", "table's", "level"],
  "leaseWrites": ["Handler", "writes", "through Tx"],
  "leaseOutcome": ["ack: settle", "if the lease", "holds, COMMIT;", "lost: all", "rolled back"],
  "advisoryTake": ["Lock the key,", "take the row"],
  "advisoryBegin": ["BEGIN on", "the lock's", "connection"],
  "advisoryWrites": ["Handler", "writes", "through Tx"],
  "advisoryOutcome": ["ack: settle,", "COMMIT;", "else roll", "back, then", "settle"],
  "advisoryUnlock": ["Unlock", "the key"]
};

# Transactions

The transactions the crate opens for a delivery: the row lock form's claim, and in transactional
mode the one a handler writes through, at the table's level.

<Diagram name="transactions" words={words} />

A subscription mounted with `.transactional()` lends its handler the delivery's transaction as
[`Tx`](api:ruststream_sqlx::Tx). Acknowledgement commits the handler's writes together with the
row's settlement, so a handler's results and the end of its job are one commit. Every other outcome
rolls the handler's writes back first, then settles the row as it would outside transactional
mode. A row the handler inserts into a queue table through `Tx` commits with the acknowledgement
too. A write through the pool, and a publish through the broker, commit on their own.

The row lock form already holds a transaction, the claim's, so its handler writes inside it. The
savepoint the subscription sets after the claim, `SAVEPOINT ruststream_claim`, is what lets a retry
discard the handler's writes with `ROLLBACK TO SAVEPOINT ruststream_claim` and still count the
attempt in the same commit. The lease and advisory lock forms commit their claims at once, so they
open a transaction for the delivery after it. In the lease form the acknowledgement takes effect
only while the row still holds the delivery's lease: a lost lease rolls the whole transaction back,
the handler's writes with it, and the settlement fails with `LeaseLost`.

Each delivery in transactional mode holds one connection for its transaction, in every form. A
batch handler cannot be mounted with `.transactional()`: the step serves single deliveries.

## Isolation and modes

| Database | Without a declaration | Levels a table may declare |
|---|---|---|
| PostgreSQL | `BEGIN`, the server's default | `read_committed`, `repeatable_read`, `serializable` |
| MySQL, MariaDB | READ COMMITTED | all four |
| SQLite | `BEGIN`, deferred | modes: `deferred`, `immediate`, `exclusive` |

A table declares a level with `isolation = ..`, or a SQLite mode with `mode = ..`, and the crate
opens the row lock form's claim and every transaction of transactional mode at it. PostgreSQL runs
READ UNCOMMITTED as READ COMMITTED, so its dialect does not open it, and a table that declares it
does not compile against a PostgreSQL broker. SQLite runs every transaction serializable: its mode
decides when a transaction takes the database's write lock, at its first write or at its start.
On SQLite a delivery's transaction holds that lock until the delivery settles, and keeps every
other writer out meanwhile, claims included.

MySQL and MariaDB open at READ COMMITTED because their default, REPEATABLE READ, locks the gaps
between the rows a locking read scans: a claim held for a handler would block every insert into
the table until it ends. A row lock table on PostgreSQL at `repeatable_read` or `serializable` fails
claims with serialization errors when claims and settlements of its rows run at once, so
`read_committed` is the practical level for that form there. A lease table in transactional mode
at either level stops at startup on PostgreSQL: such a transaction reads the row as its first
statement found it, and would not see the lease its keeper extended since.

The crate drives these transactions itself rather than through sqlx's transaction type: a
transaction dropped while open closes its connection, and the server rolls it back. The levels are
the databases' own: [PostgreSQL](https://www.postgresql.org/docs/current/transaction-iso.html),
[MySQL](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html),
[SQLite](https://www.sqlite.org/lang_transaction.html).
