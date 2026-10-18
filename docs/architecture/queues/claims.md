---
title: Claims
sidebar_position: 1
description: "Three ways to claim a row: a row lock held by an open transaction, a lease written into the row, and an advisory lock on a key, each priced in connections."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The three claim forms, each from its claim to its settlement: row lock, lease and advisory lock",
  "text": "The diagram as text",
  "rowLock": "Row lock",
  "lease": "Lease",
  "advisory": "Advisory lock",
  "rowLockClaim": ["BEGIN", "SELECT .. FOR", "UPDATE SKIP", "LOCKED"],
  "rowLockHandler": ["Handler runs,", "the claim's", "transaction", "stays open"],
  "rowLockSettle": ["Settlement", "statement,", "then COMMIT"],
  "leaseClaim": ["UPDATE ..", "locked_until,", "attempt + 1", "RETURNING"],
  "leaseCommit": ["COMMIT at", "once: no", "connection", "held"],
  "keeper": ["the lease is", "extended every", "half lease"],
  "leaseHandler": ["Handler runs,", "no transaction", "open"],
  "leaseSettle": ["Settlement", "WHERE the", "lease is still", "its own"],
  "advisoryCandidates": ["Select due", "rows with", "their keys"],
  "advisoryLock": ["Lock each key", "on its own", "connection,", "never waiting"],
  "advisoryTake": ["Take the row:", "count the", "attempt,", "read it"],
  "advisoryHandler": ["Handler runs,", "the key's lock", "held"],
  "advisorySettle": ["Settlement on", "that", "connection,", "then unlock"],
  "connections": ["A row lock and an advisory lock hold a connection per delivery", "in work; a lease holds one only while a statement runs"]
};

# Claims

Three ways to claim a row: a row lock held by an open transaction, a lease written into the row, and
an advisory lock on a key, each priced in connections.

<Diagram name="claims" words={words} />

The table's description picks the form. A table with no form set takes the row lock form; a
`locked_until` column selects the lease form; `advisory_lock = ".."` selects the advisory lock form.
The forms differ in what holds a row while its handler runs, and so in what brings the row back
when the process dies.

## Row lock

The claim opens a transaction and selects its rows with `FOR UPDATE SKIP LOCKED`. A concurrent
claim skips the locked rows instead of waiting for them. The transaction stays open while the
handler runs, and the settlement's statement commits it, so the row and its outcome change in one
commit. When the process dies, the database rolls its transactions back and the rows are due at
once. The price is one connection per delivery in work, held for the whole handler. On MySQL and
MariaDB the transaction opens at READ COMMITTED unless the table names a level, so the claim locks
the rows it takes and not the gaps between them.

## Lease

The claim writes an expiry into `locked_until`, counts the attempt and commits at once, so the
handler runs with no transaction open and no connection held. The subscription extends every lease
in work each half lease, so a handler may run longer than its lease. The expiry is the delivery's
token: a settlement names the expiry it holds, and it takes effect only while the row still holds
it. A handler whose lease ran out while it ran, because its extensions failed or stopped, finds another
claim's token in the row, and its settlement fails with
[`SqlxBrokerError::LeaseLost`](api:ruststream_sqlx::SqlxBrokerError::LeaseLost). When the process
dies, its rows return when their leases run out: thirty seconds unless set, in whole seconds. The
host computes the expiry, because every settlement compares it, so a lease table reads the host's
clock.

## Advisory lock

A claim selects due rows with their keys, tries each key's lock on a connection of its own without
waiting, passes over a key another session holds, then takes the row while it is still claimable.
Rows whose keys match go into work one at a time, so `advisory_lock = "accounts-{account}"` keeps
each account's rows apart while other accounts run. No transaction stays open, but the connection
that holds the lock stays with the delivery until it settles. When the process dies, its sessions
end and the database drops their locks. A key belongs to the database, not to a table: two tables
whose keys match wait for each other.

## Per database

| | PostgreSQL | MySQL 8.0.1+, MariaDB 10.6+ | SQLite |
|---|---|---|---|
| Row lock | `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` | no row locks: not offered |
| Lease claim | one statement: a locking select, the update of the rows it found, the rows returned | a locking select, then one update per row, in one transaction at READ COMMITTED | one `UPDATE .. RETURNING`, no order among the rows it returns |
| Advisory lock | `pg_try_advisory_lock` on a 64-bit hash of the key (`hashtextextended`, PostgreSQL 11+) | `GET_LOCK(name, 0)` on the database's name and the key; a name past 64 characters by its SHA-256 | keys kept in the process, one process per database file |
| FIFO group guard | a transaction lock on the hash of table and group | a locking read of the group's unfinished rows | none: one writer at a time |

A server older than the floor stops a subscription at startup with
[`SqlxBrokerError::ServerTooOld`](api:ruststream_sqlx::SqlxBrokerError::ServerTooOld): MySQL 8.0.1
and MariaDB 10.6 are the first releases that skip locked rows. A PostgreSQL advisory lock lives in
the session that took it, so the form needs a direct connection or a pooler in session mode. Two
processes on one SQLite file do not see each other's keys, and a row may then be delivered twice.

In a FIFO table a claim takes only the head of its group, and nothing while another row of the
group is in work. The guard in the table above takes the group in the claim's transaction, so
subscriptions to one group on separate brokers keep its order too.

The attempt is counted where the form holds the row: the row lock form counts it at settlement, in
the statement that returns the row; the lease form at the claim; the advisory lock form at the
take. So a crash spends an attempt in the lease and advisory lock forms, and not in the row lock
form, whose rollback undoes the count with everything else. A delivery reports the attempt as it
stood before its claim, so the first delivery reads 1 in every form.

The databases document the primitives: PostgreSQL
[`SELECT .. FOR UPDATE`](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
and [advisory locks](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS),
MySQL [locking reads](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html) and
[locking functions](https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html), MariaDB
[`SKIP LOCKED`](https://mariadb.com/kb/en/select/#skip-locked).
