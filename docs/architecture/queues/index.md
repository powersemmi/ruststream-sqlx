---
title: Tables
sidebar_position: 0
description: "A queue is a table of the service's own: a row is a message, a group is a queue, and the roles of the columns decide when a row is due and what an outcome does."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One queue table: a write makes a row due now or later, a claim takes due rows into work, and the outcome finishes the row, returns it or moves it to the dead letter",
  "text": "The diagram as text",
  "write": ["A publish through the broker, or the service's own insert", "writes one row through the service's SQL"],
  "table": "email_jobs, a table of the service",
  "scheduled": ["Scheduled", "retry_after lies ahead;", "due once it passes"],
  "due": ["Due", "in the group, retry_after", "passed, not finished, free"],
  "inWork": ["In work", "claimed by priority,", "then retry_after, then id"],
  "deadLetter": ["Dead letter", "another group of the table,", "or another table"],
  "finished": ["Finished", "processed_at set,", "or the row deleted"],
  "roles": ["Columns by role: id, group, priority, partition_key,", "retry_after, attempt, processed_at, locked_until,", "headers, payload. Every role but id is optional,", "and each turns one behaviour of the queue on"],
  "optional": ["A group is a queue. A table without a group column", "is one queue, read by one subscription per broker"],
  "later": "retry_after ahead",
  "now": "due now",
  "claim": "claim",
  "retry": "retry()",
  "retryAfter": "retry_after(d)",
  "spent": "attempts spent",
  "ack": "ack, drop"
};

# Tables

A queue is a table of the service's own: a row is a message, a group is a queue, and the roles of
the columns decide when a row is due and what an outcome does.

<Diagram name="table" words={words} />

The crate creates no table and runs no migration. The service writes the `CREATE TABLE`, and a
struct describes the table to the crate: [`#[derive(Inbox)]`](api:ruststream_sqlx::Inbox) reads it
from attributes, and [`InboxTable`](api:ruststream_sqlx::InboxTable) with
[`InboxSpec`](api:ruststream_sqlx::InboxSpec) is the same description written by hand. The
service's own [`Publish`](api:ruststream_sqlx::Publish) impl writes every row, so the layout of the
insert stays the service's too.

A row is claimable when four conditions hold, each only where the table has its column: the row is
in the subscription's group, its `retry_after` has passed, its `processed_at` is empty, and no lease
holds it (`locked_until` is empty or has passed). A claim takes claimable rows in claim order:
`priority` first, a smaller value earlier, then `retry_after`, then the id. A row with a future
`retry_after` is a scheduled job: the queue does not see it until its time comes.

A subscription's name selects its queue. In a table with a `group` column it is the group, and
every group is a queue of its own. In a table without one the name addresses the whole table. One
broker opens one subscription per queue: a second subscription to the same table and group stops
the service at startup with
[`SqlxBrokerError::AlreadySubscribed`](api:ruststream_sqlx::SqlxBrokerError::AlreadySubscribed).
Several brokers or several instances of the service share a queue, and the claim form keeps their
rows apart.

What a row carries to its handler depends on two roles. With a `payload` column the row is in
payload mode: the handler takes the payload, decoded by the codec. Without one the row is in row
mode: the handler takes the row itself, as the driver read it. A `headers` column keeps the
delivery's headers in one column, a JSON object of strings under the `json` feature. The headers
layout reads them from columns of their own instead, each header a column. A table with neither
delivers empty headers.

"Now" is read when a claim selects due rows, when a retry delays a row and when an ack marks it.
By default the host reads it, through [`SystemClock`](api:ruststream_sqlx::SystemClock), and binds
it in the column's own type. With `clock = DatabaseClock` the statements read the database's clock
instead:

| Database | "Now" in the statement |
|---|---|
| PostgreSQL | `statement_timestamp()` |
| MySQL, MariaDB | `UTC_TIMESTAMP(6)` |
| SQLite | `strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')`, text |

SQLite keeps times as text and compares them as text, so a table there keeps one layout of time in
every row. The lease form always reads the host's clock: a lease table on `DatabaseClock` does not
compile, for the reason [Claims](claims.md) gives.

How a claim takes a row into work is on [Claims](claims.md), and what each outcome writes is on
[Settlement](../settlement.md).
