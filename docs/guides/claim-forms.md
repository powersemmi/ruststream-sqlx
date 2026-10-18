---
sidebar_position: 4
description: "Choose how a queue table holds a job in work: a row lock, a lease or an advisory lock, and what each does with a long job and a crashed process."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Long jobs, crashes and the claim form

This guide chooses how a queue table holds a job while a handler works on it, and shows what happens to a job that runs for minutes or whose process dies.

A claimed job must stay out of other claims until it settles, and come back if its process stops.
The table's description chooses one of three [forms](../components/forms/index.md) for that, and the handler is the same in all of them.

## The three forms

| | Row lock | Lease | Advisory lock |
|---|---|---|---|
| Selected by | nothing: the default | `#[field(locked_until)]` | `#[inbox(advisory_lock = "..")]` |
| Holds a job by | the claim's transaction, `FOR UPDATE SKIP LOCKED` | an expiry written into the row | a lock on the row's key, held by a session |
| Connections | one per job in work, for the whole handler | one per statement | one per job in work |
| After a crash | the job returns at once | the job returns when its lease runs out | the job returns at once |
| A crash spends an attempt | no | yes | yes |
| Databases | Postgres, MySQL, MariaDB | all | all |

The [row lock](../components/forms/index.md) keeps the claim's transaction open while the handler runs, and the settlement commits it.
A crashed process's transaction rolls back, so its jobs return at once and their attempts are not counted.
The price is a connection per job in work.
SQLite has no row locks, so a SQLite table names one of the other forms.

The [lease](../components/forms/lease.md) commits the claim at once, so a job in work holds no connection and no transaction.
It suits long jobs, and it is the form of a SQLite queue.
The [advisory lock](../components/forms/advisory.md) holds each job by a key: a key on the id holds each job alone, and a key on another column keeps the jobs that share it in work one at a time.

## A report that renders for minutes

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/claim_forms.rs#schema"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/claim_forms.rs#lease"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/claim_forms.rs#lease"
```

</TabItem>
</Tabs>

A claim writes the lease's expiry into `locked_until` and counts the attempt.
While `render` runs, the subscription extends the lease every half lease, so a report may render for five minutes on a lease of one.
The expiry is the delivery's ownership token: the acknowledgement deletes the row only while it still holds that lease.
[`InboxQueue::lease`](api:ruststream_sqlx::InboxQueue::lease) sets the lease of one subscription, and [`SqlxBroker::lease`](api:ruststream_sqlx::SqlxBroker::lease) the broker's; it is thirty seconds unless set, in whole seconds.

## What a crash does

A process that dies mid-report stops extending its lease.
The row stays out of the queue until the lease runs out, a minute here, then the next claim takes it.
That claim counts one more attempt, so `render` sees a second attempt and logs it.
A lease is therefore how long a crash delays a job: a short lease brings it back sooner, at the price of more extensions.
A process that stops the extensions while alive, cut off from the database for longer than its lease, loses the job to another claim, and its late settlement fails with [`SqlxBrokerError::LeaseLost`](api:ruststream_sqlx::SqlxBrokerError::LeaseLost).

A row lock or an advisory lock ends with its session, so a crash returns the jobs at once.
At shutdown the broker stops extending leases: a job in work keeps its lease and settles if it can, or returns when the lease runs out.
Advisory locks are released at shutdown ([Shutdown](../components/broker/shutdown.md)).

## The pool each form needs

A row lock or an advisory lock holds a connection per job in work, so a subscription with `workers(8)` needs eight connections for its jobs alone.
A lease holds one only for each statement.
Every subscription leaves the pool one connection for the handlers, their publishes and the settlements ([Connections](../components/broker/connections.md)).
On Postgres, the advisory lock needs a direct connection or PgBouncer in session pooling, because the lock lives in the session.
On SQLite, the process keeps the advisory keys in work, so one process serves a database file.
[Running in production](production.md) sizes the pool.

## Testing

The tests in the example file run the service on SQLite in memory.
The first pauses the clock, so five minutes of rendering pass at once and the lease's extensions run as the clock reaches them; the report is delivered once.
The second writes two rows another process had claimed: one whose lease ran out, one whose lease is still in force.
Only the first comes back.
Those rows wake no subscription, so this test runs on the real clock with [`TestApp::start_live`](../components/testing/live.md) past the poll interval.
