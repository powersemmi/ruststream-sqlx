---
title: Numbers
description: "What the benchmark numbers of ruststream-sqlx measure, what still costs a message, and what the numbers do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-sqlx` measure, what still costs a message, and what the
numbers do not allow you to conclude.

## The comparison against a raw sqlx loop

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, against one database. A row is not comparable with a row another broker crate
publishes: a queue in a table does other work per message than a message broker, and every one of
its messages is a claim, a settlement and a commit.

Every number includes those round trips to the database, in all three loops. In a `database-bound`
row they took at least half of the time a message took, so the difference between the loops is a
lower bound on what the crate and the runtime do, not a measurement of it. Most inbox rows are
`indistinguishable`: the crate's work is smaller than the spread between rounds of a loop that
waits on a server.

The outbox rows measure another thing: the cost of tracking. A tracked message costs an insert
before its publish, and on the consumer's side a fetch and a mark, each a statement on its own
connection, so the outbox against no outbox is several times slower, by design. What the crate
answers for is the gap to the outbox written by hand, which runs the same statements.

## The throughput grid

With one worker the three loops do the same work in the same order. With more, the raw loop runs n
claimers, each a task that claims and deletes on its own, and the crate's loop runs n
subscriptions, each on a broker of its own. A service runs one subscription for its n workers,
which keeps a claim in flight for each free worker on PostgreSQL and MySQL. So the crate's column
beside the raw one shows what the crate costs at the same concurrency, and the gap from the crate
to the service at four and eight workers is how one subscription claims, not the dispatch. A batch
takes up to its size per claim, so one claim serves that many deliveries, and the three loops stay
close.

On SQLite a subscription keeps one claim in flight, because SQLite takes one writer at a time.
The raw loop's n claimers on one SQLite file are a shape one subscription does not take, and the
SQLite rows at four and eight workers show the price of that choice: there the service drains at a
fraction of the raw loop, and below its own rate with one worker. Single deliveries on SQLite gain
nothing from `workers(n)`; batches do, as the batch rows show.

## The cost of the code

An instruction count holds the runtime, this crate and sqlx's driver on the service's thread:
encoding each statement, the exchange with the server, decoding each row. The raw loop's count
beside it is the driver's share, so the difference is the crate and the runtime. The same count
costs more time where it misses the cache, so a count is not a time.

Every figure is the steady state, the difference between a run of 1000 deliveries and one of 2000.
Opening the pool's connections, the broker's startup checks and the first delivery are the cold
start, paid once. Read an instruction figure to within a few percent, not to the digit: a service
that talks to a real server moves between runs.

## The costs that remain

The crate keeps the family's rule: no allocation, dynamic call, extra atomic or copy per message
on the hot path beyond sqlx's own. What remains is stated here, each where it applies:

- A handler that reads its delivery's transaction, `Tx`, pays one reference-count increment per
  delivery, and one that asks for the pool through `Ctx<keys::Pool>` pays one as well. A
  subscription that does not use them pays nothing.
- A publish through a `Repository` or a route reads the broker's closed flag, one atomic load, and
  wakes the subscriptions of its table in the process, one atomic operation each.
- `Routed` looks each message's name up, one hash lookup for an exact name, and makes one dynamic
  call into the row's `Publish`. A `Repository` names its table at compile time and makes neither.
- A by-name subscription over a row whose events are the service's own pays one boxed delivery and
  one boxed settlement future per message.
- The outbox reads a pool set after startup through a `OnceLock`, one atomic load per tracked
  message, and allocates the text of each tracked message's id header.
- The row lock and advisory lock forms hold one pool connection per message or batch in work: the
  price of a lock the database releases when its holder dies.
- A publish from outside a handler, through the running app's publisher, encodes each message into
  a buffer of its own; the raw loop keeps one buffer for every message.

## What they do not mean

The stand runs PostgreSQL and MySQL in Docker on the host network of the machine the benchmark runs
on, without durability: no `fsync`, no flush of the log at commit. SQLite runs on a local file in
WAL mode with `synchronous = NORMAL`. What is measured is the cost of a message, not the disk under
the server. A service that keeps durability on pays for it, and pays the same in every loop. A
server on another host answers a question about the network rather than about this crate.

A row is one table, a small JSON body and one server on the loopback. It measures what a message
costs in this crate, not what PostgreSQL, MySQL or SQLite can carry. The window closes when the
last delivery is counted, before its settlement, in all three loops, so one settlement of a run
sits outside the number everywhere.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
