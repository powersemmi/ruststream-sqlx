# Benchmarks

A queue in a database table costs a claim and a settlement per message, and the crate adds its
own work on top: the subscription stream, the decode, the dispatch, the settlement bookkeeping.
This page says how much that work costs, measured against the same claim and settlement written
by hand on sqlx.

A scenario runs three ways in one process. **Raw sqlx loop** is a hand-written loop that runs
the statements the broker runs for the same table, in the same order, on a pool built the same
way. **ruststream-sqlx** is the same work written by hand on this crate: `SqlxBroker` connected,
the subscription read as a stream of deliveries and each one acknowledged, or the crate's
publisher sent through, with no service, no handler and no dispatch. **RustStream service** is
the application a user writes: a `#[derive(Inbox)]` table, a `#[subscriber(..)]` handler, and
`RustStream::new(..).with_broker(SqlxBroker::new(pool), ..)`.

That gives two differences. The crate against the raw loop is what this crate's own subscription
or publisher costs over the statements it runs, which is the number this repository is
responsible for. The service against the raw loop is what a reader pays end to end. The gap
between them is what the runtime costs on top of this crate.

The outbox is a plugin rather than a broker, and it is measured as one: one app in three variants,
read in [a section of its own](#the-outbox-as-a-plugin).

All three share the table, the rows, the decode into the same type, the tokio runtime and the
build. The raw loop takes its statements from the dialect the broker renders them with, so the
loops cannot drift apart. The procedure is the framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## The numbers

Messages per second on the stand's Postgres, on a multi-threaded runtime. The best of three
interleaved rounds, with the median round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw sqlx loop", "adapter": "ruststream-sqlx", "framework": "RustStream service", "adapterOverhead": "Crate over raw", "overhead": "Service over raw", "indistinguishable": "indistinguishable", "brokerBound": "database-bound", "machine": "Machine", "os": "OS", "broker": "Databases", "build": "Build", "versions": "Versions", "measured": "Measured", "codeMeasured": "Code costs measured", "instructions": "Instructions per message", "rawInstructions": "Raw loop, instructions", "allocations": "Allocations per message", "rawAllocations": "Raw loop, allocations", "cold": "Cold start (instructions / allocations)", "database": "Database and form", "delivery": "Deliveries", "workers": "Workers", "single": "one at a time", "batchOf": "batches of {size}", "forms": {"row lock": "row lock", "lease": "lease"}, "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render.", "outboxNone": "No outbox", "outboxByHand": "Outbox by hand", "outboxCrate": "ruststream-sqlx outbox", "outboxTotal": "Outbox over none", "outboxOwn": "Outbox over by hand", "outboxInstructions": "Instructions per message (none / by hand / outbox)", "outboxAllocations": "Allocations per message (none / by hand / outbox)"}'></div>

The tables are read in your browser from the document the last run wrote, so nothing on this page
is a copy that could have gone stale.

A run fills its table, then drains it. A probe run sets the number of rows, so that every
measured run lasts at least five seconds on whatever machine it is taken on. The window opens at
the first handled delivery and closes at the last, so starting the service and opening the pool
stay outside it. A publish scenario writes its rows instead of draining them, and its window runs
from the first publish to the last. A reply scenario answers every delivery into a second table
through `Routed`, the connected broker's default publisher.

A difference reported as `indistinguishable` is one smaller than the spread between rounds of
either loop. That is the honest outcome wherever the database costs far more than the dispatch:
every message here takes at least two statements and a commit, and a difference this small
disappears inside one of them. A figure below the run-to-run noise would read as precision that
was never measured, so none is published.

A row marked `database-bound` is one whose raw loop spent at least half of each message waiting
on the database: the round trips a message costs it, at the round trip the run measured, against
the time a message took. Every statement is a round trip, and so is every connection taken from
the pool, which sqlx pings before it lends one. In such a row everything above the socket does its
work inside a wait the loop was already paying for, so the row is a real result and at the same
time a lower bound on those costs rather than a measurement of them.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-sqlx/latest/benchmarks/results.json).

## Throughput

<div id="benchmark-throughput"></div>

A filled table drained on Postgres, MySQL and SQLite, on a pool of 16 connections. The service
mounts its handler with `workers(n)`; one worker is the plain mount, which handles a delivery
before it pulls the next. The raw loop runs n tasks on one pool, each a loop that claims, reads
and deletes until its claim comes back empty: the same statements and the same concurrency. The
crate's loop runs n tasks on the same pool, each a `SqlxBroker` of its own reading one
`InboxQueue` subscription. Postgres and MySQL take the row lock form. SQLite takes the lease form, because it has no row
locks; one writer at a time holds its database.

Each run fills its table first, then times the drain until the last row is handled. A run knows it
is over without polling the table or sleeping: every handled delivery counts a latch down, in all
three loops alike, and the last count wakes the waiting run.

The claims of one subscription run one at a time, each for up to one row, or up to the batch size
for a batch handler. `workers(n)` runs the handlers and their settlements in parallel; it does not
multiply the claims. With one worker the three loops do the same work in the same order. With
more, each raw task and each of the crate's subscriptions claims on its own, while the service's
one subscription claims for all of its workers. So the crate's column next to the raw one shows
what the crate costs at the same concurrency, and the gap from it to the service at four and eight
workers is the claim, not the dispatch. A batch takes up to its size in rows per claim, so one
claim serves that many deliveries.

## The crate's own code

<div id="benchmark-code"></div>

The third table is counted rather than timed: instructions under callgrind and allocations under
DHAT, for the service and for the raw loop beside it. Each scenario runs against the stand's
Postgres on a single-threaded runtime. A producer on another thread fills the table between the
start and the drain, so the fill is in neither.

What is counted is everything the service's thread runs: the framework, this crate, and sqlx's
driver encoding, sending, receiving and decoding on that thread. The database server is another
process and is not counted, and neither is the kernel's side of a system call.

Instructions and allocations are per message in the steady state: the slope between a run of 1000
deliveries and a run of 2000. The last column is what starting the service cost once: opening the
pool's connections, the broker's startup checks and the first delivery. The numbers are absolute,
the framework's own cost included; the core publishes that cost alone on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).

The service talks to a real server, and a count depends a little on how the socket hands the
driver its bytes, so it moves a little between runs. Each limit is therefore the highest count a
scenario reached over several full runs, plus a margin of 0.1%. `just bench-code` fails on an
allocation above the limit a scenario declares, and with `--baseline=main` on more than two
percent more instructions: `just bench-code --save-baseline=main` records the baseline on `main`,
and `just bench-code --baseline=main` measures a change against it. A failed run prints every
limit it breached, the old value beside the new one. A pull request that changes the cost cites
its numbers.

## The outbox as a plugin

<div id="benchmark-outbox"></div>

The outbox works over any broker, so what it costs is a cost to an ordinary RustStream app. Each
scenario is one app in three variants. **No outbox** is the app a user writes without one: a relay
answers each command with a reply, a sink consumes the reply, and a message published from outside
a handler goes straight to the broker. **Outbox by hand** is the same app with the outbox written
by the service itself in raw sqlx: the record inserted before the publish, then taken into work
and marked in the handler, on the connections the crate's middleware takes. **ruststream-sqlx
outbox** is the same app with this crate's outbox: the registry, its two layers and the republish
at startup; a message published from outside a handler goes through `Outbox::wrap`.

The outbox against no outbox is the plugin's whole cost: what tracking a message costs at all.
The outbox against the one written by hand is the crate's own overhead: what its machinery adds to
the statements a user would run anyway. An untracked message passes both layers and costs nothing
when the outbox is written by hand, so that row has no middle variant.

Four scenarios cover what the outbox does. An untracked message makes the round trip through both
layers. A tracked publish leaves from outside a handler, the way an HTTP endpoint publishes. A
tracked delivery reaches the sink, which takes its record into work and marks it. A tracked round
trip does all of it: the reply is recorded, delivered, fetched and marked.

The wall clock runs over the stand's Redis Pub/Sub through `ruststream-fred`, where the crate's
outbox example runs, and against the stand's Postgres. A feeder publishes from a worker and keeps
at most 1024 messages unconsumed, because Pub/Sub drops what a subscription's buffer cannot hold.

<div id="benchmark-outbox-throughput"></div>

The tracked round trip with the relay and the sink mounted with `workers(n)`, in each variant.

<div id="benchmark-outbox-code"></div>

The counts run the same apps over `MemoryBroker`, so no transport is in the number. One message
is in flight at a time, so the relay and the sink never wait for each other's connection.

## The costs that remain

The crate keeps the family's rule: no allocation, dynamic call, extra atomic or copy per message
on the hot path beyond sqlx's own. What remains is stated here, each where it applies:

- A handler that reads its delivery's transaction, `Tx`, pays one reference-count increment per
  delivery. A subscription that does not use it pays nothing.
- A handler that asks for the pool through `Ctx<keys::Pool>` pays one reference-count increment
  per delivery.
- A publish through a `Repository` or a route reads the broker's closed flag: one atomic load.
- A publish wakes the subscriptions of its table in the same process: one atomic operation per
  subscription it wakes.
- `Routed` looks each message's name up, one hash lookup for an exact name, and makes one dynamic
  call into the row's `Publish`. A `Repository` names its table at compile time and makes neither.
- A by-name subscription over a row whose events are all the crate's own reads the row by its
  columns, as an `InboxQueue` does. A row that runs events of its own pays one boxed delivery and
  one boxed settlement future per message.
- The outbox reads a pool set after startup through a `OnceLock`: one atomic load per tracked
  message. A pool given at construction costs nothing. A tracked message carries its record's id
  as a header, whose text and header storage the publish allocates.
- The outbox's test switch is compiled out of production builds.
- The row lock and advisory lock forms hold one pool connection per message or batch in work. That
  is the price of a lock the database releases when its holder dies.
- A publish from outside a handler, through the running app's publisher, encodes each message into
  a buffer of its own. The raw loop keeps one buffer for every message.

## The machine

<div id="benchmark-environment"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one table, a small JSON body and a database server on the loopback. It measures what a
message costs in this crate, not what Postgres, MySQL or SQLite can carry. A row here is not
comparable with a row published for a broker crate: a database queue does different work per
message than a message broker.

The window a run measures closes when the last delivery is counted, in all three loops alike.
Each settles a delivery after it is counted, so the last settlement sits outside the number
everywhere.

The stand runs every server in Docker on the host network of the machine the benchmark runs on,
without durability: no `fsync`, no flush of the log at commit. What is measured is the cost of a
message, not the disk under the server. A service that keeps durability on pays for it, and pays
the same on every side. A server on another host answers a different question, and answers it
about the network rather than about this crate.

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs every scenario and the throughput
grid, stops the stand and rewrites `docs/benchmarks/results.json` with what it measured. It takes
about half an hour and wants the machine to itself. `RUSTSTREAM_BENCH_SECONDS` sets the shortest
wall-clock run, `RUSTSTREAM_BENCH_THROUGHPUT_ROWS` the rows of a throughput run, and
`RUSTSTREAM_BENCH_PAIRS` the rounds.

```bash
just bench-code
```

The recipe starts the same stand, counts the code table under valgrind, stops the stand and rewrites
the `code` section of the same document. It needs valgrind. The recipe installs the benchmark
runner itself, at the release `Cargo.lock` pins. A leading number measures over another count of
deliveries, `just bench-code 5000`: the figures are steadier and the run is longer. The published
table is measured at the default of 1000.
