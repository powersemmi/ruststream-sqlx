---
title: Methodology
description: "How ruststream-sqlx measures itself: the three loops, the run on a real database, the database-bound arithmetic, the throughput grid and the code count."
---

# Methodology

How `ruststream-sqlx` measures itself: the three loops, the run on a real database, the
database-bound arithmetic, the throughput grid and the code count. The procedure is the
framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on a SQL database.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw sqlx loop.** sqlx, driven directly: a loop that runs the statements the broker runs for the
  same table, in the same order, on a pool built the same way. It takes its statements from the
  dialect the broker renders them with, so the two loops run the same SQL.
- **Crate by hand.** This crate and nothing above it: the broker connects, an
  [`InboxQueue`](api:ruststream_sqlx::InboxQueue) opens a subscription, and a loop pulls the
  deliveries off its stream, decodes each one, reads a field and acknowledges it; or a
  [`Repository`](api:ruststream_sqlx::Repository) and [`Routed`](api:ruststream_sqlx::Routed)
  paired into publishers. No handler, no app, no dispatch.
- **RustStream service.** The service a user writes: a `#[derive(Inbox)]` table, a `#[subscriber]`
  handler and `RustStream::new(..).with_broker(SqlxBroker::new(pool), ..)`.

Everything else is equal across the three: the table, the rows, the decode into the same small
struct, a pool of four connections, the tokio runtime with four worker threads, and the binary. A reply scenario answers every delivery into a second
table through `Routed`, the connected broker's default publisher.

The outbox's scenarios are one app in three variants that take the three places: the app with no
outbox, the same app with the outbox written by hand in raw sqlx, on the connections the crate's
middleware takes, and the same app with this crate's outbox. A message published from outside a
handler goes through [`Outbox::wrap`](api:ruststream_sqlx::outbox::Outbox::wrap) in the last one.

## The run

- **A fresh table per run.** Each run recreates its table and fills it, then drains it. A publish
  scenario writes its rows instead.
- **The window opens at the first handled delivery and closes at the last**, in all three loops,
  so starting the service and opening the pool stay outside it. A publish scenario's window runs
  from the first publish to the last. Every handled delivery counts a latch down, and the last
  count wakes the run, so a run knows it drained without polling the table or sleeping.
- **The message count comes from a probe**, which measures the raw loop's rate, so a measured run
  lasts at least five seconds on any machine.
- **The loops are interleaved**: raw, crate, service, three rounds. Each loop reports its best,
  median and worst round. The best is the headline, because noise only ever slows a run down.
- **The outbox runs over Redis Pub/Sub**, fed from a worker that keeps at most 1024 messages
  unconsumed, because Pub/Sub drops what a subscription's buffer cannot hold.

A difference smaller than the spread between the rounds of either loop is `indistinguishable`,
never a figure. A row is `database-bound` when round trips account for at least half of the time a
message took. That is arithmetic. A probe times one statement the server answers, on one
connection, before the rounds: the median of 2000 `SELECT 1` round trips, published with the
machine. Each scenario declares the round trips a message costs it, every statement and every
connection taken from the pool, which sqlx pings before it lends one. The row is flagged when the
round trip times that count reaches half the measured time per message.

## The throughput grid

Each cell drains a filled table of 20,000 rows on a pool of 16 connections, at one, four and eight
workers, with single deliveries and with batches of 64. PostgreSQL and MySQL take the row lock
form; SQLite, on a local file in WAL mode, takes the lease form, where a claim is one statement.
The raw loop runs n tasks on one pool, each a loop that claims, reads and deletes until its claim
comes back empty. The crate's loop runs n tasks on the same pool, each a broker of its own reading
one subscription. The service mounts its handler with `workers(n)` on one subscription; one worker
is the plain mount, which handles a delivery before it pulls the next. Three rounds, interleaved,
and the best, median and worst of each loop.

## The code count

Each scenario of the [cost chart](/architecture/benchmarks/) is the service a user writes, built
without the `testing` feature, started on a single-threaded runtime against the stand's PostgreSQL,
with the raw loop counted beside it. A producer on a thread and a runtime of its own fills the
table between the start and the drain, so the fill is in neither count. The outbox's scenarios run
over `MemoryBroker`, one message in flight at a time.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery, then
1000, then 2000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start. The count stops while the runtime parks waiting for the server:
that work follows the server's timing, not the code.

The instruction count still moves between runs, because how often a reply is still in flight when
the client polls for it changes how much of sqlx's reading a message takes. Over five runs of one
tree a scenario's total moved by up to about 7%, and its figure per message by up to about 15%; the
scenarios that never touch the database agree to the instruction. A run against a saved baseline
fails on ten percent more instructions in a scenario. Allocations do not move: each scenario
declares a limit, the highest count of five full runs plus 0.1%, and every run fails above it.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, PostgreSQL, MySQL and Redis in Docker
on the host network, runs every scenario and the throughput grid, stops the stand and rewrites
`docs/benchmarks/results.json`. It takes about half an hour and wants the machine to itself.
`RUSTSTREAM_BENCH_SECONDS` sets the shortest run, `RUSTSTREAM_BENCH_THROUGHPUT_ROWS` the rows of a
throughput cell, and `RUSTSTREAM_BENCH_PAIRS` the rounds. It clears `RUSTFLAGS`, so the binary is
not tied to the CPU it was built on. Both recipes build the benchmarks the way a release is built,
with fat link-time optimization across crates, so the numbers describe the code a service ships.

```bash
just bench-code
```

The code count starts the same stand and needs valgrind; it installs the benchmark runner at the
release the lock file pins. `just bench-code 5000` measures every scenario over five thousand
deliveries instead of a thousand: a steadier figure for a longer run. `just bench-code
--save-baseline=main` on `main` records a baseline, and `just bench-code --baseline=main` on a
branch compares with it.
