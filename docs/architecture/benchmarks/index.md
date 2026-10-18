---
title: Benchmarks
description: "What ruststream-sqlx costs on every message: throughput against a raw sqlx loop, the outbox as a plugin, and the instructions and allocations of its code."
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {CodeCounts, Throughput} from '@site/src/components/Benchmarks/Sqlx';
import {sqlx} from '@site/src/components/Benchmarks/sqlx';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw sqlx loop", "adapter": "Crate by hand", "framework": "RustStream service", "overhead": "Overhead", "against": "({percent} over the raw loop)", "indistinguishable": "indistinguishable", "brokerBound": "database-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw loop's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw sqlx loop", "code": "Cost of the code", "broker": "The inbox", "plugin": "The {plugin} plugin", "pluginRaw": "Without the outbox", "pluginAdapter": "Outbox written by hand", "pluginFramework": "The crate's outbox", "pluginAxis": "How far each loop falls below the best round of the app without the outbox", "pluginAgainst": "({percent} over no outbox)", "pluginOverhead": "Over the outbox by hand", "pluginOverHand": "({percent} over by hand)"};

export const grid = {"group": "{database}, {form}", "single": "one at a time", "batchOf": "batches of {size}", "workers": "workers({count})", "forms": {"row lock": "row lock form", "lease": "lease form", "outbox round trip over Redis Pub/Sub": "outbox round trip over Redis Pub/Sub"}};

export const counts = {"scenario": "Scenario", "instructions": "Service, instructions", "rawInstructions": "Raw loop, instructions", "allocations": "Service, allocations", "rawAllocations": "Raw loop, allocations", "pluginInstructions": "Instructions (no outbox / by hand / outbox)", "pluginAllocations": "Allocations (no outbox / by hand / outbox)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Databases", "round_trip": "Round trip", "rustc": "Compiler", "sqlx": "sqlx", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-sqlx` costs on every message: throughput against a raw sqlx loop, the outbox as a
plugin, and the instructions and allocations of its code.

## Against a raw sqlx loop

The first chart runs each scenario three ways on the stand's PostgreSQL. `Raw sqlx loop` is a loop
written by hand that runs the statements the broker runs for the same table, in the same order, on
a pool built the same way; it takes them from the dialect, so the two cannot drift apart. `Crate by
hand` drives this crate's subscription or publisher with a loop of its own and no runtime.
`RustStream service` is the service a user writes: a `#[derive(Inbox)]` table, a `#[subscriber]`
handler and the app. The first gap is what this crate costs over the statements it runs, and the
second is what the runtime adds on top.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw loop's best round, and a dot right of zero is slower. A
hollow dot is a loop whose difference from the raw loop is smaller than the spread of the rounds.
The table under the chart prints the best round with the median in parentheses.

Every message costs at least a claim, a settlement and a commit, each a round trip to the database.
A row marked `database-bound` is one whose raw loop spent at least half of each message waiting on
those round trips. The crate and the runtime then do their work inside a wait the raw loop pays as
well, and the row is a lower bound on their cost rather than a measurement of it.

## The outbox as a plugin

The outbox works over any broker, so it is measured as a cost to an ordinary app: one app in three
variants, over the stand's Redis Pub/Sub through `ruststream-fred` and the stand's PostgreSQL.
`Without the outbox` is the app a user writes without one. `Outbox written by hand` is the same app
with the record inserted before each publish, then fetched and marked in the handler, in raw sqlx.
`The crate's outbox` is the same app with this crate's registry, its two layers and the republish.
Against no outbox the chart shows what tracking a message costs at all; against the outbox by hand
it shows what the crate's machinery adds to the statements a user would run anyway.

## Cost of the code

The cost chart counts what one message costs on the service's thread: instructions under callgrind
and allocations under DHAT, in the steady state, against the stand's PostgreSQL. The count holds
the runtime, this crate and sqlx's driver encoding and decoding on that thread; the database server
is another process and is not counted. `Cold start` is what starting the service costs once:
opening the pool's connections, the broker's startup checks and the first delivery. The outbox's
counts run the same apps over `MemoryBroker`, so no transport is in their number.

## Results

<Results entry={sqlx} labels={labels} details="/architecture/benchmarks/methodology/" />

The counts beside the loops they are measured against: the raw sqlx loop for the inbox, and the
app without the outbox and with one written by hand for the outbox.

<CodeCounts labels={counts} />

## Throughput

A filled table drained on each database, on a pool of 16 connections, by one subscription mounted
with `workers(n)`, single deliveries or batches of 64. PostgreSQL and MySQL take the row lock form.
SQLite takes the lease form, because it has no row locks. The raw loop runs n tasks on one pool,
each claiming, reading and deleting until its claim comes back empty. The crate's loop runs n
subscriptions on the same pool, each on a broker of its own. The service runs one subscription,
which claims for all of its workers. The outbox cells mount the relay and the sink with
`workers(n)` in each of the outbox's three variants.

<Throughput labels={labels} words={grid} details="/architecture/benchmarks/methodology/" />

## The machine

<Environment entry={sqlx} labels={machine} />

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipes, and published beside the pages as
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-sqlx/latest/benchmarks/results.json).
[Methodology](methodology.md) says how they were taken, and [Numbers](numbers.md) what they allow
you to conclude.

<MovedSections />
