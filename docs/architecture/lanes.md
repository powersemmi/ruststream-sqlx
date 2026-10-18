---
title: Lanes
sidebar_position: 6
description: "One subscription keeps a claim in flight per free worker, within the pool's size less one; workers_by_key(n) keeps each partition key on one lane."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One subscription mounted with workers(4) on a pool of eight connections: a claim per free worker, and one connection left for the handlers",
  "text": "The diagram as text",
  "subscription": ["InboxQueue subscription, workers(4): a claim", "in flight for each free worker"],
  "worker0": ["Worker 1", "claim, run,", "settle"],
  "worker1": ["Worker 2", "claim, run,", "settle"],
  "worker2": ["Worker 3", "claim, run,", "settle"],
  "worker3": ["Worker 4", "claim, run,", "settle"],
  "pool": ["Pool of 8: the subscription holds up to 7,", "one stays for the handlers' own queries"],
  "oneClaim": ["One claim in flight at a time on SQLite, on a FIFO table, on a", "table with a partition_key column, and on a MySQL or", "MariaDB lease table"],
  "byKey": ["workers_by_key(n): a row runs on the lane its partition_key", "picks, so the rows of one key keep their claim order"],
  "claims": "claims"
};

# Lanes

One subscription keeps a claim in flight per free worker, within the pool's size less one;
`workers_by_key(n)` keeps each partition key on one lane.

<Diagram name="lanes" words={words} />

The runtime polls a subscription's stream once for each free worker, and a subscription of single
deliveries answers each poll with a claim; a batch subscription claims a batch at a time. It counts its own deliveries in work, so the most it has seen at once tells
it how many workers there are, and it keeps a claim in flight for each free one. A claim beside
another starts only while the pool has a connection to spare, and the subscription never holds
more than the pool's size less one: that last connection serves the handlers' own queries, their
publishes and the settlements that take one. A pool short of room runs fewer deliveries at once,
and subscriptions that share a pool leave each other room. After a claim that took nothing, the
subscription claims one at a time until a claim takes a row again, so an empty table sees one claim
per wait, not one per worker.

What a delivery in work holds depends on the form. In the row lock and advisory lock forms each
delivery holds a connection until it settles, so `workers(n)` holds up to n. In the lease form a
claim and a settlement take a connection only for their statements, and the subscription takes one
each half lease to extend its leases. In transactional mode every delivery holds the connection of
its transaction, in every form. Size the pool for every subscription that holds connections, one
more for the pool, and the connections handlers take for their own queries.

Some tables keep one claim in flight at a time, where claims that run at once would cost more than
they give. SQLite takes one writer at a time, so concurrent claims only collide. A FIFO table and a
table with a `partition_key` column could see two concurrent claims finish out of claim order. On
MySQL and MariaDB a lease claim stamps its rows in a transaction of several statements, which locks
the rows it reads, leases in work included, until it commits. The workers still run side by side;
only the claims take turns.

`workers_by_key(n)` runs n lanes, and a row goes to the lane its `partition_key` hashes to. The
rows of one key never spread over two lanes, so they run in claim order. A FIFO group goes
further: one row of a group is in work at a time, across every broker and instance that reads it,
because the claim takes the group's head alone and the dialect's guard takes the group.

A handler that computes runs on dedicated threads with `on_threads(n)`. The subscription still
claims on the app's runtime, and each thread queries through a small pool of its own, one
connection unless set. [`connection_limit`](api:ruststream_sqlx::SqlxBroker::connection_limit)
holds the service's pool and every thread's connections to what the database allows: a
subscription that would pass it does not start.

Two replicas of a service on one table share its rows without coordination: each claims on its
own, and the claim form keeps a row in one place at a time.
