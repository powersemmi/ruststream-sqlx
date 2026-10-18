---
title: Path
sidebar_position: 4
description: "The way of a row through a service: claim, delivery, decode, handler, reply and settlement, one row at a time, in batches, or as the row itself."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of a row, one at a time with its reply, in batches, and in row mode, from the claim to the settlement",
  "text": "The diagram as text",
  "single": "One at a time",
  "batch": "Batches",
  "row": "Row mode",
  "claim": ["Claim: one", "row for each", "free worker"],
  "delivery": ["InboxDelivery", "the row, its", "id, attempt"],
  "decode": ["The codec", "reads the", "payload lent", "from the row"],
  "handler": ["Handler"],
  "reply": ["Reply: one", "buffer reused,", "Routed inserts", "it"],
  "settle": ["The outcome", "settles the", "row"],
  "batchClaim": ["Claim up to", "the batch", "size in rows"],
  "batchDelivery": ["A batch of", "deliveries,", "one claim"],
  "batchDecode": ["Each payload", "decoded on", "its own"],
  "batchHandler": ["Batch", "handler"],
  "batchSettle": ["Each row", "settles; row", "lock: one", "commit"],
  "rowClaim": ["Claim: one", "row for each", "free worker"],
  "rowDelivery": ["The row as", "the driver", "read it"],
  "rowHandler": ["Handler", "takes &Row"],
  "rowSettle": ["The outcome", "settles the", "row"],
  "noCodec": "no codec"
};

# Path

The way of a row through a service: claim, delivery, decode, handler, reply and settlement, one row
at a time, in batches, or as the row itself.

<Diagram name="path" words={words} />

A claim takes rows into work by the table's [claim form](queues/claims.md), and each row becomes an
[`InboxDelivery`](api:ruststream_sqlx::InboxDelivery): the row as the driver decoded it, its id,
its attempt and what the form holds for it, a transaction, a lease token or a key's connection. In
payload mode the codec reads the payload straight from the row's column: the bytes are lent, not
copied. The headers are built on their first read, so a handler that never reads them never pays
for them. A row whose payload does not decode is settled by the subscription's decode policy.

In row mode the table has no payload column, and the handler takes the row itself, `&Row`, as the
driver read it. No codec runs and nothing is copied.

A batch is one claim: up to the batch size in rows, delivered together, each decoded on its own. A
row that does not decode is settled by the decode policy, and the batch keeps its other rows. Each
delivery of a batch settles its own row. In the row lock form the rows of a batch share the
claim's transaction, so their settlements take effect together when the last one finishes, and a
statement that fails rolls the whole batch back. In the lease and advisory lock forms each row
settles on its own.

A reply is encoded into a buffer the dispatch loop reuses, and the crate's publishers take it as a
lent slice. [`Routed`](api:ruststream_sqlx::Routed), the default reply publisher, looks the reply's
name up in the broker's routes and inserts it through the row's
[`Publish`](api:ruststream_sqlx::Publish) impl. Before the insert it checks that the broker is not
shut down and that the table can hold every header of the message. After the insert it announces
the row with `pg_notify` under `listen_notify`, and wakes the subscriptions of its table in the
process. The delivery settles after the reply, as [Settlement](settlement.md) draws.

[Benchmarks](benchmarks/index.md) publishes what a message costs on each of these paths, in
instructions and allocations, beside a raw sqlx loop running the same statements.
