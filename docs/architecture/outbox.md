---
title: Outbox
sidebar_position: 7
description: "The outbox records each publish in a table of the service's own, on any broker, marks the record once a consumer acknowledges, and resends the rest at startup."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A tracked message: recorded, sent with its record's id over any broker, fetched by the consumer and marked once its handler acknowledges",
  "text": "The diagram as text",
  "producer": ["Producer handler", "publishes a tracked message"],
  "republish": ["At startup:", "each record not", "yet processed"],
  "record": ["Publish layer: the service's", "INSERT of the record, on a", "connection of its own"],
  "broker": ["Any RustStream broker: the", "message and its record's id"],
  "fetch": ["Subscription layer: the", "record fetched by its id", "while still unprocessed"],
  "processed": ["Processed", "already: ack,", "no handler"],
  "consumer": ["Consumer handler"],
  "mark": ["ack or drop: the record", "marked, processed_at = now,", "or deleted"],
  "waits": ["retry: the", "record waits", "for a startup"],
  "guarantee": ["The record commits on its own, never in a delivery's", "transaction: a publish is delivered at least once"],
  "sends": "then sends",
  "resends": "with its id",
  "delivers": "delivers",
  "unprocessed": "unprocessed",
  "acks": "acknowledges",
  "retries": "asks again"
};

# Outbox

The outbox records each publish in a table of the service's own, on any broker, marks the record
once a consumer acknowledges, and resends the rest at startup.

<Diagram name="outbox" words={words} />

The outbox is a plugin of the RustStream runtime, not part of a queue table: two middlewares and a
startup hook that wrap the publishers and handlers of any broker, the inbox's included. A registry,
built with `outbox!` or [`Outbox::track`](api:ruststream_sqlx::outbox::Outbox::track), names the
messages it tracks and the record type each one writes. The publish layer writes a record of each
tracked message through the record's own
[`Publish`](api:ruststream_sqlx::outbox::Publish) impl, then sends the message with the record's
id in the [`x-ruststream-outbox-id`](api:ruststream_sqlx::outbox::OUTBOX_ID_HEADER) header. A
record that fails to write fails the publish, so nothing is sent without its record.

The subscription layer reads the header on the consumer's side. It fetches the record by its id,
`WHERE id = ? AND processed_at IS NULL` where the table marks its records. A record already
processed acknowledges the delivery without running the handler, which is how a copy sent twice is
handled once. A fetch that fails retries the delivery without the handler. Then the handler's
outcome settles the record: acknowledgement and drop mark it, with `processed_at` set from the
database's clock or the row deleted; `retry` and `retry_after` leave it, and the record waits for
the next startup. A mark that fails is logged, keeps the handler's outcome, and leaves the record
for the next startup too.

At startup the republish selects every unprocessed record of each name and publishes it again
through the broker's publisher, with its id, once the subscriptions are open. A record the
republish cannot read or send fails the startup. Two instances that start together may both send
the same record, so a consumer that must not process a message twice checks that itself.

## The record's own connection

A tracked publish takes a connection of its own for the record's insert, and the record commits on
it. It never joins a delivery's transaction: a record written while a transactional handler runs
commits whatever the handler's outcome. The message and its record are therefore not one commit
with the service's business data. What the outbox guarantees is that a publish is delivered at
least once: the record is written before the send, and every send that fails or is never handled
leaves a record that the next startup sends again. The usual transactional outbox, where the
business row and the event row commit together and a relay polls the table, is a queue table of
the inbox read by a handler that publishes on another broker.

## Costs

The registry is a list in the type system, one node per registered name, so a message whose name is
not registered costs one comparison of names per node and nothing else: no allocation, no database
call and no dynamic call. A tracked publish takes a connection for its insert and allocates the
text of the id header. A tracked delivery takes a connection for the fetch, gives it back before
the handler runs, so the handler can publish tracked messages of its own, and takes one again for
the mark. A pool set after the registry was built, inside `#[ruststream::app]`, is read through a
`OnceLock`: one atomic load per tracked message. A test build leaves the outbox off unless
`RUSTSTREAM_SQLX_OUTBOX=on`, and a production build always tracks: the switch is compiled out of
it.

The pattern is the [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)
with the delivery side added: the consumer's mark is what ends a record, not the send.
