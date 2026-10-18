---
title: Waking
sidebar_position: 3
description: "A subscription claims while its claims come back full, and otherwise waits its poll interval, which a publish of the same broker or a notification cuts short."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A subscription's claim loop, and what ends its wait before the poll interval",
  "text": "The diagram as text",
  "claim": ["Claim", "up to the limit: a row", "per free worker, or a batch"],
  "full": ["Did the claim", "fill its limit?"],
  "wait": ["Wait", "the poll interval,", "or less when woken"],
  "publish": ["A publish of this broker", "into the table, for", "its group"],
  "reconnect": ["The listening", "connection is back:", "every subscription"],
  "notification": ["A notification on", "the table's channel,", "on PostgreSQL"],
  "writer": ["Another process,", "a trigger: pg_notify", "(channel, group)"],
  "floor": ["The poll interval stays the floor under every wake-up:", "one second unless the broker or the subscription sets one"],
  "filled": "full",
  "fewer": "fewer rows",
  "commit": "at commit"
};

# Waking

A subscription claims while its claims come back full, and otherwise waits its poll interval, which
a publish of the same broker or a notification cuts short.

<Diagram name="waking" words={words} />

A subscription claims when the runtime polls its stream. A claim that filled its limit is followed
by the next one at once, so a backlog drains without waiting. A claim that found fewer rows than it
asked for means the queue is empty for now, and the subscription waits. A claim that fails reaches
the stream as an error item, and the next one waits one second, so a database out of reach cannot
spin the loop.

A publish through the broker wakes the subscriptions of its table on the same broker: a
[`Repository`](api:ruststream_sqlx::Repository), a route, a reply, and a message a test publishes
through the harness. In a table with groups it wakes the subscriptions of its own group alone. A
subscription busy with a claim keeps the wake-up for its next wait, which then returns at once. The
wake-up costs a publish one atomic operation per subscription it wakes, and a subscription nothing
per message. A row the service writes itself, with its own SQL or through a handler's `Tx`, wakes
nobody: it waits for the poll interval, or for a notification.

## LISTEN/NOTIFY

On PostgreSQL, [`listen_notify`](api:ruststream_sqlx::SqlxBroker::listen_notify) makes subscriptions
in other processes wake too. The broker takes one connection of the pool at connect and keeps it
until shutdown, listening there. Each subscription listens on its table's channel before its first
claim: the table's name, qualified with its schema where it has one, as in `app.jobs`. Each publish
of the broker runs `SELECT pg_notify(channel, group)` on the connection that wrote the row, right
after the write; a notification with an empty payload wakes every subscription of the table. A
notification that fails is logged, and the publish still succeeds: the poll interval reaches the
row.

PostgreSQL delivers a notification when its transaction commits, so a woken subscription finds the
row visible. A notification sent while the listening connection was lost is gone: the broker
connects again and wakes every subscription once. Channel names keep 63 bytes, so a subscription to
a table whose qualified name is longer stops at startup. The switch costs one connection for the
broker's life and one more statement per publish. A transaction that notifies also takes a lock
global to the server at commit, so notifying commits run one after another, which limits many
concurrent writers. On another database `listen_notify` does not compile.

The server documents the mechanism on
[`NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html): delivery at commit, a payload
under 8000 bytes, and the 63-byte limit on
[identifiers](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS).
The commit lock is described by the PostgreSQL developers on the
[mailing list](https://www.postgresql.org/message-id/24269.1572279925%40sss.pgh.pa.us).
