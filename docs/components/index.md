---
sidebar_position: 0
description: "ruststream-sqlx serves task queues in the service's own SQL tables and tracks what a service publishes on any broker in an outbox."
---

# Components

The parts of `ruststream-sqlx` a service uses, one page per component: the inbox broker
[`SqlxBroker`](./broker/index.md), the [queue tables](./tables/index.md) it serves, the
[messages](./messages/index.md) and [headers](./headers/index.md) a row hands its handler, the
[subscriptions](./subscriptions/index.md) and the [forms](./forms/index.md) they claim rows in,
[settlement](./settlement/index.md), [transactions](./transactions/index.md), the
[context](./context/index.md) and [time](./time/index.md) of a delivery,
[publishing](./publishing/index.md) and [waking](./wake/index.md) a subscription, the
[databases](./databases/index.md), the [AsyncAPI](./asyncapi/index.md) document, the
[tests](./testing/index.md), and the transactional [outbox](./outbox/index.md).

The crate has two halves.
The inbox keeps a task queue in a table of the service's own database: a subscription claims rows, a handler's outcome settles them, and a publish writes a row through the service's own SQL.
A task can then commit in the same transaction as the data it serves.
The outbox is a plugin over any RustStream broker: it records what a service publishes in a table of its own until a consumer has processed it, and publishes again at startup what was not processed.
The outbox needs no inbox, and a service may use both.

Every feature is off by default and only adds to the crate.
`inbox` turns the broker on and `outbox` the outbox.
`postgres`, `mysql` (MySQL and MariaDB) and `sqlite` each add a built-in dialect and its sqlx driver.
`any` serves an `AnyPool`, `chrono` or `time` the time columns, and `json` a headers column of JSON.
`testing` adds the in-process mode a [`TestApp`](api:ruststream::testing::TestApp) test connects, and the outbox's test switch.
`asyncapi` adds the table a subscription reads to the generated document.
`any`, `chrono`, `time` and `asyncapi` turn `inbox` on with them.

Beside the core's required traits, the broker serves batches of rows ([`BatchSubscriber`](api:ruststream::BatchSubscriber)) and lends a handler in row mode the row itself ([`Carries`](api:ruststream::Carries) and [`CarriesBatch`](api:ruststream::CarriesBatch)).
Its default publisher is the route table, [`Routed`](api:ruststream_sqlx::Routed), so a reply or an `Out` slot without a policy of its own writes a row through a route.
