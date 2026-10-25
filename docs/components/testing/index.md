---
title: Testing
description: "TestApp::start runs the service against a real database, SQLite in memory for a start, with the harness waiting on every publish and a pausable clock."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) runs the service against a real database, SQLite in memory for a start, with the harness waiting on every publish and a pausable clock.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<SqlxBroker<Sqlite>>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode and turns on the core's harness with it.
The crate registers `SqlxBroker<Postgres>`, `SqlxBroker<MySql>`, `SqlxBroker<Sqlite>` and `SqlxBroker<Any>`; a broker on a [dialect of the service's own](../databases/dialect.md) is registered by the service.

The in-process mode connects to the database the test's pool reaches, as `connect` does, because the service's SQL is part of what the test checks.
SQLite needs no server: a database in memory named in a shared-cache URL serves every connection that names it, and lives while one of them is open, so the test holds one beside the pool.
A test on Postgres, MySQL or MariaDB takes a database of its own on a server, with the service's migrations applied.

The harness counts what reaches a queue and waits for it.
A test's input goes in through the route and the service's [`Publish`](../publishing/index.md), as in production, and so do the service's own replies and slots.
A publish through the harness returns once every handler it woke has settled, so the assertions that follow read finished state.
A row is counted for the queue it lands in, its table and group, whatever name the subscription reads it by.
A message published before its subscription opened waits in the table and is delivered once it opens.
A row the service wrote with its own SQL before the test, a seed or a migration, is counted when its subscription opens, so `settle` waits for it as for a published message.
A row written with the service's own SQL while the test runs is counted only once a claim takes it, so `settle` does not wait for it: such a test moves the clock past the [poll interval](../subscriptions/poll-interval.md) with `tb.advance`.

The queue reads "now" from a clock the test controls.
After `tokio::time::pause()`, `tb.advance(d)` moves the queue's time by `d`, and a lease runs out or a [delayed retry](../settlement/retry-after.md) comes back when the clock passes it.
The database calls run off that clock, so a paused clock does not jump while the database answers.
A table on the [database's clock](../time/database-clock.md) reads the database's time, which the test clock does not move, and a [clock of the service's own](../time/clock.md) stays in force.
Such a test, and one that needs what only a server has, runs [live](./live.md).

A test build leaves the [outbox](../outbox/index.md) off unless the [switch](../outbox/switch.md) turns it on.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-sqlx = { version = "0.7", features = ["inbox", "sqlite", "chrono", "testing"] }
```

The first test publishes an image and reads how the handler settled it: the first attempt puts it off for a minute.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_testing.rs#testapp"
```

</TabItem>
</Tabs>

The second pauses the clock and moves it past the minute: the row comes back, and the second attempt makes the thumbnail.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_testing.rs#clock"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_testing.rs#clock"
```

</TabItem>
</Tabs>
