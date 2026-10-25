---
sidebar_position: 10
description: "Test a service on SQL with TestApp: the production app against SQLite in memory, delays on a paused clock, and the same test against Postgres."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a service on SQL

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp) against a real database: SQLite in memory, or a database of the test's own on a server.

A queue's behaviour lives in its SQL: the claim, the settlement, the service's own `Publish`.
So the harness has no model of a database.
Its [in-process mode](../components/testing/index.md) runs the service's statements against the database the test hands the service, and keeps the books the harness waits on.

The service under test makes thumbnails.
A small image is made at once.
The resizer takes a large image only on a second try, so the first delivery of a large image is put off for a minute with [`retry_after`](../components/settlement/retry-after.md).
The app is built by `service(url)`, so a test chooses the database and `main` uses the production one.
The tests need the `testing` feature of `ruststream-sqlx` in `[dev-dependencies]`.

## A small image

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

`database` creates a SQLite database in memory that every connection naming it shares.
It lives while one of those connections is open, so the test holds one beside the pool it reads the table through.
The service creates the table when it starts, as in production.
[`TestApp::start`](api:ruststream::testing::TestApp::start) connects the production app to it in process, and the test addresses the broker by its production type, `tb.broker::<SqlxBroker<Sqlite>>()`.

The image goes in through the route and the service's own `Publish`, as in production.
`publish` returns once the handler it woke has settled, so the lines after it read finished state: how `thumbnail` settled the row, and what the test's own query finds in the table.
A row the test writes with its own SQL is counted only once a claim takes it, so `settle` does not wait for it; such a test runs on the real clock and lets the poll interval pass, as [Enqueueing jobs](enqueueing.md) does.
The scenario takes the broker's handle as an argument, so the Postgres test below runs it too.

A table without a payload column hands its handler the row itself ([Rows](../components/messages/rows.md)).
A test of such a handler checks the row with `with_value` and `received_values` in place of `with` and `received`.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#clock"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#clock"
```

</TabItem>
</Tabs>

In process the queue reads "now" from the tokio clock, anchored to the system clock when the broker connected.
The test pauses that clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it: the large image's row comes due a minute later, and the second delivery makes the thumbnail.
The minute costs nothing.
The database calls run off the paused clock, so it does not jump while the database answers.
A lease runs out the same way when the clock passes it.
A table on the [database's clock](../components/time/database-clock.md) reads the database's time, which the test clock does not move, and a [clock of the service's own](../components/time/clock.md) stays in force; tests of those run on the real clock.

## The same test on Postgres

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

A service that runs on Postgres in production is tested on Postgres too: SQLite runs other SQL, and has no row locks, no notifications and no isolation levels to choose.
[`TestApp::start_live`](../components/testing/live.md) connects the app the ordinary way and waits on the real clock.
The test takes a database of its own on the server, where the service creates its table, so nothing an earlier run left is read; `live::postgres::database` is a helper of the repository's tests, and [`#[sqlx::test]`](https://docs.rs/sqlx/0.9/sqlx/attr.test.html) gives a test the same, a database with the migrations applied.
The test reads the server's address from `POSTGRES_TEST_URL` and skips without it, so `cargo test` passes on a machine without a server; `RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a failure where the server must be.
The scenario is the in-process one, unchanged.

```bash
cargo test
POSTGRES_TEST_URL=postgres://user:password@localhost:5432/postgres cargo test
```

## The outbox in tests

A test build leaves the [outbox](outbox.md) off unless `RUSTSTREAM_SQLX_OUTBOX=on` turns it on, so the service's own tests run its handlers untracked.
A test of the tracking itself runs with the switch set and skips without it ([Switch](../components/outbox/switch.md)).
