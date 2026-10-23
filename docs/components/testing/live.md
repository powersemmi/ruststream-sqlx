---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body on the real clock, against a database of the test's own on Postgres, MySQL or MariaDB."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body on the real clock, against a database of the test's own on Postgres, MySQL or MariaDB.

It connects the broker through its ordinary connect, and the queue reads the clock production reads.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it, so `tb.advance(d)` lets `d` of real time pass.
`start_live` refuses a paused clock.

The live mode is for what the in-process mode leaves to the clock and to the server: a table on the [database's clock](../time/database-clock.md), a [clock of the service's own](../time/clock.md), the [row lock form](../forms/index.md), [isolation levels](../transactions/isolation.md), [`listen_notify`](../wake/listen-notify.md), and the [MySQL and MariaDB](../databases/mysql.md) specifics.
Each test takes a database of its own on the stand and applies the service's migrations, so nothing an earlier run left is read.

The repository's live tests read the stand's address from `POSTGRES_TEST_URL`, `MYSQL_TEST_URL` or `MARIADB_TEST_URL`, and return early without one, so `cargo test` passes on a machine with no stand.
A job that stood the stand up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing address then fails the test instead of skipping it.
The stand is `docker-compose.test.yml`, which `just brokers-up` starts.
The examples of the [Postgres](../databases/postgres.md) and [MySQL](../databases/mysql.md) pages carry such tests.

## Example

The two tests share one body: one in process on a paused clock, one on the real clock, both over SQLite in memory.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_testing_live.rs#start-live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_testing_live.rs#start-live"
```

</TabItem>
</Tabs>
