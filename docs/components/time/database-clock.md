---
sidebar_position: 1
title: DatabaseClock
description: "clock = DatabaseClock makes the statements read the database's clock, so every host serving one table reads one clock; the lease form keeps the host's."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DatabaseClock

`clock = DatabaseClock` makes the statements read the database's clock, so every host serving one table reads one clock; the lease form keeps the host's.

With [`DatabaseClock`](api:ruststream_sqlx::DatabaseClock) the statements bind no time: each reads the database's own.
Postgres reads `statement_timestamp()`, MySQL and MariaDB `UTC_TIMESTAMP(6)`, and SQLite the current time as text.
Every host that claims, delays or marks a row of the table then reads one clock, whatever its own says.
The [lease form](../forms/lease.md) reads the host's clock, because each settlement names the expiry its claim wrote, so `locked_until` beside `clock = DatabaseClock` does not compile.
A table on the database's clock takes the row lock or the advisory lock form; on SQLite, which has no row locks, the [advisory lock form](../forms/advisory.md).
In process too the statements read the database's clock, which a paused tokio clock does not move, so a test of a delayed retry on such a table runs on the real clock with `TestApp::start_live`.
By hand, `.clock::<DatabaseClock>()` names it, and `Clock<DatabaseClock>` goes into the description's type.

## Example

Statements written by hosts whose clocks drift wait a second by the database's clock when the month's figures are not closed yet.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_time_database.rs#database_clock"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_time_database.rs#database_clock"
```

</TabItem>
</Tabs>
