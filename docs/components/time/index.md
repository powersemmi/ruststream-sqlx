---
title: Time
description: "A queue reads \"now\" to claim, delay and mark rows; by default the host's SystemClock, bound in the column's own type as the service's inserts do."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Time

A queue reads "now" to claim, delay and mark rows; by default the host's [`SystemClock`](api:ruststream_sqlx::SystemClock), bound in the column's own type as the service's inserts do.

A claim compares "now" with `retry_after` and with the leases in work, a [delayed retry](../settlement/retry-after.md) writes "now" plus its delay, and an acknowledgement writes "now" into `processed_at`.
A table that names no clock reads the host's system clock.
The crate binds the time in the column's own [type](./queue-time.md), so sqlx encodes it exactly as the service's own statements write times.
The clocks of the hosts that serve one table are kept in step, well within a lease; keeping them in step is the hosts' concern, as NTP does.
Where the hosts' clocks cannot be trusted, [`DatabaseClock`](./database-clock.md) makes the statements read the database's clock instead.
A clock of the service's own takes the same place ([Clock](./clock.md)).
Both are [`TimeSource`](api:ruststream_sqlx::TimeSource)s: a host clock binds "now" into the statement, and the database's clock lets the statement read its own.
In process a table on the system clock reads the tokio clock, anchored to the system clock when the broker connected.
So a test on a paused clock moves the queue's time with `tb.advance(d)`: rows come due and marks move with it.

## Example

An invoice the mail server cannot take returns in an hour by the host's clock, and a sent one is marked with the host's time.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_time.rs#time"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_time.rs#time"
```

</TabItem>
</Tabs>
