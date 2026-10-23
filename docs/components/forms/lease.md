---
sidebar_position: 1
title: Lease
description: "#[field(locked_until)] selects the lease form: the claim writes an expiry and commits, the handler holds no connection, and the lease is extended."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Lease

`#[field(locked_until)]` selects the lease form: the claim writes an expiry and commits, the handler holds no connection, and the lease is extended.

A claim writes the lease's expiry into `locked_until`, counts the attempt and commits at once, so no transaction stays open while the handler runs.
The claim reads "now" once: the rows it finds due, the leases it finds ended and the expiry it writes start from that instant.
The expiry is the delivery's ownership token: a settlement takes effect only while the row still holds it.
While the handler runs, the subscription extends the lease of every delivery in work each half lease, and each extension moves the token forward, so a handler may run longer than its lease.
A lease is how long a crashed process's row stays out of the queue: thirty seconds unless [`SqlxBroker::lease`](api:ruststream_sqlx::SqlxBroker::lease) sets another for the broker or [`InboxQueue::lease`](api:ruststream_sqlx::InboxQueue::lease) for one subscription.
It is whole seconds, at least one, and a shorter one is rounded up.
A lease runs out under a running handler only when its extensions stop: the database out of reach, the subscription closed, the broker shut down.
The next claim then takes the row, and the late holder's settlement fails with [`SqlxBrokerError::LeaseLost`](api:ruststream_sqlx::SqlxBrokerError::LeaseLost).
A delivery dropped unsettled releases its row at once; a settlement whose statement fails keeps the row until its lease runs out, as a crash does.
The claim counts the attempt, so a crash spends one too, and a delivery reports the [attempt](../context/attempt.md) as it stood before its claim.
After the broker shuts down, a delivery in work keeps its lease and settles as before, and the lease is no longer extended.
The crate computes the expiry on the host, so the form reads the host's [clock](../time/index.md): `locked_until` beside `clock = DatabaseClock` does not compile.
The column holds a [time type](../time/queue-time.md), `Option<DateTime<Utc>>` here.
By hand, `.lease(..)` names the column, and `Lease<Time>` in the description's type gives the lease's time type ([`LeaseRow`](api:ruststream_sqlx::LeaseRow)).

## Example

A report takes minutes to render under a one-minute lease, which the subscription extends while the handler runs.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_forms_lease.rs#lease"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_forms_lease.rs#lease"
```

</TabItem>
</Tabs>
