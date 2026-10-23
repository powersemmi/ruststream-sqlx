---
sidebar_position: 2
title: Clock
description: "A clock of the service's own implements Clock and takes the place of SystemClock in clock = .. or .clock::<..>(), for every statement that reads now."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Clock

A clock of the service's own implements [`Clock`](api:ruststream_sqlx::Clock) and takes the place of `SystemClock` in `clock = ..` or `.clock::<..>()`, for every statement that reads "now".

`Clock` has one function, `now() -> SystemTime`, and the crate turns its answer into each column's own type.
`#[inbox(clock = WholeSeconds)]` names the clock on the derive; by hand `.clock::<WholeSeconds>()` names it, and `Clock<WholeSeconds>` goes into the description's type.
The clock is a host clock, so every form takes it, the [lease form](../forms/lease.md) included.
Such a clock fits a service whose other tables keep times in a form of their own, whole seconds here, or a host that has to read a time other than its system clock's.
A clock that is a type parameter of the struct is not taken by [`checked`](../tables/checked.md).
In process the table reads its own clock, as in production, and a paused tokio clock does not move it.

## Example

The reminders' table reads the host's time in whole seconds, and an acknowledgement marks the row with it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_time_clock.rs#clock"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_time_clock.rs#clock"
```

</TabItem>
</Tabs>
