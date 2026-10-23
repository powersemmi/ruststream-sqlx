---
sidebar_position: 2
title: dead_letter
description: "max_attempts(n) with dead_letter(..) moves a row whose attempts are spent into another group, or into a table with the same columns."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# dead_letter

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) with [`dead_letter(..)`](api:ruststream::runtime::Mounting::dead_letter) moves a row whose attempts are spent into another group, or into a table with the same columns.

The two steps come together at the mount site, and a mount that declares one without the other stops the service at [startup](../broker/startup.md).
The cap reads the row's `#[field(attempt)]` column, which counts the deliveries, the first one included; a cap on a table without it stops startup too.
A delivery that fails its `n`-th attempt moves the row instead of returning it: a table with a group column moves it to the `dead_letter` group, a table without one into the `dead_letter` table.
A dead-letter table has the same columns as the queue's; on MySQL and MariaDB the move is two statements in one transaction.
With `max_attempts(1)` every failure moves the row at once.
A [delayed retry](./retry-after.md) obeys the cap too.
The dead-letter group is an ordinary queue of the table: an `InboxQueue` of that name reads it, or an operator reviews it in place.
A [subscription by name](../subscriptions/names.md) refuses both steps at startup.
A table may move its rows with its own SQL instead, through the `DeadLetter` event ([Events](../tables/events.md)).

## Example

A payment the card network keeps declining is tried three times, then moves to the `payments.failed` group.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_settlement_dead_letter.rs#dead_letter"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_settlement_dead_letter.rs#dead_letter"
```

</TabItem>
</Tabs>
