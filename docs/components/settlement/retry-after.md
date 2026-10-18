---
sidebar_position: 1
title: retry_after
description: "retry_after(d) hides the row until its retry_after column comes due; a table without the column returns the row at once and logs a warning."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(d)`](api:ruststream::runtime::HandlerOutcome::retry_after) hides the row until its `retry_after` column comes due; a table without the column returns the row at once and logs a warning.

The settlement writes "now" plus `d` into the row's `#[field(retry_after)]` column, and the row's next delivery reads one more attempt.
A claim takes only rows whose `retry_after` has come, so the row waits in the table, with no timer in the service and nothing lost if the process stops.
The column holds a [time type](../time/queue-time.md), and "now" comes from the table's [clock](../time/index.md).
A table without the column returns the row at once, and the runtime logs a warning.
The backoff is the handler's: it reads the delivery's [attempt](../context/attempt.md) and answers with the delay it wants.
A delayed retry counts against a [retry cap](./dead-letter.md) as an immediate one does.
In a [FIFO group](../subscriptions/fifo.md) the delayed row moves behind the rows due before it.
In process the queue reads "now" off the tokio clock, so a test on a paused clock brings the row due with `tb.advance(d)`.

## Example

An endpoint that answered "too many requests" is called again a minute later.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_settlement_retry_after.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_settlement_retry_after.rs#retry_after"
```

</TabItem>
</Tabs>
