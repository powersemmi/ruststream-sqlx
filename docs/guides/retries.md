---
sidebar_position: 3
description: "Retry a job now or later with a backoff of the handler's own, cap the attempts and move what keeps failing to a dead-letter group."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead letters

This guide retries a job now or later, caps its attempts and parks what keeps failing in a [dead-letter group](../components/settlement/dead-letter.md) of the same table.

A retry here is a statement on the job's row.
The row keeps its id and its place in the table, counts one more attempt, and a delay is a time in one of its columns.
Nothing lives only in the process: a job waiting for its retry survives a restart.

The example is a mailer that sends emails through a provider.

## The table

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/retries.rs#schema"
```

`name` is the [group](../components/tables/roles.md) a row waits in: the subscription to `emails` reads the rows whose `name` is `emails`.
`attempt` counts the deliveries of a row, the first one included.
`retry_after` is the time a row becomes due; its default makes a new row due at once.

## The subscriber

The provider refuses an email in three ways, and each needs a different outcome.
`Busy` means its rate limit is spent: a retry right now would be refused again, so the email waits.
`Unavailable` means it did not answer: `retry` tries again at once.
`Rejected` means the mailbox does not exist, and another try gets the same answer, so the email is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#send"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#send"
```

</TabItem>
</Tabs>

The handler owns the backoff.
It reads the delivery's [attempt](../components/context/attempt.md) and asks for a second more with each one: [`retry_after(d)`](../components/settlement/retry-after.md) writes "now" plus `d` into the row's `retry_after`, and no claim takes the row before then.
A table without that column returns the row at once, and the runtime logs a warning.
[`retry()`](../components/settlement/index.md) returns the row to the queue at once; its next delivery reads one more attempt.
`drop()` finishes the row as an acknowledgement does, so a rejected email leaves the table.
A row whose payload does not decode spends its attempts like any other failure ([Decoding](../components/messages/decoding.md)).

## The cap and the dead-letter group

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) reads the row's `attempt`.
The fifth delivery that fails is not retried: [`dead_letter`](../components/settlement/dead-letter.md) moves the row into the `emails.dead` group, payload and attempt as they were.
A delayed retry counts against the cap as an immediate one does.
The two steps come together: a mount that declares one without the other stops the service at startup.
A table without a group column takes a table name in `dead_letter` instead, and the row moves into that table, which has the same columns.

## Reading the dead letters

The dead-letter group is an ordinary queue of the table, and `review` reads it like any other.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`review` logs the email for a person to look at and acknowledges it.
A service that keeps its dead letters for an operator mounts no subscription on the group; the rows wait in the table.
Sending them again is one statement of the service's own, which moves them back and resets their count:

```sql
UPDATE mail_jobs SET name = 'emails', attempt = 1, locked_until = NULL WHERE name = 'emails.dead';
```

In a table with [FIFO groups](../components/subscriptions/fifo.md), `retry()` keeps a row at the head of its group, and `retry_after(d)` lets the rows due earlier pass it ([Ordering](ordering.md)).

## Testing

The tests in the example file publish an email through the harness, as the service's own `Publish` writes it.
A rejected address is dropped at once, which [`TestApp::start`](../components/testing/index.md) shows when the publish returns.
An unanswered email and a busy provider need time to pass: the delayed row and the dead letter come back on the subscriptions' next poll.
Those tests run on the real clock with [`TestApp::start_live`](../components/testing/live.md), with a backoff of one second, and `tb.advance` waits it out.
[Testing a service on SQL](testing.md) covers the harness.
