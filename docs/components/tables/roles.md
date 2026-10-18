---
sidebar_position: 1
title: Roles
description: "Each #[field(..)] role turns on one behaviour of the queue: group, priority, partition key, retry_after, attempt, processed_at, lease, headers, payload."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Roles

Each `#[field(..)]` role turns on one behaviour of the queue: group, priority, partition key, `retry_after`, attempt, `processed_at`, lease, headers, payload.

A column that plays a role switches its behaviour on, and a table without it keeps the default:

| Role | With the role | Without it |
|---|---|---|
| `group` | a subscription reads the group its name selects; a [dead letter](../settlement/dead-letter.md) moves the row to another group | a subscription reads the whole table; a dead letter moves the row into another table |
| `group, fifo = true` | one row of a group is in work at a time, in claim order ([FIFO](../subscriptions/fifo.md)) | the rows of a group run side by side |
| `priority` | a smaller value is claimed first | rows are claimed by `retry_after` where the table has it, then by id |
| `partition_key` | the delivery's partition key: under [`workers_by_key(n)`](../subscriptions/workers.md) the rows of one key run in order | the delivery has no key |
| `retry_after` | [`retry_after(d)`](../settlement/retry-after.md) hides the row for `d` | `retry_after(d)` returns the row at once, with a warning |
| `attempt` | `Ctx<keys::Attempt>` reads the [count](../context/attempt.md), and `max_attempts(n)` caps it | `Ctx<keys::Attempt>` reads `None`, and a mount with `max_attempts(n)` stops at startup |
| `processed_at` | acknowledgement marks the row | acknowledgement deletes the row |
| `locked_until` | the [lease form](../forms/lease.md) | the row lock form, or the advisory lock form under `advisory_lock` |
| `headers` | the delivery's [headers](../headers/index.md), kept in one column | the delivery's headers are empty |
| `payload` | [payload mode](../messages/index.md): the handler takes the decoded payload | [row mode](../messages/rows.md): the handler takes the row |

Every role but `id` is optional, and each plays on one column at most.
`generated` beside a role marks a column the database fills in, such as a serial id or a column with a default, and the derive's [insert](./insert.md) leaves it out.

By hand, each role is a setter of [`InboxSpec`](./inbox-spec.md).
A role a delivery reads off the row also names its marker in `type Table` and implements a small trait: `Payload` and [`PayloadRow`](api:ruststream_sqlx::PayloadRow), `Key` and [`KeyRow`](api:ruststream_sqlx::KeyRow), `Attempt` and [`AttemptRow`](api:ruststream_sqlx::AttemptRow), `Headers` and [`HeaderRow`](api:ruststream_sqlx::HeaderRow).
The roles only the queue reads, such as `group`, `priority` and `processed_at`, need no field in the struct.

## Example

The billing queue of a support table takes urgent tickets first, keeps each customer's tickets in order on one worker, and hides an open ticket for five minutes; an answered ticket stays in the table with its `processed_at` set.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_tables_roles.rs#table"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_tables_roles.rs#table"
```

</TabItem>
</Tabs>
