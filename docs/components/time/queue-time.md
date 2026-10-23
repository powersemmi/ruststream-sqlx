---
sidebar_position: 3
title: QueueTime
description: "A time column holds chrono::DateTime<Utc> or time::OffsetDateTime; SQLite compares times as text, exact for chrono and to the second for time."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# QueueTime

A time column holds `chrono::DateTime<Utc>` or `time::OffsetDateTime`; SQLite compares times as text, exact for `chrono` and to the second for `time`.

[`QueueTime`](api:ruststream_sqlx::QueueTime) is the type a `retry_after`, `processed_at` or `locked_until` column holds: `chrono::DateTime<Utc>` under the `chrono` feature, `time::OffsetDateTime` under `time`.
A field of such a role is the time or an `Option` of it ([`TimeColumn`](api:ruststream_sqlx::TimeColumn)), and an optional one holds `NULL` until the crate writes it.
The crate reads "now" and moves it forward in the column's own type, so sqlx encodes it exactly as the service writes times itself.
SQLite keeps times as text, and a claim compares them as text.
The text sqlx writes for a `chrono` time, RFC 3339 with `+00:00`, sorts as the times themselves.
The text it writes for a `time` value ends in `Z` and drops the fraction's trailing zeros, so it sorts two times right only when they fall in different seconds.
On SQLite a lease of a `time` table may therefore end up to a second late, and a delayed retry may come back up to a second early or late.
By hand, the time type is a parameter of the description's markers, as in `Lease<OffsetDateTime>` and `ProcessedAt<OffsetDateTime>`.

## Example

A backup queue keeps its lease and its mark in the `time` crate's type, and the mark reaches the table as sqlx writes a `time` value.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_time_types.rs#queue_time"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_time_types.rs#queue_time"
```

</TabItem>
</Tabs>
