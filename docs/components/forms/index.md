---
title: Forms
description: "A table takes rows by row lock unless it names another form: FOR UPDATE SKIP LOCKED in a transaction held while the handler runs."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Forms

A table takes rows by row lock unless it names another form: `FOR UPDATE SKIP LOCKED` in a transaction held while the handler runs.

A table takes its rows in one of three forms, and its description decides which:

| Form | Selected by | What holds a row in work | After a crash | Costs |
|---|---|---|---|---|
| row lock | nothing: the default | the claim's transaction | the row returns at once | a connection per message in work |
| [lease](./lease.md) | `#[field(locked_until)]` | an expiry written into the row | the row returns when the lease runs out | statements to claim, extend and settle |
| [advisory lock](./advisory.md) | `advisory_lock = ".."` | a lock on the row's key | the row returns at once | a connection per message in work |

A row lock claim locks each row with `FOR UPDATE SKIP LOCKED` in a transaction that stays open while the handler runs.
Rows that another claim holds are passed over, so claims of several workers and processes never wait for each other.
The settlement's statement, a delete or the `processed_at` mark, commits that transaction, so a row and its outcome change together.
The rows of a crashed process return at once, when the database rolls its transactions back.
The price is a connection of the pool per message in work, held for the whole handler ([Connections](../broker/connections.md)).
Postgres takes the form from 9.5, MySQL from 8.0.1 and MariaDB from 10.6; SQLite has no row locks, so a subscription to a SQLite table that names no other form does not compile.
On Postgres `read_committed` is the practical [isolation level](../transactions/isolation.md) for the form: a claim at a stricter level fails with a serialization error when claims and settlements of its rows run at once.
By hand, a description that calls neither `.lease(..)` nor `.advisory(..)` takes the row lock form.

## Example

Four workers issue invoices on Postgres; each holds its row's lock and a connection until its acknowledgement commits.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_forms_row_lock.rs#row_lock"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_forms_row_lock.rs#row_lock"
```

</TabItem>
</Tabs>
