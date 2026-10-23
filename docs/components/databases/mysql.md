---
sidebar_position: 2
title: MySQL
description: "mysql serves MySQL 8.0.1 and MariaDB 10.6 or later in every form, READ COMMITTED by default; index the claim order so a claim reads its own group."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MySQL

The `mysql` feature serves MySQL 8.0.1 and MariaDB 10.6 or later in every form, READ COMMITTED by default; index the claim order so a claim reads its own group.

Claims pass over locked rows with `SKIP LOCKED`, which [MySQL 8.0.1](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html) and [MariaDB 10.6](https://mariadb.com/kb/en/select/#skip-locked) added, and an advisory table shares that floor.
A subscription reads the server's version when it opens, and an older server stops it with [`SqlxBrokerError::ServerTooOld`](../broker/errors.md).
A claim's transaction runs at READ COMMITTED unless a row lock table declares another [level](../transactions/isolation.md).
At READ COMMITTED a claim held for a handler locks no gaps between rows and holds back no insert into its table.
A server whose binary log records statements (`binlog_format = STATEMENT`) [refuses writes](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html) at that level; the row and mixed formats accept them.

A [lease](../forms/lease.md) claim selects its rows, stamps each one with its lease and commits.
The expiry is a whole second, so a `DATETIME` column without fractions holds the token exactly.
A [dead letter](../settlement/dead-letter.md) into a table is two statements in one transaction.
A claim of the service's own comes with a [`Fetch`](../headers/fetch.md) of its own, because the crate reads rows by a list of ids on Postgres alone; a subscription without one stops at startup.
The [advisory lock form](../forms/advisory.md) takes `GET_LOCK`, whose names hold [64 characters](https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html) at most.

Index a table's claim order: `(group, priority, retry_after, id)`, for the columns the table has.
A row lock or lease claim locks every row of its group that it reads before sorting them.
Without that index a claim held for a handler keeps the rest of its group from other claims, and in the row lock form the rows of a partition key go into work out of order.
In a table with FIFO groups a claim first locks the unfinished rows of its group until its transaction ends, and the same index keeps that read to the group's own rows.
A row lock table with FIFO groups that declares `serializable` stops its subscription when it opens, with `SqlxBrokerError::Dialect`: at that level InnoDB locks every row a read touches, and a claim would wait for the group's row in work.

## Example

Two groups share one table in the lease form, and the index `(name, id)` keeps each claim to its own group's rows.
The tests run the same service on MySQL and on MariaDB.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_databases_mysql.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_databases_mysql.rs#service"
```

</TabItem>
</Tabs>
