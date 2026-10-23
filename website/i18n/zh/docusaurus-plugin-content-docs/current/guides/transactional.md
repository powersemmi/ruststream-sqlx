---
title: 数据库内的精确一次：事务处理器
sidebar_position: 6
description: "在一个事务中把处理器的写入与任务的结算一起提交，在其中串联后续任务，并让数据库之外的效果保持幂等。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 数据库内的精确一次：事务处理器

本指南把处理器的写入与任务的结算一起提交，因此效果是同一数据库中的行的任务，恰好生效一次。

队列把每个任务至少投递一次。处理器写入结果后、任务结算前崩溃，就会再次运行，把结果写两遍。当结果是保存队列的那个数据库中的行时，一个事务就能消除这种重复：写入和结算要么一起提交，要么都不提交。

示例是一个退款服务。一次退款写入它的记录和一个交给发信服务的收据任务，并调用银行卡处理商。

## 表 {/* #the-tables */}

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/transactional.rs#schema"
```

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/transactional.rs#tables"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/transactional.rs#tables"
```

</TabItem>
</Tabs>

`refund_jobs` 是本服务读取的队列。`receipt_jobs` 是发信服务的队列：本服务只向其中写入，用派生宏生成的 [`insert`](../components/tables/insert.md)；在手写形式中，用它自己的一条语句。

## 处理器 {/* #the-handler */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/transactional.rs#handler"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/transactional.rs#handler"
```

</TabItem>
</Tabs>

挂载处的 [`.transactional()`](../components/transactions/index.md) 把投递的事务借给处理器。处理器以第一个 `Ctx` 参数 `Ctx<keys::Tx<Sqlite>>` 接收它，并通过 `&mut *tx` 写入。在没有这个步骤的订阅上接收 `keys::Tx` 的处理器无法编译。

确认在一个事务中提交退款记录、收据任务和退款行的结算。其他任何结果都先回滚这些写入，再结算该行；重试仍然计入这次尝试。panic 什么也不提交。同一订单的第二次退款在 `refunds` 的主键上失败并被丢弃，因此进入队列的重复任务不会改变任何东西。

收据任务构成一条链：只有退款提交之后，发信服务才能看到它，回滚的退款则永远不会产生收据。通过 [`Ctx<keys::Pool>`](../components/context/pool.md) 的写入单独提交，与结果无关，经 Broker 的发布也是如此；两者都不加入这个事务。

## 留在事务之外的部分 {/* #what-stays-outside */}

银行卡处理商在数据库之外，因此没有事务能覆盖它。处理商已经退款之后的重试会再次调用它，所以调用带上幂等键，这里是订单，处理商只退款一次。数据库之外的每种效果都以同样的方式保持至少一次：一封邮件、一次 HTTP 调用、一次向另一个 Broker 的发布。[把事件中继到另一个 Broker](relay.md) 和 [outbox](outbox.md) 两页处理发布。

在 SQLite 上，投递的事务在第一次写入时获取数据库的写锁，一直持有到投递结算，在此期间其他所有写入者都无法进入，领取也包括在内。这里处理商的调用在两次插入之后运行，所以调用期间数据库对其他写入者保持锁定。服务器只锁住事务写入的行。在 SQLite 上，发布 [outbox](outbox.md) 所跟踪消息的处理器，要在通过 `tx` 第一次写入之前发布：记录在自己的连接上写入，否则它要等待投递的写锁。

## 隔离级别 {/* #isolation */}

每个投递的事务按表的[隔离级别](../components/transactions/isolation.md)打开，在服务器上是 `#[inbox(isolation = read_committed)]`；在 SQLite 上则按它的模式打开，即 `#[inbox(mode = immediate)]`。两者都未声明的表采用数据库的默认级别，在 MySQL 和 MariaDB 上是 READ COMMITTED。每个处理中的投递都为它的事务占用一个连接，所以通过 `keys::Pool` 或一次发布再取一个连接的处理器，需要比它的 `workers(n)` 更大的连接池。事务模式服务于逐条投递；以这种模式挂载的批处理器无法编译。在 MySQL 和 MariaDB 上，处理器若中途截断一条通过 `tx` 执行的语句，例如用 `select!` 包住一次查询，可能扰乱随后的结算，这是 sqlx-mysql 0.9.0 的一个缺陷：让每条语句执行完。

## 测试 {/* #testing */}

示例文件中的测试用 [`TestApp::start`](../components/testing/index.md) 在内存中的 SQLite 上运行服务，每次发布后读取这些表。一次退款提交一条记录和一张收据。第一次调用处理商超时的退款会重试，第二次尝试留下一条记录和一张收据，而不是两条。同一笔退款发布两次，第二次被丢弃。
