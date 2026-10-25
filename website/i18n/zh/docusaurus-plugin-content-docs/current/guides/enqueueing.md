---
title: 任务入队
sidebar_position: 2
description: "在与任务所服务的数据相同的事务中写入任务，把任务排期到以后，优先领取紧急任务，并保留已完成的任务以备审计。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 任务入队

本指南向队列表写入任务：与任务所服务的数据在同一个事务中写入、排期到以后执行，以及按优先级执行。

与独立的 Broker 相比，服务数据库中的队列有一个优势：任务与它所服务的数据一起提交。回滚的订单不会留下确认邮件，没有保存的订单也绝不会发出邮件。

示例是一个网店。结账订阅负责下单，邮件订阅立即发送订单确认，并在三天后发送评价邀请。

## 表 {/* #the-table */}

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/enqueueing.rs#schema"
```

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/enqueueing.rs#table"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/enqueueing.rs#table"
```

</TabItem>
</Tabs>

每一列开启队列的一种行为（[角色](../components/tables/roles.md)）。`retry_after` 保存一行到期的时间：领取只取时间已到的行。`priority` 决定领取的顺序：值越小越早，然后是 `retry_after` 较早的行，最后是 id 较小的行。`processed_at` 保留已完成的行：确认把时间写入这一列，而不删除该行。`generated` 标记由数据库填充的列，因此插入语句不包含这些列。在手写形式中，服务以 [`Insert`](../components/tables/insert.md) 实现自己编写这条插入语句。

## 订单事务中的任务 {/* #a-job-in-the-orders-transaction */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/enqueueing.rs#place_order"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/enqueueing.rs#place_order"
```

</TabItem>
</Tabs>

`place_order` 打开一个 sqlx 事务，在其中写入三行：订单、确认邮件和提醒邮件。派生宏生成的 [`insert`](../components/tables/insert.md) 接受该表所在数据库的任意连接，因此 `&mut *tx` 把任务放进订单的事务。它们要么一起提交，要么一起回滚：插入失败的订单不会在表中留下邮件。下单的 HTTP 端点用服务自己的连接池调用同一个函数；端点和队列共用一个连接池的服务，把它交给 [`SqlxBroker::new`](../components/broker/index.md)。

确认邮件现在就到期，而且紧急。提醒邮件三天后到期，在那之前没有领取会取走它。周期性任务也是同样的做法：它的处理器插入下一行，并设置更晚的 `retry_after`。只能存在一次的任务，则由服务自己的唯一索引加上一条单独的 `INSERT .. ON CONFLICT DO NOTHING` 语句来保证。

## 订阅 {/* #the-subscription */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/enqueueing.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/enqueueing.rs#service"
```

</TabItem>
</Tabs>

`checkout` 从上下文中取出[连接池](../components/context/pool.md)并下单；下单失败的订单以一条警告被丢弃。它自己的事务与结账行的领取相互独立：写入必须与所在行的结算一起提交的处理器，应以[事务模式](transactional.md)运行。`send` 按领取顺序读取 `email_jobs` 的每一行。它的确认用 `processed_at` 标记该行，因此已发送的邮件留在表中以备审计，领取会跳过它们。没有这一列的表在确认时删除该行。

## 任务何时被发现 {/* #when-a-job-is-seen */}

经 Broker 的发布会立即[唤醒](../components/wake/index.md)同一进程中订阅了该表的订阅：[路由](../components/publishing/route.md)、[`Repository`](../components/publishing/index.md)、应答或 `Out` 槽位都是如此。前端发布的结账请求就以这种方式唤醒 `checkout`。服务用自己的 SQL 写入的行，例如 `place_order` 写入的行，不唤醒任何订阅。订阅在它的[轮询间隔](../components/subscriptions/poll-interval.md)之后发现这一行，未设置时为一秒。Broker 的发布不加入服务的事务，所以必须与订单一起提交的任务通过 `insert` 写入，并等待这个间隔。在 Postgres 上，由服务或触发器发出的[通知](../components/wake/listen-notify.md)会提前结束等待。

## 测试 {/* #testing */}

示例文件中的测试通过测试框架发布结账请求，然后检查表和处理器。邮件行在下一次轮询时到达它的订阅，所以等待邮件的测试用 [`TestApp::start_live`](../components/testing/live.md) 启动应用，并用 `tb.advance` 让两秒过去。一个测试检查确认邮件已经发出，而提醒邮件仍在等待。一个测试把同一个订单下两次，发现只有第一次下单的邮件。一个测试在服务启动之前写入一封普通邮件和一封紧急邮件，检查紧急邮件先发出。[在 SQL 上测试服务](testing.md)介绍了测试框架。
