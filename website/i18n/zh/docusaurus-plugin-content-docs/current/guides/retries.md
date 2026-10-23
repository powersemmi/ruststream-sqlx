---
title: 重试与死信
sidebar_position: 3
description: "用处理器自己的退避策略立即或稍后重试任务，限定尝试次数，并把一直失败的任务转入死信组。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信

本指南立即或稍后重试一个任务，限定它的尝试次数，并把一直失败的任务存放到同一张表的[死信组](../components/settlement/dead-letter.md)中。

在这里，一次重试就是对任务所在行执行的一条语句。该行保留它的 id 和它在表中的位置，尝试次数加一，延迟则是它某一列中的一个时间。没有任何东西只存在于进程中：等待重试的任务在重启后依然存在。

示例是一个通过邮件服务商发送邮件的发信服务。

## 表 {/* #the-table */}

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/retries.rs#schema"
```

`name` 是一行所在的[组](../components/tables/roles.md)：对 `emails` 的订阅读取 `name` 为 `emails` 的行。`attempt` 统计一行的投递次数，包括第一次。`retry_after` 是一行到期的时间；它的默认值让新行立即到期。

## 订阅者 {/* #the-subscriber */}

服务商会以三种方式拒绝一封邮件，每种方式需要不同的结果。`Busy` 表示它的速率限额已经用完：现在重试还会被拒绝，所以这封邮件要等待。`Unavailable` 表示它没有应答：`retry` 立即再试一次。`Rejected` 表示邮箱不存在，再试一次也会得到同样的答复，所以这封邮件被丢弃。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#send"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#send"
```

</TabItem>
</Tabs>

退避策略由处理器掌握。它读取投递的[尝试次数](../components/context/attempt.md)，每多一次就多等一秒：[`retry_after(d)`](../components/settlement/retry-after.md) 把“现在”加上 `d` 写入该行的 `retry_after`，在此之前没有领取会取走这一行。没有这一列的表会立即放回该行，运行时记录一条警告。[`retry()`](../components/settlement/index.md) 立即把该行放回队列；它的下一次投递读到的尝试次数多一。`drop()` 像确认一样结束该行，因此被拒绝的邮件离开这张表。载荷无法解码的行，与其他失败一样消耗尝试次数（[解码](../components/messages/decoding.md)）。

## 上限与死信组 {/* #the-cap-and-the-dead-letter-group */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 读取该行的 `attempt`。第五次失败的投递不再重试：[`dead_letter`](../components/settlement/dead-letter.md) 把该行转入 `emails.dead` 组，载荷和尝试次数保持原样。延迟重试与立即重试一样计入上限。这两个步骤必须一起出现：只声明其中一个的挂载会让服务在启动时停止。没有组列的表在 `dead_letter` 中改为给出一个表名，该行转入那张表，它具有相同的列。

## 读取死信 {/* #reading-the-dead-letters */}

死信组是这张表中的一个普通队列，`review` 像读取其他队列一样读取它。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`review` 把邮件记入日志，留给人来查看，然后确认它。为运维人员保留死信的服务，不在这个组上挂载任何订阅；这些行留在表中等待。重新发送它们只需服务自己的一条语句，它把这些行移回原组，并重置它们的计数：

```sql
UPDATE mail_jobs SET name = 'emails', attempt = 1, locked_until = NULL WHERE name = 'emails.dead';
```

在带 [FIFO 组](../components/subscriptions/fifo.md)的表中，`retry()` 让一行保持在它的组的首位，而 `retry_after(d)` 让更早到期的行越过它（[顺序](ordering.md)）。

## 测试 {/* #testing */}

示例文件中的测试通过测试框架发布一封邮件，写入的方式与服务自己的 `Publish` 相同。被拒绝的地址立即被丢弃，[`TestApp::start`](../components/testing/index.md) 在发布返回时就能看到这一点。没有应答的邮件和繁忙的服务商需要时间流逝：延迟的行和死信在订阅的下一次轮询时才回来。这些测试用 [`TestApp::start_live`](../components/testing/live.md) 在真实时钟上运行，退避时间为一秒，由 `tb.advance` 等它过去。[在 SQL 上测试服务](testing.md)介绍了测试框架。
