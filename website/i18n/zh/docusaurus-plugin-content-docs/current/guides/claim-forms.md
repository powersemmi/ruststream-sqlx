---
title: 长任务、崩溃与领取方式
sidebar_position: 4
description: "选择队列表如何持有处理中的任务：行锁、租约或咨询锁，以及每种方式如何对待长任务和崩溃的进程。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 长任务、崩溃与领取方式

本指南选择处理器工作期间队列表如何持有任务，并说明运行数分钟的任务和进程终止的任务会发生什么。

被领取的任务在结算之前必须避开其他领取，在它的进程停止时又必须回到队列。表的描述为此选定三种[领取方式](../components/forms/index.md)之一，处理器在每种方式下都相同。

## 三种方式 {/* #the-three-forms */}

| | 行锁 | 租约 | 咨询锁 |
|---|---|---|---|
| 选定方式 | 不需要设置：默认方式 | `#[field(locked_until)]` | `#[inbox(advisory_lock = "..")]` |
| 持有任务的方式 | 领取的事务，`FOR UPDATE SKIP LOCKED` | 写入行中的到期时间 | 行的键上的锁，由一个会话持有 |
| 连接 | 每个处理中的任务一个，占用整个处理器运行期间 | 每条语句一个 | 每个处理中的任务一个 |
| 崩溃之后 | 任务立即回来 | 任务在租约到期后回来 | 任务立即回来 |
| 崩溃是否消耗一次尝试 | 否 | 是 | 是 |
| 数据库 | Postgres、MySQL、MariaDB | 全部 | 全部 |

[行锁](../components/forms/index.md)方式在处理器运行期间保持领取的事务打开，由结算提交它。崩溃进程的事务会回滚，所以它的任务立即回来，尝试次数也不计入。代价是每个处理中的任务占用一个连接。SQLite 没有行锁，所以 SQLite 表要指定另外两种方式之一。

[租约](../components/forms/lease.md)方式立即提交领取，所以处理中的任务既不占用连接，也不占用事务。它适合长任务，也是 SQLite 队列所用的方式。[咨询锁](../components/forms/advisory.md)方式按键持有每个任务：键取 id 时，每个任务单独持有；键取另一列时，共享该键的任务逐个进入处理。

## 渲染数分钟的报表 {/* #a-report-that-renders-for-minutes */}

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/claim_forms.rs#schema"
```

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/claim_forms.rs#lease"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/claim_forms.rs#lease"
```

</TabItem>
</Tabs>

领取把租约的到期时间写入 `locked_until`，并计入这次尝试。`render` 运行期间，订阅每隔半个租约续约一次，所以在一分钟的租约下，一份报表可以渲染五分钟。到期时间就是投递的所有权令牌：只有该行仍持有这个租约时，确认才会删除它。[`InboxQueue::lease`](api:ruststream_sqlx::InboxQueue::lease) 设置单个订阅的租约，[`SqlxBroker::lease`](api:ruststream_sqlx::SqlxBroker::lease) 设置 Broker 的租约；未设置时为三十秒，以整秒计。

## 崩溃时会发生什么 {/* #what-a-crash-does */}

在报表渲染到一半时终止的进程，不再为它的租约续约。在租约到期之前，该行不在队列中，这里是一分钟，随后下一次领取取走它。这次领取再计入一次尝试，所以 `render` 看到第二次尝试，并把它记入日志。因此，租约就是崩溃让任务延迟的时长：租约越短，任务回来得越早，代价是续约次数更多。进程仍然存活却停止了续约，例如与数据库断开的时间超过了租约，任务就会落入另一次领取，它迟到的结算以 [`SqlxBrokerError::LeaseLost`](api:ruststream_sqlx::SqlxBrokerError::LeaseLost) 失败。

行锁或咨询锁随它的会话一起结束，所以崩溃会让任务立即回来。shutdown 时，Broker 停止续约：处理中的任务保留它的租约，能结算就结算，否则在租约到期后回来。咨询锁在 shutdown 时释放（[关闭](../components/broker/shutdown.md)）。

## 每种方式所需的连接池 {/* #the-pool-each-form-needs */}

行锁或咨询锁为每个处理中的任务占用一个连接，所以一个 `workers(8)` 的订阅，仅它的任务就需要八个连接。租约只在执行每条语句时占用连接。每个订阅都为处理器、处理器的发布和结算在连接池中留出一个连接（[连接](../components/broker/connections.md)）。在 Postgres 上，咨询锁方式需要直接连接，或者会话模式的 PgBouncer，因为锁存在于会话中。在 SQLite 上，进程自己保存处理中的咨询锁键，所以一个数据库文件由一个进程服务。[在生产环境中运行](production.md)一页说明如何确定连接池大小。

## 测试 {/* #testing */}

示例文件中的测试在内存中的 SQLite 上运行服务。第一个测试暂停时钟，五分钟的渲染瞬间过去，租约的续约在时钟到达时运行；报表只投递一次。第二个测试写入两行，它们已被另一个进程领取：一行的租约已经到期，另一行的租约仍然有效。只有第一行回来。这些行不唤醒任何订阅，所以这个测试用 [`TestApp::start_live`](../components/testing/live.md) 在真实时钟上运行，越过轮询间隔。
