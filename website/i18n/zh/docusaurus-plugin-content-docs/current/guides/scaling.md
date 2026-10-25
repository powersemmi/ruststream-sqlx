---
title: 多个实例与唤醒 worker
sidebar_position: 7
description: "在一张队列表上运行多个实例：任务不会被取走两次，新行多快被发现，Postgres 上的通知，并行 worker 及其所需的连接池。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 多个实例与唤醒 worker

本指南在服务的多个实例上运行一个队列，每个实例中有多个 worker，并设置它们发现新任务的速度。

读取一张队列表的所有实例共享这张表。除了数据库，没有任何东西协调这些实例：每个实例各自领取行，表的[领取方式](../components/forms/index.md)保证任何一行同一时间只属于一次领取。无论采用哪种方式、使用哪种数据库，两个领取者都绝不会取走同一行。

示例渲染发票。网店用自己的 SQL 写入发票任务，渲染服务按负载需要运行任意多个实例。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/scaling.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/scaling.rs#service"
```

</TabItem>
</Tabs>

[`workers(4)`](../components/subscriptions/workers.md) 让每个实例同时处理最多四张发票。订阅为每个空闲 worker 保持一次进行中的领取，所以它的 worker 同时取行，而不是一个接一个地取。SQLite 同一时间只接受一个写入者，订阅在那里只保持一次进行中的领取；在租约形式下，这次领取为每个 worker 各取一行。同一张表上的第二个实例再增加四个 worker，任何发票都不会同时交给两者。

## 新任务多快被发现 {/* #how-fast-a-new-job-is-seen */}

只要领取总是取满，订阅就持续领取，因此积压的任务无需停顿即可排空。取到的行少于请求数的领取之后，订阅等待它的[轮询间隔](../components/subscriptions/poll-interval.md)，未设置时为一秒。新行的等待时间受三件事限制：

- 经 Broker 的发布会立即[唤醒](../components/wake/index.md)同一进程中订阅了该表的订阅，代价是每个被唤醒的订阅一次原子操作。
- 以其他途径写入的行要等待轮询间隔：另一个程序的插入、触发器、服务的 [`insert`](../components/tables/insert.md)、另一个实例的发布。这里的 `poll_interval(250 ms)` 把这段等待限制在四分之一秒以内，代价是在空表上每个订阅每秒执行四条领取语句。
- 在 Postgres 上，一条通知会结束等待。

## Postgres 上的通知 {/* #notifications-on-postgres */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/scaling.rs#notify"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/scaling.rs#notify"
```

</TabItem>
</Tabs>

[`listen_notify()`](../components/wake/listen-notify.md) 在连接池的一个连接上监听，每个订阅监听它的表的通道。Broker 的每次发布在写入之后立即执行 `pg_notify(channel, group)`，所以另一个实例中的订阅一旦能看到这一行就领取它。RustStream 之外的写入者自己发出同样的通知；上面的触发器为每次插入发出通知，它的空载荷唤醒该表的每个订阅。有了通知，轮询间隔就成为兜底，用来接住通知遗漏的行，这里是一分钟。

通知有代价，这个开关在未设置时关闭。Broker 在整个生命周期内占用连接池的一个连接。每次发布多执行一条语句。发出通知的事务在提交时获取一个服务器全局的锁，因此发出通知的事务依次提交；有大量并发写入者的服务，应在启用通知之前测量这一点（[NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)）。

## worker 与连接池 {/* #workers-and-the-pool */}

一个订阅占用的连接从不超过连接池大小减一，所以连接池始终为处理器、处理器的发布和结算留出一个连接。连接池太小，容不下它的 worker 时，同时运行的投递就少一些，但不会失败。在租约方式下，`n + 1` 的连接池让 `workers(n)` 的每个空闲 worker 同时领取；行锁方式和咨询锁方式还要为每个处理中的任务占用一个连接（[连接](../components/broker/connections.md)）。[在生产环境中运行](production.md)一页为整个服务确定连接池大小。

## 时钟 {/* #clocks */}

每个实例从它所在主机的时钟读取“现在”，用来领取、推迟和租用行（[时间](../components/time/index.md)）。服务同一张表的各台主机要让时钟保持同步，误差远小于一个租约，NTP 就能做到。做不到时，使用 [`DatabaseClock`](../components/time/database-clock.md) 的表在每条语句中读取数据库的时钟；租约方式始终使用主机的时钟，所以这样的表采用行锁方式或咨询锁方式。

## 测试 {/* #testing */}

示例文件中的测试在内存中的 SQLite 上运行服务。两个实例各有自己的连接池，在一张存有二十张发票的表上启动，它们合起来把每张发票渲染一次。另一个程序写入的行，在四分之一秒的轮询间隔内被发现。经 Broker 的发布立即到达它自己的实例。这些行不唤醒任何订阅，所以等待它们的测试用 [`TestApp::start_live`](../components/testing/live.md) 在真实时钟上运行。最后一个测试让 Postgres 服务连接它自己的数据库，发现一次插入远在一分钟的轮询之前就被领取；它从 `POSTGRES_TEST_URL` 读取服务器地址，没有这个变量时跳过。
