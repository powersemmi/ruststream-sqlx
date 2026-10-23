---
title: 设计
sidebar_position: 2
description: "ruststream-sqlx 的位置：在 RustStream 运行时与服务的 sqlx 连接池之间，SQL 为每个订阅构建一次，outbox 可以与任意 Broker 配合。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、服务的 sqlx 连接池和数据库",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、worker、结算、发布",
  "crate": "ruststream-sqlx",
  "subscriber": ["InboxSubscriber", "领取循环"],
  "delivery": ["InboxDelivery", "结算它的行"],
  "publishers": ["Repository、Routed", "插入行"],
  "descriptor": ["InboxQueue<Row>", "描述符"],
  "broker": ["SqlxBroker", "配置"],
  "dialect": ["Dialect", "每种数据库的 SQL"],
  "outbox": ["Outbox", "中间件，任意 Broker"],
  "sqlx": "sqlx",
  "pool": ["Pool<DB>", "服务自己的连接池"],
  "database": ["PostgreSQL、MySQL、MariaDB", "或 SQLite"],
  "inProcess": ["在 testing 特性下，TestApp 在进程内连接：", "同一个连接池和数据库，使用测试的时钟"],
  "mounted": "挂载到",
  "polls": "轮询流",
  "driver": "驱动程序"
};

# 设计

`ruststream-sqlx` 所处的位置：在 RustStream 运行时与服务的 sqlx 连接池之间。SQL 为每个订阅构建一次，outbox 可以与任意 Broker 配合。

<Diagram name="design" words={words} />

本 crate 在 [sqlx](https://docs.rs/sqlx/0.9/sqlx/) 之上实现核心的 Broker trait。它上面的运行时对每个 Broker 都相同。SQL 特有的一切都在本 crate 里：[`InboxQueue`](api:ruststream_sqlx::InboxQueue) 描述符、领取循环、三种领取方式、每种结果写入什么，以及每种数据库的 SQL。本 crate 支持按描述符和按名称订阅，可以领取批，把行本身交给行模式的处理器，把分区键交给按键分发，向表中发布，并在生成的文档中描述它的服务器。它不提供请求-应答，也不提供事务发布者：[事务模式](queues/transactions.md)是订阅的一个步骤，处理器通过投递的事务写入数据。

Broker 接收服务的连接池，并且从不关闭它。连接池始终属于服务：它的大小、选项和迁移都由服务决定。[`SqlxBroker::new`](api:ruststream_sqlx::SqlxBroker::new) 接收一个连接池，而 sqlx 只能在 Tokio 运行时内构建连接池。因此服务在 `main` 中构建连接池，再把它传给构建应用的函数。

## SQL {/* #the-sql */}

订阅执行的每条语句都由方言构建。PostgreSQL、MySQL 与 MariaDB，以及 SQLite 的方言是内置的。`AnyPool` 在 Broker 连接时，采用它所连到的那种数据库的方言。对于其他数据库，服务可以提供自己的方言。订阅在打开时根据表的描述构建一次语句。这个描述是一个 `const` 值，由派生宏或手写的 [`InboxSpec`](api:ruststream_sqlx::InboxSpec) 生成。因此无论使用派生宏、手写描述还是服务自己的方言，每条消息的开销都相同。随后，启动过程在服务器上预备每条语句，所以缺少表或列时，服务在第一条消息之前就会停止。

## 发布 {/* #publishing */}

重试留在原来的行中。描述符声明由数据库自己移动投递，所以 `retry_after` 是对该行 `retry_after` 列的一次 `UPDATE`，从不发布副本：[结算](settlement.md)一页用图说明了这一点。发布通过行类型自己的 [`Publish`](api:ruststream_sqlx::Publish) 实现写入一行。[`Repository`](api:ruststream_sqlx::Repository) 在编译期确定它的表。[`Routed`](api:ruststream_sqlx::Routed) 是已连接 Broker 的默认发布者，应答也使用它。它在构建 Broker 时给定的路由中查找消息的名称。两者都以借用切片的形式接收编码后的字节，因此应答的缓冲区在消息之间复用。

每条消息仍然存在的开销由本 crate 列出，并在[基准测试](/architecture/benchmarks/)中测量：

- `Routed` 做一次哈希查找，并对行的 `Publish` 做一次动态调用。调用产生的 future 原地存放在一个 1024 字节的槽位中，只有更大时才装箱。
- 一次发布读取 Broker 的关闭标志，即一次原子加载，并唤醒本进程中订阅了该表的订阅，每个订阅一次原子操作。
- 读取 `Tx` 或 `Ctx<keys::Pool>` 的处理器，每次投递付出一次引用计数递增。
- 按名称订阅、而行的事件由服务自己定义时，每条消息付出一次装箱的投递和一次装箱的结算 future。

## 进程内模式 {/* #in-process-mode */}

在 `testing` 特性下，`TestApp::start` 在进程内连接 Broker。连接池以下的部分都不替换：进程内的 Broker 连接测试的连接池所指向的数据库，执行服务的 SQL、表结构和领取方式，并在数据库返回错误的地方返回错误。内存中的 SQLite 可以在没有服务器的情况下服务一个测试。变化的是时间。Broker 从 tokio 时钟读取“现在”，并在连接时以挂钟时间为基准，因此在暂停的时钟上，测试用 `tb.advance(..)` 推动队列的时间：测试推进过 `retry_after` 之后，该行就到期了。每次数据库调用都脱离这个时钟运行，所以在数据库的应答尚未返回时，暂停的时钟不会跳到下一个计时器。测试时钟只替代 [`SystemClock`](api:ruststream_sqlx::SystemClock)：使用 `DatabaseClock` 的表读取数据库的时钟，服务自己的时钟也照常生效，与生产环境一样。

测试框架通过 Broker 的计数等待数据库的工作。消息落入测试中某个订阅读取的队列时计一次，重试把行放回队列时再计一次，因此每个到达队列的行都投递并结算之后，`tb.settle()` 才返回。

各部分如何启动和停止，见[生命周期](lifecycle.md)；一行的路径，见[路径](path.md)。
