---
title: 跟踪你发布的消息：outbox 插件
sidebar_position: 9
description: "用 outbox 插件在任意 RustStream Broker 上跟踪服务发布的消息，直到消费者处理完它，并在启动时重新发布其余消息。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 跟踪你发布的消息：outbox 插件

本指南让服务发布的每条消息都到达一个处理它的处理器，即使 Broker 什么也不保留。

有些 Broker 只把消息投递给此刻正在监听的一方，Redis Pub/Sub 和 NATS core 就是如此，而消费者可能在处理任何一条消息时中途停止。[outbox](../components/outbox/index.md) 记录服务以已注册名称发布的每条消息，在消费者处理完消息后标记记录，并在启动时重新发布未处理的记录。它是基于任意 RustStream Broker 的插件，不需要中继进程。它的 `outbox` feature 与 inbox 相互独立。

示例在结账时收款，并为已付款的订单发货，使用内存 Broker。

## 表 {/* #the-table */}

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/outbox.rs#schema"
```

这张表属于服务：由服务创建，在生产环境中用它自己的迁移创建。消费者处理完消息之前，`processed_at` 保持为空。

## 记录 {/* #the-record */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/outbox.rs#record"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/outbox.rs#record"
```

</TabItem>
</Tabs>

[`#[derive(Outbox)]`](../components/outbox/record.md) 描述这条记录：`id`、`name` 和 `payload` 是必需的，`headers` 和 `processed_at` 是可选的。服务用自己的 [`outbox::Publish`](../components/outbox/statements.md) 写入记录并返回它的 id；其他语句由本 crate 构建。在手写形式中，`OutboxSpec` 表达同样的内容，由一个 `TrackedName` 类型给出被跟踪的名称。

## 处理器 {/* #the-handlers */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/outbox.rs#handlers"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/outbox.rs#handlers"
```

</TabItem>
</Tabs>

处理器对 outbox 一无所知：跟踪由应用的中间件完成。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/outbox.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/outbox.rs#app"
```

</TabItem>
</Tabs>

[`outbox!`](../components/outbox/registry.md) 把 `payments` 连同它的记录注册进来；同一个名称注册两次无法编译。`#[ruststream::app]` 在 Tokio 运行时存在之前构建应用，而 sqlx 只能在 Tokio 运行时内构建连接池。所以注册表在启动时没有连接池，由 [`on_startup`](../components/outbox/set-pool.md) 构建连接池，并用 `set_pool` 交给它；`after_shutdown` 关闭连接池。

一条被跟踪的消息经过四个步骤：

1. [发布层](../components/outbox/tracking.md)写入记录，然后发送消息，把记录的 id 放在 `x-ruststream-outbox-id` 消息头中。记录无法写入时，发布失败。
2. 订阅层按 id 把带有 id 的投递取来处理，并运行处理器；记录已经处理过的副本直接确认，不运行处理器。
3. 处理器的确认或 drop 把记录标记为已处理；重试让记录保持原样。
4. 启动时，各订阅打开之后，[`republish`](../components/outbox/republish.md) 重新发送每条未处理的记录。

消费者在生产者的表中标记记录，所以生产者和消费者共享注册表和数据库，就像这里一个应用中的两个处理器那样。

## outbox 的保证 {/* #what-the-outbox-guarantees */}

消息至少投递一次。记录在消息发送之前写入，因此发送失败、消费者在处理器中途停止，或处理器要求再次投递消息时，记录都会留给下一次启动。两个同时启动的实例可能都再次发送同一条记录，所以不能重复处理消息的消费者要自己检查这一点。

记录在自己的连接上写入，并立即提交。它不加入服务的事务：被重试的[事务](transactional.md) inbox 处理器丢弃它自己的写入，却保留它所发布内容的记录。在 SQLite 上，这样的处理器要在通过事务第一次写入之前发布，因为记录的插入需要这个事务持有的写锁；这次写入之后的发布以 `OutboxError::WriteLockHeld` 失败（[相互作用](../components/outbox/interplay.md)）。必须与业务行一起提交的事件，是业务事务中的一行，之后再中继出去：见[把事件中继到另一个 Broker](relay.md)。

处理器之外的发布，例如一个 HTTP 端点，取得运行中应用的发布者，用注册表把它 [`wrap`](../components/outbox/wrap.md) 起来，于是它以同样的方式被跟踪。跟踪的开销是每次被跟踪的发布一个连接，每次被跟踪的投递两个连接：一个用于取出记录，一个用于结算。名称不在注册表中的消息，只付出一次名称比较。仓库中的 [`examples/outbox`](https://github.com/powersemmi/ruststream-sqlx/tree/main/examples/outbox) 在 Redis Pub/Sub 上运行同样的流程，带有迁移和它自己的取出语句。

## 测试 {/* #testing */}

测试构建默认关闭 outbox，除非 `RUSTSTREAM_SQLX_OUTBOX=on` 启用它（[开关](../components/outbox/switch.md)），所以服务自己的测试在不跟踪的情况下运行处理器。示例文件中检查跟踪的测试在没有这个开关时跳过；另有一个测试检查开关关闭时服务在不跟踪的情况下运行：

```bash
cargo test
RUSTSTREAM_SQLX_OUTBOX=on cargo test
```

开关打开时，一个测试检查一笔付款被记录、带着记录的 id 发送，并在发货后被标记。另一个测试写入一条先前的实例留下的未处理记录，发现启动之后它已经发货。两者都用 [`TestApp::start`](../components/testing/index.md) 在内存中的 SQLite 数据库上运行，服务自己的连接池按 URL 打开这个数据库。
