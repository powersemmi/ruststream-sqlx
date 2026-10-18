---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-sqlx，描述一张队列表，在 SQLite 上运行第一个订阅者，并在没有服务器的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-sqlx`，创建一张队列表，并在其上运行第一个服务。全程使用 SQLite，不需要服务器。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-sqlx = { version = "0.7", features = ["inbox", "sqlite", "chrono"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "sqlite", "derive", "chrono"] }
chrono = "0.4"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tracing = "0.1"

[dev-dependencies]
ruststream-sqlx = { version = "0.7", features = ["inbox", "sqlite", "chrono", "testing"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。`ruststream-sqlx` 为队列提供服务，`sqlx` 是服务访问自己数据库时已经在用的客户端。`testing` feature 加入测试所运行的[进程内模式](../components/testing/index.md)。

本 crate 没有默认 feature。服务按需启用：

| Feature | 提供的内容 |
|---|---|
| `inbox` | 服务自有表中的[任务队列](../components/tables/index.md)，由 [`SqlxBroker`](../components/broker/index.md) 提供服务 |
| `outbox` | 基于任意 RustStream Broker 的[事务性 outbox](../components/outbox/index.md) |
| `postgres`、`mysql`、`sqlite` | 一种内置[方言](../components/databases/index.md)及其 sqlx 驱动；`mysql` 同时服务 MySQL 和 MariaDB |
| `any` | [`AnyPool`](../components/databases/any-pool.md)，由它所连到的数据库的方言提供服务 |
| `chrono`、`time` | 以对应 crate 的类型表示的[时间列](../components/time/queue-time.md) |
| `json` | JSON 格式的[消息头列](../components/headers/index.md) |
| `testing` | `TestApp` 背后的进程内模式，以及 outbox 的[测试开关](../components/outbox/switch.md) |
| `asyncapi` | 生成的 [AsyncAPI 文档](../components/asyncapi/index.md)中的表 |

## 表 {/* #the-table */}

队列是服务自己的一张表，由服务创建：

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/getting_started.rs#schema"
```

每一行是一个任务。`id` 标识该行，`attempt` 统计它的投递次数，`payload` 保存编码后的消息。`locked_until` 保存处理中的行的[租约](../components/forms/lease.md)：SQLite 没有行锁，所以 SQLite 队列在租约下领取它的行。[长任务、崩溃与领取方式](claim-forms.md)比较了表可以采用的各种方式。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

[`#[derive(Inbox)]`](../components/tables/index.md) 描述这张表：`#[inbox(table = ..)]` 给出表名，每个 `#[field(..)]` 为一列指定[角色](../components/tables/roles.md)。`generated` 标记由数据库填充的列。在手写形式中，[`InboxSpec`](../components/tables/inbox-spec.md) 表达同样的内容。[`InboxQueue::<MakeThumbnail>::new("thumbnails")`](../components/subscriptions/index.md) 就是订阅：它领取 `thumbnail_jobs` 的行，把每个载荷交给编解码器。处理器的返回值结算该行：`ack()` 删除它，`drop()` 以同样的方式结束它，只是不生成缩略图。[`Ctx<keys::Attempt>`](../components/context/attempt.md) 读取这是该行的第几次投递。

向 `thumbnails` 发布会写入一行。[`route`](../components/publishing/route.md) 把这个名称引向 `MakeThumbnail`，再由服务自己的 [`Publish`](../components/publishing/index.md) 语句插入该行。处理器的应答和测试也以同样的方式发布。处理器之外的代码，例如一个 HTTP 端点，用派生宏生成的 [`insert`](../components/tables/insert.md) 在自己的事务中写入该行，[任务入队](enqueueing.md)一页演示了这种做法。

## 运行服务 {/* #running-the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#main"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#main"
```

</TabItem>
</Tabs>

`main` 自己启动 Tokio 运行时，因为 sqlx 只能在 Tokio 运行时内构建连接池。[`SqlxBroker::new`](../components/broker/index.md) 接收这个连接池，连接池仍然属于服务。

```bash
cargo run
```

`thumbnails.db` 中出现新行时，服务就领取它们。其他程序插入的行，会在[轮询间隔](../components/subscriptions/poll-interval.md)内被发现，未设置时为一秒。<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

## 第一个测试 {/* #the-first-test */}

示例文件中还有两个测试。每个测试在内存中建立一个 SQLite 数据库，创建这张表，然后用 `TestApp::start` 启动同一个 `service`。测试像生产环境一样通过路由发布一张图片，然后检查 `thumbnail` 如何结算它，以及表中还剩下什么：

```bash
cargo test
```

[在 SQL 上测试服务](testing.md)逐一讲解这些测试。

## 迁移到 Postgres 或 MySQL {/* #moving-to-postgres-or-mysql */}

同一个结构体也能服务 `PgPool` 或 `MySqlPool`：Broker 采用它的连接池所对应的方言。在两个 crate 中启用 `postgres` 或 `mysql`，构建对应的连接池，再用该数据库的占位符编写 `Publish<Postgres>` 或 `Publish<MySql>`。在服务器上，没有 `locked_until` 的表在[行锁](../components/forms/index.md)下领取它的行，处理器运行期间一直持有这个锁。为领取顺序建立索引，即表中已有的列按 `(group, priority, retry_after, id)` 排列；原因见[在生产环境中运行](production.md)。

## 从手写队列迁移 {/* #coming-from-a-hand-written-queue */}

基于 sqlx 的队列通常是一个 `SELECT .. FOR UPDATE SKIP LOCKED` 循环，再加几列簿记信息。下面是每个部分在本 crate 中的位置：

| 手写实现 | ruststream-sqlx |
|---|---|
| 领取循环及其语句 | 一个 [`InboxQueue`](../components/subscriptions/index.md) 订阅；语句由[方言](../components/databases/index.md)构建 |
| 处理完成后的 `DELETE` 或 `UPDATE .. SET done` | 处理器的[结果](../components/settlement/index.md) |
| 手动递增的 `attempts` 列 | `#[field(attempt)]` 和 [`Ctx<keys::Attempt>`](../components/context/attempt.md) |
| 用于延后执行的 `run_at` 列 | `#[field(retry_after)]` 和 [`retry_after(d)`](../components/settlement/retry-after.md)（[重试](retries.md)） |
| `locked_until` 列和一个回收进程 | [租约方式](../components/forms/lease.md)（[长任务](claim-forms.md)） |
| 死信表及转移到它的操作 | `max_attempts(n)` 和 [`dead_letter`](../components/settlement/dead-letter.md) |
| 与它所服务的数据一起插入的任务 | 在同一个事务中调用 [`insert`](../components/tables/insert.md)（[任务入队](enqueueing.md)） |
