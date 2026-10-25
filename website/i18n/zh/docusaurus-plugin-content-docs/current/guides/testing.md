---
title: 在 SQL 上测试服务
sidebar_position: 10
description: "用 TestApp 测试基于 SQL 的服务：以内存中的 SQLite 运行生产环境的应用，在暂停的时钟上处理延迟，并在 Postgres 上运行同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在 SQL 上测试服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是真实的数据库：内存中的 SQLite，或者服务器上测试专用的数据库。

队列的行为存在于它的 SQL 中：领取、结算，以及服务自己的 `Publish`。因此测试框架没有数据库的模型。它的[进程内模式](../components/testing/index.md)在测试交给服务的数据库上执行服务的语句，并记下测试框架所等待的账目。

被测服务生成缩略图。小图片立即处理。缩放程序只在第二次尝试时才接受大图片，所以大图片的第一次投递用 [`retry_after`](../components/settlement/retry-after.md) 推迟一分钟。应用由 `service(url)` 构建，所以测试可以选择数据库，而 `main` 使用生产环境的数据库。测试需要在 `[dev-dependencies]` 中启用 `ruststream-sqlx` 的 `testing` feature。

## 小图片 {/* #a-small-image */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

`database` 在内存中创建一个 SQLite 数据库，所有指名它的连接共享这个数据库。只要其中一个连接仍然打开，它就存在，所以测试在连接池之外再持有一个连接，并通过这个连接池读取表。表由服务在启动时自己创建，与生产环境一样。[`TestApp::start`](api:ruststream::testing::TestApp::start) 在进程内把生产环境的应用连接到这个数据库，测试按生产类型访问 Broker：`tb.broker::<SqlxBroker<Sqlite>>()`。

图片像在生产环境中一样，经由路由和服务自己的 `Publish` 进入队列。`publish` 在它唤醒的处理器完成结算后才返回，所以它之后的几行读到的是已经完成的状态：`thumbnail` 如何结算该行，以及测试自己的查询在表中找到什么。服务在应用启动之前用自己的 SQL 写入的行（初始数据或迁移），在它的订阅打开时就被计入，所以 `settle` 像等待已发布的消息一样等待它。测试运行期间这样写入的行，只有在领取取走它时才被计入，所以 `settle` 不等待它；这样的测试在真实时钟上运行，并让轮询间隔过去，[任务入队](enqueueing.md)一页就是这样做的。场景把 Broker 的句柄作为参数接收，所以下面的 Postgres 测试也运行它。

没有载荷列的表把行本身交给它的处理器（[行](../components/messages/rows.md)）。测试这样的处理器时，用 `with_value` 和 `received_values` 检查行，代替 `with` 和 `received`。

## 延迟，而无需等待 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#clock"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#clock"
```

</TabItem>
</Tabs>

在进程内，队列从 tokio 时钟读取“现在”，并在 Broker 连接时以系统时钟为基准。测试暂停这个时钟，再用 [`advance`](api:ruststream::testing::TestApp::advance) 推动它：大图片的行在一分钟后到期，第二次投递生成缩略图。这一分钟不花任何时间。数据库调用脱离暂停的时钟运行，所以数据库应答期间时钟不会跳动。时钟越过租约时，租约也以同样的方式到期。使用[数据库时钟](../components/time/database-clock.md)的表读取数据库的时间，测试时钟推动不了它，[服务自己的时钟](../components/time/clock.md)也照常生效；这些情形的测试在真实时钟上运行。

## 在 Postgres 上运行同一个测试 {/* #the-same-test-on-postgres */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

在生产环境中运行于 Postgres 的服务，也要在 Postgres 上测试：SQLite 执行的是另一套 SQL，没有行锁、没有通知，也没有可选的隔离级别。[`TestApp::start_live`](../components/testing/live.md) 以普通方式连接应用，并在真实时钟上等待。测试在服务器上取得一个专用的数据库，服务在其中创建自己的表，因此不会读到先前运行留下的任何东西；`live::postgres::database` 是本仓库测试中的辅助函数，[`#[sqlx::test]`](https://docs.rs/sqlx/0.9/sqlx/attr.test.html) 为测试提供同样的东西，即一个已应用迁移的数据库。测试从 `POSTGRES_TEST_URL` 读取服务器地址，没有这个变量时跳过，所以在没有服务器的机器上 `cargo test` 也能通过；在必须有服务器的地方，`RUSTSTREAM_REQUIRE_LIVE=1` 把这种跳过变成失败。场景就是进程内的那一个，原样不变。

```bash
cargo test
POSTGRES_TEST_URL=postgres://user:password@localhost:5432/postgres cargo test
```

## 测试中的 outbox {/* #the-outbox-in-tests */}

测试构建默认关闭 [outbox](outbox.md)，除非 `RUSTSTREAM_SQLX_OUTBOX=on` 启用它，所以服务自己的测试在不跟踪的情况下运行处理器。测试跟踪本身的测试在设置了开关时运行，没有开关时跳过（[开关](../components/outbox/switch.md)）。
