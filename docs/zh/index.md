# SQL 数据库 { #sql-databases }

`ruststream-sqlx` 通过 [`sqlx`](https://docs.rs/sqlx) 把 SQL 数据库接入
[RustStream](https://powersemmi.github.io/ruststream/) 服务。它包含两个组件：

- 收件箱（inbox）：任务队列，存放在服务自己的 Postgres、MySQL/MariaDB 和 SQLite 表中，
  作为 RustStream Broker 运行；
- 事务性发件箱（transactional outbox）：服务通过任意 RustStream Broker 发布的消息，
  会记录在服务自己的一张表中，直到消费方处理完毕。

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-sqlx = { version = "0.7", features = ["inbox", "outbox", "postgres"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres", "derive", "chrono"] }
chrono = "0.4"
serde = { version = "1", features = ["derive"] }
```

`inbox` 和 `outbox` 两个 feature 分别开启两个组件，彼此独立。
驱动 feature 选择数据库：`postgres`、`mysql`（MySQL 和 MariaDB）、`sqlite`，
或者用于 `AnyPool` 的 `any`。

## 服务表中的任务队列 { #task-queues-in-the-services-tables }

```rust
--8<-- "crates/ruststream-sqlx/examples/inbox.rs:service"
```

`#[derive(Inbox)]` 描述队列表：表名，以及每一列承担的角色。
`SqlxBroker` 为服务连接池中的队列提供服务。订阅领取行，处理器的结果决定如何处置这一行：
确认会删除这一行，重新处理会把它放回队列。发布会用服务自己的语句写入一行。

订阅以三种方式之一领取行。在行锁方式下，处理器运行期间事务一直保持打开。
在租约方式下，订阅立即提交领取。在咨询锁方式下，订阅锁住这一行的键。
用 `.transactional()` 注册的处理器通过本次投递的事务写入数据，
确认会把处理器的写入与这一行一起提交。
每一列的角色开启一种行为：分组、行进入处理的顺序、延迟重新处理、
带死信转移的尝试次数上限，以及让已处理的行保留在表中的标记。

手写 API 是与 `#[derive]` 并列的另一种方式，描述的是同一张表：结构体实现一个 trait，
表的全部设置由一个带类型的构建器给出。编译器检查这份描述，与检查 `#[derive]` 一样严格。
两种方式生成相同的语句，开销也相同。

在编译期检查 SQL 的团队，可以在 `#[derive]` 中加上 `checked`。
这样 `cargo sqlx prepare` 会把 `#[derive]` 生成的语句与服务自己的查询一起检查。
测试在 `TestApp` 中针对真实数据库运行服务自己的应用，因为服务的 SQL 也属于测试要检查的内容。
SQLite 不需要服务器。

## 事务性发件箱 { #the-transactional-outbox }

```rust
--8<-- "crates/ruststream-sqlx/examples/outbox.rs:service"
```

`#[derive(Outbox)]` 描述服务的发件箱表，`outbox!` 按它跟踪的名称注册这张表。
发布中间件在发送每条被跟踪的消息之前先写入一条记录，并把记录 ID 放进消息头。
订阅中间件根据这个 ID 领取记录，处理器确认后把记录标记为已处理。
启动时，重新发布会再次发送所有尚未处理的记录。

消息至少投递一次。同时启动的两个服务实例，可能都会再次发送同一条记录。
因此，不能重复处理消息的消费方需要自己检查。

在 `#[ruststream::app]` 中，应用在 Tokio 运行时启动之前构建，而 sqlx 只能在运行时内创建连接池。
因此，注册表启动时没有连接池，由 `on_startup` 创建连接池，并通过 `set_pool` 交给它。
测试构建默认关闭发件箱，环境中设置 `RUSTSTREAM_SQLX_OUTBOX=on` 后才会开启。

发件箱的中间件包装收件箱的处理器，与包装其他处理器的方式相同。
记录通过单独的连接写入，因此即使投递的事务回滚，记录也会提交。
被跟踪并写入收件箱表的消息，只要表中有 `headers` 列、并由服务的发布语句写入，
就会保留它的记录 ID。其余内容见
[事务性发件箱](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-transactional-outbox)。

## 如何选择 { #which-one-to-use }

收件箱适合属于服务数据的工作：任务与它所服务的数据写在同一个事务中，
队列放在服务已经在用的数据库里。发件箱适合服务通过另一个 Broker 发布、且不能丢失的消息。
一个服务可以同时使用两者。

## 其余内容在哪里 { #where-the-rest-is }

docs.rs 上的参考文档以这个 crate 自己的教程开篇，每个主题一节：

- [收件箱 Broker](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-inbox-broker)：
  队列表、它的订阅，以及处理器的结果如何处置一行。
- [宏还是手写](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#macro-or-manual)：
  通过 `InboxTable` 和带类型的构建器手写描述队列表。
- [角色](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#roles)和
  [时间](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#time)：
  每一列开启什么，以及“当前时间”从哪里来。
- [行锁、租约和咨询锁](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#row-locks-leases-or-advisory-locks)和
  [事务模式](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#transactional-mode)：
  订阅如何领取行，以及处理器如何通过本次投递的事务写入数据。
- [行模式](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#row-mode)、
  [头部布局](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-headers-layout)和
  [批处理](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#batches)：
  处理器从一行中得到什么。
- [数据库](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#databases)和
  [服务自己的方言](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#a-dialect-of-the-services-own)：
  每种数据库执行什么语句，以及为其他数据库编写方言。
- [编译期检查语句](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#statements-checked-at-compile-time)：
  `checked` 模式。
- [唤醒订阅](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#waking-a-subscription)：
  轮询间隔，以及 Postgres 上的 `LISTEN/NOTIFY`。
- [在收件箱上测试服务](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#testing-a-service-on-the-inbox)：
  在 `TestApp` 中运行服务自己的应用。
- [事务性发件箱](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-transactional-outbox)：
  投递保证、记录及其注册表、中间件、重新发布、连接池、测试和开销。

这个 crate 每条消息的开销，以及它每秒处理的消息数，与做同样工作的原生 sqlx 循环的对比，见[基准测试页面](benchmarks.md)。

处理器、路由器、编解码器和中间件都来自框架本身，它的入口页面从
[RustStream 站点](https://powersemmi.github.io/ruststream/)开始。
