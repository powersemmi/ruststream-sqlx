---
title: 在生产环境中运行
sidebar_position: 11
description: "在生产环境中运行队列：表结构及其索引、启动检查与编译期检查、连接池、数据库版本、连接池代理、关闭，以及 AsyncAPI 文档。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在生产环境中运行

本指南把一个队列带到 Postgres 上的生产环境：表结构及其索引、发现错误表结构的检查、连接池、数据库版本与连接池代理，以及关闭。

示例服务是一个发信服务，有八个 worker、五次尝试的上限和一个死信组。

## 表结构 {/* #the-schema */}

{/* single-form: the migration is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/production.rs#migration"
```

本 crate 不创建表：表结构属于服务，由服务的迁移创建，[`examples/outbox`](https://github.com/powersemmi/ruststream-sqlx/tree/main/examples/outbox) 中用 `sqlx::migrate!` 运行这些迁移。索引遵循领取顺序，即表中已有的列按 `(group, priority, retry_after, id)` 排列，这里是 `(name, job_id)`。领取在排序之前就锁住它读到的组中每一行，所以没有这个索引时，为处理器保持的领取会挡住组中其余的行，其他领取取不到它们。在带 [FIFO 组](ordering.md)的表中，同一个索引让领取对组的读取只涉及组自己的行。

## 受检语句 {/* #checked-statements */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#table"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#table"
```

</TabItem>
</Tabs>

[`checked, db = postgres`](../components/tables/checked.md) 把结构体确定的每条语句交给 sqlx 的编译期检查：领取、各种结算、组的死信转移和插入。结构体指名而表中缺少的列会让构建失败。构建对照 `DATABASE_URL` 检查，或者对照 `cargo sqlx prepare` 写出、与 crate 一起提交的 `.sqlx` 数据检查；CI 中的 `cargo sqlx prepare --check` 会让数据过期的构建失败。订阅执行的语句与没有 `checked` 时相同，所以每条消息的开销也相同。这项检查属于派生宏：手写描述的表只有启动检查。

每个订阅还会[在启动时预备它的语句](../components/broker/startup.md)。数据库缺少的表或列会让服务以 `SqlxBrokerError::Schema` 停止，错误中写明订阅、表和语句。声明了上限却没有死信的挂载，或者反过来，也会让服务停止。所以 `main` 在应用之前运行迁移，这些检查看到的就是这个版本所读取的表结构。

## 连接池 {/* #the-pool */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

这张表没有指定领取方式，所以它在[行锁](../components/forms/index.md)下领取：每封处理中的邮件在结算之前都持有它所在行的锁和一个连接。一个订阅占用的连接从不超过连接池大小减一，留下的那一个给处理器自己的查询、它们的发布和结算。因此连接池要按服务的全部订阅来确定大小：每个 `workers(n)` 计 `n` 个，再加一个，再加上处理器为自己的工作取用的连接（[连接](../components/broker/connections.md)）。[租约](claim-forms.md)表只在执行每条语句时占用连接，适合无法为每个任务留出一个连接的连接池。在 Postgres 上，[通知](scaling.md)在 Broker 的整个生命周期内再多占用一个连接。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#main"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#main"
```

</TabItem>
</Tabs>

## 数据库与连接池代理 {/* #databases-and-poolers */}

| 数据库 | 要求 |
|---|---|
| [Postgres](../components/databases/postgres.md) | `SKIP LOCKED` 需要 9.5；咨询锁方式和 FIFO 组需要 11，它们锁住 64 位哈希 |
| [MySQL、MariaDB](../components/databases/mysql.md) | MySQL 8.0.1 或 MariaDB 10.6，在启动时读取；更旧的服务器会让服务以 `ServerTooOld` 停止 |
| [SQLite](../components/databases/sqlite.md) | 租约方式或咨询锁方式；在咨询锁方式下，每个数据库文件一个进程 |

在 MySQL 和 MariaDB 上，领取以 READ COMMITTED 运行，除非行锁表声明了其他级别。以语句格式记录日志的服务器，即 `binlog_format = STATEMENT`，会拒绝这个级别下的写入；行格式和混合格式接受这些写入。在 Postgres 上，`read_committed` 是行锁表实际可用的[隔离级别](../components/transactions/isolation.md)：更严格的级别在领取与结算同时运行时，会让领取以串行化错误失败。咨询锁方式把锁保存在数据库会话中，所以它需要直接连接，或者会话模式的 PgBouncer；事务模式的连接池代理会在两条语句之间把会话交给另一个客户端。启用 [`listen_notify`](../components/wake/listen-notify.md) 的 Broker 同样在一个会话上监听。

## 关闭 {/* #shutdown */}

关闭时，运行时停止各订阅，在它的关闭超时内让处理中的处理器完成，然后[关闭 Broker](../components/broker/shutdown.md)。行锁方式的投递在结算之前一直持有它的事务；进程退出时仍未结算的投递随它的连接一起回滚，它的行回到队列。租约方式的投递保留它的租约，能结算就结算，否则它的行在租约到期后回来。Broker 释放处理中投递的咨询锁。连接池仍然属于服务，`main` 在 `run` 返回后关闭它。

## AsyncAPI 文档 {/* #the-asyncapi-document */}

启用 `asyncapi` feature 后，生成的 [AsyncAPI 文档](../components/asyncapi/index.md)描述服务器，以及每个订阅的表和组。`with_broker_labeled` 的标签给出服务器的名称，服务器条目中是连接池的主机和端口，从不包含用户名或密码：这份文档会被发布和分享。

## 测试 {/* #testing */}

示例文件中的一个测试在一个带密码 URL 的延迟连接池上构建这份文档，不做任何 I/O，发现其中有主机和端口，而任何地方都没有密码。另外两个测试连接它们自己的 Postgres 数据库运行：缺少表的数据库让启动以一个指名该表的错误停止，受检语句则正常服务队列。它们从 `POSTGRES_TEST_URL` 读取服务器地址，没有这个变量时跳过（[在 SQL 上测试服务](testing.md)）。
