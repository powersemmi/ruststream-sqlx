# SQL 数据库 { #sql-databases }

`ruststream-sqlx` 通过 [`sqlx`](https://docs.rs/sqlx) 把 SQL 数据库接入
[RustStream](https://powersemmi.github.io/ruststream/) 服务。它包含两个组件：

- 事务性发件箱（transactional outbox），可配合任意 RustStream Broker 使用。
  处理器发布的消息和它自己的写入保存在同一个事务中，事务提交后 Broker 才收到这些消息。
- 任务队列，存放在服务自己的 Postgres、MySQL/MariaDB 和 SQLite 表中。

## 其余内容在哪里 { #where-the-rest-is }

这个 crate 的参考文档在 docs.rs 上：[`ruststream-sqlx`](https://docs.rs/ruststream-sqlx)。

处理器、路由器、编解码器和中间件都来自框架本身，它的入口页面从
[RustStream 站点](https://powersemmi.github.io/ruststream/)开始。
