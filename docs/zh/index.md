# SQL 数据库 { #sql-databases }

`ruststream-sqlx` 通过 [`sqlx`](https://docs.rs/sqlx) 把 SQL 数据库接入
[RustStream](https://powersemmi.github.io/ruststream/) 服务。它包含两个组件：

- 事务性发件箱（transactional outbox），可配合任意 RustStream Broker 使用。
  发布消息时，消息会作为一条记录写入服务自己的表，并携带这条记录的 ID。
  订阅根据该 ID 领取任务并开始处理，确认后将任务标记为已处理。
  服务启动时，会重新发布所有尚未处理的记录。
- 任务队列，存放在服务自己的 Postgres、MySQL/MariaDB 和 SQLite 表中。

## 其余内容在哪里 { #where-the-rest-is }

这个 crate 的参考文档在 docs.rs 上：[`ruststream-sqlx`](https://docs.rs/ruststream-sqlx)。

处理器、路由器、编解码器和中间件都来自框架本身，它的入口页面从
[RustStream 站点](https://powersemmi.github.io/ruststream/)开始。
