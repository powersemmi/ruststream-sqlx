# SQL 数据库 { #sql-databases }

`ruststream-sqlx` 通过 [`sqlx`](https://docs.rs/sqlx) 把 SQL 数据库接入
[RustStream](https://powersemmi.github.io/ruststream/) 服务。它包含两个组件：

- 事务性发件箱（transactional outbox），可配合任意 RustStream Broker 使用。
  发布消息时，消息会作为一条记录写入服务自己的表，并携带这条记录的 ID。
  订阅根据该 ID 领取任务并开始处理，确认后将任务标记为已处理。
  服务启动时，会重新发布所有尚未处理的记录。
- 任务队列，存放在服务自己的 Postgres、MySQL/MariaDB 和 SQLite 表中。

订阅以两种方式之一领取队列中的行。在行锁方式下，订阅在事务中锁住这一行，
事务一直保持打开，直到处理器处理完这一行。在租约方式下，订阅把租约写入这一行，并立即提交事务。
因此，耗时较长的处理器不会占用事务，订阅还会在处理器运行期间为租约续期。SQLite 表使用租约方式。

订阅通过 `max_attempts(n)` 限制一条消息的投递次数，并通过 `dead_letter(..)` 指定一个分组或一张表。
一行的尝试次数耗尽后，会转入该分组或该表。两者需要一起声明。使用 `max_attempts(1)` 时，每次失败都会让该行立即转入。

这个 crate 内置了 Postgres、MySQL/MariaDB 和 SQLite 的方言。如果数据库的 sqlx 驱动由单独的 crate 提供，
或者服务要用自己的方式编写某条语句，服务可以使用自己的方言。这样的方言为其表采用的每种方式实现一个 trait，
并为按名称订阅实现一个 trait。表采用的方式在方言中不存在时，代码无法编译。

队列表归服务所有。启动时，订阅会检查它的表中有没有结构体列出的各列。
列的类型由服务自己负责。某一行无法解码为结构体时，订阅按自己的解码失败策略处置这一行。

## 其余内容在哪里 { #where-the-rest-is }

这个 crate 的参考文档在 docs.rs 上：[`ruststream-sqlx`](https://docs.rs/ruststream-sqlx)。

处理器、路由器、编解码器和中间件都来自框架本身，它的入口页面从
[RustStream 站点](https://powersemmi.github.io/ruststream/)开始。
