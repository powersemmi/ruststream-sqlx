---
slug: /
sidebar_position: 0
description: "用 ruststream-sqlx 在 RustStream 服务中使用 SQL 数据库的指南：服务自有表中的任务队列，以及事务性 outbox。"
---

# 指南

`ruststream-sqlx` 通过 [sqlx](https://docs.rs/sqlx) 把 SQL 数据库接入 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 服务。它由两部分组成。inbox 是服务自有表中的任务队列，由一个 RustStream Broker 提供服务。outbox 记录服务通过任意 RustStream Broker 发布的每条消息，直到消费者处理完它。两部分都支持 Postgres、MySQL 与 MariaDB，以及 SQLite。
