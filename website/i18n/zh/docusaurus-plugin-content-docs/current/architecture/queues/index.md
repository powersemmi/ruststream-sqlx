---
title: 表
sidebar_position: 0
description: "队列就是服务自己的一张表：一行是一条消息，一个组是一个队列，各列的角色决定一行何时到期，以及结果做什么。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一张队列表：写入让一行立即到期或稍后到期，领取把到期的行取来处理，结果结束该行、放回该行，或把它转入死信",
  "text": "以文字阅读本图",
  "write": ["经 Broker 的一次发布，或服务自己的", "一次插入，写入一行"],
  "table": "email_jobs，服务的一张表",
  "scheduled": ["已排期", "retry_after 未到：", "到时即到期"],
  "due": ["到期", "时间已到，未完成，", "没有租约占有"],
  "inWork": ["处理中", "按 priority、", "retry_after、id 领取"],
  "deadLetter": ["死信", "另一个组，", "或另一张表"],
  "finished": ["已完成", "设置了 processed_at，", "或删除了该行"],
  "roles": ["按角色划分的列：id、group、priority、", "partition_key、retry_after、attempt、", "processed_at、locked_until、headers、", "payload；除 id 外的每个角色都可选"],
  "optional": ["一个组就是一个队列。没有 group 列的表是一个队列，", "每个 Broker 用一个订阅读取它"],
  "later": "retry_after 未到",
  "now": "立即到期",
  "claim": "领取",
  "retry": "retry()",
  "retryAfter": "retry_after(d)",
  "spent": "尝试用尽",
  "ack": "ack、drop"
};

# 表

队列就是服务自己的一张表：一行是一条消息，一个组是一个队列，各列的角色决定一行何时到期，以及结果做什么。

<Diagram name="table" words={words} />

本 crate 不创建表，也不执行迁移。`CREATE TABLE` 由服务编写，再由一个结构体向本 crate 描述这张表：[`#[derive(Inbox)]`](api:ruststream_sqlx::Inbox) 从属性中读取描述，[`InboxTable`](api:ruststream_sqlx::InboxTable) 加 [`InboxSpec`](api:ruststream_sqlx::InboxSpec) 是手写的同一种描述。每一行都由服务自己的 [`Publish`](api:ruststream_sqlx::Publish) 实现写入，因此插入语句的写法也由服务决定。

一行在满足四个条件时可以领取，每个条件只在表有对应的列时才适用：该行属于订阅的组，它的 `retry_after` 已过，它的 `processed_at` 为空，并且没有租约占有它（`locked_until` 为空或已过）。领取按领取顺序取可领取的行：先看 `priority`，值越小越早，然后看 `retry_after`，最后看 id。`retry_after` 在将来的行是一个排期任务：在时间到来之前，队列看不到它。

订阅的名称选定它的队列。在有 `group` 列的表中，名称就是组，每个组都是一个独立的队列。在没有这一列的表中，名称指向整张表。一个 Broker 为每个队列打开一个订阅：对同一张表和同一个组的第二个订阅，会让服务在启动时以 [`SqlxBrokerError::AlreadySubscribed`](api:ruststream_sqlx::SqlxBrokerError::AlreadySubscribed) 停止。多个 Broker 或服务的多个实例可以共享一个队列，领取方式让它们各自的行互不交叉。

一行交给处理器什么，取决于两个角色。有 `payload` 列时，该行处于载荷模式：处理器接收由编解码器解码的载荷。没有这一列时，该行处于行模式：处理器接收驱动程序读出的行本身。`headers` 列把投递的消息头保存在一列中，在 `json` 特性下是一个由字符串组成的 JSON 对象。消息头布局则改为从各自的列中读取消息头，每个消息头一列。两者都没有的表投递空的消息头。

“现在”在三个时刻读取：领取选择到期的行时、重试推迟一行时，以及 ack 标记一行时。默认由主机通过 [`SystemClock`](api:ruststream_sqlx::SystemClock) 读取，并以该列自己的类型绑定。设置 `clock = DatabaseClock` 后，语句改为读取数据库的时钟：

| 数据库 | 语句中的“现在” |
|---|---|
| PostgreSQL | `statement_timestamp()` |
| MySQL、MariaDB | `UTC_TIMESTAMP(6)` |
| SQLite | `strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')`，文本 |

SQLite 以文本保存时间，并按文本比较，因此那里的表在每一行中都使用同一种时间格式。租约方式始终读取主机的时钟：使用 `DatabaseClock` 的租约表无法编译，原因见[领取](claims.md)。

领取如何把一行取来处理，见[领取](claims.md)；每种结果写入什么，见[结算](../settlement.md)。
