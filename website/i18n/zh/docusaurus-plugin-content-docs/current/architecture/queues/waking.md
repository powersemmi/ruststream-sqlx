---
title: 唤醒
sidebar_position: 3
description: "订阅在领取总是取满时持续领取，否则等待它的轮询间隔，同一 Broker 的发布或一条通知会提前结束这次等待。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "订阅的领取循环，以及在轮询间隔之前结束等待的事件",
  "text": "以文字阅读本图",
  "claim": ["领取", "每个空闲 worker", "一行，或一个批"],
  "full": ["这次领取", "是否取满？"],
  "wait": ["等待", "轮询间隔，被唤醒", "时提前结束"],
  "publish": ["本 Broker 向该表", "和组的", "一次发布"],
  "reconnect": ["监听连接", "已恢复：", "唤醒所有订阅"],
  "notification": ["表的通道上的", "一条通知，", "在 PostgreSQL 上"],
  "writer": ["另一个进程，", "一个触发器：pg_notify", "(channel, group)"],
  "floor": ["轮询间隔始终是每次唤醒的下限：", "未由 Broker 或订阅设置时为一秒"],
  "filled": "取满",
  "fewer": "行数不足",
  "commit": "提交时"
};

# 唤醒

订阅在领取总是取满时持续领取，否则等待它的轮询间隔，同一 Broker 的发布或一条通知会提前结束这次等待。

<Diagram name="waking" words={words} />

运行时轮询订阅的流时，订阅进行领取。取满上限的领取之后立即进行下一次领取，因此积压的消息无需等待即可排空。取到的行少于请求数的领取，说明队列暂时是空的，订阅进入等待。失败的领取以一个错误项到达流中，下一次领取等待一秒，因此数据库不可达时，循环不会空转。

经 Broker 的发布会唤醒同一 Broker 上订阅了该表的订阅：[`Repository`](api:ruststream_sqlx::Repository)、路由、应答，以及测试通过测试框架发布的消息都是如此。在有组的表中，它只唤醒自己那个组的订阅。正在领取的订阅会把唤醒留给下一次等待，那次等待随即立即返回。唤醒让发布为它唤醒的每个订阅付出一次原子操作，订阅本身在每条消息上不付出任何开销。服务自己写入的行，无论用自己的 SQL 还是通过处理器的 `Tx`，都不唤醒任何订阅：它等待轮询间隔，或者等待一条通知。

## LISTEN/NOTIFY {/* #listennotify */}

在 PostgreSQL 上，[`listen_notify`](api:ruststream_sqlx::SqlxBroker::listen_notify) 让其他进程中的订阅也被唤醒。Broker 在 connect 时从连接池取一个连接，在上面监听，一直保留到 shutdown。每个订阅在第一次领取之前监听它的表的通道：通道名就是表名，表有模式时带上模式限定，如 `app.jobs`。Broker 的每次发布在写入之后，立即在写入该行的连接上执行 `SELECT pg_notify(channel, group)`；载荷为空的通知唤醒该表的每个订阅。通知失败时会记录日志，发布仍然成功：轮询间隔会让订阅取到这一行。

PostgreSQL 在事务提交时投递通知，因此被唤醒的订阅能看到这一行。监听连接断开期间发出的通知会丢失：Broker 重新连接，并把每个订阅唤醒一次。通道名最多 63 字节，因此对限定名更长的表的订阅会在启动时停止。这个开关在 Broker 的整个生命周期内占用一个连接，并让每次发布多执行一条语句。发出通知的事务在提交时还会获取一个服务器全局的锁，因此发出通知的提交依次执行，这会限制大量并发的写入者。在其他数据库上，`listen_notify` 无法编译。

服务器在 [`NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html) 一页说明了这个机制：在提交时投递，载荷小于 8000 字节，以及[标识符](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS)的 63 字节限制。提交时的锁由 PostgreSQL 开发者在[邮件列表](https://www.postgresql.org/message-id/24269.1572279925%40sss.pgh.pa.us)中说明。
