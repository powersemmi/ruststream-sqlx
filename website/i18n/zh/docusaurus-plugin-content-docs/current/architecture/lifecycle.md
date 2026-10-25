---
title: 生命周期
sidebar_position: 3
description: "SqlxBroker 的构造函数记录配置，connect 构建连接池并确定方言，每个订阅在启动时预备自己的 SQL，shutdown 停止领取。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "以类型表示的 Broker 状态，以及每次转换在数据库上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "database": "在数据库上",
  "unconnected": ["SqlxBroker", "::from_url(..)"],
  "connected": ["ConnectedSqlx", "Broker"],
  "closed": ["ClosedSqlxBroker", "locks_released,", "connections_closed"],
  "config": ["连接池设置、路由、", "轮询间隔、租约：无 I/O"],
  "connection": ["构建连接池，取一个", "连接，确定方言；", "listen_notify 下", "保留一个用于 LISTEN"],
  "subscriptions": ["每个订阅：", "检查声明，", "构建语句，", "读取服务器版本，", "逐条预备"],
  "running": ["领取循环运行；", "续约任务每隔", "半个租约续约"],
  "stop": ["领取和续约", "停止；咨询锁", "释放；监听连接和", "构建的连接池", "关闭"],
  "afterShutdown": ["比 Broker 活得更久的发布者会返回", "SqlxBrokerError::Closed，绝不静默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`SqlxBroker::from_url`](api:ruststream_sqlx::SqlxBroker::from_url) 记录配置，connect 构建连接池并确定方言，每个订阅在启动时预备自己的 SQL，shutdown 停止领取。

<Diagram name="lifecycle" words={words} />

`from_url`、`from_connect_options`、`route`、`poll_interval`、`lease`、`connection_limit` 和 `listen_notify` 都不做 I/O，因此服务的构建器保持同步，并在运行时启动之前运行。[`SqlxBroker::new`](api:ruststream_sqlx::SqlxBroker::new) 接收服务自己在运行时内构建、并与自己的代码共用的连接池。

connect 按记录的设置构建连接池，再从中取一个连接，以此检查数据库是否可达：驱动读不懂的 URL 让启动返回 [`SqlxBrokerError::Url`](api:ruststream_sqlx::SqlxBrokerError::Url)，数据库不可达时，启动返回 [`SqlxBrokerError::Connect`](api:ruststream_sqlx::SqlxBrokerError::Connect)。Broker 在这个连接上确定它的方言，这对 `AnyPool` 很重要：它在这里得知自己连到的是哪种数据库。然后它归还这个连接。在 `listen_notify` 下，它再取一个连接，一直保留到 shutdown，用来监听通知。

各个订阅依次打开。首先检查订阅的声明：只有 `max_attempts` 而没有 `dead_letter`，或者反过来，以及在没有 `attempt` 列的表上设置上限，都会以 [`SqlxBrokerError::Declaration`](api:ruststream_sqlx::SqlxBrokerError::Declaration) 停止订阅。订阅的语句根据表的描述构建，同一个 Broker 上对同一队列的第二个订阅会被拒绝。接着，在一个连接上，方言在有版本下限的数据库上读取服务器版本，MySQL 和 MariaDB 就是这样，然后每条语句都在服务器上预备：数据库缺少的表或列会让服务在启动时以 [`SqlxBrokerError::Schema`](api:ruststream_sqlx::SqlxBrokerError::Schema) 停止，错误中写明表和语句。最后，在 `listen_notify` 下，订阅监听它的表的通道。这一阶段的每个错误都是启动错误，发生在领取任何消息之前。

服务运行期间，每个订阅按[唤醒](queues/waking.md)一页所说的方式领取。在租约方式下，一个单独的任务每隔半个租约为每个处理中的租约续约。

shutdown 设置 Broker 的关闭标志，由于与服务共用的连接池保持打开，这个标志会拒绝此前交出的每个发布者。它停止领取循环和续约任务。行锁方式或租约方式下处理中的投递照常结算：它的事务或租约仍然占有该行，只是租约不再续约。在咨询锁方式下，shutdown 释放每个处理中投递的锁，锁被释放的投递不能再结算：它的结算返回 `Closed`，它的行回到队列中。shutdown 等待每个仍在关闭的会话，关闭监听连接和它构建的连接池，然后返回 [`ClosedSqlxBroker`](api:ruststream_sqlx::ClosedSqlxBroker)。它统计两项：释放后连接回到连接池的锁数，以及改为关闭的连接数。

shutdown 本身不归还已领取的行。每种方式以自己的途径归还：行锁方式在事务回滚或连接关闭时归还，租约方式在被丢弃的投递释放租约或租约到期时归还，咨询锁方式在锁释放时归还。在服务中，运行时先排空处理器，再关闭 Broker。在排空超时时被中止的处理器会丢弃它的投递。
