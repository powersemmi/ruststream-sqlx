---
title: Outbox
sidebar_position: 7
description: "outbox 在任意 Broker 上把每次发布记录到服务自己的表中，消费者确认后标记记录，并在启动时重发其余记录。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条被跟踪的消息：先记录，再带着记录的 id 经任意 Broker 发送，由消费者取出记录，处理器确认后标记它",
  "text": "以文字阅读本图",
  "producer": ["生产方处理器", "发布一条被跟踪的消息"],
  "republish": ["启动时：", "每条尚未", "处理的记录"],
  "record": ["发布层：服务对记录的", "INSERT，在它自己的", "连接上执行"],
  "broker": ["任意 RustStream Broker：", "消息及其记录的 id"],
  "fetch": ["订阅层：按 id 取出", "记录，仅当它", "尚未处理"],
  "processed": ["已经处理：", "ack，不运行", "处理器"],
  "consumer": ["消费方处理器"],
  "mark": ["ack 或 drop：标记记录，", "processed_at = now，", "或删除"],
  "waits": ["retry：记录", "等待下一次", "启动"],
  "guarantee": ["记录单独提交，从不加入投递的事务：", "一次发布至少投递一次"],
  "sends": "然后发送",
  "resends": "带着它的 id",
  "delivers": "投递",
  "unprocessed": "未处理",
  "acks": "确认",
  "retries": "重试"
};

# Outbox

outbox 在任意 Broker 上把每次发布记录到服务自己的表中，消费者确认后标记记录，并在启动时重发其余记录。

<Diagram name="outbox" words={words} />

outbox 是 RustStream 运行时的一个插件，不属于队列表。它由两个中间件和一个启动钩子组成，包装任意 Broker 的发布者和处理器，inbox 的也包括在内。注册表用 `outbox!` 或 [`Outbox::track`](api:ruststream_sqlx::outbox::Outbox::track) 构建，列出它跟踪的消息，以及每条消息写入的记录类型。发布层通过记录自己的 [`Publish`](api:ruststream_sqlx::outbox::Publish) 实现，为每条被跟踪的消息写入一条记录，然后发送消息，并把记录的 id 放在 [`x-ruststream-outbox-id`](api:ruststream_sqlx::outbox::OUTBOX_ID_HEADER) 消息头中。记录写入失败时，发布返回错误，因此没有记录的消息不会被发送。

订阅层在消费者一侧读取这个消息头。它按 id 取出记录，在表会标记记录的情况下，条件是 `WHERE id = ? AND processed_at IS NULL`。已经处理过的记录会直接确认投递，不运行处理器，这样被发送两次的副本只处理一次。取出失败时，投递会重试，也不运行处理器。随后由处理器的结果结算记录：确认和 drop 会标记记录，把 `processed_at` 设为数据库时钟的时间，或者删除该行；`retry` 和 `retry_after` 保留记录，记录等待下一次启动。标记失败时会记录日志，保持处理器的结果，同样把记录留给下一次启动。

启动时，在各订阅打开之后，重发过程按名称选出每条未处理的记录，带着它的 id 通过 Broker 的发布者再次发布。重发过程无法读取或发送某条记录时，启动返回错误。两个同时启动的实例可能都发送同一条记录，所以不能重复处理消息的消费者要自己检查这一点。

## 记录自己的连接 {/* #the-records-own-connection */}

被跟踪的发布为插入记录单独取一个连接，记录在这个连接上提交。它从不加入投递的事务：事务处理器运行期间写入的记录，无论处理器的结果如何都会提交。因此消息及其记录与服务的业务数据不在同一次提交中。outbox 保证的是一次发布至少投递一次：记录在发送之前写入，每次失败或从未被处理的发送都会留下一条记录，由下一次启动再次发送。常见的事务性 outbox，即业务行和事件行一起提交、由中继轮询该表，在这里就是 inbox 的一张队列表，由一个向另一个 Broker 发布的处理器读取。

## 开销 {/* #costs */}

注册表是类型系统中的一个列表，每个注册的名称一个节点。因此名称未注册的消息只为每个节点付出一次名称比较，此外没有任何开销：没有内存分配，没有数据库调用，也没有动态调用。被跟踪的发布为插入取一个连接，并为 id 消息头的文本分配内存。被跟踪的投递为取出记录取一个连接，在处理器运行之前归还，以便处理器能发布自己的被跟踪消息，然后为标记再取一个连接。在 `#[ruststream::app]` 内、注册表构建之后才设置的连接池，通过 `OnceLock` 读取：每条被跟踪的消息一次原子加载。测试构建默认关闭 outbox，除非设置 `RUSTSTREAM_SQLX_OUTBOX=on`；生产构建始终跟踪，这个开关在其中被编译掉。

这个模式是[事务性 outbox](https://microservices.io/patterns/data/transactional-outbox.html)，加上了投递一侧：结束一条记录的是消费者的标记，而不是发送。
