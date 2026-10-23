---
title: 路径
sidebar_position: 4
description: "一行在服务中的路径：领取、投递、解码、处理器、应答和结算，可以逐行处理、按批处理，或者以行本身交给处理器。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一行从领取到结算的路径：逐行处理并应答、按批处理，以及行模式",
  "text": "以文字阅读本图",
  "single": "逐行",
  "batch": "按批",
  "row": "行模式",
  "claim": ["领取：每个", "空闲 worker", "一行"],
  "delivery": ["InboxDelivery", "行、id、", "尝试次数"],
  "decode": ["编解码器", "读取从行中", "借用的载荷"],
  "handler": ["处理器"],
  "reply": ["应答：复用", "一个缓冲区，", "由 Routed", "插入"],
  "settle": ["结果结算", "该行"],
  "batchClaim": ["领取最多", "批大小", "的行"],
  "batchDelivery": ["一批投递，", "一次领取"],
  "batchDecode": ["每个载荷", "单独解码"],
  "batchHandler": ["批", "处理器"],
  "batchSettle": ["每行各自", "结算；行锁：", "一次提交"],
  "rowClaim": ["领取：每个", "空闲 worker", "一行"],
  "rowDelivery": ["驱动程序", "读出的行"],
  "rowHandler": ["处理器", "接收 &Row"],
  "rowSettle": ["结果结算", "该行"],
  "noCodec": "无编解码器"
};

# 路径

一行在服务中的路径：领取、投递、解码、处理器、应答和结算。行可以逐行处理、按批处理，或者以行本身交给处理器。

<Diagram name="path" words={words} />

领取按表的[领取方式](queues/claims.md)把行取来处理，每一行变成一个 [`InboxDelivery`](api:ruststream_sqlx::InboxDelivery)：其中有驱动程序解码出的行、它的 id、它的尝试次数，以及领取方式为它持有的东西，即一个事务、一个租约令牌或一个键的连接。在载荷模式下，编解码器直接从行的列中读取载荷：字节是借用的，不做复制。消息头在第一次读取时才构建，因此从不读取消息头的处理器不为它们付出开销。载荷无法解码的行由订阅的解码策略结算。

在行模式下，表没有载荷列，处理器接收驱动程序读出的行本身，即 `&Row`。不运行编解码器，也不复制任何东西。

一个批就是一次领取：最多批大小那么多行，一起投递，每行单独解码。无法解码的行由解码策略结算，批中的其他行保留。批中的每个投递结算自己的行。在行锁方式下，一个批的各行共用领取的事务，因此它们的结算在最后一个完成时一起生效，一条语句失败就会回滚整个批。在租约方式和咨询锁方式下，每行各自结算。

应答被编码进分发循环复用的缓冲区，本 crate 的发布者以借用切片的形式接收它。默认的应答发布者 [`Routed`](api:ruststream_sqlx::Routed) 在 Broker 的路由中查找应答的名称，并通过行的 [`Publish`](api:ruststream_sqlx::Publish) 实现插入它。插入之前，它检查 Broker 尚未关闭，并且表能容纳消息的每个消息头。插入之后，在 `listen_notify` 下它用 `pg_notify` 通告这一行，并唤醒本进程中订阅了该表的订阅。投递在应答之后结算，[结算](settlement.md)一页用图说明了这一点。

[基准测试](/architecture/benchmarks/)公布每条路径上一条消息的开销，以指令数和内存分配次数计，并与执行相同语句的裸 sqlx 循环对照。
