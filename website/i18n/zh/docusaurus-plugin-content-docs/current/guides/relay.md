---
title: 把事件中继到另一个 Broker
sidebar_position: 8
description: "在一个事务中提交业务行及其事件，然后用一个 inbox 订阅把事件中继到 Kafka、NATS 或其他任意 Broker。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 把事件中继到另一个 Broker

本指南在与事件所描述的数据相同的事务中写入事件，然后把它发布到另一个 Broker。

一个服务保存订单，然后向 Kafka 发布“订单已下”，这里有两次可能脱节的写入。订单可能已经提交而发布失败，也可能已经发出发布而订单回滚了。[事务性 outbox](https://microservices.io/patterns/data/transactional-outbox.html) 把它们合在一起：事件是在订单事务中写入的一行，再由中继发布已提交的行。在本 crate 中，事件表是一张队列表，中继是一个普通的订阅。

示例是一个网店。结账请求下单并写入它的事件；中继把每个事件发布到一条总线上，这里是内存 Broker。

## 订单及其事件 {/* #the-order-and-its-event */}

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/relay.rs#schema"
```

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/relay.rs#write"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/relay.rs#write"
```

</TabItem>
</Tabs>

`checkout` 以[事务模式](transactional.md)运行：它通过投递的事务写入订单和事件。它的确认把两者与结账行的结算一起提交。插入失败的订单会回滚它的事件，不为它发布任何东西。处理器之外的代码，例如一个 HTTP 端点，在它自己的 sqlx 事务中写入同样的两行，做法与[任务入队](enqueueing.md)相同。这时事件行同样不唤醒任何订阅：中继在它的[轮询间隔](../components/subscriptions/poll-interval.md)内发现它。

## 中继 {/* #the-relay */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/relay.rs#relay"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/relay.rs#relay"
```

</TabItem>
</Tabs>

`relay` 读取 `order_events`，以每个事件本身作为对它的应答；应答类型声明了它的目的地 `orders`。应答经由总线发出：[`bindable()`](api:ruststream::Broker::bindable) 包装目标 Broker，`bind(MemoryPublish)` 为它生成一个发布令牌，`.out_reply(to_bus)` 通过这个令牌发送 `relay` 的应答。Kafka 或 NATS 服务在同样的两行中换上自己的 Broker 和发布策略。应答在事件行结算之前发布，确认删除该行。

中继是至少一次的。发布失败的应答会把事件放回队列，在发布之后、确认之前发生的崩溃会再次发布它。不能重复执行的消费者要保存事件的标识，这里是订单，并跳过已经见过的事件。

## 按聚合保持顺序 {/* #order-per-aggregate */}

`aggregate` 是事件的[分区键](../components/subscriptions/workers.md)，即订单的 id。`workers_by_key(4)` 同时中继四个订单，并让同一订单的事件保持写入时的顺序。这个顺序在一个中继订阅之内成立。在多个实例上运行、事件必须在所有实例之间按聚合保持顺序的中继，把事件放进 [FIFO 组](ordering.md)，或者采用咨询锁方式。

中继不需要“已发送”标志，也不需要自己的轮询：领取由队列负责，发布失败后的延迟是一次[重试](retries.md)，一个上限加一个死信组则搁置目标一直拒绝的事件。[跟踪你发布的消息](outbox.md)是另一种形态：outbox 插件记录处理器发布的内容，并在消费者处理之后标记它，但它的记录单独提交，在业务事务之外。

## 测试 {/* #testing */}

示例文件中的测试通过测试框架发布结账请求，并用 `tb.broker::<MemoryBroker>().published(..)` 读取到达总线的内容。一个订单只到达总线一次，它的事件行随之消失。同一个订单结账两次只发布一个事件：第二次结账失败，它的事件已经回滚。事件行不唤醒任何订阅，所以这些测试用 [`TestApp::start_live`](../components/testing/live.md) 在真实时钟上运行，越过轮询间隔。
