---
title: 顺序：FIFO 组与分区键
sidebar_position: 5
description: "保持任务的顺序：同一时间只有一个实例处理的 FIFO 组，以及在并行 worker 之间保持每个账户顺序的分区键。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 顺序：FIFO 组与分区键

本指南保持任务的顺序：一个组的全部任务，或者每个键的任务，同时让其他键并行运行。

`SKIP LOCKED` 让领取很快，因为它跳过处理中的行。它也让下一行在前一行仍在处理或等待重试时就被取走。任务必须按顺序生效的队列，要在它的表中声明这一点。

示例是一个账户服务。它把联系人变更发送到 CRM，CRM 必须按变更发生的顺序接收它们；它还记入账簿分录，分录必须在一个账户内保持顺序。

## 表 {/* #the-tables */}

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/ordering.rs#schema"
```

`crm_jobs` 上的索引遵循领取顺序，组在最前。FIFO 组的领取会读取该组未完成的行，索引让这次读取只涉及这些行（[在生产环境中运行](production.md)）。

## FIFO 组 {/* #a-fifo-group */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#fifo"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#fifo"
```

</TabItem>
</Tabs>

`#[field(group, fifo = true)]` 让 `target` 的每个组成为 [FIFO 组](../components/subscriptions/fifo.md)。无论有多少个 worker、订阅和进程在读取，一个 FIFO 组最多只有一行处于处理中。按领取顺序，组中第一个未完成的行是它的首行，领取只取首行。首行处于处理中时，后面的行都要等待，首行处在另一个实例的租约下时也是如此。`retry()` 让这次变更保持在首位，所以它会先于后面的变更再次发出。`retry_after(d)` 则会把它移到比它更早到期的行之后。

一直失败的首行会阻塞整个组。挂载处为它设置上限：

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#app"
```

</TabItem>
</Tabs>

被拒绝三次之后，这次变更转入 `crm.parked` 组，`crm` 的下一次变更成为首行。被搁置的变更在那里等人处理；它后面的变更已经在没有它的情况下发出，这就是释放这个组的代价。

各个组并行运行，每个组通过自己的订阅处理，所以组适合少数事先已知的流：每个下游系统一个，或每个租户一个。`workers(n)` 不会让 FIFO 订阅并行，它由一个 worker 服务。

## 每个账户一个键 {/* #a-key-per-account */}

账簿有很多账户，多到无法为每个账户建一个订阅。要紧的顺序在单个账户之内，不同账户可以并行运行。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#keyed"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#keyed"
```

</TabItem>
</Tabs>

`#[field(partition_key)]` 把账户作为每一行的键。应用用 [`workers_by_key(4)`](../components/subscriptions/workers.md) 挂载 `post`：四个 worker，一个账户的分录都在其中一个 worker 上，按领取的顺序运行。在这样的表上，订阅只保持一次进行中的领取，所以各次领取不会互相超越。这个顺序在一个订阅之内成立。一个账户的分录如果必须在多个实例之间保持顺序，就放进 FIFO 组，或者采用键取账户的[咨询锁方式](../components/forms/advisory.md)，它让同一个键的行在同一服务器上的所有进程之间逐个进入处理。

## 测试 {/* #testing */}

示例文件中的测试在内存中的 SQLite 上运行服务。第一个测试发布一次 CRM 拒绝的变更和一次它接受的变更：第一次变更发出三次后被搁置，然后第二次变更发出。第二个测试把 `crm` 的首行写成由另一个实例租用，发现它后面的变更在等待。第三个测试交错写入两个账户的分录，发现每个账户的分录都按顺序记入。后两个测试的行不唤醒任何订阅，所以它们用 [`TestApp::start_live`](../components/testing/live.md) 在真实时钟上运行，越过轮询间隔。
