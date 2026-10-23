---
title: 结算
sidebar_position: 5
description: "在每种领取方式下，ack、drop、retry 和 retry_after 向队列表写入什么，以及尝试计数和死信如何把一次重试变成一次转移。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const settlement = {
  "title": "一次结算：结果选定一条语句，在事务模式下除 ack 之外都会回滚处理器的写入，语句在领取方式占有该行的地方执行",
  "text": "以文字阅读本图",
  "outcome": ["处理器的结果：ack、drop、retry()、", "retry_after(d)，或到达上限时的死信"],
  "rollback": ["在事务模式下，除 ack 之外的任何结果", "都先回滚处理器写入的内容"],
  "statement": ["表为该结果准备的语句，自己的", "或方言的：一条 UPDATE、一条 DELETE，", "或转移到死信"],
  "rowLock": ["行锁：", "在领取的", "事务中执行，", "然后 COMMIT"],
  "lease": ["租约：仅当", "该行仍持有", "这次投递的", "租约时", "执行"],
  "advisory": ["咨询锁：", "在键的", "连接上执行，", "然后解锁"],
  "dropped": ["未结算就被丢弃：事务回滚、租约释放，", "或锁随它的连接一起消失"]
};

export const retries = {
  "title": "队列表上的一次重试：行统计自己的尝试次数，上限决定是把行放回队列还是转入死信",
  "text": "以文字阅读本图",
  "row": ["处理中的一行，它的 attempt 列", "统计投递次数"],
  "outcome": ["处理器返回 retry() 或 retry_after(d)"],
  "cap": ["max_attempts：尝试次数是否已到达上限？"],
  "returns": ["低于上限：该行", "带着加一的尝试", "次数立即返回，", "或在 retry_after 后"],
  "deadLetter": ["到达上限：该行", "转入它的", "死信组", "或死信表"],
  "restart": ["延迟是行的一列：它在重启后依然有效，", "也从不发布消息的副本"],
  "handled": "已处理",
  "retries": "重试",
  "below": "低于上限",
  "atCap": "到达上限"
};

# 结算

在每种领取方式下，`ack`、`drop`、`retry` 和 `retry_after` 向队列表写入什么，以及尝试计数和死信如何把一次重试变成一次转移。

<Diagram name="settlement" words={settlement} />

| 结果 | 行锁 | 租约 | 咨询锁 |
|---|---|---|---|
| `ack`、`drop` | `DELETE`，或设置 `processed_at`，然后 `COMMIT` | 同上，`WHERE locked_until` 等于投递的令牌 | 同上，在投递的连接上执行，然后解锁 |
| `retry()` | `attempt = attempt + 1`，然后 `COMMIT`；没有 `attempt` 列时回滚 | `locked_until = NULL` | 不执行语句：解锁就把行放回队列 |
| `retry_after(d)` | 把 `retry_after` 设为更晚的时间，`attempt + 1` | 把 `retry_after` 设为更晚的时间，`locked_until = NULL` | 把 `retry_after` 设为更晚的时间，然后解锁 |
| 到达上限 | 把行的组设为死信组，或把行转入死信表 | 同上，并清除租约 | 同上，然后解锁 |
| 未结算就被丢弃 | 回滚 | 立即释放租约 | 解锁，关闭它的连接 |

领取方式决定由什么占有该行，因此也决定语句在哪里执行。在行锁方式下，语句在领取的事务中执行，事务的提交就是结算：行和它的结果一起改变。在租约方式下，每条语句都带上投递的令牌，即领取时写入的到期时间，只在该行仍持有这个令牌时才改动它。发现别的令牌的结算返回 [`SqlxBrokerError::LeaseLost`](api:ruststream_sqlx::SqlxBrokerError::LeaseLost)，此时该行属于之后领取它的那次领取。在咨询锁方式下，语句在持有该键的连接上单独提交，随后解锁。语句在这里失败时，锁照样释放，行按原样回到队列。

`drop` 像 `ack` 一样结束该行：毒消息离开队列。没有 `retry_after` 列的表把 `retry_after(d)` 当作 `retry()` 处理，并在日志中记录一条警告。用 `custom(..)` 命名的服务自有事件，在同一个事务中或同一个连接上执行自己的 SQL，代替方言的语句。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

队列表自己统计投递次数：`attempt` 列。行锁方式在把行放回队列的语句中计数，租约方式在领取时计数，咨询锁方式在取行时计数。处理器通过 `Ctx<keys::Attempt>` 读取这个计数，从而自行计算退避时间：本 crate 不保存退避策略。

上限和它的去向一起在挂载处设置：`max_attempts(n)` 和 `dead_letter(..)`。对尝试次数已到达上限的行调用 retry 或 `retry_after`，会改为转移该行，并在日志中记录一条警告。在有组的表中，死信是一个组，转移就是对该行的组做一次 `UPDATE`。在没有组的表中，死信是另一张具有相同列的表：在 PostgreSQL 上是一条语句 `WITH moved AS (DELETE .. RETURNING ..) INSERT .. SELECT`；在 MySQL、MariaDB 和 SQLite 上是在一个事务中先复制、再删除。`max_attempts(1)` 在第一次失败时就转移该行。

延迟从不离开数据库。描述符声明由数据库自己移动投递，因此运行时的延迟副本在这里没有用处，其他 Broker 会为 `retry_after` 发布这种副本：行带着尚未到来的 `retry_after` 留在队列中等待，并且在服务重启后依然存在。

outbox 也按处理器的结果结算它的记录，在消费者一侧：确认和 drop 标记记录，重试把记录留给下一次启动，[Outbox](outbox.md) 一页用图说明了这一点。
