---
title: 事务
sidebar_position: 2
description: "本 crate 为一次投递打开的事务：行锁方式的领取事务，以及在事务模式下处理器写入所用、按表的隔离级别打开的事务。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "在事务模式下，每种方式中处理器写入所用的事务，以及在它之外提交的内容",
  "text": "以文字阅读本图",
  "rowLock": "行锁",
  "lease": "租约",
  "advisory": "咨询锁",
  "rowLockTx": "领取的事务",
  "leaseTx": "投递的事务",
  "advisoryTx": "投递的事务",
  "rowLockClaim": ["BEGIN，领取", "FOR UPDATE", "SKIP LOCKED"],
  "rowLockSavepoint": ["领取之后", "立即设置", "SAVEPOINT"],
  "rowLockWrites": ["处理器", "通过 Tx", "写入"],
  "rowLockOutcome": ["ack：与结算", "一起 COMMIT；", "否则 ROLLBACK", "TO SAVEPOINT"],
  "leaseClaim": ["领取打上", "租约，", "COMMIT"],
  "leaseBegin": ["按表的隔离", "级别", "BEGIN"],
  "leaseWrites": ["处理器", "通过 Tx", "写入"],
  "leaseOutcome": ["ack：租约仍", "有效则结算，", "COMMIT；", "租约丢失：", "全部回滚"],
  "advisoryTake": ["为键加锁，", "取行"],
  "advisoryBegin": ["在锁所在的", "连接上", "BEGIN"],
  "advisoryWrites": ["处理器", "通过 Tx", "写入"],
  "advisoryOutcome": ["ack：结算，", "COMMIT；", "否则先", "回滚，", "再结算"],
  "advisoryUnlock": ["为键", "解锁"]
};

# 事务

本 crate 为一次投递打开的事务：行锁方式的领取事务，以及在事务模式下处理器写入所用、按表的隔离级别打开的事务。

<Diagram name="transactions" words={words} />

以 `.transactional()` 挂载的订阅，把投递的事务以 [`Tx`](api:ruststream_sqlx::Tx) 的形式借给处理器。确认会把处理器的写入与该行的结算一起提交，因此处理器的结果和任务的结束是同一次提交。其他任何结果都会先回滚处理器的写入，再像在事务模式之外那样结算该行。处理器通过 `Tx` 插入队列表的行，也随确认一起提交。通过连接池的写入，以及通过 Broker 的发布，各自单独提交。

行锁方式已经持有一个事务，即领取的事务，所以它的处理器在这个事务中写入。订阅在领取之后设置保存点 `SAVEPOINT ruststream_claim`，正是它让重试能用 `ROLLBACK TO SAVEPOINT ruststream_claim` 丢弃处理器的写入，同时仍在同一次提交中计入尝试次数。租约方式和咨询锁方式会立即提交它们的领取，所以在领取之后为投递另开一个事务。在租约方式下，只有当该行仍持有投递的租约时，确认才生效：丢失租约会回滚整个事务，连同处理器的写入，结算返回 `LeaseLost`。

在事务模式下，无论哪种方式，每个投递都为它的事务占用一个连接。批处理器不能以 `.transactional()` 挂载：这个步骤只服务于逐条投递。

## 隔离级别与模式 {/* #isolation-and-modes */}

| 数据库 | 未声明时 | 表可以声明的级别 |
|---|---|---|
| PostgreSQL | `BEGIN`，服务器的默认级别 | `read_committed`、`repeatable_read`、`serializable` |
| MySQL、MariaDB | READ COMMITTED | 全部四种 |
| SQLite | `BEGIN`，deferred | 模式：`deferred`、`immediate`、`exclusive` |

表用 `isolation = ..` 声明隔离级别，或者在 SQLite 上用 `mode = ..` 声明模式，本 crate 以此打开行锁方式的领取事务和事务模式的每个事务。PostgreSQL 把 READ UNCOMMITTED 当作 READ COMMITTED 运行，所以它的方言不以这个级别打开事务，声明了它的表在 PostgreSQL Broker 上无法编译。SQLite 以可串行化方式运行每个事务：它的模式决定事务何时获取数据库的写锁，是在第一次写入时，还是在事务开始时。在 SQLite 上，投递的事务一直持有这个锁，直到投递结算，在此期间其他所有写入者都无法进入，领取也包括在内。

MySQL 和 MariaDB 以 READ COMMITTED 打开事务，因为它们的默认级别 REPEATABLE READ 会锁住加锁读取所扫描的行之间的间隙：为处理器保持的领取会阻塞对这张表的每一次插入，直到它结束。在 PostgreSQL 上，`repeatable_read` 或 `serializable` 级别的行锁表，在其行的领取和结算同时运行时，领取会返回串行化错误，所以在那里 `read_committed` 是这种方式实际可用的级别。在 PostgreSQL 上，以这两个级别之一运行事务模式的租约表会在启动时停止：这样的事务看到的是它第一条语句读到的行，看不到续约任务在那之后延长的租约。

本 crate 自己驱动这些事务，而不使用 sqlx 的事务类型：在打开状态下被丢弃的事务会关闭它的连接，由服务器回滚它。这些级别是各数据库自己的：[PostgreSQL](https://www.postgresql.org/docs/current/transaction-iso.html)、[MySQL](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html)、[SQLite](https://www.sqlite.org/lang_transaction.html)。
