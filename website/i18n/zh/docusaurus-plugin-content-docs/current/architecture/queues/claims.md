---
title: 领取
sidebar_position: 1
description: "领取一行的三种方式：由打开的事务持有的行锁、写入行中的租约，以及键上的咨询锁，各以连接数计价。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "三种领取方式，各自从领取到结算：行锁、租约和咨询锁",
  "text": "以文字阅读本图",
  "rowLock": "行锁",
  "lease": "租约",
  "advisory": "咨询锁",
  "rowLockClaim": ["BEGIN", "SELECT .. FOR", "UPDATE SKIP", "LOCKED"],
  "rowLockHandler": ["处理器运行，", "领取的事务", "保持打开"],
  "rowLockSettle": ["结算语句，", "然后 COMMIT"],
  "leaseClaim": ["UPDATE ..", "locked_until,", "attempt + 1", "RETURNING"],
  "leaseCommit": ["立即 COMMIT：", "不占用", "连接"],
  "leaseHandler": ["处理器运行，", "没有事务；", "每隔半个租约", "续约一次"],
  "leaseSettle": ["结算，WHERE", "租约仍是", "自己的"],
  "advisoryCandidates": ["选出到期的", "行及其键"],
  "advisoryLock": ["在各自的连接", "上为每个键", "加锁，从不", "等待"],
  "advisoryTake": ["取行：计入", "尝试次数，", "读出该行"],
  "advisoryHandler": ["处理器运行，", "持有键的锁"],
  "advisorySettle": ["在该连接上", "结算，", "然后解锁"]
};

# 领取

领取一行的三种方式：由打开的事务持有的行锁、写入行中的租约，以及键上的咨询锁，各以连接数计价。

<Diagram name="claims" words={words} />

表的描述选定领取方式。没有设置方式的表采用行锁方式；`locked_until` 列选定租约方式；`advisory_lock = ".."` 选定咨询锁方式。各种方式的区别在于处理器运行时由什么占有该行，因此进程终止时由什么让该行回来也不同。

## 行锁 {/* #row-lock */}

领取打开一个事务，用 `FOR UPDATE SKIP LOCKED` 选出它的行。并发的领取会跳过被锁住的行，而不是等待它们。处理器运行期间事务保持打开，由结算的语句提交它，因此行和它的结果在同一次提交中改变。进程终止时，数据库回滚它的事务，这些行立即到期。代价是每个处理中的投递占用一个连接，并在整个处理器运行期间保持占用。在 MySQL 和 MariaDB 上，除非表指定了隔离级别，事务以 READ COMMITTED 打开，因此领取只锁住它取到的行，而不锁住行之间的间隙。

## 租约 {/* #lease */}

领取把到期时间写入 `locked_until`，计入尝试次数并立即提交，因此处理器运行时没有打开的事务，也不占用连接。订阅每隔半个租约为每个处理中的租约续约，所以处理器的运行时间可以超过它的租约。到期时间就是投递的令牌：结算写明它持有的到期时间，只在该行仍持有这个时间时才生效。如果处理器运行期间租约已经到期，原因是续约失败或停止，它会在行中发现另一次领取的令牌，它的结算返回 [`SqlxBrokerError::LeaseLost`](api:ruststream_sqlx::SqlxBrokerError::LeaseLost)。进程终止时，它的行在租约到期后回来：未设置时为三十秒，以整秒计。到期时间由主机计算，因为每次结算都要比较它，所以租约表读取主机的时钟。

## 咨询锁 {/* #advisory-lock */}

领取选出到期的行及其键，在各自的连接上不加等待地尝试获取每个键的锁，跳过被其他会话持有的键，然后在该行仍可领取时取走它。键相同的行逐个进入处理，因此 `advisory_lock = "accounts-{account}"` 让每个账户的行彼此分开，而其他账户的行照常运行。没有事务保持打开，但持有锁的连接会一直跟随投递，直到它结算。进程终止时，它的会话结束，数据库释放这些锁。键属于数据库，而不属于某张表：键相同的两张表会互相等待。

## 各数据库 {/* #per-database */}

| | PostgreSQL | MySQL 8.0.1+、MariaDB 10.6+ | SQLite |
|---|---|---|---|
| 行锁 | `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` | 没有行锁：不提供 |
| 租约领取 | 一条语句：一次加锁读取、对找到的行的更新、返回这些行 | 一次加锁读取，然后每行一次更新，在一个 READ COMMITTED 事务中 | 一条 `UPDATE .. RETURNING`，返回的行之间没有顺序 |
| 咨询锁 | 对键的 64 位哈希（`hashtextextended`，PostgreSQL 11+）调用 `pg_try_advisory_lock` | 对数据库名和键调用 `GET_LOCK(name, 0)`；超过 64 个字符的名称取其 SHA-256 | 键保存在进程中，每个数据库文件一个进程 |
| FIFO 组的守卫 | 对表和组的哈希加事务锁 | 对组中未完成的行做加锁读取 | 无：同一时间只有一个写入者 |

低于版本下限的服务器会让订阅在启动时以 [`SqlxBrokerError::ServerTooOld`](api:ruststream_sqlx::SqlxBrokerError::ServerTooOld) 停止：MySQL 8.0.1 和 MariaDB 10.6 是最早能跳过被锁行的版本。PostgreSQL 的咨询锁存在于获取它的会话中，因此这种方式需要直接连接，或者会话模式的连接池代理。同一个 SQLite 文件上的两个进程看不到彼此的键，这时一行可能被投递两次。

在 FIFO 表中，领取只取它所在组的首行，组中有另一行处于处理中时则什么也不取。上表中的守卫在领取的事务中锁住这个组，因此在不同 Broker 上订阅同一个组，也能保持它的顺序。

尝试次数在领取方式占有该行的地方计入：行锁方式在结算时、在把行放回队列的语句中计入；租约方式在领取时计入；咨询锁方式在取行时计入。所以在租约方式和咨询锁方式下，崩溃会用掉一次尝试；在行锁方式下则不会，因为回滚会连同其他一切撤销这次计数。投递报告的是领取之前的尝试次数，因此在每种方式下，第一次投递读到的都是 1。

这些原语由各数据库的文档说明：PostgreSQL 的 [`SELECT .. FOR UPDATE`](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE) 和[咨询锁](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)，MySQL 的[加锁读取](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html)和[锁函数](https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html)，MariaDB 的 [`SKIP LOCKED`](https://mariadb.com/kb/en/select/#skip-locked)。
