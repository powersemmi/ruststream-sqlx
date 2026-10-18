---
title: 基准测试
description: "ruststream-sqlx 在每条消息上的开销：与裸 sqlx 循环对照的吞吐量，作为插件的 outbox，以及它自身代码的指令数和内存分配次数。"
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {CodeCounts, Throughput} from '@site/src/components/Benchmarks/Sqlx';
import {sqlx} from '@site/src/components/Benchmarks/sqlx';

export const labels = {"loading": "正在读取结果...", "scenario": "场景", "raw": "裸 sqlx 循环", "adapter": "手动驱动 crate", "framework": "RustStream 服务", "overhead": "额外开销", "against": "（比裸循环 {percent}）", "indistinguishable": "无法区分", "brokerBound": "受数据库限制", "measured": "测量于", "details": "这些数字是怎样测得的", "pending": "尚未公布结果：{brokers}。", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动", "table": "以表格显示数字", "axis": "各循环比裸循环最佳一轮低多少", "rounds": "各轮，从最佳到最慢", "best": "最佳一轮", "median": "中位一轮", "noise": "轮次的波动范围", "comparison": "与裸 sqlx 循环对照", "code": "代码的开销", "broker": "inbox", "plugin": "{plugin} 插件", "pluginRaw": "不用 outbox", "pluginAdapter": "手写的 outbox", "pluginFramework": "crate 自带的 outbox", "pluginAxis": "各循环比不用 outbox 的应用最佳一轮低多少", "pluginAgainst": "（比不用 outbox {percent}）", "pluginOverhead": "比手写 outbox", "pluginOverHand": "（比手写 {percent}）"};

export const grid = {"group": "{database}，{form}", "single": "逐条投递", "batchOf": "每批 {size} 条", "workers": "workers({count})", "forms": {"row lock": "行锁方式", "lease": "租约方式", "outbox round trip over Redis Pub/Sub": "经 Redis Pub/Sub 的 outbox 往返"}};

export const counts = {"scenario": "场景", "instructions": "服务，指令数", "rawInstructions": "裸循环，指令数", "allocations": "服务，内存分配", "rawAllocations": "裸循环，内存分配", "pluginInstructions": "指令数（不用 outbox / 手写 / outbox）", "pluginAllocations": "内存分配（不用 outbox / 手写 / outbox）"};

export const machine = {"cpu": "处理器", "architecture": "架构", "cpu_frequency": "频率", "cores": "核心数", "memory": "内存", "memory_speed": "内存速度", "os": "操作系统", "broker": "数据库", "round_trip": "往返时间", "rustc": "编译器", "sqlx": "sqlx", "profile": "构建配置", "features": "特性", "rustflags": "RUSTFLAGS", "valgrind": "Valgrind"};

# 基准测试

`ruststream-sqlx` 在每条消息上的开销：与裸 sqlx 循环对照的吞吐量，作为插件的 outbox，以及它自身代码的指令数和内存分配次数。

## 与裸 sqlx 循环对照 {/* #against-a-raw-sqlx-loop */}

第一张图在测试环境的 PostgreSQL 上把每个场景用三种方式运行。`裸 sqlx 循环` 是手写的循环，它在以同样方式构建的连接池上，按同样的顺序执行 Broker 对同一张表执行的那些语句；这些语句取自方言，因此两者不会走样。`手动驱动 crate` 用一个自己的循环驱动本 crate 的订阅或发布者，没有运行时。`RustStream 服务` 是用户写的服务：一张 `#[derive(Inbox)]` 表、一个 `#[subscriber]` 处理器和应用。第一项差值是本 crate 在它所执行的语句之外的开销，第二项是运行时在其上增加的开销。

每个循环画成一条横条，从最佳一轮延伸到最慢的一轮，圆点标出最佳一轮，缺口标出中位一轮。坐标轴的零点是裸循环的最佳一轮，圆点在零点右侧表示更慢。空心圆点表示该循环与裸循环的差异小于各轮的波动范围。图表下方的表格列出最佳一轮，括号里是中位数。

每条消息至少要付出一次领取、一次结算和一次提交，每一项都是一次到数据库的往返。标为 `受数据库限制` 的行，其裸循环在每条消息上至少有一半时间在等待这些往返。这时本 crate 和运行时的工作发生在裸循环同样要付出的等待之内，该行给出的是它们开销的下限，而不是对开销的测量。

## 作为插件的 outbox {/* #the-outbox-as-a-plugin */}

outbox 可以与任意 Broker 配合，所以把它作为普通应用的一项开销来测量：同一个应用的三个变体，经 `ruststream-fred` 运行在测试环境的 Redis Pub/Sub 和 PostgreSQL 上。`不用 outbox` 是用户不使用 outbox 时写的应用。`手写的 outbox` 是同一个应用，在每次发布前插入记录，再在处理器中取出并标记记录，用裸 sqlx 实现。`crate 自带的 outbox` 是同一个应用，使用本 crate 的注册表、两个中间件层和重发。与不用 outbox 对照，图表显示跟踪一条消息本身的开销；与手写的 outbox 对照，它显示本 crate 的机制在用户无论如何都会执行的语句之外增加了多少。

## 代码的开销 {/* #cost-of-the-code */}

开销图统计一条消息在服务线程上的开销：稳态下 callgrind 测得的指令数和 DHAT 测得的内存分配次数，连接测试环境的 PostgreSQL。计数包括运行时、本 crate，以及 sqlx 驱动程序在该线程上的编码和解码；数据库服务器是另一个进程，不计入。`冷启动` 是启动服务一次性付出的开销：打开连接池的连接、Broker 的启动检查和第一次投递。outbox 的计数让同样的应用运行在 `MemoryBroker` 上，因此其中不含任何传输的开销。

## 结果 {/* #results */}

<Results entry={sqlx} labels={labels} details="/architecture/benchmarks/methodology/" />

下面的计数与它们各自的对照循环并列：inbox 对照裸 sqlx 循环，outbox 对照不用 outbox 的应用和手写 outbox 的应用。

<CodeCounts labels={counts} />

## 吞吐量 {/* #throughput */}

在每种数据库上，用一个以 `workers(n)` 挂载的订阅排空一张已填满的表，连接池有 16 个连接，分别使用逐条投递和每批 64 条。PostgreSQL 和 MySQL 采用行锁方式。SQLite 没有行锁，因此采用租约方式。裸循环在一个连接池上运行 n 个任务，每个任务领取、读取并删除，直到领取返回空。本 crate 的循环在同一个连接池上运行 n 个订阅，每个订阅使用自己的 Broker。服务运行一个订阅，由它为所有 worker 领取。outbox 的单元格在 outbox 的三个变体中，都以 `workers(n)` 挂载中继和接收端。

<Throughput labels={labels} words={grid} details="/architecture/benchmarks/methodology/" />

## 机器 {/* #the-machine */}

<Environment entry={sqlx} labels={machine} />

数字来自本版本的 `docs/benchmarks/results.json`，由基准测试命令写入，并作为 [`benchmarks/results.json`](https://powersemmi.github.io/ruststream-sqlx/latest/benchmarks/results.json) 与页面一起发布。[方法论](methodology.md)说明它们是怎样测得的，[数字](numbers.md)说明可以从中得出什么结论。

<MovedSections />
