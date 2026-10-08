# 基准测试 { #benchmarks }

数据库表里的队列，每条消息都要付出一次领取和一次结算，crate 还要在上面加上自己的活：订阅的流、
解码、分发、结算的记账。这一页说明这些活花了多少，参照物是同样的领取和结算用 sqlx 手写一遍。

每个场景在同一个进程里跑三遍。**裸 sqlx 循环**是手写的循环：它按同样的顺序、在同样配置的连接池
上，执行 Broker 对同一张表执行的那些语句。**ruststream-sqlx** 是用这个 crate 手写的同一份活：连接
好的 `SqlxBroker`，把订阅当作投递的流来读并逐条确认，或者经 crate 自己的发布器发布；这里没有服务、
没有处理器，也没有分发。**RustStream 服务**是用户会写的那个应用：一张 `#[derive(Inbox)]` 表、
一个 `#[subscriber(..)]` 处理器，再加 `RustStream::new(..).with_broker(SqlxBroker::new(pool), ..)`。

由此得到两个差值。crate 对裸循环，是这个 crate 自己的订阅或发布器在它所执行的语句之外多花的开销，
这个数字由本仓库负责。服务对裸循环，是读者从头到尾要付的开销。两者之间的差距，就是运行时在这个
crate 之上多花的开销。

outbox 不是 Broker，而是一个插件，也按插件来测：同一个应用的三个变体，放在
[单独的一节](#the-outbox-as-a-plugin)里解读。

三者共用表、行、解码成的同一个类型、tokio 运行时和构建。裸循环的语句取自 Broker 生成语句所用的
同一个方言，所以几个循环不会走样。这套流程属于框架本身，写在
[RustStream 基准测试页](https://powersemmi.github.io/ruststream/latest/zh/benchmarks/#methodology)
上；这一页公布它在这台机器上得出的结果。

## 数字 { #the-numbers }

在测试环境的 Postgres 上、多线程运行时里的每秒消息数。三个交错轮次中的最佳值，括号里是中位的
一轮。越大越好。

<div id="benchmark-results" data-benchmark-results="../../benchmarks/results.json" data-benchmark-labels='{"loading": "正在加载公布的结果...", "scenario": "场景", "raw": "裸 sqlx 循环", "adapter": "ruststream-sqlx", "framework": "RustStream 服务", "adapterOverhead": "crate 对裸循环", "overhead": "服务对裸循环", "indistinguishable": "无法区分", "brokerBound": "受数据库限制", "machine": "机器", "os": "操作系统", "broker": "数据库", "build": "构建", "versions": "版本", "measured": "测量于", "codeMeasured": "代码开销测量", "instructions": "每条消息的指令数", "rawInstructions": "裸循环的指令数", "allocations": "每条消息的内存分配次数", "rawAllocations": "裸循环的分配次数", "cold": "冷启动（指令 / 分配）", "database": "数据库与形式", "delivery": "投递方式", "workers": "工作者", "single": "逐条", "batchOf": "每批 {size} 条", "forms": {"row lock": "行锁", "lease": "租约"}, "unavailable": "读不到结果。它们公布在 {url}。", "outboxNone": "不用 outbox", "outboxByHand": "手写 outbox", "outboxCrate": "ruststream-sqlx 的 outbox", "outboxTotal": "outbox 对不用", "outboxOwn": "outbox 对手写", "outboxInstructions": "每条消息的指令数（不用 / 手写 / outbox）", "outboxAllocations": "每条消息的内存分配次数（不用 / 手写 / outbox）", "unknownSchema": "公布的结果声明的 schema 是 {schema}，这一页不渲染它。"}'></div>

表格由浏览器从上一次运行写下的文档读出，所以这一页上没有任何会过期的副本。

一次运行先填表，再把它取空。行数由一次试探运行设定，所以无论在哪台机器上测，每次计量的运行都至少
持续五秒。窗口从第一次处理完的投递开始，到最后一次结束，所以启动服务和打开连接池都在窗口之外。
发布场景不取行而是写行，它的窗口从第一次发布到最后一次。答复场景把每次投递的答复经 `Routed`，即
已连接 Broker 的默认发布器，写进第二张表。

标为「无法区分」的差值，小于两个循环中任何一个的轮次之间的离散范围。凡是数据库本身比分发
贵得多的地方，这就是诚实的结果：这里每条消息至少要两条语句和一次提交，这样大小的差值就淹没在其中
一条里。低于运行噪声的数字读起来像是从未测到过的精度，所以不公布。

标着「受数据库限制」的一行，是裸循环每条消息至少有一半时间在等数据库的那一行：一条消息要它付出的
往返次数，乘上这次运行测到的往返时间，再和一条消息花的时间相比。每条语句是一次往返，从连接池取出的
每条连接也是一次：sqlx 的连接池在借出连接之前会先 ping 它。在这样的一行里，套接字之上的一切，都是在
循环本来就要付的那段等待里做自己的活，所以它是真实的结果，同时也只是这些开销的下界，而不是对它们的
测量。

同一次运行的机器可读形式在
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-sqlx/latest/benchmarks/results.json)，
框架的站点用它拼出跨 Broker 的汇总表。

## 吞吐量 { #throughput }

<div id="benchmark-throughput"></div>

在 Postgres、MySQL 和 SQLite 上，用 16 个连接的连接池把一张填满的表取空。服务用 `workers(n)`
挂载处理器；一个工作者就是普通的挂载方式，处理完一次投递才取下一次。裸循环在同一个连接池上跑
n 个任务，每个任务都是一个循环：领取、读取、删除，直到领取返回空为止。语句相同，并发度也相同。
crate 的循环在同一个连接池上跑 n 个任务，每个任务是一个自己的 `SqlxBroker`，读一个 `InboxQueue`
订阅。
Postgres 和 MySQL 用行锁形式。SQLite 没有行锁，所以用租约形式；它的数据库同一时刻只有一个写者。

每次运行先填表，再计时到最后一行处理完。运行无需轮询表、也无需睡眠就知道自己结束了：每次处理完
的投递都让一个计数闩减一，三个循环里都一样，最后一次计数会唤醒等待中的运行。

一个订阅的领取一次只跑一个，每次最多一行，批处理器则最多一批的大小。`workers(n)` 让处理器和它们
的结算并行执行，但不会让领取变多。只有一个工作者时，三个循环按同样的顺序做同样的活。工作者多了
以后，每个裸任务和 crate 的每个订阅各自领取，而服务的唯一一个订阅替它所有的工作者领取。所以 crate
那一列和裸循环并排，显示的是同样并发度下 crate 的开销；在四个和八个工作者时，它和服务之间的差距
来自领取，而不是分发。一批一次领取最多一批大小的行，所以一次领取能服务同样多的投递。

## crate 自己的代码 { #the-crates-own-code }

<div id="benchmark-code"></div>

第三张表是数出来的，不是计时得来的：指令数用 callgrind，内存分配用 DHAT，服务和旁边的裸循环都有。
每个场景都在单线程运行时上、对着测试环境的 Postgres 运行。另一个线程上的生产者在启动和取数之间
填表，所以填表不落在任何一个测量区域里。

计入的是服务线程执行的一切：框架、这个 crate，以及 sqlx 驱动在这个线程上做的编码、发送、接收和
解码。数据库服务器是另一个进程，不计入，内核在系统调用里做的事也不计入。

指令数和分配次数都是稳态下每条消息的值：1000 次投递的运行与 2000 次投递的运行之间的斜率。最后一列
是启动服务一次性花掉的：打开连接池的连接、Broker 启动时的检查和第一次投递。这些数字是绝对值，
框架自身的开销也在其中；单独的框架开销由核心公布在它的
[基准测试页](https://powersemmi.github.io/ruststream/latest/zh/benchmarks/)上。

服务连着真实的服务器，计数又随套接字把字节交给驱动的方式而定，所以在两次运行之间会有少许浮动。
因此每个场景的上限取它在几次完整运行里达到的最大值，再加 0.1% 的余量。某个场景的分配超过它声明的
上限时，`just bench-code` 就会失败；加上 `--baseline=main` 时，指令数多出百分之二以上也算失败：`just bench-code --save-baseline=main` 在 `main` 上记录基线，
`just bench-code --baseline=main` 拿改动和它比较。失败的运行会打印它超出的每一个上限，旧值和新值
并列。改变开销的拉取请求要附上自己的数字。

## 作为插件的 outbox { #the-outbox-as-a-plugin }

<div id="benchmark-outbox"></div>

outbox 可以跑在任何 Broker 之上，所以它的开销是加在一个普通 RustStream 应用上的开销。每个场景都是
同一个应用的三个变体。**不用 outbox** 是用户不用它时写的应用：中继器对每条命令回一个答复，接收器
消费这个答复，从处理器之外发布的消息直接交给 Broker。**手写 outbox** 是同一个应用，只是 outbox 由
服务自己用裸 sqlx 写：发布之前插入记录，在处理器里把记录取来处理并标记，用的是 crate 的中间件会取
的那几条连接。**ruststream-sqlx 的 outbox** 是同一个应用加上这个 crate 的 outbox：注册表、它的两层
中间件和启动时的重新发布；从处理器之外发布的消息经过 `Outbox::wrap`。

outbox 对不用 outbox，是插件的全部开销：跟踪一条消息总共要花多少。outbox 对手写 outbox，是 crate
自身的开销：在用户本来就要执行的语句之外，它的机制多加了什么。不被跟踪的消息穿过两层中间件，而在
手写的 outbox 里什么也不花，所以这一行没有中间的变体。

四个场景覆盖 outbox 做的事。不被跟踪的消息穿过两层中间件走一个来回。被跟踪的发布从处理器之外发出，
就像 HTTP 端点那样发布。被跟踪的投递到达接收器，接收器把它的记录取来处理并标记。被跟踪的来回把这些
全做一遍：答复被记录、投递、取来处理并标记。

计时跑在测试环境的 Redis Pub/Sub 上，经 `ruststream-fred`，和这个 crate 的 outbox 示例一样，并对着
测试环境的 Postgres。发送方在一个工作线程上发布，最多让 1024 条消息处于未消费状态，因为 Pub/Sub
会丢掉订阅缓冲区放不下的消息。

<div id="benchmark-outbox-throughput"></div>

被跟踪的来回，中继器和接收器都用 `workers(n)` 挂载，三个变体各测一遍。

<div id="benchmark-outbox-code"></div>

计数把同样的应用跑在 `MemoryBroker` 上，所以数字里没有传输。同一时刻只有一条消息在途，所以中继器
和接收器从不互相等待连接。

## 剩下的开销 { #the-costs-that-remain }

这个 crate 遵守整个家族的规则：热路径上每条消息除 sqlx 自身之外，没有内存分配、动态调用、额外的
原子操作和拷贝。剩下的开销列在这里，各自注明适用的地方：

- 读取自己投递的事务 `Tx` 的处理器，每次投递付出一次引用计数递增。不用它的订阅什么也不付。
- 通过 `Ctx<keys::Pool>` 取连接池的处理器，每次投递付出一次引用计数递增。
- 经 `Repository` 或路由的发布要读一次 Broker 的关闭标志：一次原子读取。
- 一次发布会唤醒同一进程里它那张表的订阅：每唤醒一个订阅一次原子操作。
- `Routed` 要查每条消息的名字，精确名字一次哈希查找，再对行的 `Publish` 做一次动态调用。
  `Repository` 在编译期就定下了表，两样都不做。
- 按名字的订阅，如果行的事件全是 crate 自己的，就按列读行，和 `InboxQueue` 一样。行有自己的事件时，
  每条消息付出一次装箱的投递和一个装箱的结算 future。
- outbox 通过 `OnceLock` 读取启动后才设置的连接池：每条被跟踪的消息一次原子读取。构造时就给出的
  连接池没有开销。被跟踪的消息在头部带着它的记录 id，发布要为这段文本和头部存储分配内存。
- outbox 的测试开关在编译时就从生产构建里去掉。
- 行锁形式和咨询锁形式，每条在处理中的消息或每一批占用连接池的一条连接。这是持有者死去时数据库
  会自动释放的那种锁的代价。
- 不在处理器里、而是通过运行中应用的发布者进行的发布，会把每条消息编码到它自己的缓冲区里。裸循环
  所有消息共用一个缓冲区。

## 机器 { #the-machine }

<div id="benchmark-environment"></div>

构建标志和数字一起公布，因为它们会改变数字：用 `-C target-cpu=native` 构建的二进制得出的结果，
在别的机器上无法复现，所以配方在构建之前会清空这个变量。

## 这些数字不代表什么 { #what-they-do-not-mean }

这里是一张表、一个很小的 JSON 消息体和回环地址上的一台数据库服务器。测的是一条消息在这个 crate
里的开销，而不是 Postgres、MySQL 或 SQLite 能承载多少。这里的一行不能和为某个消息 Broker 的 crate
公布的一行相比：数据库里的队列对每条消息做的活不一样。

一次运行测量的窗口在最后一次投递被计数时关闭，三个循环都一样。每个循环都在计数之后才结算投递，
所以最后一次结算在哪里都落在数字之外。

测试环境把所有服务器都放在运行基准测试的那台机器上，跑在 Docker 里，用主机网络，并且不保证持久：
没有 `fsync`，提交时也不刷写日志。测的是一条消息的开销，而不是服务器底下的磁盘。保持持久性开启的
服务要为它付出代价，而且每个循环付的都一样。另一台主机上的服务器回答的是另一个问题，而且回答的是
关于网络的问题，不是关于这个 crate 的。

这些数字是一台机器在某一天的快照。它们在一台专门留给这次运行的机器上手动重测：这一页关心的差值，
比共享机器的噪声还小。

## 自己运行 { #running-it-yourself }

```bash
just bench
```

这个配方用 `docker-compose.test.yml` 启动测试环境，跑完每个场景和吞吐量网格，关掉测试环境，并用
测得的结果重写 `docs/benchmarks/results.json`。它要花半小时左右，并且需要独占这台机器。
`RUSTSTREAM_BENCH_SECONDS` 设定按时钟计时的运行至少持续多久，`RUSTSTREAM_BENCH_THROUGHPUT_ROWS` 设定
吞吐量运行的行数，`RUSTSTREAM_BENCH_PAIRS` 设定轮数。

```bash
just bench-code
```

这个配方启动同一个测试环境，在 valgrind 下数出代码表，关掉测试环境，并重写同一份文档里的 `code`
一节。它需要 valgrind。配方会自己安装基准测试的运行器，版本是 `Cargo.lock` 锁定的那个。开头给一个
数字就按另一个投递次数来测，比如 `just bench-code 5000`：数字更稳，运行也更久。公布的表是按默认的
1000 测的。
