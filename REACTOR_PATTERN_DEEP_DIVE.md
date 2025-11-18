# Reactor 模式深度解析

## 目录

1. [Reactor 模式的起源](#1-reactor-模式的起源)
2. [为什么叫 Reactor？](#2-为什么叫-reactor)
3. [Reactor 模式的核心思想](#3-reactor-模式的核心思想)
4. [Reactor 模式的演进](#4-reactor-模式的演进)
5. [Netty 中的 Reactor 实现](#5-netty-中的-reactor-实现)
6. [Kafka 中的 Reactor 实现](#6-kafka-中的-reactor-实现)
7. [Redis 中的 Reactor 实现](#7-redis-中的-reactor-实现)
8. [Nginx 中的 Reactor 实现](#8-nginx-中的-reactor-实现)
9. [Node.js 中的 Reactor 实现](#9-nodejs-中的-reactor-实现)
10. [各系统 Reactor 实现对比](#10-各系统-reactor-实现对比)
11. [Reactor vs Proactor](#11-reactor-vs-proactor)
12. [总结](#12-总结)

---

## 1. Reactor 模式的起源

### 1.1 历史背景

**Reactor 模式**最早由 **Douglas C. Schmidt** 在 1995 年的论文中提出，标题是：

> **"Reactor: An Object Behavioral Pattern for Demultiplexing and Dispatching Handles for Synchronous Events"**

这篇论文发表在《Pattern Languages of Program Design》第二卷中，奠定了现代高性能服务器架构的理论基础。

### 1.2 核心论文要点

Schmidt 在论文中指出，构建高性能事件驱动服务器面临的核心挑战：

```
┌────────────────────────────────────────────────────────────┐
│  传统服务器设计的问题（1990s）                              │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  模型1: 阻塞式 I/O + 进程/线程 per 连接                    │
│                                                             │
│  问题:                                                      │
│  1. 大量线程/进程 → 内存耗尽                               │
│  2. 频繁上下文切换 → CPU 浪费                              │
│  3. 同步阻塞 → 资源利用率低                                │
│                                                             │
│  示例: Apache HTTP Server (传统 MPM prefork 模式)         │
│  每个请求一个进程 → 10000个并发 = 10000个进程!            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Reactor 模式应运而生，提出了一个革命性的解决方案：**
> "使用**单线程**处理**多个并发连接**，通过**事件驱动**而非线程/进程切换来实现高并发。"

---

## 2. 为什么叫 Reactor？

### 2.1 名称的由来

**"Reactor"** 这个词来自**化学反应器（Chemical Reactor）**的类比：

```
化学反应器（Chemical Reactor）的特性:
┌─────────────────────────────────────────────────────────┐
│                                                          │
│  1. 接收多种反应物（Multiple Inputs）                   │
│  2. 检测反应条件（Monitor Conditions）                  │
│  3. 触发化学反应（Trigger Reactions）                   │
│  4. 产生多种产物（Multiple Outputs）                    │
│                                                          │
│  核心: 反应器**响应**外部条件变化，**触发**相应反应    │
│                                                          │
└──────────────────────────────────────────────────────────┘

网络 Reactor 的特性:
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  1. 接收多个 I/O 源（Multiple I/O Handles）             │
│  2. 监听 I/O 事件（Monitor I/O Events）                 │
│  3. 分发事件处理（Dispatch Event Handlers）             │
│  4. 产生多个响应（Multiple Responses）                  │
│                                                          │
│  核心: Reactor **响应** I/O 事件，**触发**相应处理      │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 2.2 核心隐喻

**Reactor = React（反应/响应）+ or（者）**

- **React（反应）**: 不是主动发起，而是**被动响应**外部事件
- **Event-Driven（事件驱动）**: 由事件驱动执行，而非按顺序执行
- **Demultiplexing（多路分解）**: 将多个输入源的事件分解到对应的处理器

**关键思想**：
> "不是我去轮询你（Polling），而是你有事件了通知我（Event Notification），我再**反应**（React）"

---

## 3. Reactor 模式的核心思想

### 3.1 UML 类图（Douglas Schmidt 原始设计）

```
┌─────────────────────────────────────────────────────────────┐
│  Reactor 模式核心组件                                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────────────────────────────┐               │
│  │  Initiation Dispatcher (初始化分发器)    │               │
│  │  - register_handler()                    │               │
│  │  - remove_handler()                      │               │
│  │  - handle_events()  ← 核心事件循环      │               │
│  └──────────────┬───────────────────────────┘               │
│                 │                                            │
│                 ▼                                            │
│  ┌──────────────────────────────────────────┐               │
│  │  Synchronous Event Demultiplexer         │               │
│  │  (同步事件多路分解器)                    │               │
│  │  - select() / poll() / epoll()           │               │
│  │  - kqueue() / IOCP                       │               │
│  └──────────────┬───────────────────────────┘               │
│                 │                                            │
│                 ▼                                            │
│  ┌──────────────────────────────────────────┐               │
│  │  Handle (句柄)                           │               │
│  │  - file descriptor                       │               │
│  │  - socket                                │               │
│  └──────────────────────────────────────────┘               │
│                                                              │
│  ┌──────────────────────────────────────────┐               │
│  │  Event Handler (事件处理器) ← 接口      │               │
│  │  - handle_event(handle)                  │               │
│  └──────────────┬───────────────────────────┘               │
│                 │                                            │
│       ┌─────────┼─────────┬─────────────┐                   │
│       ▼         ▼         ▼             ▼                   │
│  ┌────────┐┌────────┐┌────────┐  ┌────────┐               │
│  │Accept  ││Read    ││Write   │  │Close   │               │
│  │Handler ││Handler ││Handler │  │Handler │               │
│  └────────┘└────────┘└────────┘  └────────┘               │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 3.2 核心工作流程

```
┌────────────────────────────────────────────────────────────┐
│  Reactor 事件循环（伪代码）                                 │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  // 1. 初始化阶段                                          │
│  reactor = new Reactor()                                    │
│  reactor.register(acceptor, OP_ACCEPT)                      │
│                                                             │
│  // 2. 事件循环                                            │
│  while (true) {                                             │
│      // 2.1 多路分解：等待 I/O 事件就绪                    │
│      ready_handles = demultiplexer.select(timeout)          │
│                                                             │
│      // 2.2 事件分发：对每个就绪的句柄                     │
│      for each handle in ready_handles {                     │
│          // 2.3 获取对应的事件处理器                       │
│          handler = get_handler(handle)                      │
│                                                             │
│          // 2.4 调用处理器处理事件（React!）               │
│          handler.handle_event(handle)                       │
│      }                                                      │
│  }                                                          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 关键特性

| 特性 | 说明 | 优势 |
|------|------|------|
| **I/O 多路复用** | select/poll/epoll 等系统调用 | 单线程监听多个 fd |
| **事件驱动** | 由 I/O 事件触发处理，而非轮询 | CPU 高效利用 |
| **非阻塞 I/O** | 所有 I/O 操作设置为非阻塞 | 防止单个连接阻塞整个系统 |
| **回调机制** | 通过 Handler 接口回调处理 | 解耦事件检测和事件处理 |
| **单线程** | 最简形式只有一个事件循环线程 | 无锁、无上下文切换 |

---

## 4. Reactor 模式的演进

### 4.1 演进路径

```
1995: Douglas Schmidt 提出 Reactor 模式
         ↓
    Single Reactor Single Thread
    (单 Reactor 单线程)
         ↓
    Single Reactor Multi Thread
    (单 Reactor 多线程)
         ↓
    Multi Reactor Multi Thread
    (多 Reactor 多线程 / Main-Sub Reactor)
         ↓
现代变种：
    - Netty: EventLoopGroup
    - Kafka: Acceptor + Processors
    - Nginx: Master-Worker
```

### 4.2 演进对比

#### **(1) Single Reactor Single Thread**

```
┌────────────────────────────────────────┐
│  Client1, Client2, ..., ClientN        │
└──────────────┬─────────────────────────┘
               │
               ▼
     ┌─────────────────────┐
     │   Single Reactor    │
     │   - Selector        │
     │   - OP_ACCEPT       │
     │   - OP_READ         │
     │   - OP_WRITE        │
     │   - Event Handler   │
     └─────────────────────┘
       (单线程处理所有事件)
```

**代表**: Redis (6.0 之前)

**优点**:
- ✅ 无锁，实现简单
- ✅ 无上下文切换，CPU 利用率高

**缺点**:
- ❌ 单核 CPU，无法利用多核
- ❌ Handler 阻塞会影响整个系统
- ❌ 无法处理计算密集型任务

---

#### **(2) Single Reactor Multi Thread**

```
┌────────────────────────────────────────┐
│  Client1, Client2, ..., ClientN        │
└──────────────┬─────────────────────────┘
               │
               ▼
     ┌─────────────────────┐
     │   Single Reactor    │
     │   - Selector        │
     │   - OP_ACCEPT       │
     │   - OP_READ/WRITE   │
     └──────────┬──────────┘
                │
       ┌────────┴────────┬────────┐
       ▼                 ▼        ▼
   ┌────────┐       ┌────────┐┌────────┐
   │Worker 1│       │Worker 2││Worker N│
   │Business│  ...  │Handler ││Handler │
   │Logic   │       │        ││        │
   └────────┘       └────────┘└────────┘
```

**代表**: Tomcat NIO Connector (简化版)

**优点**:
- ✅ 业务逻辑由线程池处理，不阻塞 I/O
- ✅ 充分利用多核 CPU

**缺点**:
- ❌ 单个 Reactor 成为瓶颈（高并发场景）
- ❌ Reactor 负责所有 I/O，压力大

---

#### **(3) Multi Reactor Multi Thread (Main-Sub Reactor)**

```
┌────────────────────────────────────────┐
│  Client1, Client2, ..., ClientN        │
└──────────────┬─────────────────────────┘
               │
               ▼
     ┌─────────────────────┐
     │   Main Reactor      │
     │   - OP_ACCEPT Only  │
     │   - Round Robin     │
     └──────────┬──────────┘
                │
        ┌───────┼───────┬───────┐
        ▼       ▼       ▼       ▼
   ┌────────┐┌────────┐┌────────┐
   │Sub R 0 ││Sub R 1 ││Sub R N │
   │OP_READ ││OP_READ ││OP_READ │
   │OP_WRITE││OP_WRITE││OP_WRITE│
   └───┬────┘└───┬────┘└───┬────┘
       │         │         │
       └─────────┴─────────┘
                 │
         ┌───────┴───────┐
         │ Worker Pool   │
         │ (业务逻辑)    │
         └───────────────┘
```

**代表**: Netty, Kafka, Nginx

**优点**:
- ✅ Main Reactor 专注连接接受，快速
- ✅ 多个 Sub Reactor 并行处理 I/O
- ✅ 充分利用多核 CPU
- ✅ 故障隔离（一个 Sub Reactor 挂了不影响其他）

**缺点**:
- ⚠️ 实现复杂
- ⚠️ 调试困难（多线程）

---

## 5. Netty 中的 Reactor 实现

### 5.1 Netty 架构

Netty 是最经典的 **Multi Reactor Multi Thread** 实现。

```java
// Netty 核心启动代码
ServerBootstrap bootstrap = new ServerBootstrap();
bootstrap.group(bossGroup, workerGroup)  // ← 两组 EventLoopGroup
         .channel(NioServerSocketChannel.class)
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(new MyHandler());
             }
         });
```

### 5.2 核心组件

```
┌─────────────────────────────────────────────────────────┐
│  Netty Reactor 实现                                      │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌────────────────────────────────────┐                 │
│  │  Boss EventLoopGroup               │                 │
│  │  (Main Reactor Group)              │                 │
│  │  - 通常 1 个线程                   │                 │
│  │  - 只处理 OP_ACCEPT                │                 │
│  │  - 分配连接给 Worker               │                 │
│  └──────────────┬─────────────────────┘                 │
│                 │                                        │
│                 ▼                                        │
│  ┌────────────────────────────────────┐                 │
│  │  Worker EventLoopGroup             │                 │
│  │  (Sub Reactor Group)               │                 │
│  │  - N 个 EventLoop 线程             │                 │
│  │  - 每个 EventLoop:                 │                 │
│  │    • 独立的 Selector               │                 │
│  │    • 独立的 TaskQueue              │                 │
│  │    • 处理 OP_READ/WRITE            │                 │
│  │    • 执行 ChannelHandler           │                 │
│  └────────────────────────────────────┘                 │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 5.3 EventLoop 源码分析

```java
// NioEventLoop.java (Netty 4.x)
protected void run() {
    for (;;) {
        try {
            // 1. 等待 I/O 事件或任务
            switch (selectStrategy.calculateStrategy(selectNowSupplier, hasTasks())) {
                case SelectStrategy.CONTINUE:
                    continue;
                case SelectStrategy.SELECT:
                    // 阻塞在 select() 上
                    select(wakenUp.getAndSet(false));

                    // 检查是否被唤醒
                    if (wakenUp.get()) {
                        selector.wakeup();
                    }
                default:
            }

            // 2. 处理 I/O 事件
            processSelectedKeys();

            // 3. 处理异步任务队列
            runAllTasks();

        } catch (Throwable t) {
            handleLoopException(t);
        }
    }
}

// 处理就绪的 I/O 事件
private void processSelectedKeys() {
    if (selectedKeys != null) {
        processSelectedKeysOptimized();
    } else {
        processSelectedKeysPlain(selector.selectedKeys());
    }
}

private void processSelectedKeysOptimized() {
    for (int i = 0; i < selectedKeys.size; ++i) {
        final SelectionKey k = selectedKeys.keys[i];
        final Object a = k.attachment();

        if (a instanceof AbstractNioChannel) {
            // 处理 NIO Channel
            processSelectedKey(k, (AbstractNioChannel) a);
        } else {
            @SuppressWarnings("unchecked")
            NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
            processSelectedKey(k, task);
        }
    }
}
```

### 5.4 Netty 的关键设计

| 特性 | 实现 | 优势 |
|------|------|------|
| **EventLoop = Thread** | 一个 EventLoop 绑定一个线程 | 无锁设计，线程安全 |
| **Channel 绑定 EventLoop** | 一个 Channel 生命周期绑定同一个 EventLoop | 顺序保证，无锁 |
| **TaskQueue** | 每个 EventLoop 有任务队列 | 跨线程通信 |
| **优化的 SelectionKey** | 使用数组代替 HashSet | 遍历更快 |
| **ChannelPipeline** | 责任链模式 | 灵活的处理器链 |

### 5.5 Netty vs 标准 Reactor

| 维度 | 标准 Reactor | Netty |
|------|-------------|-------|
| **Main Reactor** | 1 个 Acceptor 线程 | Boss EventLoopGroup (1个) |
| **Sub Reactor** | N 个 Processor 线程 | Worker EventLoopGroup (N个) |
| **Handler** | 独立线程池 | 可选，默认在 EventLoop 中执行 |
| **优化** | 基础实现 | 零拷贝、内存池、优化的 Selector |

---

## 6. Kafka 中的 Reactor 实现

### 6.1 Kafka 架构

Kafka 的 Reactor 实现是**最符合 Douglas Schmidt 原始论文**的实现之一。

```
┌─────────────────────────────────────────────────────────┐
│  Kafka SocketServer Reactor 实现                         │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌────────────────────────────────────┐                 │
│  │  Acceptor (Main Reactor)           │                 │
│  │  - 1 个线程/Listener                │                 │
│  │  - ServerSocketChannel.accept()    │                 │
│  │  - Round-Robin 分配                │                 │
│  └──────────────┬─────────────────────┘                 │
│                 │                                        │
│         ┌───────┼───────┬───────┐                       │
│         ▼       ▼       ▼       ▼                       │
│  ┌──────────────────────────────────┐                   │
│  │  Processor (Sub Reactor)         │                   │
│  │  - N 个线程 (num.network.threads)│                   │
│  │  - 独立 Selector                 │                   │
│  │  - 7 步事件循环:                 │                   │
│  │    1. configureNewConnections    │                   │
│  │    2. processNewResponses        │                   │
│  │    3. poll()                     │                   │
│  │    4. processCompletedReceives   │                   │
│  │    5. processCompletedSends      │                   │
│  │    6. processDisconnected        │                   │
│  │    7. closeExcessConnections     │                   │
│  └──────────────┬───────────────────┘                   │
│                 │                                        │
│                 ▼                                        │
│  ┌──────────────────────────────────┐                   │
│  │  RequestChannel                  │                   │
│  │  - requestQueue (全局)           │                   │
│  │  - callbackQueue (优先)          │                   │
│  │  - responseQueue (每个Processor) │                   │
│  └──────────────┬───────────────────┘                   │
│                 │                                        │
│                 ▼                                        │
│  ┌──────────────────────────────────┐                   │
│  │  RequestHandler (Worker Pool)    │                   │
│  │  - M 个线程 (num.io.threads)     │                   │
│  │  - KafkaApis 业务逻辑            │                   │
│  │  - 磁盘 I/O (读写日志)           │                   │
│  └──────────────────────────────────┘                   │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 6.2 Processor 核心循环（源码）

```scala
// SocketServer.scala:907-934
override def run(): Unit = {
  try {
    while (shouldRun.get()) {
      try {
        // === Reactor 事件循环 7 步 ===

        // 步骤1: 配置新连接（Acceptor 分配过来的）
        configureNewConnections()

        // 步骤2: 处理响应队列（Handler 返回的）
        processNewResponses()

        // 步骤3: I/O 多路复用（核心！）
        poll()  // selector.poll(timeout)

        // 步骤4: 处理完成的接收
        processCompletedReceives()

        // 步骤5: 处理完成的发送
        processCompletedSends()

        // 步骤6: 处理断开连接
        processDisconnected()

        // 步骤7: 关闭过多连接（配额控制）
        closeExcessConnections()

      } catch {
        case e: Throwable => processException("...", e)
      }
    }
  } finally {
    closeAll()
  }
}
```

### 6.3 Kafka 的独特设计

| 特性 | Kafka 实现 | 设计理由 |
|------|-----------|---------|
| **Mute/Unmute** | 请求接收后 mute，响应发送后 unmute | 保证单连接请求顺序 |
| **双队列** | requestQueue + callbackQueue | 延迟操作优先处理 |
| **ConnectionQuotas** | IP/Listener/Broker 三级配额 | 防御 DoS 攻击 |
| **MemoryPool** | CAS 无锁内存池 | 控制内存使用 |
| **私有 ResponseQueue** | 每个 Processor 独立 | 避免全局锁竞争 |

### 6.4 Kafka vs Netty

| 维度 | Kafka | Netty |
|------|-------|-------|
| **主要用途** | 消息队列 Broker | 通用网络框架 |
| **Main Reactor** | Acceptor (1个) | Boss EventLoopGroup |
| **Sub Reactor** | Processor (N个) | Worker EventLoopGroup |
| **Worker** | RequestHandler (独立线程池) | 可选，默认在 EventLoop 执行 |
| **背压机制** | Mute/Unmute + 阻塞队列 | Channel.isWritable() |
| **零拷贝** | sendfile (Fetch 响应) | FileRegion |
| **配额** | 内置 ConnectionQuotas | 无，需自行实现 |

---

## 7. Redis 中的 Reactor 实现

### 7.1 Redis 架构（6.0 之前）

Redis 是**最经典的 Single Reactor Single Thread** 实现。

```
┌─────────────────────────────────────────────────────┐
│  Redis 单线程 Reactor (6.0 之前)                    │
├─────────────────────────────────────────────────────┤
│                                                      │
│  Client1, Client2, ..., ClientN                      │
│         │                                            │
│         ▼                                            │
│  ┌─────────────────────────────────────┐            │
│  │  Single Reactor (Main Thread)       │            │
│  │  - aeEventLoop                      │            │
│  │  - aeApiPoll() ← epoll/kqueue       │            │
│  │  - 处理所有事件:                    │            │
│  │    • OP_ACCEPT                      │            │
│  │    • OP_READ                        │            │
│  │    • OP_WRITE                       │            │
│  │    • Time Events (定时器)          │            │
│  │  - 执行命令                         │            │
│  │  - 写响应                           │            │
│  └─────────────────────────────────────┘            │
│                                                      │
│  特点:                                               │
│  ✅ 无锁、无上下文切换                               │
│  ✅ 实现简单、可靠                                   │
│  ❌ 单核 CPU，无法利用多核                           │
│  ❌ 慢操作会阻塞整个系统（如 KEYS *）               │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### 7.2 Redis 核心事件循环（C 代码）

```c
// ae.c - Redis Event Loop
void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    while (!eventLoop->stop) {
        // 执行 beforesleep 回调
        if (eventLoop->beforesleep != NULL)
            eventLoop->beforesleep(eventLoop);

        // 核心：处理文件事件和时间事件
        aeProcessEvents(eventLoop, AE_ALL_EVENTS|AE_CALL_AFTER_SLEEP);
    }
}

int aeProcessEvents(aeEventLoop *eventLoop, int flags) {
    int processed = 0, numevents;

    // 1. 计算最近的时间事件超时时间
    if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
        shortest = aeSearchNearestTimer(eventLoop);

    if (shortest) {
        tvp = &tv;
        aeGetTime(&now_sec, &now_ms);
        tvp->tv_sec = shortest->when_sec - now_sec;
        tvp->tv_usec = (shortest->when_ms - now_ms) * 1000;
    } else {
        tvp = NULL;  // 无限等待
    }

    // 2. I/O 多路复用（核心！）
    numevents = aeApiPoll(eventLoop, tvp);

    // 3. 处理文件事件（I/O 事件）
    for (j = 0; j < numevents; j++) {
        aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
        int mask = eventLoop->fired[j].mask;
        int fd = eventLoop->fired[j].fd;

        // 可读事件
        if (fe->mask & mask & AE_READABLE) {
            fe->rfileProc(eventLoop, fd, fe->clientData, mask);
        }
        // 可写事件
        if (fe->mask & mask & AE_WRITABLE) {
            fe->wfileProc(eventLoop, fd, fe->clientData, mask);
        }
        processed++;
    }

    // 4. 处理时间事件
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    return processed;
}
```

### 7.3 Redis 6.0+ 的演进

```
┌─────────────────────────────────────────────────────┐
│  Redis 6.0+ Multi-threaded I/O                      │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌─────────────────────────────────────┐            │
│  │  Main Thread (主线程)               │            │
│  │  - aeEventLoop                      │            │
│  │  - 执行命令（单线程）               │            │
│  │  - 分发读写任务                     │            │
│  └──────────┬──────────────────────────┘            │
│             │                                        │
│     ┌───────┼───────┬───────┐                       │
│     ▼       ▼       ▼       ▼                       │
│  ┌─────┐┌─────┐┌─────┐┌─────┐                      │
│  │I/O 0││I/O 1││I/O 2││I/O N│                      │
│  │Thread││     ││     ││     │                      │
│  │读取  ││写入 ││读取 ││写入 │                      │
│  └─────┘└─────┘└─────┘└─────┘                      │
│                                                      │
│  改进:                                               │
│  ✅ I/O 操作多线程（读写 Socket）                   │
│  ✅ 命令执行仍然单线程（保证一致性）                │
│  ✅ 充分利用多核 CPU                                 │
│                                                      │
└──────────────────────────────────────────────────────┘
```

---

## 8. Nginx 中的 Reactor 实现

### 8.1 Nginx 架构

Nginx 使用 **Master-Worker 多进程 Reactor** 模型。

```
┌─────────────────────────────────────────────────────┐
│  Nginx Master-Worker Reactor                        │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌─────────────────────────────────────┐            │
│  │  Master Process (主进程)            │            │
│  │  - 读取配置                         │            │
│  │  - 管理 Worker 进程                 │            │
│  │  - 信号处理                         │            │
│  └──────────┬──────────────────────────┘            │
│             │                                        │
│     ┌───────┼───────┬───────┐                       │
│     ▼       ▼       ▼       ▼                       │
│  ┌────────────────────────────────┐                 │
│  │  Worker Process (工作进程)     │                 │
│  │  - N 个进程（通常 = CPU 核数） │                 │
│  │  - 每个 Worker:                 │                 │
│  │    • 独立的事件循环             │                 │
│  │    • epoll/kqueue               │                 │
│  │    • 处理客户端请求             │                 │
│  │    • 无共享状态（独立）         │                 │
│  └────────────────────────────────┘                 │
│                                                      │
│  ┌────────────────────────────────┐                 │
│  │  连接分配 (惊群问题解决)       │                 │
│  │  - accept_mutex (互斥锁)        │                 │
│  │  - EPOLLEXCLUSIVE (Linux 4.5+) │                 │
│  │  - SO_REUSEPORT (负载均衡)     │                 │
│  └────────────────────────────────┘                 │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### 8.2 Worker 事件循环（C 代码）

```c
// ngx_process_cycle.c
static void ngx_worker_process_cycle(ngx_cycle_t *cycle, void *data) {
    ngx_int_t worker = (intptr_t) data;

    ngx_process = NGX_PROCESS_WORKER;
    ngx_worker = worker;

    ngx_worker_process_init(cycle, worker);

    // Worker 主循环
    for ( ;; ) {
        // 如果收到退出信号
        if (ngx_exiting) {
            ngx_worker_process_exit(cycle);
        }

        // 核心：处理事件
        ngx_process_events_and_timers(cycle);

        // 如果收到终止信号
        if (ngx_terminate) {
            return;
        }

        // 如果收到重启信号
        if (ngx_quit) {
            ngx_quit = 0;
            ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                         "gracefully shutting down");
            ngx_setproctitle("worker process is shutting down");

            if (!ngx_exiting) {
                ngx_close_listening_sockets(cycle);
                ngx_exiting = 1;
            }
        }

        // 如果收到重新打开日志信号
        if (ngx_reopen) {
            ngx_reopen = 0;
            ngx_reopen_files(cycle, -1);
        }
    }
}

void ngx_process_events_and_timers(ngx_cycle_t *cycle) {
    ngx_uint_t  flags;
    ngx_msec_t  timer, delta;

    timer = ngx_event_find_timer();  // 找到最近的定时器
    flags = NGX_UPDATE_TIME;

    // 获取 accept 互斥锁（解决惊群问题）
    if (ngx_use_accept_mutex) {
        if (ngx_accept_disabled > 0) {
            ngx_accept_disabled--;
        } else {
            if (ngx_trylock_accept_mutex(cycle) == NGX_ERROR) {
                return;
            }
            flags |= NGX_POST_EVENTS;
        }
    }

    // 核心：调用 epoll_wait / kqueue
    (void) ngx_process_events(cycle, timer, flags);

    // 处理 accept 事件
    ngx_event_process_posted(cycle, &ngx_posted_accept_events);

    // 释放 accept 互斥锁
    if (ngx_accept_mutex_held) {
        ngx_shmtx_unlock(&ngx_accept_mutex);
    }

    // 处理读写事件
    ngx_event_process_posted(cycle, &ngx_posted_events);
}
```

### 8.3 Nginx 的独特设计

| 特性 | Nginx 实现 | 优势 |
|------|-----------|------|
| **多进程** | Master + N Workers | 故障隔离，一个 Worker 挂了不影响其他 |
| **无共享** | 每个 Worker 独立 | 无锁，无上下文切换 |
| **惊群解决** | accept_mutex / SO_REUSEPORT | 避免多进程竞争 accept |
| **异步 I/O** | epoll/kqueue 非阻塞 | 高并发处理 |
| **事件驱动** | 回调机制 | 高效 |

### 8.4 Nginx vs Kafka vs Netty

| 维度 | Nginx | Kafka | Netty |
|------|-------|-------|-------|
| **并发模型** | 多进程 Reactor | 多线程 Reactor | 多线程 Reactor |
| **Main Reactor** | 无，每个 Worker 独立 | Acceptor | Boss EventLoopGroup |
| **Sub Reactor** | N 个 Worker 进程 | N 个 Processor | N 个 EventLoop |
| **语言** | C | Scala/Java | Java |
| **零拷贝** | sendfile | sendfile | FileRegion |
| **适用场景** | HTTP 反向代理 | 消息队列 | 通用网络框架 |

---

## 9. Node.js 中的 Reactor 实现

### 9.1 Node.js 架构

Node.js 基于 **libuv** 实现 Reactor 模式，是**单线程事件循环**的经典代表。

```
┌─────────────────────────────────────────────────────┐
│  Node.js Event Loop (基于 libuv)                    │
├─────────────────────────────────────────────────────┤
│                                                      │
│   ┌─────────────────────────────────┐               │
│   │  JavaScript Code (单线程)      │               │
│   │  - 业务逻辑                     │               │
│   │  - 回调注册                     │               │
│   └──────────┬──────────────────────┘               │
│              │                                       │
│              ▼                                       │
│   ┌─────────────────────────────────┐               │
│   │  Event Loop (libuv)             │               │
│   │  ┌─────────────────────────┐    │               │
│   │  │ 1. Timers               │    │               │
│   │  │    (setTimeout/Interval)│    │               │
│   │  └─────────────────────────┘    │               │
│   │  ┌─────────────────────────┐    │               │
│   │  │ 2. Pending Callbacks    │    │               │
│   │  │    (I/O 回调)           │    │               │
│   │  └─────────────────────────┘    │               │
│   │  ┌─────────────────────────┐    │               │
│   │  │ 3. Poll                 │    │               │
│   │  │    (epoll/kqueue)       │    │ ← 核心！     │
│   │  │    (等待 I/O 事件)      │    │               │
│   │  └─────────────────────────┘    │               │
│   │  ┌─────────────────────────┐    │               │
│   │  │ 4. Check                │    │               │
│   │  │    (setImmediate)       │    │               │
│   │  └─────────────────────────┘    │               │
│   │  ┌─────────────────────────┐    │               │
│   │  │ 5. Close Callbacks      │    │               │
│   │  │    (socket.on('close')) │    │               │
│   │  └─────────────────────────┘    │               │
│   └─────────────────────────────────┘               │
│              │                                       │
│              ▼                                       │
│   ┌─────────────────────────────────┐               │
│   │  Thread Pool (libuv)            │               │
│   │  - 文件 I/O                     │               │
│   │  - DNS 查询                     │               │
│   │  - crypto 操作                  │               │
│   └─────────────────────────────────┘               │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### 9.2 libuv 核心代码

```c
// uv_run (libuv)
int uv_run(uv_loop_t* loop, uv_run_mode mode) {
  int timeout;
  int r;

  r = uv__loop_alive(loop);
  if (!r)
    uv__update_time(loop);

  while (r != 0 && loop->stop_flag == 0) {
    // 1. 更新当前时间
    uv__update_time(loop);

    // 2. 运行定时器
    uv__run_timers(loop);

    // 3. 运行 pending 回调
    ran_pending = uv__run_pending(loop);

    // 4. 运行 idle/prepare
    uv__run_idle(loop);
    uv__run_prepare(loop);

    // 5. 计算 poll 超时时间
    timeout = 0;
    if ((mode == UV_RUN_ONCE && !ran_pending) || mode == UV_RUN_DEFAULT)
      timeout = uv_backend_timeout(loop);

    // 6. Poll I/O (核心！epoll/kqueue)
    uv__io_poll(loop, timeout);

    // 7. 运行 check
    uv__run_check(loop);

    // 8. 关闭回调
    uv__run_closing_handles(loop);

    if (mode == UV_RUN_ONCE) {
      uv__update_time(loop);
      uv__run_timers(loop);
    }

    r = uv__loop_alive(loop);
    if (mode == UV_RUN_ONCE || mode == UV_RUN_NOWAIT)
      break;
  }

  return r;
}

// epoll 实现 (Linux)
void uv__io_poll(uv_loop_t* loop, int timeout) {
  struct epoll_event events[1024];
  int nevents, i;

  // epoll_wait (核心 I/O 多路复用)
  nevents = epoll_wait(loop->backend_fd,
                       events,
                       ARRAY_SIZE(events),
                       timeout);

  // 处理就绪事件
  for (i = 0; i < nevents; i++) {
    struct epoll_event* pe = events + i;
    uv__io_t* w = pe->data.ptr;

    // 调用回调
    w->cb(loop, w, pe->events);
  }
}
```

### 9.3 Node.js 示例

```javascript
const net = require('net');

// 创建服务器 (Reactor 初始化)
const server = net.createServer((socket) => {
  console.log('Client connected');

  // 注册 OP_READ 事件处理器
  socket.on('data', (data) => {
    console.log('Received:', data.toString());
    // Echo back
    socket.write('Echo: ' + data);
  });

  // 注册关闭事件处理器
  socket.on('end', () => {
    console.log('Client disconnected');
  });
});

// 监听端口 (注册 OP_ACCEPT)
server.listen(8080, () => {
  console.log('Server listening on port 8080');
});

// Event Loop 自动运行
// 等价于 while(true) { epoll_wait(); dispatch_events(); }
```

### 9.4 Node.js 特点

| 特性 | Node.js | 说明 |
|------|---------|------|
| **单线程** | JavaScript 执行单线程 | 无锁，简单 |
| **事件循环** | libuv 实现 | 跨平台（epoll/kqueue/IOCP） |
| **线程池** | 文件 I/O 用线程池 | 避免阻塞事件循环 |
| **回调地狱** | 早期问题 | 现代用 async/await 解决 |
| **高并发** | 适合 I/O 密集型 | 不适合 CPU 密集型 |

---

## 10. 各系统 Reactor 实现对比

### 10.1 对比表

| 系统 | 模式 | Main Reactor | Sub Reactor | Worker | 语言 | 适用场景 |
|------|------|-------------|-------------|--------|------|---------|
| **Netty** | Multi Reactor Multi Thread | Boss EventLoopGroup | Worker EventLoopGroup (N个) | 可选 | Java | 通用网络框架 |
| **Kafka** | Multi Reactor Multi Thread | Acceptor (1个) | Processor (N个) | Handler (M个) | Scala/Java | 消息队列 |
| **Redis 6.0-** | Single Reactor Single Thread | Main Thread | 无 | 无 | C | 内存数据库 |
| **Redis 6.0+** | Single Reactor + I/O Threads | Main Thread | 无 | I/O Threads | C | 内存数据库 |
| **Nginx** | Multi Process Reactor | 无 (Worker独立) | N Worker 进程 | 无 | C | HTTP 服务器 |
| **Node.js** | Single Reactor Single Thread | Event Loop | 无 | Thread Pool (文件I/O) | C/JavaScript | Web 后端 |

### 10.2 演进总结

```
时间线：Reactor 模式的演进

1995: Douglas Schmidt 提出 Reactor 模式
         ↓
2000s: Single Reactor Single Thread
       - Redis (简单、高效)
       - Node.js (JavaScript 生态)
         ↓
2005: Single Reactor Multi Thread
      - Tomcat NIO Connector
         ↓
2010s: Multi Reactor Multi Thread
       - Netty (2011 正式版)
       - Kafka (2011)
       - Nginx (一直使用多进程)
         ↓
2020: 混合模式
      - Redis 6.0 (单线程 + I/O 多线程)
      - Kafka 优化 (Mute/Unmute 背压)
```

---

## 11. Reactor vs Proactor

### 11.1 核心区别

| 维度 | Reactor | Proactor |
|------|---------|----------|
| **I/O 操作** | 应用程序自己读写 | 操作系统完成读写 |
| **通知机制** | I/O **就绪**通知 | I/O **完成**通知 |
| **阻塞性** | 非阻塞 I/O | 异步 I/O |
| **系统调用** | select/poll/epoll | IOCP (Windows), io_uring (Linux) |
| **代表** | Netty, Kafka, Redis | Windows IOCP, Boost.Asio |

### 11.2 流程对比

**Reactor (同步非阻塞)**:
```
1. 应用程序: 注册 OP_READ 到 Reactor
2. Reactor:   select() 阻塞等待
3. 内核:      数据到达，select() 返回
4. Reactor:   通知应用程序 "可以读了"
5. 应用程序: read() 系统调用读取数据  ← 应用程序自己读
6. 应用程序: 处理数据
```

**Proactor (异步 I/O)**:
```
1. 应用程序: 发起异步 read() 请求，提供 buffer
2. 内核:      数据到达，自动拷贝到 buffer
3. 内核:      完成后通知应用程序 "读完了"    ← 操作系统读
4. 应用程序: 处理 buffer 中的数据（已经读好了）
```

### 11.3 为什么 Linux 主要用 Reactor？

| 原因 | 说明 |
|------|------|
| **AIO 支持差** | Linux 的 AIO (io_submit) 性能不佳 |
| **epoll 成熟** | epoll 经过多年优化，性能优秀 |
| **应用复杂度** | Reactor 应用层代码更简单 |
| **io_uring 新兴** | Linux 5.1+ 引入，逐渐成熟 |

**Windows IOCP** 是真正成熟的 Proactor 实现。

---

## 12. 总结

### 12.1 Reactor 模式的本质

**Reactor = React（反应）+ or（者）**

核心思想：
1. **事件驱动**: 由事件触发，而非轮询
2. **I/O 多路复用**: 单线程管理多个连接
3. **非阻塞**: 所有 I/O 操作非阻塞
4. **回调机制**: 事件发生时调用处理器

### 12.2 为什么广泛使用？

| 优势 | 说明 |
|------|------|
| **高并发** | C10K/C100K 问题的标准解决方案 |
| **资源高效** | 少量线程处理大量连接 |
| **低延迟** | 无线程切换开销 |
| **可扩展** | 易于扩展到多核（Multi Reactor） |

### 12.3 选择指南

| 场景 | 推荐模式 | 代表 |
|------|---------|------|
| **内存数据库** | Single Reactor Single Thread | Redis |
| **通用网络框架** | Multi Reactor Multi Thread | Netty |
| **消息队列** | Multi Reactor + Worker Pool | Kafka |
| **HTTP 服务器** | Multi Process Reactor | Nginx |
| **Web 后端** | Single Reactor (事件循环) | Node.js |

### 12.4 关键要点

1. **Reactor 不是银弹**: CPU 密集型任务不适合
2. **演进路径清晰**: 单线程 → 多线程 → 多 Reactor
3. **实现多样化**: 每个系统根据自己的需求优化
4. **核心不变**: I/O 多路复用 + 事件驱动 + 非阻塞

---

**参考文献**:
- Douglas C. Schmidt, "Reactor: An Object Behavioral Pattern for Demultiplexing and Dispatching Handles for Synchronous Events", 1995
- Netty 官方文档: https://netty.io/
- Kafka 源码: `kafka/core/src/main/scala/kafka/network/SocketServer.scala`
- Redis 源码: `redis/src/ae.c`
- Nginx 源码: `nginx/src/event/ngx_event.c`
- libuv 文档: http://docs.libuv.org/

---

**作者**: Claude (Sonnet 4.5)
**完成时间**: 2025-11-17
**文档目的**: 深入理解 Reactor 模式的起源、演进和在各大开源系统中的实现
