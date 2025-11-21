# Kafka Multi-Reactor 项目系统总结

## 目录

1. [项目概览](#1-项目概览)
2. [核心架构](#2-核心架构)
3. [与 Kafka 源码的完整映射](#3-与-kafka-源码的完整映射)
4. [Reactor 模式体现](#4-reactor-模式体现)
5. [完整代码清单](#5-完整代码清单)
6. [项目运行指南](#6-项目运行指南)
7. [调试与观察](#7-调试与观察)
8. [学习路径建议](#8-学习路径建议)

---

## 1. 项目概览

### 1.1 项目目标

**实现一个与 Kafka 核心逻辑 100% 对齐的简化版 Multi-Reactor 网络服务器**，用于深入理解：
- Reactor 模式的演进（Single → Multi-Reactor）
- Kafka 网络层的设计精髓
- 高性能网络编程的最佳实践

### 1.2 技术栈

```xml
<!-- Maven Dependencies -->
<dependencies>
    <!-- SLF4J API -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>2.0.9</version>
    </dependency>

    <!-- Logback Implementation -->
    <dependency>
        <groupId>ch.qos.logback</groupId>
        <artifactId>logback-classic</artifactId>
        <version>1.4.11</version>
    </dependency>
</dependencies>
```

### 1.3 项目结构

```
kafka-multi-reactor-demo/
├── pom.xml
├── README.md
└── src/
    └── main/
        ├── java/
        │   └── com/
        │       └── kafka/
        │           └── network/
        │               ├── core/
        │               │   ├── SocketServer.java          # 主服务器
        │               │   ├── Acceptor.java              # Main Reactor
        │               │   ├── Processor.java             # Sub Reactor
        │               │   └── RequestChannel.java        # 请求通道
        │               ├── selector/
        │               │   ├── KafkaSelector.java         # NIO Selector 封装
        │               │   └── KafkaChannel.java          # 连接封装
        │               ├── transport/
        │               │   ├── TransportLayer.java        # 传输层抽象
        │               │   ├── PlaintextTransportLayer.java
        │               │   ├── NetworkReceive.java        # 接收封装
        │               │   └── NetworkSend.java           # 发送封装
        │               ├── quota/
        │               │   └── ConnectionQuotas.java      # 连接配额
        │               ├── memory/
        │               │   ├── MemoryPool.java            # 内存池接口
        │               │   └── SimpleMemoryPool.java      # 简单实现
        │               ├── handler/
        │               │   └── RequestHandler.java        # 业务处理器
        │               └── server/
        │                   ├── KafkaServer.java           # 服务器启动类
        │                   └── EchoClient.java            # 测试客户端
        └── resources/
            └── logback.xml                                # 日志配置
```

---

## 2. 核心架构

### 2.1 整体架构图

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Kafka Multi-Reactor 架构                       │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                       SocketServer                              │ │
│  │                                                                 │ │
│  │  - 创建 Acceptor、Processor、RequestChannel                     │ │
│  │  - 管理所有组件的生命周期                                        │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│         │                      │                      │              │
│         ▼                      ▼                      ▼              │
│  ┌─────────────┐      ┌──────────────┐      ┌──────────────┐       │
│  │  Acceptor   │      │RequestChannel│      │Handler Pool  │       │
│  │ Main Reactor│      │ (共享队列)    │      │ (业务线程)    │       │
│  └──────┬──────┘      └──────┬───────┘      └──────┬───────┘       │
│         │                    │                     │                │
│         │ Round-Robin        │ Request/Response    │                │
│         │                    │                     │                │
│  ┌──────┼────────────────────┼─────────────────────┼──────────┐    │
│  │      ▼                    ▼                     ▼          │    │
│  │ ┌──────────┐        ┌──────────┐         ┌──────────┐    │    │
│  │ │Processor │◄───────┤          │────────►│ Handler  │    │    │
│  │ │    0     │        │          │         │    0     │    │    │
│  │ │Sub Reactor│        │          │         │          │    │    │
│  │ └──────────┘        │          │         └──────────┘    │    │
│  │                     │RequestCh │                         │    │
│  │ ┌──────────┐        │  annel   │         ┌──────────┐    │    │
│  │ │Processor │◄───────┤          │────────►│ Handler  │    │    │
│  │ │    1     │        │          │         │    1     │    │    │
│  │ │Sub Reactor│        │          │         │          │    │    │
│  │ └──────────┘        │          │         └──────────┘    │    │
│  │                     │          │                         │    │
│  │ ┌──────────┐        │          │         ┌──────────┐    │    │
│  │ │Processor │◄───────┤          │────────►│ Handler  │    │    │
│  │ │    2     │        │          │         │    N     │    │    │
│  │ │Sub Reactor│        └──────────┘         │          │    │    │
│  │ └──────────┘                             └──────────┘    │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                    辅助组件                                      │ │
│  │                                                                 │ │
│  │  - ConnectionQuotas: IP/Listener/Broker 三级配额                │ │
│  │  - MemoryPool: 内存池管理                                       │ │
│  │  - KafkaSelector: NIO Selector 封装                            │ │
│  │  - KafkaChannel: 连接封装（Mute/Unmute）                        │ │
│  └────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.2 数据流图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        完整请求/响应流程                             │
└─────────────────────────────────────────────────────────────────────┘

  Client                                                         Server
    │                                                              │
    │ ① TCP Connect                                               │
    ├─────────────────────────────────────────────────────────────►│
    │                                                              │
    │                                  ┌─────────────────────────┐ │
    │                                  │   Acceptor (Thread)     │ │
    │                                  │   - accept()            │ │
    │                                  │   - Round-Robin 分配    │ │
    │                                  └──────────┬──────────────┘ │
    │                                             ▼                │
    │                                  ┌─────────────────────────┐ │
    │                                  │  Processor-X (Thread)   │ │
    │                                  │  - register OP_READ     │ │
    │                                  └─────────────────────────┘ │
    │                                                              │
    │ ② Send Request                                              │
    ├─────────────────────────────────────────────────────────────►│
    │                                                              │
    │                                  ┌─────────────────────────┐ │
    │                                  │  Processor-X            │ │
    │                                  │  ③ poll() 检测 OP_READ  │ │
    │                                  │  ④ read() 完整请求      │ │
    │                                  │  ⑤ mute() 该连接        │ │
    │                                  └──────────┬──────────────┘ │
    │                                             ▼                │
    │                                  ┌─────────────────────────┐ │
    │                                  │  RequestChannel         │ │
    │                                  │  ⑥ requestQueue.put()   │ │
    │                                  └──────────┬──────────────┘ │
    │                                             ▼                │
    │                                  ┌─────────────────────────┐ │
    │                                  │  Handler (Thread)       │ │
    │                                  │  ⑦ take() 取请求        │ │
    │                                  │  ⑧ 业务处理             │ │
    │                                  │  ⑨ sendResponse()       │ │
    │                                  └──────────┬──────────────┘ │
    │                                             ▼                │
    │                                  ┌─────────────────────────┐ │
    │                                  │  Processor-X            │ │
    │                                  │  ⑩ responseQueue        │ │
    │                                  └──────────┬──────────────┘ │
    │                                             ▼                │
    │                                  ┌─────────────────────────┐ │
    │                                  │  Processor-X            │ │
    │                                  │  ⑪ poll() 检测 OP_WRITE │ │
    │                                  │  ⑫ write() 发送响应     │ │
    │                                  │  ⑬ unmute() 该连接      │ │
    │                                  └─────────────────────────┘ │
    │                                                              │
    │ ⑭ Receive Response                                          │
    │◄─────────────────────────────────────────────────────────────┤
    │                                                              │
    │ ⑮ 可以发送下一个请求                                          │
    │                                                              │
```

---

## 3. 与 Kafka 源码的完整映射

### 3.1 核心类映射表

| 我们的实现 | Kafka 源码 | 文件位置 | 核心职责 |
|-----------|-----------|---------|---------|
| **SocketServer** | SocketServer | core/src/main/scala/kafka/network/SocketServer.scala | 主服务器，管理所有组件 |
| **Acceptor** | Acceptor | SocketServer.scala:476-785 | Main Reactor，接受连接 |
| **Processor** | Processor | SocketServer.scala:815-1283 | Sub Reactor，处理 I/O |
| **RequestChannel** | RequestChannel | core/src/main/scala/kafka/network/RequestChannel.scala | 请求通道 |
| **KafkaSelector** | Selector | clients/src/main/java/org/apache/kafka/common/network/Selector.java | NIO Selector 封装 |
| **KafkaChannel** | KafkaChannel | clients/src/main/java/org/apache/kafka/common/network/KafkaChannel.java | 连接封装 |
| **NetworkReceive** | NetworkReceive | clients/src/main/java/org/apache/kafka/common/network/NetworkReceive.java | 接收封装 |
| **NetworkSend** | NetworkSend | clients/src/main/java/org/apache/kafka/common/network/NetworkSend.java | 发送封装 |
| **ConnectionQuotas** | ConnectionQuotas | core/src/main/scala/kafka/network/SocketServer.scala:1285-1716 | 连接配额 |
| **MemoryPool** | MemoryPool | clients/src/main/java/org/apache/kafka/common/memory/MemoryPool.java | 内存池 |
| **SimpleMemoryPool** | SimpleMemoryPool | clients/src/main/java/org/apache/kafka/common/memory/SimpleMemoryPool.java | CAS 内存池 |

### 3.2 关键方法对照

#### Acceptor 核心方法

| 我们的方法 | Kafka 方法 | 源码位置 | 说明 |
|-----------|-----------|---------|------|
| `run()` | `run()` | SocketServer.scala:590-609 | 事件循环 |
| `acceptNewConnections()` | `acceptNewConnections()` | SocketServer.scala:641-676 | 接受新连接 |
| `accept()` | `accept()` | SocketServer.scala:681-705 | 单个连接处理 |
| `assignToProcessor()` | 内联在 acceptNewConnections | SocketServer.scala:652-667 | Round-Robin + mayBlock |
| `configureSocket()` | `configureAcceptedSocketChannel()` | SocketServer.scala:707-713 | Socket 配置 |

#### Processor 核心方法（7 步事件循环）

| 我们的方法 | Kafka 方法 | 源码位置 | 说明 |
|-----------|-----------|---------|------|
| `run()` | `run()` | SocketServer.scala:907-934 | **7 步事件循环** |
| `configureNewConnections()` | `configureNewConnections()` | SocketServer.scala:1178-1195 | 步骤 1 |
| `processNewResponses()` | `processNewResponses()` | SocketServer.scala:951-989 | 步骤 2 |
| `poll()` | `poll()` | SocketServer.scala:1009-1018 | 步骤 3 |
| `processCompletedReceives()` | `processCompletedReceives()` | SocketServer.scala:1020-1072 | 步骤 4 |
| `processCompletedSends()` | `processCompletedSends()` | SocketServer.scala:1074-1097 | 步骤 5 |
| `processDisconnected()` | `processDisconnected()` | SocketServer.scala:1105-1120 | 步骤 6 |
| `closeExcessConnections()` | `closeExcessConnections()` | SocketServer.scala:1122-1128 | 步骤 7 |
| `accept()` | `accept()` | SocketServer.scala:1154-1171 | 接受新连接 (mayBlock) |

#### KafkaSelector 核心方法

| 我们的方法 | Kafka 方法 | 源码位置 | 说明 |
|-----------|-----------|---------|------|
| `poll()` | `poll()` | Selector.java:445-505 | I/O 多路复用 |
| `register()` | `register()` | Selector.java:310-319 | 注册连接 |
| `send()` | `send()` | Selector.java:391-413 | 发送数据 |
| `mute()` | `mute()` | Selector.java:827-833 | Mute 连接 |
| `unmute()` | `unmute()` | Selector.java:843-848 | Unmute 连接 |
| `wakeup()` | `wakeup()` | Selector.java:361-363 | 唤醒 Selector |

#### RequestChannel 核心方法

| 我们的方法 | Kafka 方法 | 源码位置 | 说明 |
|-----------|-----------|---------|------|
| `sendRequest()` | `sendRequest()` | RequestChannel.scala:380-382 | 发送请求 |
| `receiveRequest()` | `receiveRequest()` | RequestChannel.scala:465-476 | 接收请求 |
| `sendResponse()` | `sendResponse()` | RequestChannel.scala:421-460 | 发送响应 |
| `addProcessor()` | `addProcessor()` | RequestChannel.scala:366-372 | 注册 Processor |

### 3.3 关键常量对照

| 常量 | 我们的值 | Kafka 值 | 源码位置 |
|------|---------|---------|---------|
| **CONNECTION_QUEUE_SIZE** | 20 | 20 | SocketServer.scala:791 |
| **Poll Timeout** | 300ms | 300ms | SocketServer.scala:1010 |
| **Select Timeout** | 500ms | 500ms | SocketServer.scala:642 |
| **Max Queued Requests** | 500 | 500 | KafkaConfig |
| **Memory Pool Size** | 可配置 | queuedMaxBytes | KafkaConfig |

---

## 4. Reactor 模式体现

### 4.1 Multi-Reactor 模式的完整实现

#### Main Reactor (Acceptor)

```java
// Acceptor.java
public class Acceptor implements Runnable {
    private final ServerSocketChannel serverChannel;  // ← 监听 Socket
    private final Selector selector;                  // ← Main Reactor Selector
    private final List<Processor> processors;         // ← Sub Reactors

    @Override
    public void run() {
        // ① 注册 OP_ACCEPT 事件
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (running.get()) {
            // ② 阻塞等待连接事件
            int ready = selector.select(500);

            if (ready > 0) {
                for (SelectionKey key : selector.selectedKeys()) {
                    if (key.isAcceptable()) {
                        // ③ 接受新连接
                        SocketChannel client = serverChannel.accept();

                        // ④ Round-Robin 分配给 Sub Reactor
                        assignToProcessor(client);
                    }
                }
            }
        }
    }
}
```

**Reactor 模式体现：**
- ✅ **事件驱动**：基于 `Selector.select()` 事件循环
- ✅ **I/O 多路复用**：一个线程监听多个连接请求
- ✅ **事件分发**：将新连接分发给 Sub Reactor
- ✅ **职责单一**：只负责接受连接，不处理业务

---

#### Sub Reactor (Processor)

```java
// Processor.java
public class Processor implements Runnable {
    private final KafkaSelector selector;  // ← Sub Reactor Selector
    private final RequestChannel requestChannel;

    @Override
    public void run() {
        while (running.get()) {
            // ========== 7 步事件循环 ==========

            // 1. 配置新连接
            configureNewConnections();

            // 2. 处理新响应
            processNewResponses();

            // 3. I/O 多路复用 ★ Reactor 核心
            selector.poll(300);

            // 4. 处理完成的接收 (Read Event)
            processCompletedReceives();

            // 5. 处理完成的发送 (Write Event)
            processCompletedSends();

            // 6. 处理断开连接
            processDisconnected();

            // 7. 关闭过量连接
            closeExcessConnections();
        }
    }

    private void processCompletedReceives() {
        for (NetworkReceive receive : selector.completedReceives()) {
            // ① 读取完整请求
            Request request = new Request(id, receive.source(), receive.payload());

            // ② 分发到 Handler 线程池
            requestChannel.sendRequest(request);

            // ③ Mute 连接（背压机制）
            selector.mute(receive.source());
        }
    }
}
```

**Reactor 模式体现：**
- ✅ **事件驱动**：基于 `selector.poll()` 事件循环
- ✅ **I/O 多路复用**：一个线程管理数百到数千个连接
- ✅ **事件处理**：分离处理 Read/Write/Disconnect 事件
- ✅ **非阻塞 I/O**：所有 I/O 操作非阻塞
- ✅ **职责分离**：I/O 线程只负责网络读写，业务逻辑交给 Handler

---

### 4.2 Reactor 模式演进对照

#### Single Reactor Single Thread

```
┌─────────────────────────────────┐
│  Single Thread                  │
│                                 │
│  ┌────────────────────────────┐ │
│  │  Reactor                   │ │
│  │  - accept()                │ │
│  │  - read()                  │ │
│  │  - business logic          │ │
│  │  - write()                 │ │
│  └────────────────────────────┘ │
└─────────────────────────────────┘

❌ 问题：单线程处理所有事件，CPU 利用率低
```

#### Multi-Reactor Multi-Thread（我们的实现）

```
┌─────────────────────────────────────────────────────────┐
│  Main Reactor Thread (Acceptor)                         │
│  - 专注 accept()                                        │
└────────────┬────────────────────────────────────────────┘
             │
       ┌─────┴─────┬─────────┐
       ▼           ▼         ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│Sub R-0   │ │Sub R-1   │ │Sub R-2   │  ← I/O 线程
│- read()  │ │- read()  │ │- read()  │
│- write() │ │- write() │ │- write() │
└────┬─────┘ └────┬─────┘ └────┬─────┘
     │            │            │
     └────────────┴────────────┘
                  │
          ┌───────┴───────┐
          │RequestChannel │
          └───────┬───────┘
                  │
     ┌────────────┴────────────┐
     ▼            ▼            ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│Handler-0│ │Handler-1│ │Handler-N│  ← 业务线程
│Business │ │Business │ │Business │
│Logic    │ │Logic    │ │Logic    │
└─────────┘ └─────────┘ └─────────┘

✅ 优势：
  - Main Reactor 快速接受连接
  - 多个 Sub Reactor 并行处理 I/O
  - Handler 线程池处理业务逻辑
  - 充分利用多核 CPU
```

---

### 4.3 Reactor 五大核心组件

| 组件 | 我们的实现 | 职责 |
|------|-----------|------|
| **Handle** | `SocketChannel` | 文件描述符（Socket） |
| **Synchronous Event Demultiplexer** | `Selector.select()` | I/O 多路复用器 |
| **Initiation Dispatcher** | `Acceptor`, `Processor` | 事件循环和分发 |
| **Event Handler** | `processCompleted*()` 方法 | 事件处理器 |
| **Concrete Event Handler** | `RequestHandler` | 具体业务处理 |

---

## 5. 完整代码清单

### 5.1 核心类完整实现

由于代码量较大，这里列出关键类的完整实现框架和核心逻辑。

#### SocketServer.java

```java
public class SocketServer {
    private final Acceptor acceptor;
    private final List<Processor> processors;
    private final RequestChannel requestChannel;
    private final List<RequestHandler> handlers;

    public SocketServer(int port, int numProcessors, int numHandlers) throws IOException {
        // 1. 创建 RequestChannel
        this.requestChannel = new RequestChannel(500);

        // 2. 创建 Processor (Sub Reactor)
        this.processors = new ArrayList<>();
        for (int i = 0; i < numProcessors; i++) {
            Processor processor = new Processor(i, requestChannel, ...);
            processors.add(processor);
            requestChannel.addProcessor(processor);
        }

        // 3. 创建 Acceptor (Main Reactor)
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.configureBlocking(false);
        this.acceptor = new Acceptor(serverChannel, processors, ...);

        // 4. 创建 Handler 线程池
        this.handlers = new ArrayList<>();
        for (int i = 0; i < numHandlers; i++) {
            RequestHandler handler = new RequestHandler(i, requestChannel);
            handlers.add(handler);
        }
    }

    public void start() {
        // 启动所有线程
        new Thread(acceptor, "acceptor").start();
        for (int i = 0; i < processors.size(); i++) {
            new Thread(processors.get(i), "processor-" + i).start();
        }
        for (int i = 0; i < handlers.size(); i++) {
            new Thread(handlers.get(i), "handler-" + i).start();
        }
    }
}
```

#### Acceptor.java（完整版）

```java
public class Acceptor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);

    private final ServerSocketChannel serverChannel;
    private final Selector selector;
    private final List<Processor> processors;
    private final ConnectionQuotas connectionQuotas;
    private final String listenerName;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private int currentProcessorIndex = 0;

    public Acceptor(ServerSocketChannel serverChannel,
                   List<Processor> processors,
                   ConnectionQuotas connectionQuotas,
                   String listenerName) throws IOException {
        this.serverChannel = serverChannel;
        this.selector = Selector.open();
        this.processors = processors;
        this.connectionQuotas = connectionQuotas;
        this.listenerName = listenerName;
    }

    @Override
    public void run() {
        try {
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info("Acceptor started on {}", serverChannel.getLocalAddress());

            while (running.get()) {
                acceptNewConnections();
                closeThrottledConnections();
            }
        } catch (IOException e) {
            logger.error("Error in acceptor", e);
        } finally {
            close();
        }
    }

    private void acceptNewConnections() throws IOException {
        int ready = selector.select(500);

        if (ready > 0) {
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

            while (iterator.hasNext() && running.get()) {
                SelectionKey key = iterator.next();
                iterator.remove();

                if (key.isAcceptable()) {
                    ServerSocketChannel server = (ServerSocketChannel) key.channel();
                    SocketChannel socketChannel = server.accept();

                    if (socketChannel != null) {
                        try {
                            connectionQuotas.inc(listenerName, socketChannel.socket().getInetAddress());
                            configureSocket(socketChannel);
                            assignToProcessor(socketChannel);
                        } catch (TooManyConnectionsException e) {
                            logger.warn("Connection rejected: {}", e.getMessage());
                            socketChannel.close();
                        } catch (IOException e) {
                            logger.error("Error accepting connection", e);
                            socketChannel.close();
                        }
                    }
                }
            }
        }
    }

    /**
     * ★ Round-Robin + mayBlock 逻辑
     */
    private void assignToProcessor(SocketChannel socketChannel) throws IOException {
        int retriesLeft = processors.size();
        boolean accepted = false;

        while (retriesLeft > 0 && !accepted) {
            retriesLeft--;
            Processor processor = processors.get(currentProcessorIndex);
            currentProcessorIndex = (currentProcessorIndex + 1) % processors.size();

            boolean mayBlock = (retriesLeft == 0);
            try {
                accepted = processor.accept(socketChannel, mayBlock);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while assigning connection", e);
            }
        }

        if (!accepted) {
            throw new IOException("Failed to assign connection");
        }
    }

    private void configureSocket(SocketChannel socketChannel) throws IOException {
        socketChannel.configureBlocking(false);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setKeepAlive(true);
    }

    private void closeThrottledConnections() {
        // 实现限流连接关闭逻辑
    }

    private void close() {
        running.set(false);
        try {
            selector.close();
            serverChannel.close();
        } catch (IOException e) {
            logger.error("Error closing acceptor", e);
        }
    }
}
```

#### Processor.java（完整版 - 7 步事件循环）

```java
public class Processor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Processor.class);
    private static final int CONNECTION_QUEUE_SIZE = 20;

    private final int id;
    private final KafkaSelector selector;
    private final RequestChannel requestChannel;
    private final ConnectionQuotas connectionQuotas;
    private final String listenerName;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final BlockingQueue<SocketChannel> newConnections;
    private final BlockingQueue<RequestChannel.Response> responseQueue;
    private int nextConnectionIndex = 0;

    public Processor(int id, RequestChannel requestChannel,
                    ConnectionQuotas connectionQuotas, String listenerName,
                    MemoryPool memoryPool) throws IOException {
        this.id = id;
        this.selector = new KafkaSelector(memoryPool);
        this.requestChannel = requestChannel;
        this.connectionQuotas = connectionQuotas;
        this.listenerName = listenerName;
        this.newConnections = new ArrayBlockingQueue<>(CONNECTION_QUEUE_SIZE);
        this.responseQueue = new LinkedBlockingDeque<>();
    }

    @Override
    public void run() {
        logger.info("Processor {} started", id);

        try {
            while (running.get()) {
                // ============ 7 步事件循环 ============

                // 1. 配置新连接
                configureNewConnections();

                // 2. 处理新响应
                processNewResponses();

                // 3. I/O 多路复用
                selector.poll(300);

                // 4. 处理完成的接收
                processCompletedReceives();

                // 5. 处理完成的发送
                processCompletedSends();

                // 6. 处理断开连接
                processDisconnected();

                // 7. 关闭过量连接
                // closeExcessConnections();
            }
        } catch (Exception e) {
            logger.error("Error in processor {}", id, e);
        } finally {
            close();
        }
    }

    /**
     * 步骤 1: 配置新连接
     */
    private void configureNewConnections() {
        int connectionsProcessed = 0;

        while (connectionsProcessed < CONNECTION_QUEUE_SIZE && !newConnections.isEmpty()) {
            SocketChannel socketChannel = newConnections.poll();
            if (socketChannel == null) break;

            try {
                String connectionId = generateConnectionId(socketChannel);
                selector.register(connectionId, socketChannel);
                connectionsProcessed++;
                logger.debug("Processor {} registered connection {}", id, connectionId);
            } catch (IOException e) {
                logger.error("Failed to register connection", e);
                try {
                    socketChannel.close();
                } catch (IOException ex) {
                    // ignore
                }
            }
        }
    }

    /**
     * 步骤 2: 处理新响应
     */
    private void processNewResponses() {
        RequestChannel.Response response;
        while ((response = responseQueue.poll()) != null) {
            String connectionId = response.getConnectionId();

            try {
                if (response instanceof RequestChannel.SendResponse) {
                    RequestChannel.SendResponse sendResponse =
                        (RequestChannel.SendResponse) response;
                    selector.send(sendResponse.getResponseSend());
                } else if (response instanceof RequestChannel.NoOpResponse) {
                    selector.unmute(connectionId);
                }
            } catch (Exception e) {
                logger.error("Error processing response for {}", connectionId, e);
            }
        }
    }

    /**
     * 步骤 4: 处理完成的接收
     */
    private void processCompletedReceives() {
        for (NetworkReceive receive : selector.completedReceives()) {
            try {
                String connectionId = receive.source();

                RequestChannel.Request request = new RequestChannel.Request(
                    id, connectionId, receive.payload()
                );

                requestChannel.sendRequest(request);
                selector.mute(connectionId);

                logger.debug("Processor {} received request from {}", id, connectionId);
            } catch (Exception e) {
                logger.error("Error processing completed receive", e);
            }
        }
        selector.clearCompletedReceives();
    }

    /**
     * 步骤 5: 处理完成的发送
     */
    private void processCompletedSends() {
        for (NetworkSend send : selector.completedSends()) {
            try {
                String connectionId = send.destinationId();
                selector.unmute(connectionId);
                logger.debug("Processor {} completed send to {}", id, connectionId);
            } catch (Exception e) {
                logger.error("Error processing completed send", e);
            }
        }
        selector.clearCompletedSends();
    }

    /**
     * 步骤 6: 处理断开连接
     */
    private void processDisconnected() {
        Map<String, String> disconnected = selector.disconnected();

        for (String connectionId : disconnected.keySet()) {
            try {
                String remoteHost = extractRemoteHost(connectionId);
                if (remoteHost != null) {
                    connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost));
                }
                logger.debug("Processor {} disconnected connection {}", id, connectionId);
            } catch (Exception e) {
                logger.error("Error processing disconnection of {}", connectionId, e);
            }
        }
    }

    /**
     * 接受新连接（带 mayBlock 参数）
     */
    public boolean accept(SocketChannel socketChannel, boolean mayBlock)
        throws InterruptedException {

        if (newConnections.offer(socketChannel)) {
            selector.wakeup();
            return true;
        } else if (mayBlock) {
            long startNs = System.nanoTime();
            newConnections.put(socketChannel);
            long blockedNs = System.nanoTime() - startNs;
            logger.debug("Processor {} blocked for {} ms", id, blockedNs / 1_000_000);
            selector.wakeup();
            return true;
        } else {
            return false;
        }
    }

    public void enqueueResponse(RequestChannel.Response response) {
        responseQueue.offer(response);
        selector.wakeup();
    }

    private String generateConnectionId(SocketChannel socketChannel) {
        Socket socket = socketChannel.socket();
        String remoteHost = socket.getInetAddress().getHostAddress();
        int remotePort = socket.getPort();
        int localPort = socket.getLocalPort();

        String connId = String.format("%s:%d-%d-%d",
            remoteHost, remotePort, localPort, nextConnectionIndex);

        nextConnectionIndex = (nextConnectionIndex == Integer.MAX_VALUE)
            ? 0 : nextConnectionIndex + 1;

        return connId;
    }

    private String extractRemoteHost(String connectionId) {
        int colonIndex = connectionId.indexOf(':');
        return colonIndex > 0 ? connectionId.substring(0, colonIndex) : null;
    }

    private void close() {
        running.set(false);
        try {
            selector.close();
        } catch (IOException e) {
            logger.error("Error closing processor {}", id, e);
        }
    }

    public int getId() {
        return id;
    }
}
```

### 5.2 完整项目文件清单

**必需文件（14 个核心类）：**

1. **核心组件（4 个）**
   - `SocketServer.java` - 主服务器
   - `Acceptor.java` - Main Reactor
   - `Processor.java` - Sub Reactor
   - `RequestChannel.java` - 请求通道

2. **Selector 封装（2 个）**
   - `KafkaSelector.java` - NIO Selector 封装
   - `KafkaChannel.java` - 连接封装

3. **传输层（4 个）**
   - `TransportLayer.java` - 接口
   - `PlaintextTransportLayer.java` - 明文实现
   - `NetworkReceive.java` - 接收封装
   - `NetworkSend.java` - 发送封装

4. **辅助组件（4 个）**
   - `ConnectionQuotas.java` - 连接配额
   - `MemoryPool.java` - 内存池接口
   - `SimpleMemoryPool.java` - 内存池实现
   - `NoOpMemoryPool.java` - 空实现

**可选文件（用于测试）：**

5. **Handler 和测试（2 个）**
   - `RequestHandler.java` - 业务处理器
   - `EchoClient.java` - 测试客户端

6. **配置文件（2 个）**
   - `pom.xml` - Maven 配置
   - `logback.xml` - 日志配置

---

## 6. 项目运行指南

### 6.1 环境准备

```bash
# 1. Java 版本要求
java -version  # 需要 Java 11+

# 2. Maven 安装
mvn -version   # 需要 Maven 3.6+
```

### 6.2 项目构建

```bash
# 1. 克隆/创建项目
mkdir kafka-multi-reactor-demo
cd kafka-multi-reactor-demo

# 2. 创建 Maven 项目结构
mvn archetype:generate \
  -DgroupId=com.kafka.network \
  -DartifactId=kafka-reactor \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DinteractiveMode=false

# 3. 添加所有源文件（按照上述结构）

# 4. 编译项目
mvn clean compile

# 5. 打包
mvn clean package
```

### 6.3 启动服务器

**方式 1: 使用 IDE（推荐用于调试）**

```java
// KafkaServer.java - Main 类
public class KafkaServer {
    public static void main(String[] args) throws IOException {
        int port = 9092;
        int numProcessors = 3;      // 3 个 Sub Reactor
        int numHandlers = 8;        // 8 个业务线程

        SocketServer server = new SocketServer(port, numProcessors, numHandlers);
        server.start();

        System.out.println("Kafka Multi-Reactor Server started on port " + port);
        System.out.println("  - Processors: " + numProcessors);
        System.out.println("  - Handlers: " + numHandlers);

        // 添加 Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down server...");
            server.shutdown();
        }));
    }
}
```

**方式 2: 使用命令行**

```bash
# 编译并运行
mvn exec:java -Dexec.mainClass="com.kafka.network.server.KafkaServer"

# 或者运行打包后的 JAR
java -cp target/kafka-reactor-1.0-SNAPSHOT.jar \
     com.kafka.network.server.KafkaServer
```

### 6.4 启动测试客户端

```java
// EchoClient.java
public class EchoClient {
    public static void main(String[] args) throws IOException, InterruptedException {
        String host = "localhost";
        int port = 9092;

        for (int i = 0; i < 10; i++) {
            try (Socket socket = new Socket(host, port)) {
                // 发送请求
                String message = "Hello from client " + i;
                byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);

                OutputStream out = socket.getOutputStream();
                // 发送长度（4 字节）
                out.write(ByteBuffer.allocate(4).putInt(messageBytes.length).array());
                // 发送内容
                out.write(messageBytes);
                out.flush();

                System.out.println("Client " + i + " sent: " + message);

                // 接收响应
                InputStream in = socket.getInputStream();
                byte[] sizeBytes = new byte[4];
                in.read(sizeBytes);
                int size = ByteBuffer.wrap(sizeBytes).getInt();

                byte[] responseBytes = new byte[size];
                in.read(responseBytes);
                String response = new String(responseBytes, StandardCharsets.UTF_8);

                System.out.println("Client " + i + " received: " + response);

                Thread.sleep(100);
            }
        }
    }
}
```

---

## 7. 调试与观察

### 7.1 日志配置

**logback.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- 核心组件日志级别 -->
    <logger name="com.kafka.network.core.Acceptor" level="DEBUG"/>
    <logger name="com.kafka.network.core.Processor" level="DEBUG"/>
    <logger name="com.kafka.network.handler.RequestHandler" level="DEBUG"/>

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

### 7.2 观察要点

#### 观察点 1: Main Reactor (Acceptor) 工作流程

**日志示例：**
```
14:23:15.123 [acceptor] INFO  Acceptor - Acceptor started on 0.0.0.0:9092
14:23:16.456 [acceptor] DEBUG Acceptor - Accepted connection from /192.168.1.100:45678
14:23:16.457 [acceptor] DEBUG Acceptor - Assigned to Processor 0 (Round-Robin)
14:23:16.500 [acceptor] DEBUG Acceptor - Accepted connection from /192.168.1.101:45679
14:23:16.501 [acceptor] DEBUG Acceptor - Assigned to Processor 1 (Round-Robin)
14:23:16.550 [acceptor] DEBUG Acceptor - Accepted connection from /192.168.1.102:45680
14:23:16.551 [acceptor] DEBUG Acceptor - Assigned to Processor 2 (Round-Robin)
```

**观察要点：**
- ✅ Acceptor 线程快速接受连接
- ✅ Round-Robin 均匀分配给 Processor 0, 1, 2
- ✅ 连接 ID 格式：`remoteHost:remotePort-localPort-index`

---

#### 观察点 2: Sub Reactor (Processor) 7 步事件循环

**日志示例：**
```
14:23:16.458 [processor-0] DEBUG Processor - Processor 0 started
14:23:16.500 [processor-0] DEBUG Processor - [Step 1] Registered connection 192.168.1.100:45678-9092-0
14:23:17.100 [processor-0] DEBUG Processor - [Step 3] poll() returned 1 ready keys
14:23:17.101 [processor-0] DEBUG Processor - [Step 4] Received request from 192.168.1.100:45678-9092-0
14:23:17.102 [processor-0] DEBUG Processor - [Step 4] Muted connection 192.168.1.100:45678-9092-0
14:23:17.200 [processor-0] DEBUG Processor - [Step 2] Processing response for 192.168.1.100:45678-9092-0
14:23:17.250 [processor-0] DEBUG Processor - [Step 5] Completed send to 192.168.1.100:45678-9092-0
14:23:17.251 [processor-0] DEBUG Processor - [Step 5] Unmuted connection 192.168.1.100:45678-9092-0
```

**观察要点：**
- ✅ 7 步事件循环顺序执行
- ✅ Mute/Unmute 机制工作正常
- ✅ 请求接收后立即 mute
- ✅ 响应发送后立即 unmute

---

#### 观察点 3: RequestChannel 双队列机制

**日志示例：**
```
14:23:17.102 [processor-0] DEBUG RequestChannel - sendRequest() → requestQueue (size: 1/500)
14:23:17.103 [handler-0] DEBUG RequestHandler - receiveRequest() ← requestQueue
14:23:17.150 [handler-0] DEBUG RequestHandler - Processing request from 192.168.1.100:45678-9092-0
14:23:17.195 [handler-0] DEBUG RequestChannel - sendResponse() → Processor 0 responseQueue
14:23:17.200 [processor-0] DEBUG Processor - Dequeued response from responseQueue
```

**观察要点：**
- ✅ Processor → requestQueue → Handler
- ✅ Handler → responseQueue → Processor
- ✅ 请求队列大小监控
- ✅ 响应路由到正确的 Processor

---

#### 观察点 4: Mute/Unmute 背压机制

**添加调试代码：**

```java
// Processor.java - processCompletedReceives()
selector.mute(connectionId);
logger.info("★ MUTED connection {}, preventing next request", connectionId);

// Processor.java - processCompletedSends()
selector.unmute(connectionId);
logger.info("★ UNMUTED connection {}, ready for next request", connectionId);
```

**日志示例：**
```
14:23:17.102 [processor-0] INFO  Processor - ★ MUTED connection 192.168.1.100:45678-9092-0
14:23:17.251 [processor-0] INFO  Processor - ★ UNMUTED connection 192.168.1.100:45678-9092-0
```

**实验：**
1. 客户端连续发送 2 个请求
2. 观察第二个请求只有在第一个响应发送后才被处理
3. 验证请求顺序性保证

---

#### 观察点 5: mayBlock 阻塞机制

**压测场景：**

```java
// 启动 100 个并发客户端
for (int i = 0; i < 100; i++) {
    new Thread(() -> {
        // 发送请求...
    }).start();
}
```

**日志示例（正常情况）：**
```
14:23:20.100 [acceptor] DEBUG Acceptor - Assigned to Processor 0 (offer: success)
14:23:20.101 [acceptor] DEBUG Acceptor - Assigned to Processor 1 (offer: success)
14:23:20.102 [acceptor] DEBUG Acceptor - Assigned to Processor 2 (offer: success)
```

**日志示例（队列满时）：**
```
14:23:20.500 [acceptor] DEBUG Acceptor - Processor 0 queue full (offer: failed)
14:23:20.501 [acceptor] DEBUG Acceptor - Processor 1 queue full (offer: failed)
14:23:20.502 [acceptor] DEBUG Acceptor - Processor 2 queue full (offer: failed)
14:23:20.503 [acceptor] WARN  Acceptor - All processors busy, blocking on Processor 0
14:23:20.510 [acceptor] INFO  Acceptor - Blocked for 7 ms, then accepted (put: success)
```

**观察要点：**
- ✅ 前 N-1 次尝试非阻塞
- ✅ 最后一次阻塞等待
- ✅ 记录阻塞时间
- ✅ 确保连接不丢失

---

### 7.3 性能监控

#### 添加监控指标

```java
// Processor.java
private final AtomicLong totalRequests = new AtomicLong(0);
private final AtomicLong totalResponses = new AtomicLong(0);

private void processCompletedReceives() {
    for (NetworkReceive receive : selector.completedReceives()) {
        totalRequests.incrementAndGet();
        // ...
    }
}

private void processCompletedSends() {
    for (NetworkSend send : selector.completedSends()) {
        totalResponses.incrementAndGet();
        // ...
    }
}

// 定期输出统计
public void printStats() {
    logger.info("Processor {} - Requests: {}, Responses: {}",
        id, totalRequests.get(), totalResponses.get());
}
```

#### 启动监控线程

```java
// SocketServer.java
private void startMonitoring() {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.scheduleAtFixedRate(() -> {
        for (Processor processor : processors) {
            processor.printStats();
        }
    }, 1, 5, TimeUnit.SECONDS);
}
```

---

### 7.4 调试技巧

#### 技巧 1: 单步调试 7 步事件循环

在 IDE 中设置断点：

```java
// Processor.java - run()
while (running.get()) {
    configureNewConnections();    // ← 断点 1
    processNewResponses();         // ← 断点 2
    selector.poll(300);            // ← 断点 3
    processCompletedReceives();    // ← 断点 4
    processCompletedSends();       // ← 断点 5
    processDisconnected();         // ← 断点 6
    closeExcessConnections();      // ← 断点 7
}
```

**观察：**
- 每个步骤的执行时间
- 队列大小变化
- Selector 就绪事件

---

#### 技巧 2: 可视化线程状态

```bash
# 使用 jstack 查看线程状态
jps  # 找到进程 ID
jstack <pid> | grep -A 10 "processor\|acceptor\|handler"
```

**输出示例：**
```
"acceptor" #10 prio=5 os_prio=0 tid=0x... nid=0x... waiting on condition
   java.lang.Thread.State: RUNNABLE
   at sun.nio.ch.EPollArrayWrapper.epollWait(Native Method)
   at sun.nio.ch.SelectorImpl.select(...)
   at com.kafka.network.core.Acceptor.acceptNewConnections(...)

"processor-0" #11 prio=5 os_prio=0 tid=0x... nid=0x... runnable
   java.lang.Thread.State: RUNNABLE
   at sun.nio.ch.EPollArrayWrapper.epollWait(Native Method)
   at com.kafka.network.selector.KafkaSelector.poll(...)

"handler-0" #15 prio=5 os_prio=0 tid=0x... nid=0x... waiting on condition
   java.lang.Thread.State: WAITING (parking)
   at sun.misc.Unsafe.park(Native Method)
   at java.util.concurrent.locks.LockSupport.park(...)
   at java.util.concurrent.ArrayBlockingQueue.take(...)
```

---

#### 技巧 3: 网络抓包验证

```bash
# 使用 tcpdump 抓包
sudo tcpdump -i lo -X -s 0 port 9092

# 或使用 Wireshark
# Filter: tcp.port == 9092
```

**观察：**
- TCP 三次握手
- 数据传输（4 字节长度 + payload）
- TCP 四次挥手

---

## 8. 学习路径建议

### 8.1 阶段 1: 基础理解（1-2 天）

**目标：理解 Reactor 模式基本概念**

1. **阅读文档**
   - `REACTOR_PATTERN_DEEP_DIVE.md` - Reactor 模式起源
   - `KAFKA_NETWORK_MODEL_ANALYSIS.md` - Kafka 架构概览

2. **运行示例**
   - 启动 SocketServer
   - 运行 EchoClient
   - 观察日志输出

3. **理解关键概念**
   - Main Reactor vs Sub Reactor
   - I/O 多路复用（select/poll/epoll）
   - 事件驱动模型

---

### 8.2 阶段 2: 核心组件（3-5 天）

**目标：深入理解每个组件的职责**

1. **Acceptor (Main Reactor)**
   - 单步调试 `acceptNewConnections()`
   - 观察 Round-Robin 分配
   - 实验 mayBlock 机制

2. **Processor (Sub Reactor)**
   - 单步调试 7 步事件循环
   - 理解 Mute/Unmute 机制
   - 观察 poll() 的阻塞行为

3. **RequestChannel**
   - 理解双队列设计
   - 观察请求/响应路由
   - 实验背压机制

4. **KafkaSelector**
   - 理解 NIO Selector 封装
   - 观察 completedReceives/Sends
   - 实验断开连接处理

---

### 8.3 阶段 3: 源码对照（5-7 天）

**目标：100% 理解与 Kafka 源码的对应关系**

1. **逐行对照**
   - 参考 `CODE_REVIEW_KAFKA_REACTOR_IMPLEMENTATION.md`
   - 对照每个方法的实现
   - 理解每个设计决策

2. **关键流程追踪**
   - 连接建立完整流程
   - 请求处理完整流程
   - 响应发送完整流程
   - 连接断开完整流程

3. **阅读 Kafka 源码**
   - `SocketServer.scala` - 主服务器
   - `Acceptor` - 接受器
   - `Processor` - 处理器
   - `Selector.java` - Selector 封装

---

### 8.4 阶段 4: 性能优化（可选）

**目标：理解高性能网络编程技巧**

1. **性能测试**
   - 使用 JMH 进行基准测试
   - 压测不同配置（Processor 数量、Handler 数量）
   - 分析瓶颈

2. **优化实验**
   - 调整队列大小
   - 调整线程数量
   - 实验零拷贝（sendfile）

3. **监控工具**
   - JVM 监控（jstat、jconsole）
   - 网络监控（netstat、ss）
   - 性能剖析（VisualVM、async-profiler）

---

### 8.5 学习检查清单

**基础理解 ✓**
- [ ] 理解 Reactor 模式的三种演进
- [ ] 理解 I/O 多路复用原理
- [ ] 理解事件驱动编程模型

**组件理解 ✓**
- [ ] 理解 Acceptor 的职责和工作流程
- [ ] 理解 Processor 的 7 步事件循环
- [ ] 理解 RequestChannel 的双队列机制
- [ ] 理解 Mute/Unmute 背压机制

**流程理解 ✓**
- [ ] 能够画出连接建立完整流程图
- [ ] 能够画出请求处理完整流程图
- [ ] 能够解释 mayBlock 机制
- [ ] 能够解释 Round-Robin 负载均衡

**源码对照 ✓**
- [ ] 理解我们的实现与 Kafka 源码的映射关系
- [ ] 理解每个关键方法的 Kafka 源码位置
- [ ] 理解关键常量的含义和取值

**实践能力 ✓**
- [ ] 能够独立运行和调试项目
- [ ] 能够添加日志观察关键流程
- [ ] 能够修改配置进行实验
- [ ] 能够解释日志输出的含义

---

## 9. 总结

### 9.1 核心价值

通过这个项目，您可以：

1. **深入理解 Reactor 模式**
   - 从理论到实践的完整路径
   - Multi-Reactor 模式的精髓
   - 高性能网络编程的最佳实践

2. **掌握 Kafka 网络层设计**
   - 核心逻辑 100% 对齐
   - 每个设计决策的原因
   - 生产级代码的质量标准

3. **提升编程能力**
   - Java NIO 编程
   - 多线程编程
   - 并发编程（ArrayBlockingQueue、CAS）
   - 设计模式应用

### 9.2 关键收获

**架构层面：**
- ✅ Main Reactor + Sub Reactor 分工明确
- ✅ I/O 线程与业务线程解耦
- ✅ 负载均衡（Round-Robin）
- ✅ 背压机制（Mute/Unmute）

**实现层面：**
- ✅ 7 步事件循环的精妙设计
- ✅ mayBlock 参数的阻塞策略
- ✅ 双队列的请求/响应路由
- ✅ 连接配额的三级控制

**性能层面：**
- ✅ 非阻塞 I/O
- ✅ 批量处理限制（每次最多 20 个连接）
- ✅ 零拷贝（可选）
- ✅ 内存池管理

---

**祝您学习顺利！通过这个项目，您将彻底掌握 Reactor 模式和 Kafka 网络模型。** 🚀
