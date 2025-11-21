# Kafka Multi-Reactor 架构深度分析

## 目录
1. [总体架构](#总体架构)
2. [核心类详解](#核心类详解)
3. [调用关系与数据流](#调用关系与数据流)
4. [完整请求处理流程](#完整请求处理流程)
5. [关键设计模式](#关键设计模式)
6. [内存生命周期管理](#内存生命周期管理)

---

## 总体架构

### 架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                          SocketServer (主协调者)                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  启动和管理所有组件：                                             │ │
│  │  - 1个Acceptor (Main Reactor)                                   │ │
│  │  - 3个Processor (Sub Reactors)                                  │ │
│  │  - 8个Handler (Business Threads)                                │ │
│  │  - 1个RequestChannel (共享队列)                                  │ │
│  │  - 1个ConnectionQuotas (连接配额)                                │ │
│  │  - 1个MemoryPool (内存池)                                        │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                    ▼               ▼               ▼
        ┌───────────────┐  ┌──────────────┐  ┌──────────────┐
        │   Acceptor    │  │  Processor[] │  │  Handler[]   │
        │ (Main Reactor)│  │(Sub Reactors)│  │  (Workers)   │
        └───────────────┘  └──────────────┘  └──────────────┘
                │                  │                  │
                │                  │                  │
        [ServerSocket]      [KafkaSelector]    [RequestChannel]
                │                  │                  │
                └──────────────────┴──────────────────┘
                            共享组件
```

### 组件职责矩阵

| 组件 | 数量 | 线程模型 | 主要职责 | Kafka源码对应 |
|------|------|----------|---------|--------------|
| **SocketServer** | 1 | 主线程 | 启动和协调所有组件 | SocketServer.scala:101 |
| **Acceptor** | 1 | 1个独立线程 | 接受新连接，Round-Robin分配 | SocketServer.scala:476-785 |
| **Processor** | 3 | 每个1个线程 | I/O多路复用，事件处理 | SocketServer.scala:815-1283 |
| **Handler** | 8 | 每个1个线程 | 业务逻辑处理 | KafkaRequestHandler |
| **RequestChannel** | 1 | 无（共享数据结构） | 请求/响应路由 | RequestChannel.scala:344-499 |
| **ConnectionQuotas** | 1 | 无（线程安全） | 连接配额管理 | SocketServer.scala:189-287 |
| **MemoryPool** | 1 | 无（线程安全） | 内存分配管理 | MemoryPool.java:22 |

---

## 核心类详解

### 1. SocketServer - 主协调者

**文件**: `src/main/java/com/kafka/reactor/server/SocketServer.java`

#### 职责
- 启动和管理所有网络组件
- 协调Acceptor、Processor、Handler之间的关系
- 提供统一的启动和关闭接口

#### 关键字段

```java
public class SocketServer {
    // 监听器配置
    private final String listenerName;           // "PLAINTEXT"
    private final InetSocketAddress address;     // 监听地址和端口
    private final int numProcessors;             // Processor数量 (默认3)
    private final int numHandlers;               // Handler数量 (默认8)

    // 核心组件
    private RequestChannel requestChannel;        // 请求响应队列
    private ConnectionQuotas connectionQuotas;    // 连接配额管理
    private MemoryPool memoryPool;                // 内存池

    // Reactor组件
    private Acceptor acceptor;                    // Main Reactor
    private List<Processor> processors;           // Sub Reactors列表
    private List<Thread> processorThreads;        // Processor线程列表

    // 业务处理组件
    private List<RequestHandler> handlers;        // Handler列表
    private List<Thread> handlerThreads;          // Handler线程列表
}
```

#### 关键方法

```java
// 启动服务器
public void start() throws IOException {
    // 1. 创建共享组件
    requestChannel = new RequestChannel(DEFAULT_REQUEST_QUEUE_SIZE);
    connectionQuotas = new ConnectionQuotas(...);
    memoryPool = new SimpleMemoryPool(...);

    // 2. 创建并启动Processor (Sub Reactors)
    for (int i = 0; i < numProcessors; i++) {
        Processor processor = new Processor(i, ...);
        Thread thread = new Thread(processor);
        thread.start();
    }

    // 3. 创建并启动Acceptor (Main Reactor)
    acceptor = new Acceptor(listenerName, address, processors);
    acceptor.init();
    Thread acceptorThread = new Thread(acceptor);
    acceptorThread.start();

    // 4. 创建并启动Handler (Workers)
    for (int i = 0; i < numHandlers; i++) {
        RequestHandler handler = new RequestHandler(i, requestChannel);
        Thread thread = new Thread(handler);
        thread.start();
    }
}

// 关闭服务器
public void shutdown() {
    // 按顺序关闭：Acceptor -> Processors -> Handlers
    acceptor.shutdown();
    processors.forEach(Processor::shutdown);
    handlers.forEach(RequestHandler::shutdown);
}
```

#### 调用关系
```
SocketServer.start()
    ├─> RequestChannel.new()
    ├─> ConnectionQuotas.new()
    ├─> SimpleMemoryPool.new()
    ├─> Processor.new() × 3
    │   └─> Thread.start() × 3
    ├─> Acceptor.new()
    │   ├─> Acceptor.init()
    │   └─> Thread.start()
    └─> RequestHandler.new() × 8
        └─> Thread.start() × 8
```

---

### 2. Acceptor - Main Reactor（主反应器）

**文件**: `src/main/java/com/kafka/reactor/network/Acceptor.java`

#### 职责
- 监听ServerSocketChannel，接受新连接
- 使用Round-Robin算法分配连接到Processor
- 实现背压机制（mayBlock逻辑）

#### 关键字段

```java
public class Acceptor implements Runnable {
    private final String listenerName;              // 监听器名称
    private final InetSocketAddress address;        // 监听地址
    private final List<Processor> processors;       // Processor列表（Round-Robin）

    private ServerSocketChannel serverSocketChannel; // 服务端Socket
    private Selector nioSelector;                    // NIO Selector
    private int currentProcessorIndex = 0;           // Round-Robin索引
    private final AtomicBoolean running;             // 运行状态
}
```

#### 核心算法：Round-Robin + mayBlock背压

```java
private void assignToProcessor(SocketChannel socketChannel) throws IOException {
    int retriesLeft = processors.size();  // N次重试机会
    boolean accepted = false;

    while (retriesLeft > 0 && !accepted) {
        retriesLeft--;

        // Round-Robin选择Processor
        Processor processor = processors.get(currentProcessorIndex);
        currentProcessorIndex = (currentProcessorIndex + 1) % processors.size();

        // 关键：最后一次尝试才阻塞
        boolean mayBlock = (retriesLeft == 0);

        // 尝试分配
        accepted = processor.accept(socketChannel, mayBlock);
    }
}
```

#### 状态转换图

```
   [等待连接]
       │
       │ select() 返回 OP_ACCEPT
       ▼
   [接受连接]
       │
       │ serverSocketChannel.accept()
       ▼
   [获取SocketChannel]
       │
       │ 配置非阻塞、TCP_NODELAY
       ▼
   [Round-Robin选择Processor]
       │
       ├─ 尝试1: mayBlock=false, offer()
       ├─ 尝试2: mayBlock=false, offer()
       └─ 尝试3: mayBlock=true,  put() ← 阻塞！
           │
           ▼
       [分配成功]
```

#### 调用链路

```
Acceptor.run()
    └─> nioSelector.select(500ms)
        └─> [OP_ACCEPT事件]
            └─> acceptConnection(key)
                └─> ServerSocketChannel.accept()
                    └─> assignToProcessor(socketChannel)
                        └─> Processor.accept(channel, mayBlock)
                            ├─ mayBlock=false: offer() → 成功或失败
                            └─ mayBlock=true:  put()   → 阻塞等待
```

#### 关键点
1. **一个Acceptor只有一个ServerSocketChannel**
2. **Round-Robin保证负载均衡**
3. **mayBlock实现背压**：最后一次尝试会阻塞，确保连接不丢失

---

### 3. Processor - Sub Reactor（子反应器）

**文件**: `src/main/java/com/kafka/reactor/network/Processor.java`

#### 职责
- I/O多路复用：管理多个客户端连接
- 执行7步事件循环
- 实现Mute/Unmute流控机制
- 协调Selector和RequestChannel

#### 关键字段

```java
public class Processor implements Runnable {
    private final int id;                           // Processor ID (0, 1, 2)
    private final String listenerName;              // 监听器名称
    private final RequestChannel requestChannel;    // 请求通道（共享）
    private final ConnectionQuotas connectionQuotas; // 连接配额（共享）
    private final MemoryPool memoryPool;            // 内存池（共享）
    private final KafkaSelector selector;           // 自己的Selector

    // 新连接队列（容量20）
    private final BlockingQueue<SocketChannel> newConnections =
        new ArrayBlockingQueue<>(CONNECTION_QUEUE_SIZE);

    private final AtomicBoolean running;            // 运行状态
    private int connectionIndex = 0;                // 连接ID生成器
}
```

#### 核心算法：7步事件循环

```java
@Override
public void run() {
    while (running.get()) {
        // ========== 7-STEP EVENT LOOP ==========

        // Step 1: 配置新连接（从newConnections队列）
        configureNewConnections();

        // Step 2: 处理新响应（从ResponseQueue）
        processNewResponses();

        // Step 3: I/O多路复用（selector.poll()）
        selector.poll(POLL_TIMEOUT_MS);

        // Step 4: 处理完成的接收（completedReceives）
        processCompletedReceives();

        // Step 5: 处理完成的发送（completedSends）
        processCompletedSends();

        // Step 6: 处理断开连接（disconnected）
        processDisconnected();

        // Step 7: 关闭过多连接（简化版未实现）
        // closeExcessConnections();
    }
}
```

#### Step 1: configureNewConnections() - 详解

```java
private void configureNewConnections() {
    int connectionsProcessed = 0;

    // 批处理限制：每次最多处理20个
    while (connectionsProcessed < CONNECTION_QUEUE_SIZE && !newConnections.isEmpty()) {
        SocketChannel socketChannel = newConnections.poll();

        if (socketChannel != null) {
            // 1. 生成连接ID: "remoteHost:remotePort-localPort-index"
            String connectionId = generateConnectionId(socketChannel);

            // 2. 检查连接配额
            InetAddress address = socketChannel.socket().getInetAddress();
            if (!connectionQuotas.inc(listenerName, address)) {
                socketChannel.close();  // 配额超限，关闭
                continue;
            }

            // 3. 注册到Selector
            selector.register(connectionId, socketChannel);

            connectionsProcessed++;
        }
    }
}
```

#### Step 2: processNewResponses() - 详解

```java
private void processNewResponses() {
    Response response;

    // 从自己的ResponseQueue中获取响应
    while ((response = requestChannel.receiveResponse(id)) != null) {
        String connectionId = response.request.connectionId;

        // 1. 创建NetworkSend（带4字节长度头）
        NetworkSend send = NetworkSend.createWithSize(connectionId, response.buffer);

        // 2. 发送到Selector
        selector.send(send);

        // 3. Unmute连接，允许新请求
        selector.unmute(connectionId);
    }
}
```

#### Step 3: selector.poll() - 详解

这一步委托给 `KafkaSelector.poll()`，执行I/O多路复用。

#### Step 4: processCompletedReceives() - 详解

```java
private void processCompletedReceives() {
    for (NetworkReceive receive : selector.completedReceives()) {
        String connectionId = receive.source();

        // 1. Mute连接（保证请求顺序）
        selector.mute(connectionId);

        // 2. 创建Request对象
        RequestChannel.Request request = new RequestChannel.Request(
            id,                  // processorId
            connectionId,        // connectionId
            receive.payload()    // ByteBuffer
        );

        // 3. 发送到RequestChannel（共享队列）
        requestChannel.sendRequest(request);
    }
}
```

**关键点**：Mute连接保证在响应发送前，不会接收新的请求，从而保证请求/响应的顺序性。

#### accept() 方法 - mayBlock机制

```java
public boolean accept(SocketChannel socketChannel, boolean mayBlock)
    throws InterruptedException {

    // 尝试非阻塞添加
    if (newConnections.offer(socketChannel)) {
        wakeup();  // 唤醒selector
        return true;
    }
    // 如果队列满且允许阻塞
    else if (mayBlock) {
        newConnections.put(socketChannel);  // 阻塞等待
        wakeup();
        return true;
    }
    // 队列满且不允许阻塞
    else {
        return false;  // 让Acceptor尝试下一个Processor
    }
}
```

#### Mute/Unmute流控机制

```
[正常状态] ─────────────────────────────────────────────┐
    │                                                   │
    │ 收到完整请求                                        │
    ▼                                                   │
[Mute连接]  ← OP_READ被移除，停止读取                     │
    │                                                   │
    │ 请求进入RequestChannel                             │
    │ Handler处理                                        │
    │ 响应返回                                           │
    │                                                   │
    ▼                                                   │
[发送响应]                                               │
    │                                                   │
    │ 响应发送完成                                        │
    ▼                                                   │
[Unmute连接] ← OP_READ被恢复，允许读取 ───────────────────┘
```

---

### 4. KafkaSelector - I/O多路复用器

**文件**: `src/main/java/com/kafka/reactor/network/KafkaSelector.java`

#### 职责
- 封装Java NIO Selector
- 管理多个KafkaChannel
- 执行I/O操作：read、write
- 维护completedReceives、completedSends、disconnected列表

#### 关键字段

```java
public class KafkaSelector {
    private final Selector nioSelector;                    // NIO Selector
    private final Map<String, KafkaChannel> channels;      // 连接ID -> KafkaChannel

    // 完成的I/O操作列表（每次poll清空）
    private final List<NetworkReceive> completedReceives;  // 完成的接收
    private final List<NetworkSend> completedSends;        // 完成的发送
    private final Set<String> disconnected;                // 断开的连接

    private final MemoryPool memoryPool;                   // 内存池
    private final int maxReceiveSize;                      // 最大接收大小
}
```

#### 核心方法：poll() - I/O多路复用主循环

```java
public void poll(long timeoutMs) throws IOException {
    // 1. 清空上次的结果
    completedReceives.clear();
    completedSends.clear();
    disconnected.clear();

    // 2. 执行select（阻塞最多timeoutMs毫秒）
    int readyKeys = nioSelector.select(timeoutMs);

    if (readyKeys > 0) {
        Set<SelectionKey> selectedKeys = nioSelector.selectedKeys();
        Iterator<SelectionKey> iterator = selectedKeys.iterator();

        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();  // 必须手动移除

            KafkaChannel channel = (KafkaChannel) key.attachment();

            try {
                // 3. 处理可读事件
                if (key.isReadable()) {
                    read(channel);
                }

                // 4. 处理可写事件
                if (key.isWritable()) {
                    write(channel);
                }
            } catch (IOException e) {
                close(channel);
                disconnected.add(channel.id());
            }
        }
    }
}
```

#### read() 方法 - 处理读事件

```java
private void read(KafkaChannel channel) throws IOException {
    String nodeId = channel.id();
    long bytesReceived = channel.read();

    if (bytesReceived < 0) {
        // 连接被远程关闭（EOF）
        close(channel);
        disconnected.add(nodeId);
    } else {
        // 检查是否接收完成
        NetworkReceive receive = channel.maybeCompleteReceive();

        if (receive != null) {
            // 接收完成，添加到列表
            completedReceives.add(receive);
        }
    }
}
```

**关键点**：使用 `maybeCompleteReceive()` 而不是 `clearReceive()`，避免过早释放内存。

#### write() 方法 - 处理写事件

```java
private void write(KafkaChannel channel) throws IOException {
    String nodeId = channel.id();
    long bytesSent = channel.write();

    // 检查是否发送完成
    NetworkSend send = channel.maybeCompleteSend();

    if (send != null) {
        // 发送完成，添加到列表
        completedSends.add(send);
    }
}
```

**关键点**：使用 `maybeCompleteSend()` 获取已完成的send对象。

#### register() 方法 - 注册新连接

```java
public void register(String id, SocketChannel socketChannel) throws IOException {
    // 1. 创建TransportLayer
    TransportLayer transportLayer = new TransportLayer(id, socketChannel);

    // 2. 注册到Selector（初始兴趣：OP_READ）
    SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_READ);

    // 3. 创建KafkaChannel
    KafkaChannel channel = new KafkaChannel(id, transportLayer, memoryPool, maxReceiveSize);

    // 4. 关联
    transportLayer.setKey(key);
    key.attach(channel);

    // 5. 保存到Map
    channels.put(id, channel);
}
```

#### mute/unmute 方法

```java
public void mute(String id) {
    KafkaChannel channel = channels.get(id);
    if (channel != null) {
        channel.mute();  // 移除OP_READ兴趣
    }
}

public void unmute(String id) {
    KafkaChannel channel = channels.get(id);
    if (channel != null) {
        channel.unmute();  // 恢复OP_READ兴趣
    }
}
```

---

### 5. KafkaChannel - 连接封装

**文件**: `src/main/java/com/kafka/reactor/network/KafkaChannel.java`

#### 职责
- 封装单个客户端连接
- 管理NetworkReceive和NetworkSend
- 实现Mute/Unmute流控
- 提供read/write接口

#### 关键字段

```java
public class KafkaChannel {
    private final String id;                      // 连接ID
    private final TransportLayer transportLayer;  // 底层Socket封装
    private final MemoryPool memoryPool;          // 内存池
    private final int maxReceiveSize;             // 最大接收大小

    private NetworkReceive receive;               // 当前接收对象
    private NetworkSend send;                     // 当前发送对象
    private boolean muted = false;                // Mute状态
}
```

#### read() 方法 - 读取数据

```java
public long read() throws IOException {
    // 1. 如果没有接收对象，创建一个
    if (receive == null) {
        receive = new NetworkReceive(id, maxReceiveSize, memoryPool);
    }

    // 2. 从Socket读取数据
    long bytesRead = receive.readFrom(transportLayer.socketChannel());

    // 3. 记录日志（如果完成）
    if (receive.complete()) {
        log.debug("Completed receive from {}: {} bytes", id, receive.size());
    }

    return bytesRead;
}
```

#### write() 方法 - 写入数据

```java
public long write() throws IOException {
    if (send == null) {
        return 0;
    }

    // 写入数据到Socket
    long bytesWritten = send.writeTo(transportLayer.socketChannel());

    if (send.completed()) {
        log.debug("Completed send to {}: {} bytes", id, send.size());
    }

    return bytesWritten;
}
```

#### maybeCompleteReceive() - 核心方法

```java
public NetworkReceive maybeCompleteReceive() {
    if (receive != null && receive.complete()) {
        NetworkReceive result = receive;
        receive = null;  // 从channel移除，但不调用close()
        return result;   // 返回给调用者
    }
    return null;
}
```

**关键设计**：
- **不调用** `receive.close()`，保持 `payloadBuffer` 有效
- 允许下游（Processor、Handler）继续读取数据
- 内存稍后由GC回收或在适当时机释放

#### maybeCompleteSend() - 核心方法

```java
public NetworkSend maybeCompleteSend() {
    if (send != null && send.completed()) {
        NetworkSend result = send;
        send = null;  // 清空

        // 移除OP_WRITE兴趣
        SelectionKey key = selectionKey();
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }

        return result;
    }
    return null;
}
```

#### mute/unmute - 流控实现

```java
public void mute() {
    if (!muted) {
        SelectionKey key = selectionKey();
        if (key != null && key.isValid()) {
            // 移除OP_READ兴趣
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
            muted = true;
        }
    }
}

public void unmute() {
    if (muted) {
        SelectionKey key = selectionKey();
        if (key != null && key.isValid()) {
            // 恢复OP_READ兴趣
            key.interestOps(key.interestOps() | SelectionKey.OP_READ);
            muted = false;
        }
    }
}
```

**工作原理**：
- Mute：移除 `OP_READ` 兴趣，Selector不再通知可读事件
- Unmute：恢复 `OP_READ` 兴趣，Selector恢复通知可读事件

---

### 6. NetworkReceive - 网络接收

**文件**: `src/main/java/com/kafka/reactor/network/NetworkReceive.java`

#### 职责
- 实现Kafka消息格式：[4字节长度] + [payload]
- 两阶段读取：先读长度，再读payload
- 管理接收缓冲区

#### 关键字段

```java
public class NetworkReceive {
    private final String source;                  // 来源连接ID
    private final MemoryPool memoryPool;          // 内存池
    private final int maxSize;                    // 最大大小

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);  // 4字节长度缓冲
    private ByteBuffer payloadBuffer;             // Payload缓冲区
    private int size = -1;                        // 消息大小（-1表示未读取）
    private boolean complete = false;             // 是否完成
}
```

#### 核心算法：两阶段读取

```java
public long readFrom(ReadableByteChannel channel) throws IOException {
    long totalBytesRead = 0;

    // ========== Phase 1: 读取4字节长度头 ==========
    if (size == -1) {
        int bytesRead = channel.read(sizeBuffer);
        totalBytesRead += bytesRead;

        if (!sizeBuffer.hasRemaining()) {
            // 长度头读取完成
            sizeBuffer.flip();
            size = sizeBuffer.getInt();

            // 验证大小
            if (size < 0) {
                throw new IOException("Invalid message size: " + size);
            }
            if (maxSize != UNLIMITED && size > maxSize) {
                throw new IOException("Message size exceeds limit");
            }

            // 从内存池分配缓冲区
            payloadBuffer = memoryPool.tryAllocate(size);
            if (payloadBuffer == null) {
                throw new IOException("Failed to allocate memory");
            }
        }
    }

    // ========== Phase 2: 读取payload ==========
    if (payloadBuffer != null && payloadBuffer.hasRemaining()) {
        int bytesRead = channel.read(payloadBuffer);
        totalBytesRead += bytesRead;

        if (!payloadBuffer.hasRemaining()) {
            // Payload读取完成
            payloadBuffer.flip();
            complete = true;
        }
    }

    return totalBytesRead;
}
```

**状态转换**：
```
[初始状态]
  size = -1
  sizeBuffer未满
     │
     ▼
[Phase 1: 读取长度]
  读取4字节
     │
     ▼
[长度读取完成]
  size = 读取的值
  分配payloadBuffer
     │
     ▼
[Phase 2: 读取payload]
  读取size字节
     │
     ▼
[接收完成]
  complete = true
  payloadBuffer.flip()
```

#### close() 方法 - 内存释放

```java
public void close() {
    if (payloadBuffer != null) {
        memoryPool.release(payloadBuffer);
        // 注意：不设置payloadBuffer = null
        // 其他组件可能还需要读取
    }
}
```

**关键设计**：
- 只释放内存到池，不修改 `payloadBuffer` 引用
- 允许下游继续读取数据
- 符合Kafka的MemoryPool语义

---

### 7. NetworkSend - 网络发送

**文件**: `src/main/java/com/kafka/reactor/network/NetworkSend.java`

#### 职责
- 实现Kafka消息格式：[4字节长度] + [payload]
- 使用Scatter/Gather I/O优化发送
- 跟踪发送进度

#### 关键字段

```java
public class NetworkSend {
    private final String destination;             // 目标连接ID
    private final ByteBuffer[] buffers;           // 缓冲区数组（Scatter/Gather）
    private long totalSize;                       // 总大小
    private long remaining;                       // 剩余字节
    private boolean pending = true;               // 是否待发送
}
```

#### 构造方法：支持多缓冲区

```java
public NetworkSend(String destination, ByteBuffer... buffers) {
    this.destination = destination;
    this.buffers = buffers;

    // 计算总大小
    for (ByteBuffer buffer : buffers) {
        remaining += buffer.remaining();
    }
    totalSize = remaining;
}
```

#### 工厂方法：创建带长度头的发送

```java
public static NetworkSend createWithSize(String destination, ByteBuffer payload) {
    int size = payload.remaining();

    // 创建长度头缓冲区
    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    sizeBuffer.putInt(size);
    sizeBuffer.flip();

    // 返回包含两个缓冲区的NetworkSend
    return new NetworkSend(destination, sizeBuffer, payload);
}
```

**消息格式**：
```
┌─────────────┬──────────────────────────┐
│ 4字节长度头  │     Payload数据          │
│  (int)      │   (可变长度)              │
└─────────────┴──────────────────────────┘
  sizeBuffer        payloadBuffer
```

#### writeTo() 方法 - Scatter/Gather I/O

```java
public long writeTo(GatheringByteChannel channel) throws IOException {
    // 使用Gathering写：一次系统调用写入多个缓冲区
    long written = channel.write(buffers);
    remaining -= written;

    if (remaining <= 0) {
        pending = false;  // 发送完成
    }

    return written;
}
```

**优势**：
- 避免内存拷贝（长度头 + payload无需合并）
- 一次系统调用写入多个缓冲区
- 性能更高

---

### 8. RequestChannel - 请求响应通道

**文件**: `src/main/java/com/kafka/reactor/network/RequestChannel.java`

#### 职责
- 连接网络层（Processor）和业务层（Handler）
- 管理请求队列和响应队列
- 路由响应到正确的Processor

#### 关键字段

```java
public class RequestChannel {
    private final int queueSize;                                  // 队列大小

    // 共享请求队列（所有Processor写入，所有Handler读取）
    private final BlockingQueue<Request> requestQueue;

    // 每个Processor有自己的响应队列
    private final Map<Integer, BlockingQueue<Response>> responseQueues;
}
```

#### 架构图

```
      Processor-0           Processor-1           Processor-2
          │                     │                     │
          │ sendRequest()       │                     │
          ▼                     ▼                     ▼
      ┌─────────────────────────────────────────────────┐
      │            requestQueue (共享)                   │
      │         ArrayBlockingQueue<Request>             │
      │              容量: 500                           │
      └─────────────────────────────────────────────────┘
                              │
                              │ receiveRequest()
                              ▼
                    Handler-0, 1, 2, 3, 4, 5, 6, 7
                              │
                              │ sendResponse()
                              ▼
      ┌─────────────┬─────────────┬─────────────┐
      │responseQueue│responseQueue│responseQueue│
      │ Processor-0 │ Processor-1 │ Processor-2 │
      └─────────────┴─────────────┴─────────────┘
          │               │               │
          │ receiveResponse()             │
          ▼               ▼               ▼
      Processor-0     Processor-1     Processor-2
```

#### Request 类 - 请求封装

```java
public static class Request {
    public final int processorId;          // 来源Processor ID
    public final String connectionId;      // 来源连接ID
    public final ByteBuffer buffer;        // 请求数据
    public final long receivedTimeMs;      // 接收时间

    public Request(int processorId, String connectionId, ByteBuffer buffer) {
        this.processorId = processorId;
        this.connectionId = connectionId;
        this.buffer = buffer;
        this.receivedTimeMs = System.currentTimeMillis();
    }
}
```

#### Response 类 - 响应封装

```java
public static class Response {
    public final int processorId;          // 目标Processor ID
    public final Request request;          // 原始请求（包含connectionId）
    public final ByteBuffer buffer;        // 响应数据

    public Response(int processorId, Request request, ByteBuffer buffer) {
        this.processorId = processorId;
        this.request = request;
        this.buffer = buffer;
    }
}
```

#### 关键方法

```java
// Processor发送请求到共享队列
public void sendRequest(Request request) throws InterruptedException {
    requestQueue.put(request);  // 阻塞等待
}

// Handler从共享队列接收请求
public Request receiveRequest(long timeoutMs) throws InterruptedException {
    return requestQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
}

// Handler发送响应到Processor的响应队列
public void sendResponse(Response response) throws InterruptedException {
    BlockingQueue<Response> responseQueue = responseQueues.get(response.processorId);
    responseQueue.put(response);
}

// Processor从自己的响应队列接收响应
public Response receiveResponse(int processorId) {
    BlockingQueue<Response> responseQueue = responseQueues.get(processorId);
    return responseQueue.poll();  // 非阻塞
}
```

---

### 9. RequestHandler - 业务处理器

**文件**: `src/main/java/com/kafka/reactor/server/RequestHandler.java`

#### 职责
- 从RequestChannel接收请求
- 执行业务逻辑（示例：Echo服务）
- 发送响应回RequestChannel

#### 关键字段

```java
public class RequestHandler implements Runnable {
    private final int id;                          // Handler ID
    private final RequestChannel requestChannel;   // 请求通道
    private final AtomicBoolean running;           // 运行状态
}
```

#### 主循环

```java
@Override
public void run() {
    while (running.get()) {
        // 1. 从RequestChannel接收请求（阻塞300ms）
        Request request = requestChannel.receiveRequest(300);

        if (request != null) {
            // 2. 处理请求
            processRequest(request);
        }
    }
}
```

#### processRequest() - 业务逻辑

```java
private void processRequest(Request request) throws InterruptedException {
    // 1. 读取请求数据
    ByteBuffer requestBuffer = request.buffer;
    byte[] requestBytes = new byte[requestBuffer.remaining()];
    requestBuffer.get(requestBytes);
    String requestData = new String(requestBytes, StandardCharsets.UTF_8);

    // 2. 执行业务逻辑（示例：Echo）
    String responseData = "ECHO: " + requestData;

    // 3. 创建响应
    ByteBuffer responseBuffer = ByteBuffer.wrap(
        responseData.getBytes(StandardCharsets.UTF_8)
    );

    // 4. 发送响应
    Response response = new Response(
        request.processorId,
        request,
        responseBuffer
    );
    requestChannel.sendResponse(response);
}
```

---

### 10. 辅助类

#### ConnectionQuotas - 连接配额管理

**文件**: `src/main/java/com/kafka/reactor/quota/ConnectionQuotas.java`

**三级配额**：
1. **Broker级别**：整个服务器的总连接数
2. **Listener级别**：单个监听器的连接数
3. **IP级别**：单个IP的连接数

```java
public class ConnectionQuotas {
    private final AtomicInteger totalCount;                    // Broker级别
    private final Map<String, AtomicInteger> listenerCounts;   // Listener级别
    private final Map<InetAddress, AtomicInteger> ipCounts;    // IP级别

    public synchronized boolean inc(String listenerName, InetAddress address) {
        // 检查三级配额
        if (totalCount.get() >= maxConnections) return false;
        if (ipCounts.get(address).get() >= maxConnectionsPerIp) return false;

        // 通过，增加计数
        totalCount.incrementAndGet();
        listenerCounts.get(listenerName).incrementAndGet();
        ipCounts.computeIfAbsent(address, k -> new AtomicInteger(0)).incrementAndGet();
        return true;
    }
}
```

#### SimpleMemoryPool - 内存池

**文件**: `src/main/java/com/kafka/reactor/common/SimpleMemoryPool.java`

**CAS操作**：
```java
public class SimpleMemoryPool implements MemoryPool {
    private final long sizeBytes;                              // 总大小
    private final AtomicLong availableMemory;                  // 可用内存

    @Override
    public ByteBuffer tryAllocate(int size) {
        long current;
        long newValue;
        do {
            current = availableMemory.get();
            newValue = current - size;

            if (newValue < 0) return null;  // 内存不足
        } while (!availableMemory.compareAndSet(current, newValue));  // CAS

        return ByteBuffer.allocate(size);
    }

    @Override
    public void release(ByteBuffer buffer) {
        availableMemory.addAndGet(buffer.capacity());
    }
}
```

#### TransportLayer - Socket封装

**文件**: `src/main/java/com/kafka/reactor/network/TransportLayer.java`

**职责**：封装SocketChannel，提供统一接口

```java
public class TransportLayer {
    private final String channelId;
    private final SocketChannel socketChannel;
    private SelectionKey key;

    public TransportLayer(String channelId, SocketChannel socketChannel) throws IOException {
        this.channelId = channelId;
        this.socketChannel = socketChannel;
        this.socketChannel.configureBlocking(false);  // 非阻塞模式
    }

    public SelectionKey selectionKey() {
        return key;
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }
}
```

---

## 调用关系与数据流

### 完整调用链路图

```
1. 客户端连接
   │
   ▼
2. Acceptor.run()
   └─> ServerSocketChannel.accept()
       └─> SocketChannel (客户端连接)
           │
           ▼
3. Acceptor.assignToProcessor()
   └─> Processor.accept(socketChannel, mayBlock)
       └─> newConnections.offer() / put()
           └─> selector.wakeup()
               │
               ▼
4. Processor.run() [7步循环]
   │
   ├─> Step 1: configureNewConnections()
   │   └─> selector.register(connectionId, socketChannel)
   │       └─> KafkaChannel.new()
   │
   ├─> Step 3: selector.poll(300)
   │   └─> nioSelector.select(300)
   │       └─> [OP_READ事件]
   │           │
   │           ▼
   │       read(channel)
   │       └─> channel.read()
   │           └─> receive.readFrom(socketChannel)
   │               └─> [读取4字节] → [读取payload]
   │                   │
   │                   ▼
   │               channel.maybeCompleteReceive()
   │               └─> return receive (不调用close)
   │                   └─> completedReceives.add(receive)
   │
   ├─> Step 4: processCompletedReceives()
   │   ├─> selector.mute(connectionId)
   │   └─> requestChannel.sendRequest(request)
   │       │
   │       ▼
5. RequestHandler.run()
   └─> requestChannel.receiveRequest()
       └─> processRequest(request)
           ├─> 读取request.buffer
           ├─> 业务处理（Echo）
           └─> requestChannel.sendResponse(response)
               │
               ▼
6. Processor.run() [继续7步循环]
   │
   ├─> Step 2: processNewResponses()
   │   ├─> requestChannel.receiveResponse(id)
   │   ├─> NetworkSend.createWithSize(connectionId, buffer)
   │   ├─> selector.send(send)
   │   └─> selector.unmute(connectionId)
   │
   ├─> Step 3: selector.poll(300)
   │   └─> [OP_WRITE事件]
   │       │
   │       ▼
   │   write(channel)
   │   └─> channel.write()
   │       └─> send.writeTo(socketChannel)
   │           │
   │           ▼
   │       channel.maybeCompleteSend()
   │       └─> return send
   │           └─> completedSends.add(send)
   │
   └─> Step 5: processCompletedSends()
       └─> [发送完成，继续下一个请求]
```

### 数据流图

```
客户端 ───[TCP]──→ ServerSocket
                      │
                      │ accept()
                      ▼
                  SocketChannel
                      │
                      │ Round-Robin
                      ▼
              ┌───────┴───────┬───────┐
              ▼               ▼       ▼
         Processor-0     Processor-1  Processor-2
              │               │           │
              │ [newConnections队列]      │
              ▼               ▼           ▼
         KafkaSelector   KafkaSelector  KafkaSelector
              │               │           │
              │ [OP_READ]     │           │
              ▼               ▼           ▼
         NetworkReceive  NetworkReceive  NetworkReceive
              │               │           │
              │ [完成接收]     │           │
              ▼               ▼           ▼
              └───────┬───────┴───────┘
                      │
                      │ Request
                      ▼
              ┌───────────────┐
              │RequestChannel │
              │  (requestQueue)│
              └───────────────┘
                      │
                      │ 竞争获取
                      ▼
         ┌────────────┼────────────┐
         ▼            ▼            ▼
    Handler-0    Handler-1 ... Handler-7
         │            │            │
         │ 处理 Echo   │            │
         ▼            ▼            ▼
         └────────────┬────────────┘
                      │
                      │ Response (带processorId)
                      ▼
              ┌───────────────┐
              │RequestChannel │
              │(responseQueues)│
              └───────────────┘
                      │
         ┌────────────┼────────────┐
         ▼            ▼            ▼
    Processor-0  Processor-1  Processor-2
         │            │            │
         │ receiveResponse(id)     │
         ▼            ▼            ▼
    NetworkSend  NetworkSend  NetworkSend
         │            │            │
         │ [OP_WRITE] │            │
         ▼            ▼            ▼
         └────────────┬────────────┘
                      │
                      │ writeTo()
                      ▼
                  SocketChannel
                      │
                      │ [TCP]
                      ▼
                   客户端
```

---

## 完整请求处理流程

### 时序图

```
客户端    Acceptor    Processor-0    KafkaSelector    KafkaChannel    RequestChannel    Handler-1
  │          │            │               │                │                │              │
  ├─连接────→│            │               │                │                │              │
  │          ├─accept()──→│               │                │                │              │
  │          │            ├─newConnections│                │                │              │
  │          │            │    .put()     │                │                │              │
  │          │            ├─wakeup()─────→│                │                │              │
  │          │            │               │                │                │              │
  │          │            │[循环Step1]    │                │                │              │
  │          │            ├─configure─────→│                │                │              │
  │          │            │  NewConnections│                │                │              │
  │          │            │               ├─register()────→│                │              │
  │          │            │               │                │                │              │
  ├─发送请求─→│            │               │                │                │              │
  │          │            │[循环Step3]    │                │                │              │
  │          │            ├─selector.poll()│                │                │              │
  │          │            │               ├─[OP_READ]─────→│                │              │
  │          │            │               │                ├─read()         │              │
  │          │            │               │                │  readFrom()    │              │
  │          │            │               │                │  [4字节+payload]│             │
  │          │            │               │                ├─complete()     │              │
  │          │            │               │                │                │              │
  │          │            │               ├─maybeComplete──┤                │              │
  │          │            │               │  Receive()     │                │              │
  │          │            │               │                │                │              │
  │          │            │[循环Step4]    │                │                │              │
  │          │            ├─processCompleted                │                │              │
  │          │            │  Receives()   │                │                │              │
  │          │            ├─mute()────────→│                │                │              │
  │          │            ├─sendRequest()─────────────────────────────────→│              │
  │          │            │               │                │                │              │
  │          │            │               │                │                ├─receiveRequest()
  │          │            │               │                │                │              │
  │          │            │               │                │                │              ├─processRequest()
  │          │            │               │                │                │              │  [Echo处理]
  │          │            │               │                │                │              │
  │          │            │               │                │                ├─sendResponse()│
  │          │            │               │                │                │              │
  │          │            │[循环Step2]    │                │                │              │
  │          │            ├─receiveResponse()──────────────────────────────┤              │
  │          │            ├─createWithSize()│                │                │              │
  │          │            ├─selector.send()→│                │                │              │
  │          │            │               ├─setSend()─────→│                │              │
  │          │            ├─unmute()──────→│                │                │              │
  │          │            │               │                │                │              │
  │          │            │[循环Step3]    │                │                │              │
  │          │            ├─selector.poll()│                │                │              │
  │          │            │               ├─[OP_WRITE]────→│                │              │
  │          │            │               │                ├─write()        │              │
  │          │            │               │                │  writeTo()     │              │
  │          │            │               │                │  [Scatter/Gather]            │
  │          │            │               ├─maybeComplete──┤                │              │
  │          │            │               │  Send()        │                │              │
  │          │            │               │                │                │              │
  ←─收到响应─┤            │               │                │                │              │
  │          │            │               │                │                │              │
```

### 详细步骤说明

#### 阶段1：连接建立（T1-T5）

| 时间 | 组件 | 操作 | 说明 |
|------|------|------|------|
| T1 | 客户端 | connect() | 发起TCP连接 |
| T2 | Acceptor | accept() | ServerSocket接受连接 |
| T3 | Acceptor | assignToProcessor() | Round-Robin选择Processor-0 |
| T4 | Processor-0 | accept(channel, mayBlock) | 添加到newConnections队列 |
| T5 | Processor-0 | wakeup() | 唤醒selector |

#### 阶段2：连接注册（T6-T8）

| 时间 | 组件 | 操作 | 说明 |
|------|------|------|------|
| T6 | Processor-0 | configureNewConnections() | Step 1 |
| T7 | KafkaSelector | register() | 注册到Selector |
| T8 | KafkaChannel | new() | 创建KafkaChannel对象 |

#### 阶段3：接收请求（T9-T16）

| 时间 | 组件 | 操作 | 说明 |
|------|------|------|------|
| T9 | 客户端 | send() | 发送请求数据 |
| T10 | Processor-0 | selector.poll() | Step 3: I/O多路复用 |
| T11 | KafkaSelector | read(channel) | 处理OP_READ事件 |
| T12 | KafkaChannel | read() | 读取数据 |
| T13 | NetworkReceive | readFrom() | 两阶段读取 |
| T14 | NetworkReceive | complete() | 读取完成 |
| T15 | KafkaChannel | maybeCompleteReceive() | 返回receive（不close） |
| T16 | KafkaSelector | completedReceives.add() | 添加到完成列表 |

#### 阶段4：处理请求（T17-T22）

| 时间 | 组件 | 操作 | 说明 |
|------|------|------|------|
| T17 | Processor-0 | processCompletedReceives() | Step 4 |
| T18 | Processor-0 | selector.mute() | Mute连接 |
| T19 | Processor-0 | requestChannel.sendRequest() | 发送到共享队列 |
| T20 | Handler-1 | receiveRequest() | 从队列获取 |
| T21 | Handler-1 | processRequest() | Echo处理 |
| T22 | Handler-1 | sendResponse() | 发送到responseQueue[0] |

#### 阶段5：发送响应（T23-T30）

| 时间 | 组件 | 操作 | 说明 |
|------|------|------|------|
| T23 | Processor-0 | processNewResponses() | Step 2 |
| T24 | Processor-0 | receiveResponse(0) | 从自己的responseQueue获取 |
| T25 | Processor-0 | NetworkSend.createWithSize() | 创建send对象 |
| T26 | Processor-0 | selector.send() | 发送到Selector |
| T27 | KafkaChannel | setSend() | 设置send，添加OP_WRITE |
| T28 | Processor-0 | selector.unmute() | Unmute连接 |
| T29 | Processor-0 | selector.poll() | Step 3: I/O多路复用 |
| T30 | KafkaSelector | write(channel) | 处理OP_WRITE事件 |
| T31 | KafkaChannel | write() | 写入数据 |
| T32 | NetworkSend | writeTo() | Scatter/Gather写入 |
| T33 | KafkaChannel | maybeCompleteSend() | 返回send，移除OP_WRITE |
| T34 | 客户端 | recv() | 接收响应 |

---

## 关键设计模式

### 1. Reactor模式

**定义**：使用单线程或少量线程处理多个并发连接的I/O事件。

**实现**：
- **Main Reactor（Acceptor）**：处理连接接受
- **Sub Reactor（Processor）**：处理I/O读写
- **Handler Pool**：处理业务逻辑

### 2. Producer-Consumer模式

**位置1**：Acceptor → Processor
- Producer: Acceptor
- Consumer: Processor
- Queue: newConnections (ArrayBlockingQueue, 容量20)

**位置2**：Processor → Handler
- Producer: Processor
- Consumer: Handler
- Queue: RequestChannel.requestQueue

**位置3**：Handler → Processor
- Producer: Handler
- Consumer: Processor
- Queue: RequestChannel.responseQueues (每个Processor一个)

### 3. 对象池模式

**MemoryPool**：
- 预分配内存，避免频繁申请
- CAS操作保证线程安全
- release()只更新计数，不修改ByteBuffer

### 4. 享元模式

**KafkaSelector**：
- 一个Processor维护一个Selector
- Selector管理多个KafkaChannel
- 共享Selector的select()操作

### 5. 装饰器模式

**层次结构**：
```
SocketChannel (Java NIO原生)
    ↓
TransportLayer (封装基本操作)
    ↓
KafkaChannel (添加mute/unmute、receive/send管理)
    ↓
KafkaSelector (添加多路复用)
```

### 6. 策略模式

**背压策略**：
- 策略1：非阻塞（offer()）
- 策略2：阻塞（put()）
- 根据mayBlock参数选择

---

## 内存生命周期管理

### NetworkReceive的生命周期

```
┌─────────────────────────────────────────────────────┐
│ 阶段1: 创建和读取                                     │
├─────────────────────────────────────────────────────┤
│ 1. KafkaChannel.read()                              │
│    └─> new NetworkReceive(id, maxSize, memoryPool) │
│                                                     │
│ 2. NetworkReceive.readFrom(channel)                │
│    ├─> memoryPool.tryAllocate(size)                │
│    │   └─> payloadBuffer = ByteBuffer.allocate()   │
│    └─> channel.read(payloadBuffer)                 │
│                                                     │
│ 3. NetworkReceive.complete() == true                │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│ 阶段2: 从Channel分离                                  │
├─────────────────────────────────────────────────────┤
│ 4. KafkaSelector.read()                            │
│    └─> channel.maybeCompleteReceive()              │
│        ├─> result = receive                         │
│        ├─> receive = null  // 从channel移除         │
│        └─> return result   // 不调用close()         │
│                                                     │
│ 5. completedReceives.add(result)                   │
│    // payloadBuffer仍然有效！                        │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│ 阶段3: 传递给Processor                               │
├─────────────────────────────────────────────────────┤
│ 6. Processor.processCompletedReceives()            │
│    └─> Request request = new Request(              │
│           processorId,                              │
│           connectionId,                             │
│           receive.payload()  // payloadBuffer      │
│        )                                            │
│                                                     │
│ 7. requestChannel.sendRequest(request)             │
│    // payloadBuffer通过request传递                  │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│ 阶段4: Handler处理                                   │
├─────────────────────────────────────────────────────┤
│ 8. Handler.receiveRequest()                        │
│    └─> request = requestChannel.receiveRequest()  │
│                                                     │
│ 9. Handler.processRequest(request)                │
│    ├─> buffer = request.buffer  // payloadBuffer  │
│    ├─> buffer.get(bytes)        // 读取数据        │
│    └─> process(bytes)                              │
│                                                     │
│ // payloadBuffer被Handler成功读取                   │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│ 阶段5: 内存释放（可选）                               │
├─────────────────────────────────────────────────────┤
│ 10. NetworkReceive.close()                         │
│     └─> memoryPool.release(payloadBuffer)          │
│         // 释放到池，但不设置为null                   │
│                                                     │
│ 11. GC回收                                          │
│     └─> 当没有引用时，由GC回收                        │
└─────────────────────────────────────────────────────┘
```

### 关键设计决策

#### 为什么 maybeCompleteReceive() 不调用 close()？

**错误方式**（会导致NPE）：
```java
NetworkReceive receive = channel.currentReceive();
completedReceives.add(receive);
channel.clearReceive();  // ← 调用receive.close()
                         // ← payloadBuffer被设置为null
// 后续Handler读取request.buffer时 → NPE!
```

**正确方式**（当前实现）：
```java
NetworkReceive receive = channel.maybeCompleteReceive();
// receive从channel分离，但payloadBuffer保持有效
completedReceives.add(receive);
// 后续Handler可以正常读取payloadBuffer
```

#### MemoryPool语义

Kafka的MemoryPool设计：
```java
// 分配
ByteBuffer buffer = memoryPool.tryAllocate(1024);
availableMemory -= 1024;  // 更新计数

// 释放
memoryPool.release(buffer);
availableMemory += 1024;  // 恢复计数
// buffer本身不变，仍可读取！
```

**优势**：
1. 允许"释放后读取"
2. 避免内存拷贝
3. 简化生命周期管理
4. 由GC负责最终回收

---

## 总结

### 核心理念

1. **异步非阻塞I/O**：使用Java NIO实现高并发
2. **多路复用**：一个线程管理多个连接
3. **Reactor模式**：Main Reactor + Sub Reactors
4. **背压机制**：mayBlock + ArrayBlockingQueue
5. **流控机制**：Mute/Unmute保证顺序
6. **内存管理**：MemoryPool + 延迟释放

### 性能优化点

1. **Scatter/Gather I/O**：避免内存拷贝
2. **批处理**：每次最多处理20个连接
3. **CAS操作**：无锁内存池
4. **Round-Robin**：负载均衡
5. **零拷贝思想**：ByteBuffer直接传递

### 对齐Kafka源码

| 概念 | 我们的实现 | Kafka源码 |
|------|-----------|----------|
| Main Reactor | Acceptor | SocketServer.Acceptor |
| Sub Reactor | Processor | SocketServer.Processor |
| 7步事件循环 | Processor.run() | Processor.run() |
| mayBlock | accept(channel, mayBlock) | accept() with mayBlock |
| Mute/Unmute | selector.mute/unmute | channel.mute/unmute |
| 队列容量 | 20 | CONNECTION_QUEUE_SIZE=20 |
| 内存管理 | maybeCompleteReceive | maybeCompleteReceive |

---

**这个实现是学习Kafka网络模型的完美起点，所有核心概念都已体现！** 🎉
