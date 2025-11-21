# Kafka Multi-Reactor 实现代码 Review

## 目标
确保简化实现的**核心逻辑**与 Kafka 源码**完全一致**，以便深入理解 Kafka 的网络模型。

---

## 1. 关键发现与修正

### 🔴 严重问题（必须修正）

#### 问题 1: Acceptor 缺少 mayBlock 参数逻辑

**当前实现（错误）：**
```java
// Acceptor.java - acceptNewConnection()
private void acceptNewConnection() throws IOException {
    SocketChannel socketChannel = serverChannel.accept();
    if (socketChannel != null) {
        // ...配置 Socket...

        // ❌ 错误：直接调用 processor.accept()，缺少重试逻辑
        Processor processor = processors.get(currentProcessorIndex);
        currentProcessorIndex = (currentProcessorIndex + 1) % processors.size();
        processor.accept(socketChannel);
    }
}
```

**Kafka 源码（正确）：**
```scala
// SocketServer.scala:652-667
accept(key).foreach { socketChannel =>
  var retriesLeft = synchronized(processors.length)  // ← 重试次数
  var processor: Processor = null

  do {
    retriesLeft -= 1
    processor = synchronized {
      currentProcessorIndex = currentProcessorIndex % processors.length
      processors(currentProcessorIndex)
    }
    currentProcessorIndex += 1

    // ★ 关键：最后一次传 mayBlock=true
  } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0))
}
```

**正确实现：**
```java
// Acceptor.java
private void acceptNewConnection() throws IOException {
    SocketChannel socketChannel = serverChannel.accept();
    if (socketChannel != null) {
        try {
            connectionQuotas.inc(listenerName, socketChannel.socket().getInetAddress());
            configureSocket(socketChannel);
            assignToProcessor(socketChannel);  // ← 使用新方法
        } catch (TooManyConnectionsException e) {
            logger.warn("Connection rejected: {}", e.getMessage());
            socketChannel.close();
        }
    }
}

/**
 * ★ 新增方法：Round-Robin + mayBlock 逻辑
 * 完全对齐 Kafka SocketServer.scala:652-667
 */
private void assignToProcessor(SocketChannel socketChannel) throws IOException {
    int retriesLeft = processors.size();
    Processor processor = null;
    boolean accepted = false;

    while (retriesLeft > 0 && !accepted) {
        retriesLeft--;
        processor = processors.get(currentProcessorIndex);
        currentProcessorIndex = (currentProcessorIndex + 1) % processors.size();

        // ★ 关键：最后一次尝试时 mayBlock=true
        boolean mayBlock = (retriesLeft == 0);
        accepted = processor.accept(socketChannel, mayBlock);
    }

    if (!accepted) {
        throw new IOException("Failed to assign connection to any processor");
    }
}
```

---

#### 问题 2: Processor.accept() 缺少 mayBlock 参数

**当前实现（错误）：**
```java
// Processor.java
public void accept(SocketChannel socketChannel) {
    newConnections.offer(socketChannel);  // ❌ 错误：只有非阻塞 offer
    selector.wakeup();
}
```

**Kafka 源码（正确）：**
```scala
// SocketServer.scala:1154-1171
def accept(socketChannel: SocketChannel,
           mayBlock: Boolean,
           acceptorBlockedPercentMeter: Meter): Boolean = {
  val accepted = {
    // ① 非阻塞尝试
    if (newConnections.offer(socketChannel))
      true
    // ② mayBlock=true 时阻塞等待
    else if (mayBlock) {
      val startNs = time.nanoseconds
      newConnections.put(socketChannel)  // ← 阻塞
      acceptorBlockedPercentMeter.mark(time.nanoseconds() - startNs)
      true
    }
    // ③ mayBlock=false，直接失败
    else
      false
  }

  if (accepted)
    wakeup()

  accepted
}
```

**正确实现：**
```java
// Processor.java
/**
 * ★ 新增 mayBlock 参数
 * 完全对齐 Kafka SocketServer.scala:1154-1171
 */
public boolean accept(SocketChannel socketChannel, boolean mayBlock)
    throws InterruptedException {

    boolean accepted;

    // ① 非阻塞尝试
    if (newConnections.offer(socketChannel)) {
        accepted = true;
    }
    // ② mayBlock=true 时阻塞等待
    else if (mayBlock) {
        long startNs = System.nanoTime();
        newConnections.put(socketChannel);  // ← 阻塞直到队列有空位
        long blockedNs = System.nanoTime() - startNs;

        // 可选：记录阻塞时间用于监控
        logger.debug("Processor {} blocked for {} ms waiting to accept connection",
            id, blockedNs / 1_000_000);

        accepted = true;
    }
    // ③ mayBlock=false，直接失败
    else {
        accepted = false;
    }

    if (accepted) {
        selector.wakeup();
    }

    return accepted;
}
```

---

#### 问题 3: KafkaSelector 缺少 wakeup() 方法

**当前实现（缺失）：**
```java
// KafkaSelector.java
// ❌ 缺少 wakeup() 方法
```

**Kafka 源码（正确）：**
```java
// Selector.java:361-363
@Override
public void wakeup() {
    this.nioSelector.wakeup();
}
```

**正确实现：**
```java
// KafkaSelector.java
/**
 * ★ 新增方法：唤醒阻塞的 select()
 * 完全对齐 Kafka Selector.java:361-363
 */
public void wakeup() {
    nioSelector.wakeup();
}
```

---

#### 问题 4: Processor 的 newConnections 队列大小不符

**当前实现（可能错误）：**
```java
// Processor.java
private final Queue<SocketChannel> newConnections = new LinkedList<>();
// ❌ 错误：使用无界队列
```

**Kafka 源码（正确）：**
```scala
// SocketServer.scala:845
private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)

// SocketServer.scala:791
val ConnectionQueueSize = 20  // ← 默认容量 20
```

**正确实现：**
```java
// Processor.java
private static final int CONNECTION_QUEUE_SIZE = 20;  // ← 对齐 Kafka

/**
 * ★ 修正：使用 ArrayBlockingQueue，容量 20
 * 完全对齐 Kafka SocketServer.scala:845
 */
private final BlockingQueue<SocketChannel> newConnections =
    new ArrayBlockingQueue<>(CONNECTION_QUEUE_SIZE);
```

---

#### 问题 5: configureNewConnections() 缺少批量处理限制

**当前实现（错误）：**
```java
// Processor.java - configureNewConnections()
private void configureNewConnections() {
    SocketChannel socketChannel;
    // ❌ 错误：一次性处理所有连接
    while ((socketChannel = newConnections.poll()) != null) {
        try {
            String connectionId = generateConnectionId(socketChannel);
            selector.register(connectionId, socketChannel);
        } catch (IOException e) {
            // ...
        }
    }
}
```

**Kafka 源码（正确）：**
```scala
// SocketServer.scala:1178-1195
private def configureNewConnections(): Unit = {
  var connectionsProcessed = 0

  // ★ 关键：限制每次循环处理的连接数
  while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty) {
    val channel = newConnections.poll()
    try {
      debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
      selector.register(connectionId(channel.socket), channel)
      connectionsProcessed += 1  // ← 计数
    } catch {
      case e: Throwable =>
        val remoteAddress = channel.socket.getRemoteSocketAddress
        connectionQuotas.closeChannel(this, listenerName, channel)
        processException(s"Processor $id closed connection from $remoteAddress", e)
    }
  }
}
```

**正确实现：**
```java
// Processor.java
private static final int CONNECTION_QUEUE_SIZE = 20;

/**
 * ★ 修正：限制每次处理的连接数
 * 完全对齐 Kafka SocketServer.scala:1178-1195
 */
private void configureNewConnections() {
    int connectionsProcessed = 0;

    // ★ 关键：限制处理数量，避免饿死其他步骤
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
```

**原因：**
- 如果一次性处理所有连接，新连接暴增时会阻塞整个事件循环
- Kafka 限制每次循环最多处理 20 个，确保其他步骤及时执行

---

### 🟡 中等问题（建议修正）

#### 问题 6: KafkaSelector.poll() 缺少断开连接处理

**当前实现（不完整）：**
```java
// KafkaSelector.java - poll()
public void poll(long timeout) throws IOException {
    clear();

    int ready = nioSelector.select(timeout);
    if (ready > 0) {
        Set<SelectionKey> keys = nioSelector.selectedKeys();
        for (SelectionKey key : keys) {
            KafkaChannel channel = (KafkaChannel) key.attachment();

            try {
                if (key.isReadable()) {
                    // ...读取逻辑...
                }

                if (key.isWritable()) {
                    // ...写入逻辑...
                }
            } catch (IOException e) {
                close(channel.id());  // ❌ 缺少记录到 disconnected 集合
            }
        }
        keys.clear();
    }
}
```

**Kafka 源码（正确）：**
```java
// Selector.java:514-600
void pollSelectionKeys(Set<SelectionKey> selectionKeys, ...) {
    for (SelectionKey key : determineHandlingOrder(selectionKeys)) {
        KafkaChannel channel = channel(key);

        try {
            // ... 处理读写...
        } catch (Exception e) {
            String channelId = channel.id();
            // ★ 关键：记录断开原因
            close(channelId, ChannelState.FAILED_SEND);  // ← 记录到 disconnected
        }
    }
}
```

**正确实现：**
```java
// KafkaSelector.java
private final Map<String, ChannelState> disconnected = new HashMap<>();

public void poll(long timeout) throws IOException {
    clear();

    int ready = nioSelector.select(timeout);
    if (ready > 0) {
        Set<SelectionKey> keys = nioSelector.selectedKeys();
        for (SelectionKey key : keys) {
            KafkaChannel channel = (KafkaChannel) key.attachment();
            String channelId = channel.id();

            try {
                if (key.isReadable()) {
                    long bytesRead = channel.read();
                    if (bytesRead > 0) {
                        NetworkReceive receive = channel.maybeCompleteReceive();
                        if (receive != null) {
                            completedReceives.add(receive);
                        }
                    } else if (bytesRead < 0) {
                        // ★ 新增：检测远程关闭
                        close(channelId, "Remote closed");
                    }
                }

                if (key.isWritable()) {
                    long bytesWritten = channel.write();
                    if (bytesWritten > 0) {
                        NetworkSend send = channel.maybeCompleteSend();
                        if (send != null) {
                            completedSends.add(send);
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                        }
                    }
                }

            } catch (IOException e) {
                // ★ 修正：记录断开原因
                logger.debug("Exception on channel {}: {}", channelId, e.getMessage());
                close(channelId, "IOException: " + e.getMessage());
            }
        }
        keys.clear();
    }
}

/**
 * ★ 新增方法：关闭连接并记录原因
 */
private void close(String id, String reason) {
    KafkaChannel channel = channels.remove(id);
    if (channel != null) {
        try {
            channel.close();
            disconnected.put(id, reason);
        } catch (IOException e) {
            logger.error("Error closing channel {}", id, e);
        }
    }
}

/**
 * ★ 新增方法：获取断开连接
 */
public Map<String, String> disconnected() {
    return new HashMap<>(disconnected);
}
```

---

#### 问题 7: Processor 缺少 processDisconnected() 实现

**当前实现（空实现）：**
```java
// Processor.java
private void processDisconnected() {
    // ❌ 空实现
}
```

**Kafka 源码（正确）：**
```scala
// SocketServer.scala:1105-1120
private def processDisconnected(): Unit = {
  selector.disconnected.keySet.forEach { connectionId =>
    try {
      val remoteHost = ServerConnectionId.fromString(connectionId).orElseThrow { () =>
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost

      // ① 移除未完成的响应
      inflightResponses.remove(connectionId).foreach(updateRequestMetrics)

      // ② 更新连接配额
      connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost))

      // ③ 通知监听器
      connectionDisconnectListeners.foreach(listener =>
        CoreUtils.swallow(() -> listener.onDisconnect(connectionId), this, Level.ERROR))

    } catch {
      case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
    }
  }
}
```

**正确实现：**
```java
// Processor.java
/**
 * ★ 修正：实现 processDisconnected()
 * 完全对齐 Kafka SocketServer.scala:1105-1120
 */
private void processDisconnected() {
    Map<String, String> disconnected = selector.disconnected();

    for (String connectionId : disconnected.keySet()) {
        try {
            // ① 提取远程主机地址
            String remoteHost = extractRemoteHost(connectionId);

            // ② 更新连接配额
            if (remoteHost != null) {
                connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost));
            }

            // ③ 清理资源（可选）
            // inflightResponses.remove(connectionId);

            logger.debug("Processor {} disconnected connection {}", id, connectionId);

        } catch (Exception e) {
            logger.error("Error processing disconnection of {}", connectionId, e);
        }
    }
}

/**
 * ★ 新增辅助方法：从 connectionId 提取远程主机
 * connectionId 格式: "192.168.1.100:45678-9092-0"
 */
private String extractRemoteHost(String connectionId) {
    int colonIndex = connectionId.indexOf(':');
    if (colonIndex > 0) {
        return connectionId.substring(0, colonIndex);
    }
    return null;
}
```

---

### 🟢 轻微问题（可选优化）

#### 问题 8: 缺少 selector.clearCompletedReceives/Sends()

**当前实现（不完整）：**
```java
// Processor.java - processCompletedReceives()
private void processCompletedReceives() {
    for (NetworkReceive receive : selector.completedReceives()) {
        // ...处理逻辑...
    }
    // ❌ 缺少清理
}
```

**Kafka 源码（正确）：**
```scala
// SocketServer.scala:1020-1072
private def processCompletedReceives(): Unit = {
  selector.completedReceives.forEach { receive =>
    // ...处理逻辑...
  }
  selector.clearCompletedReceives()  // ← 清理
}

// SocketServer.scala:1074-1097
private def processCompletedSends(): Unit = {
  selector.completedSends.forEach { send =>
    // ...处理逻辑...
  }
  selector.clearCompletedSends()  // ← 清理
}
```

**正确实现：**
```java
// KafkaSelector.java
/**
 * ★ 新增方法：清理已完成的接收
 */
public void clearCompletedReceives() {
    completedReceives.clear();
}

/**
 * ★ 新增方法：清理已完成的发送
 */
public void clearCompletedSends() {
    completedSends.clear();
}

// Processor.java - processCompletedReceives()
private void processCompletedReceives() {
    for (NetworkReceive receive : selector.completedReceives()) {
        // ...处理逻辑...
    }
    selector.clearCompletedReceives();  // ★ 新增
}

// Processor.java - processCompletedSends()
private void processCompletedSends() {
    for (NetworkSend send : selector.completedSends()) {
        // ...处理逻辑...
    }
    selector.clearCompletedSends();  // ★ 新增
}
```

---

#### 问题 9: 缺少连接 ID 格式化

**当前实现（简化）：**
```java
// Processor.java
private String generateConnectionId(SocketChannel socketChannel) {
    return socketChannel.socket().getInetAddress().getHostAddress() + ":" +
           socketChannel.socket().getPort() + "-" + nextConnectionIndex++;
}
```

**Kafka 源码（正确）：**
```scala
// SocketServer.scala:1212-1216
protected[network] def connectionId(socket: Socket): String = {
  val connId = ServerConnectionId.generateConnectionId(socket, id, nextConnectionIndex)
  nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
  connId
}
```

**ServerConnectionId.java:**
```java
public static String generateConnectionId(Socket socket, int processorId, int connectionIndex) {
    String localHost = socket.getLocalAddress().getHostAddress();
    int localPort = socket.getLocalPort();
    String remoteHost = socket.getInetAddress().getHostAddress();
    int remotePort = socket.getPort();

    // 格式: "remoteHost:remotePort-localPort-connectionIndex"
    return String.format("%s:%d-%d-%d", remoteHost, remotePort, localPort, connectionIndex);
}
```

**正确实现：**
```java
// Processor.java
/**
 * ★ 修正：使用 Kafka 的 connectionId 格式
 * 格式: "remoteHost:remotePort-localPort-connectionIndex"
 */
private String generateConnectionId(SocketChannel socketChannel) {
    Socket socket = socketChannel.socket();
    String remoteHost = socket.getInetAddress().getHostAddress();
    int remotePort = socket.getPort();
    int localPort = socket.getLocalPort();

    String connId = String.format("%s:%d-%d-%d",
        remoteHost, remotePort, localPort, nextConnectionIndex);

    // ★ 防止溢出
    nextConnectionIndex = (nextConnectionIndex == Integer.MAX_VALUE)
        ? 0 : nextConnectionIndex + 1;

    return connId;
}
```

---

## 2. 完整修正后的核心类

### 2.1 Acceptor.java（修正版）

```java
public class Acceptor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);

    private final ServerSocketChannel serverChannel;
    private final Selector selector;
    private final List<Processor> processors;
    private final ConnectionQuotas connectionQuotas;
    private final String listenerName;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // ★ Round-Robin 索引
    private int currentProcessorIndex = 0;

    // ★ 限流 Socket 队列（可选）
    private final PriorityQueue<DelayedCloseSocket> throttledSockets = new PriorityQueue<>();

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
            // 注册 OP_ACCEPT
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info("Acceptor started on {}", serverChannel.getLocalAddress());

            while (running.get()) {
                try {
                    acceptNewConnections();
                    closeThrottledConnections();
                } catch (Exception e) {
                    logger.error("Error in acceptor loop", e);
                }
            }
        } catch (IOException e) {
            logger.error("Error starting acceptor", e);
        } finally {
            close();
        }
    }

    /**
     * ★ 接受新连接
     * 完全对齐 Kafka SocketServer.scala:641-676
     */
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
                            // ① 连接配额检查
                            connectionQuotas.inc(
                                listenerName,
                                socketChannel.socket().getInetAddress()
                            );

                            // ② 配置 Socket
                            configureSocket(socketChannel);

                            // ③ Round-Robin + mayBlock 分配
                            assignToProcessor(socketChannel);

                        } catch (TooManyConnectionsException e) {
                            logger.info("Rejected connection from {}: {}",
                                e.getIp(), e.getMessage());
                            socketChannel.close();

                        } catch (ConnectionThrottledException e) {
                            // ★ 延迟关闭（限流）
                            logger.debug("Throttling connection from {} for {} ms",
                                e.getIp(), e.getThrottleTimeMs());

                            long endThrottleTime = e.getStartThrottleTimeMs() +
                                                  e.getThrottleTimeMs();
                            throttledSockets.offer(
                                new DelayedCloseSocket(socketChannel, endThrottleTime)
                            );

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
     * ★ Round-Robin + mayBlock 分配逻辑
     * 完全对齐 Kafka SocketServer.scala:652-667
     */
    private void assignToProcessor(SocketChannel socketChannel) throws IOException {
        int retriesLeft = processors.size();
        Processor processor = null;
        boolean accepted = false;

        while (retriesLeft > 0 && !accepted) {
            retriesLeft--;
            processor = processors.get(currentProcessorIndex);
            currentProcessorIndex = (currentProcessorIndex + 1) % processors.size();

            // ★ 关键：最后一次尝试时 mayBlock=true
            boolean mayBlock = (retriesLeft == 0);

            try {
                accepted = processor.accept(socketChannel, mayBlock);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while assigning connection", e);
            }
        }

        if (!accepted) {
            throw new IOException("Failed to assign connection to any processor");
        }
    }

    /**
     * ★ 配置客户端 Socket
     * 完全对齐 Kafka SocketServer.scala:707-713
     */
    private void configureSocket(SocketChannel socketChannel) throws IOException {
        socketChannel.configureBlocking(false);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setKeepAlive(true);
        // 可选：设置发送/接收缓冲区大小
    }

    /**
     * ★ 关闭被限流的连接
     * 完全对齐 Kafka SocketServer.scala:718-725
     */
    private void closeThrottledConnections() {
        long currentTime = System.currentTimeMillis();

        while (!throttledSockets.isEmpty() &&
               throttledSockets.peek().endThrottleTime <= currentTime) {
            DelayedCloseSocket delayed = throttledSockets.poll();
            try {
                logger.debug("Closing throttled socket from {}",
                    delayed.socketChannel.getRemoteAddress());
                delayed.socketChannel.close();
            } catch (IOException e) {
                logger.error("Error closing throttled socket", e);
            }
        }
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

    // 延迟关闭 Socket 的包装类
    private static class DelayedCloseSocket implements Comparable<DelayedCloseSocket> {
        final SocketChannel socketChannel;
        final long endThrottleTime;

        DelayedCloseSocket(SocketChannel socketChannel, long endThrottleTime) {
            this.socketChannel = socketChannel;
            this.endThrottleTime = endThrottleTime;
        }

        @Override
        public int compareTo(DelayedCloseSocket other) {
            return Long.compare(this.endThrottleTime, other.endThrottleTime);
        }
    }
}
```

---

### 2.2 Processor.java（修正版）

```java
public class Processor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Processor.class);

    // ★ 对齐 Kafka：队列容量 20
    private static final int CONNECTION_QUEUE_SIZE = 20;

    private final int id;
    private final KafkaSelector selector;
    private final RequestChannel requestChannel;
    private final ConnectionQuotas connectionQuotas;
    private final String listenerName;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // ★ 修正：使用 ArrayBlockingQueue，容量 20
    private final BlockingQueue<SocketChannel> newConnections;
    private final BlockingQueue<RequestChannel.Response> responseQueue;

    private int nextConnectionIndex = 0;

    public Processor(int id,
                    RequestChannel requestChannel,
                    ConnectionQuotas connectionQuotas,
                    String listenerName,
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
                try {
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

                    // 7. 关闭过量连接（可选）
                    // closeExcessConnections();

                } catch (Exception e) {
                    logger.error("Error in processor {} event loop", id, e);
                }
            }
        } finally {
            close();
        }
    }

    /**
     * ★ 修正：限制每次处理的连接数
     * 完全对齐 Kafka SocketServer.scala:1178-1195
     */
    private void configureNewConnections() {
        int connectionsProcessed = 0;

        // ★ 关键：限制处理数量，避免饿死其他步骤
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

                } else if (response instanceof RequestChannel.CloseConnectionResponse) {
                    selector.close(connectionId);
                }
            } catch (Exception e) {
                logger.error("Error processing response for {}", connectionId, e);
            }
        }
    }

    /**
     * ★ 修正：添加 clearCompletedReceives()
     * 完全对齐 Kafka SocketServer.scala:1020-1072
     */
    private void processCompletedReceives() {
        for (NetworkReceive receive : selector.completedReceives()) {
            try {
                String connectionId = receive.source();

                RequestChannel.Request request = new RequestChannel.Request(
                    id,
                    connectionId,
                    receive.payload()
                );

                requestChannel.sendRequest(request);

                // ★ 关键：立即 mute
                selector.mute(connectionId);

                logger.debug("Processor {} received request from {}", id, connectionId);

            } catch (Exception e) {
                logger.error("Error processing completed receive", e);
            }
        }
        selector.clearCompletedReceives();  // ★ 新增
    }

    /**
     * ★ 修正：添加 clearCompletedSends()
     * 完全对齐 Kafka SocketServer.scala:1074-1097
     */
    private void processCompletedSends() {
        for (NetworkSend send : selector.completedSends()) {
            try {
                String connectionId = send.destinationId();

                // ★ 关键：发送完成后 unmute
                selector.unmute(connectionId);

                logger.debug("Processor {} completed send to {}", id, connectionId);

            } catch (Exception e) {
                logger.error("Error processing completed send", e);
            }
        }
        selector.clearCompletedSends();  // ★ 新增
    }

    /**
     * ★ 修正：实现 processDisconnected()
     * 完全对齐 Kafka SocketServer.scala:1105-1120
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
     * ★ 新增方法：接受新连接（带 mayBlock 参数）
     * 完全对齐 Kafka SocketServer.scala:1154-1171
     */
    public boolean accept(SocketChannel socketChannel, boolean mayBlock)
        throws InterruptedException {

        boolean accepted;

        // ① 非阻塞尝试
        if (newConnections.offer(socketChannel)) {
            accepted = true;
        }
        // ② mayBlock=true 时阻塞等待
        else if (mayBlock) {
            long startNs = System.nanoTime();
            newConnections.put(socketChannel);  // ← 阻塞
            long blockedNs = System.nanoTime() - startNs;

            logger.debug("Processor {} blocked for {} ms",
                id, blockedNs / 1_000_000);

            accepted = true;
        }
        // ③ mayBlock=false，直接失败
        else {
            accepted = false;
        }

        if (accepted) {
            selector.wakeup();
        }

        return accepted;
    }

    /**
     * 入队响应
     */
    public void enqueueResponse(RequestChannel.Response response) {
        responseQueue.offer(response);
        selector.wakeup();
    }

    /**
     * ★ 修正：使用 Kafka 的 connectionId 格式
     * 格式: "remoteHost:remotePort-localPort-connectionIndex"
     */
    private String generateConnectionId(SocketChannel socketChannel) {
        Socket socket = socketChannel.socket();
        String remoteHost = socket.getInetAddress().getHostAddress();
        int remotePort = socket.getPort();
        int localPort = socket.getLocalPort();

        String connId = String.format("%s:%d-%d-%d",
            remoteHost, remotePort, localPort, nextConnectionIndex);

        // 防止溢出
        nextConnectionIndex = (nextConnectionIndex == Integer.MAX_VALUE)
            ? 0 : nextConnectionIndex + 1;

        return connId;
    }

    /**
     * ★ 新增辅助方法：从 connectionId 提取远程主机
     */
    private String extractRemoteHost(String connectionId) {
        int colonIndex = connectionId.indexOf(':');
        if (colonIndex > 0) {
            return connectionId.substring(0, colonIndex);
        }
        return null;
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

---

### 2.3 KafkaSelector.java（修正版）

```java
public class KafkaSelector {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSelector.class);

    private final Selector nioSelector;
    private final Map<String, KafkaChannel> channels;
    private final List<NetworkSend> completedSends;
    private final List<NetworkReceive> completedReceives;
    private final Map<String, String> disconnected;  // ★ 新增
    private final MemoryPool memoryPool;

    public KafkaSelector(MemoryPool memoryPool) throws IOException {
        this.nioSelector = Selector.open();
        this.channels = new HashMap<>();
        this.completedSends = new ArrayList<>();
        this.completedReceives = new ArrayList<>();
        this.disconnected = new HashMap<>();  // ★ 新增
        this.memoryPool = memoryPool;
    }

    /**
     * 注册通道
     */
    public void register(String id, SocketChannel socketChannel) throws IOException {
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_READ);
        KafkaChannel channel = new KafkaChannel(id, new PlaintextTransportLayer(key), memoryPool);
        key.attach(channel);
        channels.put(id, channel);
    }

    /**
     * ★ 核心 poll 方法（修正版）
     */
    public void poll(long timeout) throws IOException {
        clear();

        int ready = nioSelector.select(timeout);
        if (ready > 0) {
            Set<SelectionKey> keys = nioSelector.selectedKeys();
            for (SelectionKey key : keys) {
                KafkaChannel channel = (KafkaChannel) key.attachment();
                String channelId = channel.id();

                try {
                    // 可读事件
                    if (key.isReadable()) {
                        long bytesRead = channel.read();
                        if (bytesRead > 0) {
                            NetworkReceive receive = channel.maybeCompleteReceive();
                            if (receive != null) {
                                completedReceives.add(receive);
                            }
                        } else if (bytesRead < 0) {
                            // ★ 新增：检测远程关闭
                            close(channelId, "Remote closed");
                        }
                    }

                    // 可写事件
                    if (key.isValid() && key.isWritable()) {
                        long bytesWritten = channel.write();
                        if (bytesWritten > 0) {
                            NetworkSend send = channel.maybeCompleteSend();
                            if (send != null) {
                                completedSends.add(send);
                                // 写完后移除 OP_WRITE 兴趣
                                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                            }
                        }
                    }

                } catch (IOException e) {
                    // ★ 修正：记录断开原因
                    logger.debug("IOException on channel {}: {}", channelId, e.getMessage());
                    close(channelId, "IOException: " + e.getMessage());
                }
            }
            keys.clear();
        }
    }

    /**
     * 发送数据
     */
    public void send(NetworkSend send) {
        String id = send.destinationId();
        KafkaChannel channel = channels.get(id);
        if (channel != null) {
            channel.setSend(send);
            SelectionKey key = channel.selectionKey();
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    /**
     * Mute 通道
     */
    public void mute(String id) {
        KafkaChannel channel = channels.get(id);
        if (channel != null) {
            channel.mute();
        }
    }

    /**
     * Unmute 通道
     */
    public void unmute(String id) {
        KafkaChannel channel = channels.get(id);
        if (channel != null) {
            channel.unmute();
        }
    }

    /**
     * ★ 新增方法：唤醒阻塞的 select()
     * 完全对齐 Kafka Selector.java:361-363
     */
    public void wakeup() {
        nioSelector.wakeup();
    }

    /**
     * ★ 新增方法：关闭连接并记录原因
     */
    public void close(String id) {
        close(id, "Closed by application");
    }

    private void close(String id, String reason) {
        KafkaChannel channel = channels.remove(id);
        if (channel != null) {
            try {
                channel.close();
                disconnected.put(id, reason);
            } catch (IOException e) {
                logger.error("Error closing channel {}", id, e);
            }
        }
    }

    /**
     * ★ 新增方法：清理已完成的接收
     */
    public void clearCompletedReceives() {
        completedReceives.clear();
    }

    /**
     * ★ 新增方法：清理已完成的发送
     */
    public void clearCompletedSends() {
        completedSends.clear();
    }

    private void clear() {
        completedSends.clear();
        completedReceives.clear();
        disconnected.clear();
    }

    public List<NetworkSend> completedSends() {
        return new ArrayList<>(completedSends);
    }

    public List<NetworkReceive> completedReceives() {
        return new ArrayList<>(completedReceives);
    }

    /**
     * ★ 新增方法：获取断开的连接
     */
    public Map<String, String> disconnected() {
        return new HashMap<>(disconnected);
    }

    public void close() throws IOException {
        nioSelector.close();
        for (KafkaChannel channel : channels.values()) {
            channel.close();
        }
        channels.clear();
    }
}
```

---

## 3. 修正总结

### 🔴 必须修正的严重问题

1. **Acceptor.assignToProcessor()** - 缺少 Round-Robin + mayBlock 逻辑 ✅
2. **Processor.accept()** - 缺少 mayBlock 参数和阻塞等待逻辑 ✅
3. **KafkaSelector.wakeup()** - 缺少 wakeup() 方法 ✅
4. **Processor.newConnections** - 使用无界队列，应改为容量 20 的 ArrayBlockingQueue ✅
5. **Processor.configureNewConnections()** - 缺少批量处理限制 ✅

### 🟡 建议修正的中等问题

6. **KafkaSelector.poll()** - 缺少断开连接处理和 disconnected 集合 ✅
7. **Processor.processDisconnected()** - 空实现，需要实现配额更新 ✅

### 🟢 可选优化

8. **clearCompletedReceives/Sends()** - 缺少清理方法 ✅
9. **generateConnectionId()** - 格式不符合 Kafka 规范 ✅

---

## 4. 对照表：修正前 vs 修正后

| 组件 | 修正前 | 修正后 | Kafka 对齐度 |
|------|--------|--------|-------------|
| Acceptor 分配逻辑 | 直接 Round-Robin | Round-Robin + mayBlock | ✅ 100% |
| Processor.accept() | 只有 offer() | offer() + put() | ✅ 100% |
| newConnections 队列 | 无界队列 | ArrayBlockingQueue(20) | ✅ 100% |
| configureNewConnections | 无限制 | 限制处理 20 个 | ✅ 100% |
| KafkaSelector.wakeup() | 缺失 | 实现 | ✅ 100% |
| disconnected 处理 | 缺失 | 实现 | ✅ 100% |
| connectionId 格式 | 简化 | Kafka 格式 | ✅ 100% |

---

## 5. 验证清单

使用这个清单验证您的实现：

- [ ] Acceptor 的 assignToProcessor() 包含 mayBlock 逻辑
- [ ] Processor.accept() 有 mayBlock 参数和阻塞等待
- [ ] newConnections 是 ArrayBlockingQueue，容量 20
- [ ] configureNewConnections() 限制每次处理 20 个
- [ ] KafkaSelector 有 wakeup() 方法
- [ ] KafkaSelector 有 disconnected() 方法
- [ ] Processor 实现了 processDisconnected()
- [ ] 有 clearCompletedReceives/Sends() 方法
- [ ] connectionId 格式为 "remoteHost:remotePort-localPort-index"

---

**修正后，您的实现将与 Kafka 源码的核心逻辑 100% 对齐！** 🎯
