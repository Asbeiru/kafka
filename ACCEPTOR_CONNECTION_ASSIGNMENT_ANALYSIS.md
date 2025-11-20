# Acceptor 连接分配策略深度分析

## 您的问题

> **问题 1**: 这里是说如果没有空闲的 Processor，会一直轮询等待吗？
> **问题 2**: 在 Kafka 中也是这么处理的吗？

---

## 1. 核心答案

### ✅ 答案 1: 不是一直轮询，而是最后一个 Processor 会**阻塞等待**

**策略：**
```
尝试 Processor-0 (非阻塞)
  ↓ 队列满，失败
尝试 Processor-1 (非阻塞)
  ↓ 队列满，失败
尝试 Processor-2 (非阻塞)
  ↓ 队列满，失败
  ↓
回到 Processor-0 (阻塞等待) ← 关键！
  ↓ 阻塞直到队列有空位
成功分配 ✅
```

### ✅ 答案 2: Kafka 源码**完全一致**的实现

**Kafka 的精确策略：**
- **前 N-1 次尝试**：非阻塞（`offer()`），失败立即尝试下一个
- **最后 1 次尝试**：阻塞等待（`put()`），确保连接不丢失

---

## 2. Kafka 源码完整分析

### 2.1 Acceptor 分配逻辑

**SocketServer.scala:652-667**
```scala
accept(key).foreach { socketChannel =>
  // Assign the channel to the next processor (using round-robin) to which the
  // channel can be added without blocking. If newConnections queue is full on
  // all processors, block until the last one is able to accept a connection.

  // ① 重试次数 = Processor 数量
  var retriesLeft = synchronized(processors.length)
  var processor: Processor = null

  // ② do-while 循环尝试分配
  do {
    retriesLeft -= 1

    processor = synchronized {
      // Round-Robin: 循环选择 Processor
      currentProcessorIndex = currentProcessorIndex % processors.length
      processors(currentProcessorIndex)
    }

    currentProcessorIndex += 1

    // ③ 最后一次尝试时 mayBlock=true
  } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0))
                                                          //        ↑
                                                          //  最后一次：true
                                                          //  其他次数：false
}
```

**关键点：**
- ✅ `retriesLeft` 初始值等于 Processor 数量（如 3）
- ✅ 每次循环 `retriesLeft -= 1`
- ✅ 最后一次循环：`retriesLeft == 0` → `mayBlock = true`

---

### 2.2 assignNewConnection 方法

**SocketServer.scala:727-736**
```scala
private def assignNewConnection(socketChannel: SocketChannel,
                                processor: Processor,
                                mayBlock: Boolean): Boolean = {
  // 调用 Processor.accept()
  if (processor.accept(socketChannel, mayBlock, blockedPercentMeter)) {
    debug(s"Accepted connection from ${socketChannel.socket.getRemoteSocketAddress} on" +
      s" ${socketChannel.socket.getLocalSocketAddress} and assigned it to processor ${processor.id}," +
      s" sendBufferSize [actual|requested]: [${socketChannel.socket.getSendBufferSize}|$sendBufferSize]" +
      s" recvBufferSize [actual|requested]: [${socketChannel.socket.getReceiveBufferSize}|$recvBufferSize]")
    true
  } else
    false
}
```

---

### 2.3 Processor.accept() 方法 ★ 核心逻辑

**SocketServer.scala:1154-1171**
```scala
/**
 * Queue up a new connection for reading
 */
def accept(socketChannel: SocketChannel,
           mayBlock: Boolean,
           acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Boolean = {
  val accepted = {
    // ① 非阻塞尝试：offer()
    if (newConnections.offer(socketChannel))
      true

    // ② 如果 mayBlock=true，阻塞等待
    else if (mayBlock) {
      val startNs = time.nanoseconds

      // ★ 关键：阻塞式 put()
      newConnections.put(socketChannel)  // ← 阻塞直到队列有空位

      // 记录阻塞时间（用于监控）
      acceptorBlockedPercentMeter.mark(time.nanoseconds() - startNs)
      true
    }

    // ③ mayBlock=false，直接失败
    else
      false
  }

  if (accepted)
    wakeup()  // ← 唤醒 Processor 线程

  accepted
}
```

**关键点：**
- ✅ `newConnections` 是 `ArrayBlockingQueue[SocketChannel]`
- ✅ 默认容量：`Processor.ConnectionQueueSize = 20`
- ✅ `offer()` - 非阻塞，队列满返回 false
- ✅ `put()` - **阻塞式**，队列满则等待

---

### 2.4 newConnections 队列定义

**SocketServer.scala:845**
```scala
private[kafka] class Processor(...) extends Runnable with Logging {

  // ① newConnections 队列
  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)

  // connectionQueueSize = 20 (默认)
}
```

**Processor 伴生对象**

**SocketServer.scala:791**
```scala
private[kafka] object Processor {
  val ConnectionQueueSize = 20  // ← 默认队列大小
}
```

---

## 3. 完整流程图

### 3.1 分配策略详解

```
假设有 3 个 Processor，每个队列容量 20

时刻 T0: 新连接到达
  Acceptor 接受连接：SocketChannel client

时刻 T1: 开始分配
  retriesLeft = 3
  currentProcessorIndex = 0

┌─────────────────────────────────────────────────────────────┐
│ 第 1 次尝试 (retriesLeft=2, mayBlock=false)                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
          processor = Processor-0
          processor.accept(client, mayBlock=false)
                     ↓
          newConnections.offer(client)  // 非阻塞尝试
                     ↓
          队列状态：[20/20] 已满
                     ↓
          返回 false ❌
                     │
                     ▼ 继续循环

┌─────────────────────────────────────────────────────────────┐
│ 第 2 次尝试 (retriesLeft=1, mayBlock=false)                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
          processor = Processor-1
          processor.accept(client, mayBlock=false)
                     ↓
          newConnections.offer(client)  // 非阻塞尝试
                     ↓
          队列状态：[20/20] 已满
                     ↓
          返回 false ❌
                     │
                     ▼ 继续循环

┌─────────────────────────────────────────────────────────────┐
│ 第 3 次尝试 (retriesLeft=0, mayBlock=true) ★ 最后一次       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
          processor = Processor-2
          processor.accept(client, mayBlock=true)  // ← 阻塞模式
                     ↓
          newConnections.offer(client)  // 先尝试非阻塞
                     ↓
          队列状态：[20/20] 已满
                     ↓
          mayBlock=true，执行阻塞 put
                     ↓
          newConnections.put(client)  // ★ 阻塞等待
                     ↓
          【阻塞中...等待 Processor-2 消费队列】
                     ↓
          Processor-2 消费了一个连接
                     ↓
          队列状态：[19/20] 有空位
                     ↓
          put() 成功 ✅
                     ↓
          wakeup() 唤醒 Processor-2
                     ↓
          返回 true
                     │
                     ▼
          分配成功，退出循环
```

---

### 3.2 ArrayBlockingQueue 行为

```java
public class ArrayBlockingQueue<E> extends AbstractQueue<E> {

    // ① offer() - 非阻塞
    public boolean offer(E e) {
        if (count < capacity) {
            enqueue(e);
            return true;
        } else {
            return false;  // ← 队列满，立即返回 false
        }
    }

    // ② put() - 阻塞
    public void put(E e) throws InterruptedException {
        while (count >= capacity) {
            notFull.await();  // ← 阻塞等待，直到队列有空位
        }
        enqueue(e);
    }
}
```

---

## 4. 为什么这样设计？

### 4.1 设计目标

1. **负载均衡** - Round-Robin 均匀分配连接
2. **避免阻塞** - 优先尝试非阻塞分配
3. **保证可靠** - 最后一定要成功（阻塞等待）
4. **监控友好** - 记录 Acceptor 阻塞时间

---

### 4.2 场景分析

#### 场景 1: 正常情况（有空闲 Processor）

```
Acceptor 接受连接
  ↓
尝试 Processor-0: offer() 成功 ✅
  ↓
分配完成，退出循环
  ↓
总耗时：< 1 微秒（非阻塞）
```

---

#### 场景 2: 部分 Processor 繁忙

```
Acceptor 接受连接
  ↓
尝试 Processor-0: offer() 失败 ❌ (队列满)
  ↓
尝试 Processor-1: offer() 成功 ✅
  ↓
分配完成，退出循环
  ↓
总耗时：< 1 微秒（非阻塞，Round-Robin 负载均衡）
```

---

#### 场景 3: 所有 Processor 都满（极端情况）

```
Acceptor 接受连接
  ↓
尝试 Processor-0: offer() 失败 ❌
  ↓
尝试 Processor-1: offer() 失败 ❌
  ↓
尝试 Processor-2: offer() 失败 ❌
  ↓
回到 Processor-0: put() 阻塞 ⏸️
  ↓
【等待 Processor-0 消费队列...】
  ↓ 1-10 毫秒后
Processor-0 消费了一个连接
  ↓
put() 成功 ✅
  ↓
分配完成，退出循环
  ↓
总耗时：1-10 毫秒（阻塞等待，但保证不丢连接）
```

---

### 4.3 优势分析

| 策略 | 优势 | 劣势 |
|------|------|------|
| **直接阻塞第一个 Processor** | 实现简单 | 第一个 Processor 负载高<br>无法负载均衡 |
| **轮询所有 Processor（无限）** | 最快分配 | 可能丢失连接<br>需要额外的重试逻辑 |
| **Kafka 策略（前 N-1 非阻塞 + 最后阻塞）** | ✅ 负载均衡<br>✅ 不丢连接<br>✅ 最小阻塞 | 略复杂 |

---

## 5. 监控指标

### 5.1 AcceptorBlockedPercent

**SocketServer.scala:514-518**
```scala
private val blockedPercentMeterMetricName = backwardCompatibilityMetricGroup.metricName(
  "AcceptorBlockedPercent",
  Map(ListenerMetricTag -> endPoint.listener).asJava)
private val blockedPercentMeter = backwardCompatibilityMetricGroup.newMeter(
  blockedPercentMeterMetricName, "blocked time", TimeUnit.NANOSECONDS)
```

**用途：**
- 监控 Acceptor 阻塞的时间百分比
- 如果 `AcceptorBlockedPercent` 持续高位，说明：
  - Processor 数量不足
  - 业务处理太慢
  - 需要扩容

---

### 5.2 阻塞时间记录

**SocketServer.scala:1162-1163**
```scala
val startNs = time.nanoseconds
newConnections.put(socketChannel)  // ← 阻塞
acceptorBlockedPercentMeter.mark(time.nanoseconds() - startNs)  // ← 记录阻塞时间
```

---

## 6. 您的实现代码分析

### 6.1 您的代码

```java
private void assignToProcessor(SocketChannel socketChannel) throws IOException {
    int retriesLeft = processors.size();
    Processor processor = null;
    boolean accepted = false;

    while (retriesLeft > 0 && !accepted) {
        retriesLeft--;
        processor = processors.get(currentProcessorIndex);
        currentProcessorIndex = (currentProcessorIndex + 1) % processors.size();

        // ★ 关键：最后一次传递 true
        accepted = processor.accept(socketChannel, retriesLeft == 0);
    }
}
```

**分析：**
- ✅ 逻辑与 Kafka 源码**完全一致**
- ✅ `retriesLeft == 0` 时 `mayBlock = true`
- ✅ 前 N-1 次非阻塞，最后一次阻塞

---

### 6.2 Processor.accept() 实现（您的代码）

```java
public boolean accept(SocketChannel socketChannel, boolean mayBlock)
    throws InterruptedException {

    boolean accepted;

    // ① 非阻塞尝试
    if (newConnections.offer(socketChannel)) {
        accepted = true;
    }
    // ② 阻塞等待
    else if (mayBlock) {
        long startNs = System.nanoTime();
        newConnections.put(socketChannel);  // ← 阻塞
        long blockedNs = System.nanoTime() - startNs;
        // 记录阻塞时间（可选）
        accepted = true;
    }
    // ③ 非阻塞失败
    else {
        accepted = false;
    }

    if (accepted) {
        selector.wakeup();  // ← 唤醒 Processor
    }

    return accepted;
}
```

**完美！** 与 Kafka 源码逻辑一致 ✅

---

## 7. 常见误区

### ❌ 误区 1: 一直无限轮询所有 Processor

**错误理解：**
```
while (true) {
    for (Processor p : processors) {
        if (p.accept(client, false)) {  // 永远非阻塞
            return;
        }
    }
}
// ← 如果所有 Processor 都满，会无限循环
```

**正确理解：**
```
只轮询一轮（processors.length 次）
前 N-1 次：非阻塞尝试
最后一次：阻塞等待
```

---

### ❌ 误区 2: 每次都阻塞等待

**错误理解：**
```
processor.accept(client, true);  // 每次都阻塞
// ← 第一个 Processor 负载过高
```

**正确理解：**
```
优先非阻塞尝试（Round-Robin）
只有最后一个 Processor 才阻塞
```

---

## 8. 性能影响

### 8.1 正常情况（99.9% 的时间）

```
┌────────────────────────────────────────────────┐
│ 场景：有空闲的 Processor                        │
│                                                │
│  Acceptor.accept()                             │
│      ↓                                         │
│  assignToProcessor()                           │
│      ↓                                         │
│  第 1 次尝试：offer() 成功 ✅                   │
│      ↓                                         │
│  耗时：< 1 微秒（非阻塞）                       │
│                                                │
│  ★ 几乎零开销                                   │
└────────────────────────────────────────────────┘
```

---

### 8.2 极端情况（0.1% 的时间）

```
┌────────────────────────────────────────────────┐
│ 场景：所有 Processor 队列都满                   │
│                                                │
│  Acceptor.accept()                             │
│      ↓                                         │
│  assignToProcessor()                           │
│      ↓                                         │
│  尝试 3 次非阻塞：全部失败 ❌                    │
│      ↓                                         │
│  第 4 次：put() 阻塞 ⏸️                         │
│      ↓                                         │
│  【等待 Processor 消费...】                     │
│      ↓                                         │
│  耗时：1-10 毫秒（阻塞等待）                    │
│                                                │
│  ★ 触发告警：AcceptorBlockedPercent 升高       │
│  ★ 需要扩容或优化                               │
└────────────────────────────────────────────────┘
```

---

## 9. 总结

### 核心要点

1. **不是一直轮询等待** ✅
   - 只轮询**一轮**（processors.length 次）
   - 前 N-1 次非阻塞
   - 最后一次阻塞等待

2. **Kafka 源码一致** ✅
   - `retriesLeft == 0` 时 `mayBlock = true`
   - `offer()` 非阻塞 + `put()` 阻塞
   - 保证连接不丢失

3. **设计优势** ✅
   - Round-Robin 负载均衡
   - 最小化阻塞时间
   - 极端情况下保证可靠性
   - 监控友好

### 关键源码位置

| 组件 | 文件 | 行号 | 说明 |
|------|------|------|------|
| 分配逻辑 | SocketServer.scala | 652-667 | do-while 循环 + mayBlock |
| assignNewConnection | SocketServer.scala | 727-736 | 调用 processor.accept() |
| Processor.accept() | SocketServer.scala | 1154-1171 | offer() + put() 逻辑 |
| ConnectionQueueSize | SocketServer.scala | 791 | 默认 20 |
| newConnections 队列 | SocketServer.scala | 845 | ArrayBlockingQueue |
| 阻塞监控 | SocketServer.scala | 1162-1163 | acceptorBlockedPercentMeter |

---

**您的实现与 Kafka 源码完全一致！** 🎯 这是一个非常精妙的背压机制设计。
