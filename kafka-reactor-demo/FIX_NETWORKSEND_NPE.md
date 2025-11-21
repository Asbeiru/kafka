# 修复 NetworkSend NullPointerException 问题

## 问题描述

当客户端发送消息时，服务端在处理写完成事件时报 `NullPointerException`：

```
java.lang.NullPointerException: null
	at com.zhouzhou.transport.NetworkSend.<init>(NetworkSend.java:28)
	at com.zhouzhou.selector.KafkaSelector.poll(KafkaSelector.java:126)
```

## 根本原因

在原实现中，`KafkaSelector` 的 `write()` 方法在发送完成后，尝试创建新的 `NetworkSend` 对象并传入 `null`：

```java
// 错误的实现
if (!channel.hasSend()) {
    completedSends.add(new NetworkSend(channel.id(), null));  // ← 传入 null!
    // ...
}
```

然后 `NetworkSend` 构造函数尝试访问 `data.remaining()`，导致 NPE：

```java
public NetworkSend(String destination, ByteBuffer data) {
    sizeBuffer.putInt(data.remaining());  // ← data 是 null，抛出 NPE!
}
```

## 解决方案 - 完全对齐 Kafka 源码

参考 Kafka 源码的实现方式：

### Kafka 源码参考

**Selector.java:445-458**
```java
private void write(KafkaChannel channel) throws IOException {
    String nodeId = channel.id();
    long bytesSent = channel.write();
    NetworkSend send = channel.maybeCompleteSend();  // ← 调用 maybeCompleteSend()
    if (send != null) {
        this.completedSends.add(send);  // ← 添加实际的 send 对象
        this.sensors.recordBytesSent(nodeId, bytesSent);
    }
}
```

**KafkaChannel.java:245-253**
```java
public NetworkSend maybeCompleteSend() {
    if (send != null && send.completed()) {
        NetworkSend result = send;
        send = null;  // 清空当前 send
        return result;  // 返回已完成的 send
    }
    return null;
}
```

## 修改内容

### 1. KafkaChannel.java

**新增方法：`maybeCompleteSend()`**

```java
/**
 * Returns the send that has been completed, or null if the send has not been completed.
 * The send is only cleared from the channel once this method is invoked.
 *
 * Aligns with Kafka's KafkaChannel.maybeCompleteSend() (KafkaChannel.java:245-253)
 */
public NetworkSend maybeCompleteSend() {
    if (send != null && send.completed()) {
        NetworkSend result = send;
        send = null;  // Clear the send

        // Remove OP_WRITE interest
        SelectionKey key = selectionKey();
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }

        return result;
    }
    return null;
}
```

**新增方法：`hasSend()`**

```java
/**
 * Returns true if there is a send in progress.
 */
public boolean hasSend() {
    return send != null;
}
```

**修改方法：`clearSend()`**

将 `clearSend()` 改为 `private`，因为现在通过 `maybeCompleteSend()` 来管理 send 的生命周期：

```java
/**
 * Clear the send and remove OP_WRITE interest.
 * This method is used during channel close.
 */
private void clearSend() {
    send = null;
    SelectionKey key = selectionKey();
    if (key != null && key.isValid()) {
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }
}
```

### 2. KafkaSelector.java

**修改方法：`write()`**

```java
/**
 * Write to a channel.
 * Aligns with Kafka's Selector.write() (Selector.java:445-458)
 */
private void write(KafkaChannel channel) throws IOException {
    String nodeId = channel.id();
    long bytesSent = channel.write();

    // Get completed send if any
    NetworkSend send = channel.maybeCompleteSend();

    // We may complete the send with bytesSent < 1 if `TransportLayer.hasPendingWrites` was true
    if (send != null) {
        completedSends.add(send);
        log.debug("Completed send to {}: {} bytes", nodeId, send.size());
    }
}
```

**关键变化：**
- 调用 `channel.maybeCompleteSend()` 而不是 `channel.currentSend()` + `channel.clearSend()`
- 使用实际完成的 `NetworkSend` 对象，而不是创建新的 `new NetworkSend(id, null)`

### 3. NetworkSend.java

**无需修改** - 保持原样，不需要处理 `null` 参数。

### 4. Processor.java

**无需修改** - 已经正确使用 `selector.completedSends()`。

## 修改总结

| 文件 | 修改类型 | 说明 |
|------|----------|------|
| KafkaChannel.java | 新增方法 | `maybeCompleteSend()` - 返回并清空已完成的 send |
| KafkaChannel.java | 新增方法 | `hasSend()` - 检查是否有 send 在进行中 |
| KafkaChannel.java | 方法可见性 | `clearSend()` 改为 private |
| KafkaSelector.java | 方法重写 | `write()` - 使用 `maybeCompleteSend()` 获取完成的 send |
| NetworkSend.java | 无修改 | 保持不变 |
| Processor.java | 无修改 | 保持不变 |

## 对齐验证

✅ **完全对齐 Kafka 源码实现**

- `KafkaChannel.maybeCompleteSend()` ← 对齐 `KafkaChannel.java:245-253`
- `KafkaSelector.write()` ← 对齐 `Selector.java:445-458`
- 使用实际的 `NetworkSend` 对象，不创建 `null` 参数的新对象

## 测试验证

修复后，运行服务端和客户端，应该能正常收发消息：

**预期日志输出：**
```
15:35:52.468 [kafka-request-handler-2] INFO  RequestHandler - Processing: Hello Kafka Reactor!
15:35:52.503 [kafka-network-processor-0] DEBUG KafkaSelector - Completed send to 127.0.0.1:9092-...: 35 bytes
15:35:52.504 [kafka-network-processor-0] DEBUG Processor - Processor 0 completed send to 127.0.0.1:9092-...
```

**客户端输出：**
```
Sent: Hello Kafka Reactor!
Received: ECHO: Hello Kafka Reactor!
```

## 参考

- Kafka Selector: `clients/src/main/java/org/apache/kafka/common/network/Selector.java:445-458`
- Kafka KafkaChannel: `clients/src/main/java/org/apache/kafka/common/network/KafkaChannel.java:245-253`
