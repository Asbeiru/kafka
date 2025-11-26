# KRaft 模块化学习指南 - 第二部分

**接上文：模块 5-9**

---

## 模块 5: 批处理系统 (Batching) ⭐⭐

**学习目标：** 理解 Kafka 如何通过批处理提高性能

**预计时间：** 2-3 周

**核心思想：** 将多个小的写入请求合并成一个大的批次，减少网络开销和磁盘 I/O

### 5.1 核心类列表

| 文件 | 位置 | 行数 | 用途 |
|------|------|------|------|
| `BatchAccumulator.java` | `raft.internals` | ~350 | 批次累积器 |
| `BatchBuilder.java` | `raft.internals` | ~200 | 批次构建器 |
| `BatchMemoryPool.java` | `raft.internals` | ~150 | 内存池管理 |
| `MemoryBatchReader.java` | `raft.internals` | ~100 | 内存批次读取器 |
| `RecordsBatchReader.java` | `raft.internals` | ~120 | 记录批次读取器 |
| `RecordsIterator.java` | `raft.internals` | ~80 | 记录迭代器 |

### 5.2 BatchAccumulator 核心实现

**文件：** `org.apache.kafka.raft.internals.BatchAccumulator`

**核心职责：**
1. 累积多个写入请求
2. 在达到阈值或超时时创建批次
3. 管理内存使用

**核心字段和方法：**

```java
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.record.MemoryRecords;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 批次累积器
 *
 * 负责将多个小的写入请求累积成一个大批次。
 *
 * 工作流程：
 * 1. 应用调用 append() 追加记录
 * 2. 累积器将记录添加到当前批次
 * 3. 当批次大小达到阈值或超时时，返回批次
 * 4. 批次被写入日志
 */
public class BatchAccumulator<T> implements Closeable {

    // 锁，保护并发访问
    private final ReentrantLock lock = new ReentrantLock();

    // 当前正在累积的批次
    private BatchBuilder<T> currentBatch;

    // 已完成的批次队列
    private final Deque<CompletedBatch<T>> completedBatches;

    // 内存池
    private final BatchMemoryPool memoryPool;

    // 配置参数
    private final int lingerMs;        // 等待时间
    private final int maxBatchSize;    // 最大批次大小

    // 当前的 Leader Epoch
    private int leaderEpoch;

    // 日志结束偏移量
    private long logEndOffset;

    /**
     * CompletedBatch - 已完成的批次
     */
    static class CompletedBatch<T> {
        final long baseOffset;
        final int epoch;
        final List<T> records;
        final MemoryRecords data;
        final long appendTimestamp;

        CompletedBatch(
            long baseOffset,
            int epoch,
            List<T> records,
            MemoryRecords data,
            long appendTimestamp
        ) {
            this.baseOffset = baseOffset;
            this.epoch = epoch;
            this.records = records;
            this.data = data;
            this.appendTimestamp = appendTimestamp;
        }
    }

    public BatchAccumulator(
        int leaderEpoch,
        long logEndOffset,
        int lingerMs,
        int maxBatchSize,
        BatchMemoryPool memoryPool
    ) {
        this.leaderEpoch = leaderEpoch;
        this.logEndOffset = logEndOffset;
        this.lingerMs = lingerMs;
        this.maxBatchSize = maxBatchSize;
        this.memoryPool = memoryPool;
        this.completedBatches = new ArrayDeque<>();
        this.currentBatch = null;
    }

    /**
     * 追加记录（核心方法）
     *
     * @param records 要追加的记录
     * @param currentTimeMs 当前时间
     * @return 偏移量（如果批次已完成）
     */
    public long append(List<T> records, long currentTimeMs) {
        lock.lock();
        try {
            // 1. 如果当前没有批次，创建新批次
            if (currentBatch == null) {
                currentBatch = new BatchBuilder<>(
                    leaderEpoch,
                    logEndOffset,
                    currentTimeMs,
                    maxBatchSize,
                    memoryPool
                );
            }

            // 2. 追加记录到当前批次
            boolean appended = currentBatch.append(records);

            if (!appended) {
                // 当前批次已满，完成它并创建新批次
                completeBatch(currentTimeMs);

                // 创建新批次并重试
                currentBatch = new BatchBuilder<>(
                    leaderEpoch,
                    logEndOffset,
                    currentTimeMs,
                    maxBatchSize,
                    memoryPool
                );

                appended = currentBatch.append(records);
                if (!appended) {
                    throw new IllegalStateException("Record too large for batch");
                }
            }

            // 3. 检查是否需要完成批次
            if (shouldCompleteBatch(currentTimeMs)) {
                completeBatch(currentTimeMs);
            }

            return logEndOffset;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 完成当前批次
     */
    private void completeBatch(long currentTimeMs) {
        if (currentBatch == null) {
            return;
        }

        // 构建 MemoryRecords
        MemoryRecords records = currentBatch.build();

        // 创建 CompletedBatch
        CompletedBatch<T> completed = new CompletedBatch<>(
            logEndOffset,
            leaderEpoch,
            currentBatch.records(),
            records,
            currentTimeMs
        );

        // 添加到完成队列
        completedBatches.add(completed);

        // 更新日志结束偏移量
        logEndOffset += currentBatch.recordCount();

        // 清空当前批次
        currentBatch = null;
    }

    /**
     * 判断是否应该完成批次
     */
    private boolean shouldCompleteBatch(long currentTimeMs) {
        if (currentBatch == null) {
            return false;
        }

        // 条件1: 批次已满
        if (currentBatch.isFull()) {
            return true;
        }

        // 条件2: 等待时间已到
        long elapsedMs = currentTimeMs - currentBatch.createdTimeMs();
        if (elapsedMs >= lingerMs) {
            return true;
        }

        return false;
    }

    /**
     * 轮询已完成的批次
     */
    public List<CompletedBatch<T>> drain() {
        lock.lock();
        try {
            if (completedBatches.isEmpty()) {
                return Collections.emptyList();
            }

            List<CompletedBatch<T>> drained = new ArrayList<>(completedBatches);
            completedBatches.clear();
            return drained;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 强制刷新当前批次
     */
    public void forceDrain(long currentTimeMs) {
        lock.lock();
        try {
            if (currentBatch != null) {
                completeBatch(currentTimeMs);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 更新 Leader Epoch（当发生选举时）
     */
    public void updateLeaderEpoch(int newEpoch, long newLogEndOffset) {
        lock.lock();
        try {
            // 清空所有未完成的批次
            if (currentBatch != null) {
                currentBatch.close();
                currentBatch = null;
            }
            completedBatches.clear();

            // 更新状态
            this.leaderEpoch = newEpoch;
            this.logEndOffset = newLogEndOffset;

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (currentBatch != null) {
                currentBatch.close();
            }
            completedBatches.clear();
        } finally {
            lock.unlock();
        }
    }

    public long logEndOffset() {
        lock.lock();
        try {
            return logEndOffset;
        } finally {
            lock.unlock();
        }
    }
}
```

**设计思想：**

1. **批处理优化**：
   - 减少系统调用次数
   - 减少网络往返
   - 提高磁盘写入效率

2. **双重阈值**：
   - 大小阈值：批次达到最大大小时完成
   - 时间阈值：等待时间到达时完成

3. **内存管理**：
   - 使用内存池避免频繁分配
   - 及时释放已完成批次的内存

### 5.3 BatchBuilder 核心实现

**文件：** `org.apache.kafka.raft.internals.BatchBuilder`

```java
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.record.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 批次构建器
 *
 * 负责构建一个单独的批次（MemoryRecords）。
 */
public class BatchBuilder<T> implements Closeable {

    private final int leaderEpoch;
    private final long baseOffset;
    private final long createdTimeMs;
    private final int maxBatchSize;

    // 记录列表
    private final List<T> records;

    // 内存缓冲区（从内存池分配）
    private final ByteBuffer buffer;
    private final MemoryRecordsBuilder recordsBuilder;

    // 序列化器
    private final Serde<T> serde;

    public BatchBuilder(
        int leaderEpoch,
        long baseOffset,
        long createdTimeMs,
        int maxBatchSize,
        BatchMemoryPool memoryPool,
        Serde<T> serde
    ) {
        this.leaderEpoch = leaderEpoch;
        this.baseOffset = baseOffset;
        this.createdTimeMs = createdTimeMs;
        this.maxBatchSize = maxBatchSize;
        this.serde = serde;
        this.records = new ArrayList<>();

        // 从内存池分配缓冲区
        this.buffer = memoryPool.allocate(maxBatchSize);

        // 创建 MemoryRecordsBuilder
        this.recordsBuilder = MemoryRecords.builder(
            buffer,
            RecordBatch.CURRENT_MAGIC_VALUE,
            CompressionType.NONE,
            TimestampType.CREATE_TIME,
            baseOffset,
            createdTimeMs,
            leaderEpoch
        );
    }

    /**
     * 追加记录
     */
    public boolean append(List<T> newRecords) {
        for (T record : newRecords) {
            // 序列化记录
            byte[] serialized = serde.serialize(record);

            // 检查是否还有空间
            if (!recordsBuilder.hasRoomFor(
                createdTimeMs,
                null,  // key
                serialized,  // value
                Record.EMPTY_HEADERS
            )) {
                return false;
            }

            // 追加到构建器
            recordsBuilder.append(
                createdTimeMs,
                null,
                serialized
            );

            // 添加到记录列表
            records.add(record);
        }

        return true;
    }

    /**
     * 构建最终的 MemoryRecords
     */
    public MemoryRecords build() {
        return recordsBuilder.build();
    }

    public boolean isFull() {
        return buffer.remaining() < 128;  // 保留一些空间
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public int recordCount() {
        return records.size();
    }

    public List<T> records() {
        return Collections.unmodifiableList(records);
    }

    @Override
    public void close() {
        // 释放缓冲区回内存池
        buffer.clear();
    }
}
```

### 5.4 BatchMemoryPool 实现

**文件：** `org.apache.kafka.raft.internals.BatchMemoryPool`

```java
package org.apache.kafka.raft.internals;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 批次内存池
 *
 * 管理批次构建所需的内存缓冲区，避免频繁分配和释放。
 */
public class BatchMemoryPool {

    private final int bufferSize;
    private final int maxBuffers;
    private final ArrayBlockingQueue<ByteBuffer> freeBuffers;

    public BatchMemoryPool(int bufferSize, int maxBuffers) {
        this.bufferSize = bufferSize;
        this.maxBuffers = maxBuffers;
        this.freeBuffers = new ArrayBlockingQueue<>(maxBuffers);
    }

    /**
     * 分配缓冲区
     */
    public ByteBuffer allocate(int size) {
        if (size > bufferSize) {
            throw new IllegalArgumentException(
                "Requested size " + size + " exceeds buffer size " + bufferSize
            );
        }

        ByteBuffer buffer = freeBuffers.poll();
        if (buffer == null) {
            // 池中没有可用缓冲区，分配新的
            buffer = ByteBuffer.allocate(bufferSize);
        }

        buffer.clear();
        buffer.limit(size);
        return buffer;
    }

    /**
     * 释放缓冲区
     */
    public void release(ByteBuffer buffer) {
        buffer.clear();

        // 尝试放回池中
        freeBuffers.offer(buffer);
    }

    public int availableBuffers() {
        return freeBuffers.size();
    }
}
```

### 5.5 学习检查清单

- [ ] 理解批处理的性能优势
- [ ] 实现 BatchAccumulator
- [ ] 实现 BatchBuilder
- [ ] 实现内存池管理
- [ ] 理解何时完成批次
- [ ] 测试批处理的正确性

---

## 模块 6: 网络通信 (Networking) ⭐⭐

**学习目标：** 理解 Raft 节点间的网络通信

**预计时间：** 2-3 周

### 6.1 核心类列表

| 文件 | 位置 | 行数 | 用途 |
|------|------|------|------|
| `NetworkChannel.java` | `raft` | ~50 | 网络通道接口 |
| `KafkaNetworkChannel.java` | `raft` | ~400 | Kafka网络实现 |
| `RequestManager.java` | `raft` | ~400 | 请求连接管理 |
| `RaftRequest.java` | `raft` | ~100 | 请求基类 |
| `RaftResponse.java` | `raft` | ~100 | 响应基类 |
| `RaftMessage.java` | `raft` | ~50 | 消息接口 |

### 6.2 RequestManager 核心实现

**文件：** `org.apache.kafka.raft.RequestManager`

**核心职责：**
1. 管理到其他节点的连接
2. 追踪未完成的请求
3. 处理请求超时和重试

**核心实现：**

```java
package org.apache.kafka.raft;

import java.util.*;

/**
 * 请求管理器
 *
 * 管理到所有其他 Raft 节点的连接和请求。
 */
public class RequestManager {

    // 节点 ID -> 连接状态
    private final Map<Integer, ConnectionState> connections;

    // 请求超时时间
    private final int requestTimeoutMs;

    // 重试退避时间
    private final int retryBackoffMs;

    /**
     * ConnectionState - 单个节点的连接状态
     */
    static class ConnectionState {
        final int nodeId;

        // 待发送的请求队列
        final Deque<RaftRequest.Outbound> pendingRequests;

        // 已发送但未响应的请求
        final Map<Integer, InflightRequest> inflightRequests;

        // 连接状态
        boolean isReady;
        long lastConnectAttemptMs;

        ConnectionState(int nodeId) {
            this.nodeId = nodeId;
            this.pendingRequests = new ArrayDeque<>();
            this.inflightRequests = new HashMap<>();
            this.isReady = false;
            this.lastConnectAttemptMs = 0;
        }

        /**
         * 添加请求到待发送队列
         */
        void add(RaftRequest.Outbound request) {
            pendingRequests.add(request);
        }

        /**
         * 获取下一个要发送的请求
         */
        Optional<RaftRequest.Outbound> next() {
            return Optional.ofNullable(pendingRequests.poll());
        }

        /**
         * 标记请求已发送
         */
        void markSent(int correlationId, RaftRequest.Outbound request, long currentTimeMs) {
            inflightRequests.put(
                correlationId,
                new InflightRequest(request, currentTimeMs)
            );
        }

        /**
         * 完成请求
         */
        Optional<InflightRequest> complete(int correlationId) {
            return Optional.ofNullable(inflightRequests.remove(correlationId));
        }

        /**
         * 获取超时的请求
         */
        List<InflightRequest> timeoutRequests(long currentTimeMs, int timeoutMs) {
            List<InflightRequest> timedOut = new ArrayList<>();

            Iterator<Map.Entry<Integer, InflightRequest>> iter =
                inflightRequests.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry<Integer, InflightRequest> entry = iter.next();
                InflightRequest inflight = entry.getValue();

                if (currentTimeMs - inflight.sendTimeMs >= timeoutMs) {
                    timedOut.add(inflight);
                    iter.remove();
                }
            }

            return timedOut;
        }
    }

    /**
     * InflightRequest - 已发送的请求
     */
    static class InflightRequest {
        final RaftRequest.Outbound request;
        final long sendTimeMs;

        InflightRequest(RaftRequest.Outbound request, long sendTimeMs) {
            this.request = request;
            this.sendTimeMs = sendTimeMs;
        }
    }

    public RequestManager(
        Set<Integer> voterIds,
        int requestTimeoutMs,
        int retryBackoffMs
    ) {
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.connections = new HashMap<>();

        // 为每个投票者创建连接状态
        for (int voterId : voterIds) {
            connections.put(voterId, new ConnectionState(voterId));
        }
    }

    /**
     * 发送请求到指定节点
     */
    public void send(int destinationId, RaftRequest.Outbound request) {
        ConnectionState conn = connections.get(destinationId);
        if (conn == null) {
            throw new IllegalArgumentException("Unknown node: " + destinationId);
        }

        conn.add(request);
    }

    /**
     * 轮询可发送的请求
     */
    public List<RaftRequest.Outbound> pollReadyRequests(long currentTimeMs) {
        List<RaftRequest.Outbound> ready = new ArrayList<>();

        for (ConnectionState conn : connections.values()) {
            // 如果连接未就绪，尝试重连
            if (!conn.isReady) {
                if (shouldAttemptConnect(conn, currentTimeMs)) {
                    conn.isReady = true;
                    conn.lastConnectAttemptMs = currentTimeMs;
                }
            }

            // 如果连接就绪，获取待发送请求
            if (conn.isReady) {
                Optional<RaftRequest.Outbound> next = conn.next();
                next.ifPresent(ready::add);
            }
        }

        return ready;
    }

    /**
     * 标记请求已发送
     */
    public void onRequestSent(
        int destinationId,
        int correlationId,
        RaftRequest.Outbound request,
        long currentTimeMs
    ) {
        ConnectionState conn = connections.get(destinationId);
        if (conn != null) {
            conn.markSent(correlationId, request, currentTimeMs);
        }
    }

    /**
     * 处理响应
     */
    public void onResponseReceived(
        int sourceId,
        int correlationId,
        long currentTimeMs
    ) {
        ConnectionState conn = connections.get(sourceId);
        if (conn != null) {
            conn.complete(correlationId);
        }
    }

    /**
     * 处理超时请求
     */
    public List<InflightRequest> handleTimeouts(long currentTimeMs) {
        List<InflightRequest> allTimedOut = new ArrayList<>();

        for (ConnectionState conn : connections.values()) {
            List<InflightRequest> timedOut = conn.timeoutRequests(
                currentTimeMs,
                requestTimeoutMs
            );

            allTimedOut.addAll(timedOut);

            // 如果有超时，标记连接为未就绪
            if (!timedOut.isEmpty()) {
                conn.isReady = false;
            }
        }

        return allTimedOut;
    }

    private boolean shouldAttemptConnect(ConnectionState conn, long currentTimeMs) {
        return currentTimeMs - conn.lastConnectAttemptMs >= retryBackoffMs;
    }
}
```

### 6.3 学习检查清单

- [ ] 理解请求/响应模型
- [ ] 实现 RequestManager
- [ ] 理解连接管理
- [ ] 实现超时处理
- [ ] 实现重试逻辑

---

## 模块 7: 快照管理 (Snapshot) ⭐⭐

**学习目标：** 理解 Raft 快照的作用和实现

**预计时间：** 2-3 周

**核心思想：** 定期创建快照以避免日志无限增长

### 7.1 核心类列表

| 文件 | 位置 | 行数 | 用途 |
|------|------|------|------|
| `SnapshotReader.java` | `snapshot` | ~50 | 快照读取器接口 |
| `SnapshotWriter.java` | `snapshot` | ~50 | 快照写入器接口 |
| `RawSnapshotReader.java` | `snapshot` | ~80 | 原始快照读取器 |
| `RawSnapshotWriter.java` | `snapshot` | ~80 | 原始快照写入器 |
| `FileRawSnapshotReader.java` | `snapshot` | ~150 | 文件读取实现 |
| `FileRawSnapshotWriter.java` | `snapshot` | ~200 | 文件写入实现 |
| `RecordsSnapshotReader.java` | `snapshot` | ~100 | 记录快照读取器 |
| `RecordsSnapshotWriter.java` | `snapshot` | ~120 | 记录快照写入器 |
| `SnapshotPath.java` | `snapshot` | ~100 | 快照路径管理 |
| `Snapshots.java` | `snapshot` | ~150 | 快照工具类 |

### 7.2 快照文件格式

**命名规则：**
```
<offset>-<epoch>-<timestamp>.snapshot

示例：
00000000000000001000-0000000005-1234567890.snapshot
```

**文件内容：**
1. Snapshot Header Record
2. 应用状态数据（多个批次）
3. Snapshot Footer Record

### 7.3 核心接口

```java
package org.apache.kafka.snapshot;

/**
 * 快照读取器接口
 */
public interface SnapshotReader<T> extends Closeable {
    OffsetAndEpoch snapshotId();
    long lastContainedLogOffset();
    int lastContainedLogEpoch();
    long lastContainedLogTimestamp();
    Iterator<Batch<T>> iterator();
}

/**
 * 快照写入器接口
 */
public interface SnapshotWriter<T> extends Closeable {
    OffsetAndEpoch snapshotId();
    long lastContainedLogOffset();
    int lastContainedLogEpoch();
    void append(List<T> records);
    void freeze();
    void close();
}
```

### 7.4 学习检查清单

- [ ] 理解快照的作用
- [ ] 实现快照读取
- [ ] 实现快照写入
- [ ] 理解快照与日志的关系
- [ ] 实现快照加载

---

## 模块 8: 成员变更 (Membership) ⭐⭐

**学习目标：** 理解动态成员变更的实现

**预计时间：** 2-3 周

### 8.1 核心类列表

| 文件 | 位置 | 行数 | 用途 |
|------|------|------|------|
| `VoterSet.java` | `raft` | ~300 | 投票者集合 |
| `AddVoterHandler.java` | `raft.internals` | ~200 | 添加投票者 |
| `RemoveVoterHandler.java` | `raft.internals` | ~200 | 移除投票者 |
| `UpdateVoterHandler.java` | `raft.internals` | ~150 | 更新投票者 |
| `VoterSetHistory.java` | `raft.internals` | ~200 | 投票者历史 |

### 8.2 VoterSet 核心实现

```java
package org.apache.kafka.raft;

import java.util.*;

/**
 * 投票者集合
 *
 * 表示某个时刻的投票者集合。
 */
public class VoterSet {

    private final Map<Integer, VoterNode> voters;

    /**
     * VoterNode - 单个投票者
     */
    public static class VoterNode {
        private final int voterId;
        private final UUID directoryId;
        private final Endpoints endpoints;

        public VoterNode(int voterId, UUID directoryId, Endpoints endpoints) {
            this.voterId = voterId;
            this.directoryId = directoryId;
            this.endpoints = endpoints;
        }

        public int voterId() {
            return voterId;
        }

        public UUID directoryId() {
            return directoryId;
        }

        public Endpoints endpoints() {
            return endpoints;
        }
    }

    public VoterSet(Map<Integer, VoterNode> voters) {
        this.voters = Collections.unmodifiableMap(new HashMap<>(voters));
    }

    public Set<Integer> voterIds() {
        return voters.keySet();
    }

    public boolean isVoter(int nodeId) {
        return voters.containsKey(nodeId);
    }

    public boolean isVoter(ReplicaKey replicaKey) {
        VoterNode voter = voters.get(replicaKey.id());
        return voter != null &&
            voter.directoryId().equals(replicaKey.directoryId());
    }

    public int majority() {
        return voters.size() / 2 + 1;
    }

    public Optional<VoterNode> voter(int voterId) {
        return Optional.ofNullable(voters.get(voterId));
    }

    /**
     * 添加投票者（返回新的 VoterSet）
     */
    public VoterSet addVoter(VoterNode newVoter) {
        Map<Integer, VoterNode> newVoters = new HashMap<>(voters);
        newVoters.put(newVoter.voterId(), newVoter);
        return new VoterSet(newVoters);
    }

    /**
     * 移除投票者（返回新的 VoterSet）
     */
    public VoterSet removeVoter(int voterId) {
        Map<Integer, VoterNode> newVoters = new HashMap<>(voters);
        newVoters.remove(voterId);
        return new VoterSet(newVoters);
    }

    @Override
    public String toString() {
        return "VoterSet(" + voters + ")";
    }
}
```

### 8.3 成员变更流程

```
1. Leader 收到 AddVoter 请求
2. Leader 追加 VotersRecord 到日志
3. 等待 VotersRecord 被提交（多数派确认）
4. 更新本地 VoterSet
5. 开始使用新的多数派计算
```

### 8.4 学习检查清单

- [ ] 理解成员变更的挑战
- [ ] 实现 VoterSet
- [ ] 实现添加投票者
- [ ] 实现移除投票者
- [ ] 理解联合共识（Joint Consensus）

---

## 模块 9: 核心引擎 KafkaRaftClient ⭐⭐⭐

**学习目标：** 整合所有模块，实现完整的 Raft 客户端

**预计时间：** 4-6 周

**文件：** `org.apache.kafka.raft.KafkaRaftClient`
**行数：** ~2,500 行
**这是最核心、最复杂的类！**

### 9.1 核心职责

1. **协调所有模块**
2. **实现事件循环（poll）**
3. **处理所有 Raft 消息**
4. **管理状态转移**
5. **协调日志复制**

### 9.2 核心方法列表

```java
package org.apache.kafka.raft;

public class KafkaRaftClient<T> implements RaftClient<T> {

    // ===== 初始化 =====
    public void initialize();

    // ===== 事件循环（核心）=====
    public void poll();

    // ===== Leader 操作 =====
    public long scheduleAppend(int epoch, List<T> records);
    private void maybeAppendBatches(long currentTimeMs);

    // ===== 消息处理 =====
    private void handleInboundMessage(RaftMessage message, long currentTimeMs);

    // Vote 消息
    private VoteResponse handleVoteRequest(RaftRequest.Inbound<VoteRequest> request);
    private void handleVoteResponse(RaftResponse.Inbound<VoteResponse> response);

    // BeginQuorumEpoch 消息
    private BeginQuorumEpochResponse handleBeginQuorumEpochRequest(...);
    private void handleBeginQuorumEpochResponse(...);

    // EndQuorumEpoch 消息
    private EndQuorumEpochResponse handleEndQuorumEpochRequest(...);
    private void handleEndQuorumEpochResponse(...);

    // Fetch 消息
    private FetchResponse handleFetchRequest(RaftRequest.Inbound<FetchRequest> request);
    private void handleFetchResponse(RaftResponse.Inbound<FetchResponse> response);

    // ===== 状态转移 =====
    private void transitionToUnattached(int epoch);
    private void transitionToFollower(int epoch, int leaderId);
    private void transitionToCandidate(long currentTimeMs);
    private void transitionToLeader(long currentTimeMs);
    private void transitionToResigned(List<Integer> preferredSuccessors);

    // ===== 选举 =====
    private void maybeFireElectionTimeout(long currentTimeMs);
    private boolean isElectionTimeout(long currentTimeMs);
    private void initiateElection(long currentTimeMs);

    // ===== 日志复制 =====
    private void maybeFireFetchTimeout(long currentTimeMs);
    private void sendFetchRequests(long currentTimeMs);
    private void updateFollowerHighWatermark(...);

    // ===== 工具方法 =====
    private boolean isLogUpToDate(...);
    private void updateLeaderEndOffsetAndTimestamp(...);
    private void notifyListeners(...);
}
```

### 9.3 事件循环实现

**poll() 方法是整个系统的心脏：**

```java
@Override
public void poll() {
    long currentTimeMs = time.milliseconds();

    try {
        // 1. 处理网络消息
        List<RaftMessage> messages = channel.poll(pollTimeoutMs);
        for (RaftMessage message : messages) {
            handleInboundMessage(message, currentTimeMs);
        }

        // 2. 处理超时
        maybeFireElectionTimeout(currentTimeMs);
        maybeFireFetchTimeout(currentTimeMs);

        // 3. Leader 特定逻辑
        if (quorum.isLeader()) {
            // 追加批次到日志
            maybeAppendBatches(currentTimeMs);

            // 发送 Fetch 请求到 Follower
            sendFetchRequests(currentTimeMs);

            // 检查 Quorum 健康
            LeaderState<T> leaderState = (LeaderState<T>) quorum.currentState();
            if (!leaderState.isQuorumHealthy(currentTimeMs)) {
                // Quorum 不健康，考虑辞职
                log.warn("Quorum is unhealthy, considering resignation");
            }
        }

        // 4. Candidate 特定逻辑
        if (quorum.isCandidate()) {
            // 重发 Vote 请求到未响应的节点
            resendVoteRequests(currentTimeMs);
        }

        // 5. Follower 特定逻辑
        if (quorum.isFollower()) {
            // 发送 Fetch 请求到 Leader
            maybeSendFetchRequest(currentTimeMs);
        }

    } catch (Exception e) {
        log.error("Unexpected error in poll", e);
    }
}
```

### 9.4 学习检查清单

- [ ] 理解事件循环的设计
- [ ] 实现所有消息处理器
- [ ] 实现状态转移逻辑
- [ ] 整合所有模块
- [ ] 测试完整的 Raft 流程

---

## 模块 10: 监控指标 (Metrics)

**学习目标：** 实现性能监控

**预计时间：** 1-2 周

### 10.1 核心类

**文件：** `org.apache.kafka.raft.internals.KafkaRaftMetrics`
**行数：** ~800 行

**关键指标：**
```
- 选举次数
- 选举延迟
- 日志追加速率
- 高水位滞后
- Fetch 延迟
- 当前状态
- 当前 Leader
```

---

## 总结：完整的学习路线

```
模块1: 基础设施 (1-2周)
  ↓
模块2: 状态机 (3-4周) ⭐⭐⭐
  ↓
模块3: 选举机制 (2-3周) ⭐⭐
  ↓
模块4: 日志复制 (3-4周) ⭐⭐⭐
  ↓
模块5: 批处理系统 (2-3周) ⭐⭐
  ↓
模块6: 网络通信 (2-3周) ⭐⭐
  ↓
模块7: 快照管理 (2-3周) ⭐⭐
  ↓
模块8: 成员变更 (2-3周) ⭐⭐
  ↓
模块9: 核心引擎 (4-6周) ⭐⭐⭐
  ↓
模块10: 监控指标 (1-2周)
```

**总计：** 23-35 周（约 5-8 个月）

---

## 实践建议

1. **严格按模块顺序学习**
2. **每个模块完成后编写单元测试**
3. **在模块9之前实现一个简化的 KafkaRaftClient**
4. **每周记录学习笔记**
5. **画图理解流程和状态转移**

**恭喜你完成了完整的模块化学习指南！🎉**
