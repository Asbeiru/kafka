# KRaft 核心类映射参考

> 本文档详细列出 Apache Kafka KRaft 的核心类及其在我们重写项目中的对应关系

---

## 核心引擎类

### KafkaRaftClient → RaftEngine

**Kafka 原类**:
```java
// 位置: org.apache.kafka.raft.KafkaRaftClient
// 文件: raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java
// 大小: ~5000 行

public final class KafkaRaftClient<T> implements RaftClient<T> {
    // 核心成员
    private final QuorumState quorumState;
    private final ReplicatedLog log;
    private final NetworkChannel channel;
    private final BatchAccumulator<T> accumulator;

    // 关键方法
    public void poll() { }  // 主事件循环
    public CompletableFuture<Long> scheduleAppend(...) { }
    private void pollLeader() { }
    private void pollFollower() { }
    private void pollCandidate() { }
}
```

**我们的实现**:
```java
// 位置: io.github.mini.raft.core.RaftEngine
// 文件: raft-core/src/main/java/io/github/mini/raft/core/RaftEngine.java

public class RaftEngine implements Runnable {
    // 对应的成员
    private final QuorumState quorumState;
    private final ReplicatedLog log;
    private final NetworkChannel network;
    private final BatchAccumulator accumulator;

    // 对应的方法
    public void run() { }  // 主循环
    public CompletableFuture<Long> append(...) { }
    private void pollLeader() { }
    private void pollFollower() { }
    private void pollCandidate() { }
}
```

**关键实现点**:
1. 主循环处理网络事件
2. 根据当前状态分发到不同的 poll 方法
3. 处理超时事件
4. 协调各个管理器

---

## 状态机类

### QuorumState

**Kafka 原类**:
```java
// 位置: org.apache.kafka.raft.QuorumState
// 关键代码段:

public class QuorumState {
    private final OptionalInt localId;
    private final Time time;
    private final QuorumStateStore store;
    private volatile EpochState state;

    // 状态转换方法
    public void transitionToUnattached(int epoch) {
        durableTransitionTo(new UnattachedState(...));
    }

    public void transitionToCandidate(int epoch) {
        durableTransitionTo(new CandidateState(...));
    }

    // 持久化转换
    private void durableTransitionTo(EpochState state) {
        // 1. 先写磁盘
        if (state需要持久化) {
            store.writeElectionState(...);
        }
        // 2. 再更新内存
        this.state = state;
    }
}
```

**我们的实现**: 完全相同的设计

**关键学习点**:
- ⚠️ **先持久化，再更新内存** - 这是正确性的关键！
- 所有状态转换都要经过 `transitionTo*` 方法
- 使用 `volatile` 保证可见性

---

### EpochState → State

**Kafka 原类**:
```java
// 位置: org.apache.kafka.raft.EpochState
public interface EpochState {
    int epoch();
    ElectionState election();
}
```

**我们的实现**:
```java
public interface State {
    StateType type();
    int epoch();
    String name();
}
```

---

### LeaderState

**Kafka 原类**:
```java
// 位置: org.apache.kafka.raft.LeaderState
// 关键实现:

public class LeaderState implements EpochState {
    // 复制状态管理
    private Map<Integer, ReplicaState> voterStates;
    private Map<ReplicaKey, ReplicaState> observerStates;

    // 高水位计算
    public boolean updateHighWatermark(...) {
        // 1. 收集所有 replica 的 match offset
        List<Long> endOffsets = new ArrayList<>();
        endOffsets.add(log.endOffset().offset);

        for (ReplicaState state : voterStates.values()) {
            endOffsets.add(state.endOffset.offset);
        }

        // 2. 排序并找中位数
        endOffsets.sort(Long::compareTo);
        long newHighWatermark = endOffsets.get(voterStates.size() / 2);

        // 3. 更新
        if (newHighWatermark > highWatermark.offset) {
            highWatermark = new LogOffsetMetadata(newHighWatermark);
            return true;
        }
        return false;
    }

    // Check Quorum
    private boolean checkQuorumTimerExpired() {
        return time.milliseconds() >= checkQuorumDeadline;
    }
}
```

**关键学习点**:
1. **高水位 = 多数派的最小 match offset**
2. Check Quorum 需要定期检查活跃 followers
3. 区分 voter 和 observer

---

## 日志存储类

### ReplicatedLog

**Kafka 原类**:
```java
// 位置: org.apache.kafka.raft.ReplicatedLog
public interface ReplicatedLog extends AutoCloseable {
    // 追加
    LogAppendInfo appendAsLeader(Records records, int epoch);
    LogAppendInfo appendAsFollower(Records records, int epoch);

    // 读取
    LogFetchInfo read(long startOffset, Isolation isolation);

    // 截断
    void truncateTo(long offset);

    // 验证
    ValidOffsetAndEpoch validateOffsetAndEpoch(long offset, int epoch);

    // 高水位
    LogOffsetMetadata highWatermark();
    void updateHighWatermark(LogOffsetMetadata offsetMetadata);

    // Epoch 管理
    OffsetAndEpoch endOffsetForEpoch(int epoch);
    void initializeLeaderEpoch(int epoch);
}
```

**关键实现**:
```java
// validateOffsetAndEpoch 的实现逻辑:
default ValidOffsetAndEpoch validateOffsetAndEpoch(long offset, int epoch) {
    // 1. 如果偏移量 < log start，需要快照
    if (offset < startOffset()) {
        return ValidOffsetAndEpoch.snapshot(latestSnapshotId());
    }

    // 2. 找到该 epoch 的结束位置
    OffsetAndEpoch endOffset = endOffsetForEpoch(epoch);

    // 3. 比较
    if (endOffset.epoch() != epoch || endOffset.offset() < offset) {
        return ValidOffsetAndEpoch.diverging(endOffset);
    }

    return ValidOffsetAndEpoch.valid(new OffsetAndEpoch(offset, epoch));
}
```

**关键学习点**:
1. Leader 和 Follower 追加有不同的验证逻辑
2. `validateOffsetAndEpoch` 用于检测日志分歧
3. 需要维护 epoch 到 offset 的映射

---

### KafkaMetadataLog (Scala)

**Kafka 原类**:
```scala
// 位置: core/src/main/scala/kafka/raft/KafkaMetadataLog.scala
class KafkaMetadataLog(
    log: UnifiedLog,  // 底层的 Kafka Log
    // ...
) extends ReplicatedLog {

    override def appendAsLeader(records: Records, epoch: Int): LogAppendInfo = {
        // 使用 UnifiedLog 的追加
        val appendInfo = log.appendAsLeader(records, leaderEpoch = epoch)
        appendInfo
    }

    override def read(startOffset: Long, isolation: Isolation): LogFetchInfo = {
        val maxOffset = isolation match {
            case Isolation.COMMITTED => log.highWatermark
            case Isolation.UNCOMMITTED => log.logEndOffset
        }

        val fetchInfo = log.read(startOffset, maxLength, maxOffset)
        // ...
    }
}
```

**我们的实现**: 自己实现简化版的 Log

---

## 复制类

### BatchAccumulator

**Kafka 原类**:
```java
// 位置: org.apache.kafka.raft.internals.BatchAccumulator
public class BatchAccumulator<T> implements Closeable {
    private final int epoch;
    private final long baseOffset;
    private final int lingerMs;
    private final int maxBatchSize;

    // 当前批次
    private BatchBuilder<T> currentBatch;

    // 已完成的批次队列
    private final ConcurrentLinkedQueue<CompletedBatch<T>> completed;

    // 追加方法
    public long append(int epoch, List<T> records, boolean delayDrain) {
        // 加锁
        appendLock.lock();
        try {
            // 检查 epoch
            if (epoch != this.epoch) {
                throw new NotLeaderException(...);
            }

            // 分配或复用批次
            BatchBuilder<T> batch = maybeAllocateBatch(records);

            // 追加到批次
            long lastOffset = nextOffset + records.size() - 1;
            batch.appendRecords(records);
            nextOffset += records.size();

            // 检查是否需要完成批次
            if (batch.isFull() || !delayDrain) {
                completeBatch(batch);
            }

            return lastOffset;
        } finally {
            appendLock.unlock();
        }
    }

    // 排空批次
    public List<CompletedBatch<T>> drain() {
        List<CompletedBatch<T>> batches = new ArrayList<>();
        CompletedBatch<T> batch;
        while ((batch = completed.poll()) != null) {
            batches.add(batch);
        }
        return batches;
    }
}
```

**关键学习点**:
1. 使用锁保护 append 操作
2. 批次完成后放入队列，异步写入日志
3. 支持 `delayDrain` 用于批量优化

---

### ReplicaState

**Kafka 原类**:
```java
// 这是 LeaderState 的内部类
public class LeaderState {
    public static class ReplicaState {
        public LogOffsetMetadata endOffset;  // 下一条要发送的
        public long lastFetchTimestamp;
        public long lastCaughtUpTimestamp;

        // 更新
        public boolean updateLeaderEndOffset(
            OffsetAndEpoch endOffsetAndEpoch,
            long currentTimeMs
        ) {
            // 如果 Follower 追上了
            if (endOffsetAndEpoch.offset >= endOffset.offset) {
                this.endOffset = new LogOffsetMetadata(endOffsetAndEpoch.offset);
                this.lastCaughtUpTimestamp = currentTimeMs;
                return true;
            }
            return false;
        }
    }
}
```

**我们的实现**:
```java
public class ReplicaState {
    private long matchOffset;   // 已确认复制的最高偏移量
    private long nextOffset;    // 下一条要发送的偏移量

    public void updateMatchOffset(long offset) {
        this.matchOffset = Math.max(this.matchOffset, offset);
        this.nextOffset = Math.max(this.nextOffset, offset + 1);
    }
}
```

---

## 选举类

### 投票请求处理

**Kafka 原类**:
```java
// KafkaRaftClient.handleVoteRequest()
private VoteResponseData handleVoteRequest(
    RaftRequest.Inbound requestMetadata,
    VoteRequestData request,
    long currentTimeMs
) {
    // 1. 检查 epoch
    if (request.candidateEpoch() > quorum.epoch()) {
        transitionToUnattached(request.candidateEpoch());
    }

    // 2. 检查是否已投票
    Optional<Integer> votedId = quorum.election().votedId();
    if (!request.isPreVote() && votedId.isPresent()) {
        if (votedId.get() != request.candidateId()) {
            return buildVoteResponse(false);
        }
    }

    // 3. 检查日志
    boolean logUpToDate =
        request.lastLogEpoch() > log.lastFetchedEpoch() ||
        (request.lastLogEpoch() == log.lastFetchedEpoch() &&
         request.lastLogOffset() >= log.endOffset().offset - 1);

    if (!logUpToDate) {
        return buildVoteResponse(false);
    }

    // 4. 投票
    if (!request.isPreVote()) {
        quorum.transitionToVoted(request.candidateEpoch(), request.candidateId());
    }

    return buildVoteResponse(true);
}
```

**关键学习点**:
1. PreVote 不持久化
2. 日志比较：先 epoch，后 offset
3. 投票后要持久化

---

## 网络类

### NetworkChannel

**Kafka 原类**:
```java
// 位置: org.apache.kafka.raft.NetworkChannel
public interface NetworkChannel extends AutoCloseable {
    // 发送请求
    void send(RaftRequest.Outbound request);

    // 唤醒（用于中断 poll）
    void wakeup();

    // 轮询
    List<RaftMessage> receive(long timeoutMs);
}
```

**KafkaNetworkChannel 实现**:
```java
// 位置: org.apache.kafka.raft.KafkaNetworkChannel
public class KafkaNetworkChannel implements NetworkChannel {
    private final NetworkClient client;
    private final Selector selector;

    @Override
    public void send(RaftRequest.Outbound request) {
        // 构造 ClientRequest
        ClientRequest clientRequest = buildClientRequest(...);

        // 发送
        client.send(clientRequest, time.milliseconds());
    }

    @Override
    public List<RaftMessage> receive(long timeoutMs) {
        // 轮询 NetworkClient
        client.poll(timeoutMs, time.milliseconds());

        // 收集响应
        List<RaftMessage> messages = new ArrayList<>();
        for (ClientResponse response : client.responses()) {
            messages.add(parseResponse(response));
        }

        return messages;
    }
}
```

**我们的实现**: 使用 gRPC 简化

---

## 快照类

### SnapshotWriter / SnapshotReader

**Kafka 原类**:
```java
// 位置: org.apache.kafka.snapshot.SnapshotWriter
public interface SnapshotWriter<T> extends AutoCloseable {
    // 追加到快照
    void append(List<T> records);

    // 冻结快照（完成写入）
    void freeze();

    // 快照 ID
    OffsetAndEpoch snapshotId();
}

// 位置: org.apache.kafka.snapshot.RawSnapshotWriter
public interface RawSnapshotWriter extends AutoCloseable {
    void append(UnalignedMemoryRecords records);
    void freeze();
    long sizeInBytes();
}
```

**FileRawSnapshotWriter 实现**:
```java
// 位置: org.apache.kafka.snapshot.FileRawSnapshotWriter
public final class FileRawSnapshotWriter implements RawSnapshotWriter {
    private final Path tempPath;    // 临时文件路径
    private final Path finalPath;   // 最终文件路径
    private final FileChannel channel;

    @Override
    public void append(UnalignedMemoryRecords records) {
        // 写入临时文件
        records.writeTo(channel);
    }

    @Override
    public void freeze() {
        // 1. 关闭文件
        channel.close();

        // 2. 重命名为最终路径（原子操作）
        Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
    }
}
```

**关键学习点**:
1. 先写临时文件
2. freeze 时原子重命名
3. 快照命名: `{offset}-{epoch}.snapshot`

---

## 成员变更类

### VoterSet

**Kafka 原类**:
```java
// 位置: org.apache.kafka.raft.VoterSet
public final class VoterSet {
    private final Map<Integer, VoterNode> voters;

    public static class VoterNode {
        private final ReplicaKey voterKey;
        private final Endpoints listeners;
        private final SupportedVersionRange supportedKRaftVersion;
    }

    // 创建新的 VoterSet（不可变）
    public VoterSet addVoter(VoterNode voter) {
        Map<Integer, VoterNode> newVoters = new HashMap<>(voters);
        newVoters.put(voter.voterKey().id(), voter);
        return new VoterSet(newVoters);
    }

    public VoterSet removeVoter(int voterId) {
        Map<Integer, VoterNode> newVoters = new HashMap<>(voters);
        newVoters.remove(voterId);
        return new VoterSet(newVoters);
    }
}
```

**关键学习点**:
1. VoterSet 是**不可变**的
2. 每次变更返回新的 VoterSet
3. 支持动态端点和版本信息

---

## 关键设计原则总结

### 1. 持久化顺序

```java
// ✅ 正确的顺序:
void transitionToCandidate(int epoch) {
    // 1. 先持久化
    store.writeElectionState(new ElectionState(epoch, nodeId));

    // 2. 再更新内存
    this.state = new CandidateState(epoch);

    // 3. 最后发送网络消息
    sendVoteRequests();
}

// ❌ 错误的顺序:
void transitionToCandidate(int epoch) {
    // 先更新内存 - 崩溃会丢失状态！
    this.state = new CandidateState(epoch);

    // 再持久化
    store.writeElectionState(...);
}
```

### 2. 高水位更新

```java
// 只能提交当前 epoch 的日志
boolean updateHighWatermark() {
    // 计算多数派的 match offset
    long newHW = calculateQuorumMatchOffset();

    // 验证这个位置是当前 epoch 的
    OffsetAndEpoch hwOffsetAndEpoch = log.endOffsetForEpoch(currentEpoch);
    if (hwOffsetAndEpoch.offset() < newHW) {
        // 不能提交旧 epoch 的日志
        newHW = hwOffsetAndEpoch.offset();
    }

    if (newHW > highWatermark) {
        highWatermark = newHW;
        return true;
    }
    return false;
}
```

### 3. 日志截断

```java
// 只在发现分歧时截断
void handleFetchResponse(FetchResponse response) {
    if (response.hasDiverging()) {
        long divergingOffset = response.getDiverging().offset();

        // 截断到分歧点
        log.truncateTo(divergingOffset);

        logger.info("Truncated log to {}", divergingOffset);
    }
}
```

### 4. 状态转换

```
所有状态转换必须经过 QuorumState:

Unattached --electionTimeout--> Prospective --preVoteMajority--> Candidate --voteMajority--> Leader
    ↑                                ↓                                 ↓                        ↓
    └────────────────────────────────┴─────────────────────────────────┴────────────────────────┘
                            (higherEpoch 或 fetchTimeout)
```

---

## 参考文档

- [KRaft KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500)
- [Raft 论文](https://raft.github.io/raft.pdf)
- [Kafka 源码](https://github.com/apache/kafka)

---

这个映射文档会持续更新，随着我们实现的推进而完善。
