# KRaft 核心类实现顺序表 - 完整版

**设计理念：** 按照依赖关系从底层到顶层，从简单到复杂，确保每个类实现时所依赖的类都已完成。

**总计：** 84个核心类，11个实现阶段，预计25-35工作日

---

## 📊 实现顺序总览

```
阶段1: 基础数据结构 (无依赖)           ████░░░░░░░░ 2-3天
阶段2: 核心接口定义                   ████░░░░░░░░ 1-2天
阶段3: 配置和存储                     ████░░░░░░░░ 2-3天
阶段4: 网络和消息                     ██████░░░░░░ 3-4天
阶段5: 批处理系统 ⭐                  ██████░░░░░░ 3-4天
阶段6: 状态机实现 ⭐⭐⭐              ████████████ 7-10天
阶段7: 状态管理器                     ████░░░░░░░░ 2-3天
阶段8: Voter管理                      ████░░░░░░░░ 2-3天
阶段9: 高级功能                       ████░░░░░░░░ 2-3天
阶段10: 快照管理                      ████░░░░░░░░ 2-3天
阶段11: 核心引擎 KafkaRaftClient ⭐⭐⭐ ████████████ 7-10天
```

---

## 阶段1: 基础数据结构（无依赖，最先实现）

**原则：** 这些是纯数据类，没有任何依赖，是整个系统的基石

**预计时间：** 2-3天

### 1.1 偏移量和元数据类

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `OffsetAndEpoch` | raft | ~100 | `offset: long`<br>`epoch: int` | `compareTo()`<br>`equals()`<br>`toString()` | 表示某个epoch的偏移量 |
| `LogOffsetMetadata` | raft | ~150 | `offset: long`<br>`segmentBaseOffset: Optional<Long>`<br>`relativePositionInSegment: Optional<Integer>` | `offset()`<br>`metadata()`<br>`compareTo()` | 日志偏移量元数据 |
| `ValidOffsetAndEpoch` | raft | ~80 | `kind: Kind`<br>`offsetAndEpoch: OffsetAndEpoch` | `isValid()`<br>`offsetAndEpoch()` | 有效的偏移量 |

**实现顺序：**
```
1. OffsetAndEpoch          ← 最简单，先实现
2. LogOffsetMetadata       ← 依赖OffsetAndEpoch
3. ValidOffsetAndEpoch     ← 依赖OffsetAndEpoch
```

**代码骨架：**
```java
// 1. OffsetAndEpoch.java
public class OffsetAndEpoch implements Comparable<OffsetAndEpoch> {
    private final long offset;
    private final int epoch;

    public OffsetAndEpoch(long offset, int epoch) {
        this.offset = offset;
        this.epoch = epoch;
    }

    public long offset() { return offset; }
    public int epoch() { return epoch; }

    @Override
    public int compareTo(OffsetAndEpoch other) {
        if (this.epoch != other.epoch) {
            return Integer.compare(this.epoch, other.epoch);
        }
        return Long.compare(this.offset, other.offset);
    }
}
```

### 1.2 Leader和选举相关

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `LeaderAndEpoch` | raft | ~100 | `leaderId: OptionalInt`<br>`epoch: int` | `leaderId()`<br>`epoch()`<br>`isLeader(int)` | Leader信息 |
| `ElectionState` | raft | ~150 | `epoch: int`<br>`leaderIdOpt: OptionalInt`<br>`votedIdOpt: OptionalInt`<br>`voters: Set<Integer>` | `epoch()`<br>`hasLeader()`<br>`hasVoted()` | 选举状态 |

**实现顺序：**
```
1. LeaderAndEpoch    ← 简单不可变类
2. ElectionState     ← 依赖LeaderAndEpoch
```

### 1.3 端点和副本标识

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `Endpoints` | raft | ~120 | `listeners: Map<ListenerName, InetSocketAddress>` | `address(ListenerName)`<br>`allAddresses()` | 节点端点信息 |
| `ReplicaKey` | raft | ~80 | `id: int`<br>`directoryId: Uuid` | `id()`<br>`directoryId()`<br>`equals()` | 副本唯一标识 |

### 1.4 批次和日志相关

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `Batch<T>` | raft | ~100 | `baseOffset: long`<br>`epoch: int`<br>`records: List<T>`<br>`appendTimestamp: long` | `baseOffset()`<br>`records()`<br>`sizeInBytes()` | 单个批次 |
| `LogAppendInfo` | raft | ~80 | `startOffset: long`<br>`endOffset: long`<br>`lastOffset: LogOffsetMetadata` | `startOffset()`<br>`endOffset()` | 日志追加信息 |
| `LogFetchInfo` | raft | ~100 | `batches: Records`<br>`highWatermark: LogOffsetMetadata` | `records()`<br>`highWatermark()` | 日志获取信息 |

### 1.5 枚举和常量类

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `Isolation` | raft | ~50 | - | `COMMITTED`<br>`UNCOMMITTED` | 隔离级别枚举 |

**阶段1完成检查清单：**
- [ ] 所有类编译通过
- [ ] 所有equals/hashCode正确实现
- [ ] 所有toString可读性好
- [ ] 单元测试覆盖率>80%

---

## 阶段2: 核心接口定义

**原则：** 定义系统的契约，不包含实现

**预计时间：** 1-2天

### 2.1 主要接口表

| 接口名 | 位置 | 核心方法 | 说明 | 实现类（稍后阶段） |
|--------|------|---------|------|------------------|
| `RaftClient<T>` | raft | `initialize()`<br>`register(Listener)`<br>`scheduleAppend()`<br>`read()`<br>`poll()`<br>`shutdown()` | Raft客户端主接口 | KafkaRaftClient |
| `EpochState` | raft | `epoch()`<br>`election()`<br>`electionTimeoutMs()`<br>`canGrantVote()`<br>`highWatermark()` | 状态基接口 | 7个状态实现类 |
| `ReplicatedLog` | raft | `read()`<br>`appendAsLeader()`<br>`appendAsFollower()`<br>`truncateTo()`<br>`endOffset()`<br>`updateHighWatermark()` | 复制日志接口 | KafkaMetadataLog |
| `NetworkChannel` | raft | `send()`<br>`poll()`<br>`wakeup()`<br>`updateEndpoints()` | 网络通道接口 | KafkaNetworkChannel |
| `RaftMessage` | raft | `correlationId()`<br>`sourceId()` | 消息基接口 | RaftRequest/Response |
| `BatchReader<T>` | raft | `hasNext()`<br>`next()`<br>`close()` | 批次读取器接口 | 多个实现 |

### 2.2 嵌套接口

```java
// RaftClient.Listener<T> 接口
public interface RaftClient<T> {
    interface Listener<T> {
        void handleCommit(BatchReader<T> reader);
        void handleLoadSnapshot(SnapshotReader<T> reader);
        void handleLeaderChange(LeaderAndEpoch leader);
    }
}
```

**阶段2完成检查清单：**
- [ ] 所有接口方法签名准确
- [ ] 所有javadoc完整
- [ ] 接口之间的依赖关系清晰

---

## 阶段3: 配置和存储

**原则：** 配置管理和状态持久化

**预计时间：** 2-3天

### 3.1 配置类

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `QuorumConfig` | raft | ~200 | `electionTimeoutMs: int`<br>`fetchTimeoutMs: int`<br>`appendLingerMs: int`<br>`requestTimeoutMs: int`<br>`retryBackoffMs: int` | 所有配置的getter方法 | Quorum配置 |

**完整字段列表：**
```java
public class QuorumConfig {
    // 选举配置
    private final int electionTimeoutMs;          // 默认1000ms
    private final int electionBackoffMaxMs;       // 默认1000ms

    // Fetch配置
    private final int fetchTimeoutMs;             // 默认2000ms

    // 批处理配置
    private final int appendLingerMs;             // 默认25ms

    // 网络配置
    private final int requestTimeoutMs;           // 默认2000ms
    private final int retryBackoffMs;             // 默认20ms

    // Getter方法...
}
```

### 3.2 状态存储

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `QuorumStateStore` | raft | ~80 | - | `readElectionState()`<br>`writeElectionState()` | 状态存储接口 |
| `FileQuorumStateStore` | raft | ~200 | `stateFile: File`<br>`dataBuffer: ByteBuffer` | `readElectionState()`<br>`writeElectionState()` | 文件实现 |

**实现要点：**
```java
// QuorumStateData JSON格式
{
  "leaderId": 1,
  "leaderEpoch": 5,
  "votedId": 1,
  "appliedOffset": 1000,
  "currentVoters": [
    {"voterId": 1},
    {"voterId": 2},
    {"voterId": 3}
  ]
}
```

### 3.3 工具类

| 类名 | 位置 | 行数 | 核心方法 | 说明 |
|------|------|------|---------|------|
| `RaftUtil` | raft | ~950 | `hasValidLeaderEpoch()`<br>`createLeaderChangeRecord()`<br>`parseVoterConnections()`<br>`randomBackoff()` | 各种工具方法 |

---

## 阶段4: 网络和消息

**原则：** 网络通信和消息序列化

**预计时间：** 3-4天

### 4.1 消息基类

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `RaftRequest` | raft | ~100 | - | - | 请求基类 |
| `RaftRequest.Outbound` | raft | ~80 | `correlationId: int`<br>`data: ApiMessage`<br>`destinationId: int`<br>`createdTimeMs: long` | `correlationId()`<br>`destinationId()` | 出站请求 |
| `RaftRequest.Inbound` | raft | ~80 | `correlationId: int`<br>`data: ApiMessage`<br>`sourceId: int` | `correlationId()`<br>`sourceId()` | 入站请求 |
| `RaftResponse` | raft | ~100 | - | - | 响应基类 |
| `RaftResponse.Outbound` | raft | ~80 | `correlationId: int`<br>`data: ApiMessage`<br>`destinationId: int` | - | 出站响应 |
| `RaftResponse.Inbound` | raft | ~80 | `correlationId: int`<br>`data: ApiMessage`<br>`sourceId: int` | - | 入站响应 |

### 4.2 网络通道实现

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `KafkaNetworkChannel` | raft | ~400 | `client: NetworkClient`<br>`requestManager: RequestManager`<br>`messageQueue: List<RaftMessage>` | `send()`<br>`poll()`<br>`wakeup()` | Kafka网络实现 |
| `RequestManager` | raft | ~400 | `connections: Map<Integer, ConnectionState>`<br>`requestTimeoutMs: int`<br>`retryBackoffMs: int` | `send()`<br>`pollReadyRequests()`<br>`handleTimeouts()` | 请求连接管理 |

**RequestManager核心结构：**
```java
public class RequestManager {
    // 每个节点的连接状态
    static class ConnectionState {
        final int nodeId;
        final Deque<RaftRequest.Outbound> pendingRequests;
        final Map<Integer, InflightRequest> inflightRequests;
        boolean isReady;
        long lastConnectAttemptMs;
    }

    static class InflightRequest {
        final RaftRequest.Outbound request;
        final long sendTimeMs;
    }

    // 核心方法
    public void send(int destinationId, RaftRequest.Outbound request);
    public List<RaftRequest.Outbound> pollReadyRequests(long currentTimeMs);
    public void onRequestSent(int destinationId, int correlationId, ...);
    public void onResponseReceived(int sourceId, int correlationId, ...);
    public List<InflightRequest> handleTimeouts(long currentTimeMs);
}
```

### 4.3 消息队列

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `RaftMessageQueue` | raft | ~50 | - | `add()`<br>`poll()` | 消息队列接口 |
| `BlockingMessageQueue` | raft.internals | ~150 | `queue: BlockingQueue<RaftMessage>`<br>`wakeupSource: WakeupSource` | `add()`<br>`poll()`<br>`wakeup()` | 阻塞队列实现 |

---

## 阶段5: 批处理系统 ⭐ 性能关键

**原则：** 高效的日志批处理，这是性能的关键

**预计时间：** 3-4天

### 5.1 核心批处理类

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `BatchAccumulator<T>` | raft.internals | ~350 | `currentBatch: BatchBuilder<T>`<br>`completedBatches: Deque<CompletedBatch<T>>`<br>`memoryPool: BatchMemoryPool`<br>`lingerMs: int`<br>`leaderEpoch: int`<br>`logEndOffset: long` | `append()`<br>`drain()`<br>`forceDrain()`<br>`updateLeaderEpoch()` | 批次累积器 ⭐⭐⭐ |

**BatchAccumulator核心实现：**
```java
public class BatchAccumulator<T> {
    // ===== 核心字段 =====
    private final ReentrantLock lock = new ReentrantLock();
    private BatchBuilder<T> currentBatch;
    private final Deque<CompletedBatch<T>> completedBatches;
    private final BatchMemoryPool memoryPool;
    private final int lingerMs;
    private final int maxBatchSize;
    private int leaderEpoch;
    private long logEndOffset;

    // ===== 核心方法 =====

    // 1. 追加记录（最重要）
    public long append(List<T> records, long currentTimeMs) {
        lock.lock();
        try {
            // 如果没有当前批次，创建新批次
            if (currentBatch == null) {
                currentBatch = new BatchBuilder<>(...);
            }

            // 追加到当前批次
            boolean appended = currentBatch.append(records);

            if (!appended) {
                // 当前批次已满，完成它
                completeBatch(currentTimeMs);
                // 创建新批次并重试
                currentBatch = new BatchBuilder<>(...);
                appended = currentBatch.append(records);
            }

            // 检查是否需要完成批次
            if (shouldCompleteBatch(currentTimeMs)) {
                completeBatch(currentTimeMs);
            }

            return logEndOffset;
        } finally {
            lock.unlock();
        }
    }

    // 2. 完成批次
    private void completeBatch(long currentTimeMs) {
        if (currentBatch == null) return;

        MemoryRecords records = currentBatch.build();
        CompletedBatch<T> completed = new CompletedBatch<>(
            logEndOffset, leaderEpoch, currentBatch.records(),
            records, currentTimeMs
        );

        completedBatches.add(completed);
        logEndOffset += currentBatch.recordCount();
        currentBatch = null;
    }

    // 3. 判断是否应该完成批次
    private boolean shouldCompleteBatch(long currentTimeMs) {
        if (currentBatch == null) return false;

        // 批次已满
        if (currentBatch.isFull()) return true;

        // 等待时间已到
        long elapsedMs = currentTimeMs - currentBatch.createdTimeMs();
        if (elapsedMs >= lingerMs) return true;

        return false;
    }

    // 4. 排空已完成批次
    public List<CompletedBatch<T>> drain() {
        lock.lock();
        try {
            List<CompletedBatch<T>> drained = new ArrayList<>(completedBatches);
            completedBatches.clear();
            return drained;
        } finally {
            lock.unlock();
        }
    }

    // 5. 强制刷新
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

    // 6. 更新Leader Epoch（当选举发生时）
    public void updateLeaderEpoch(int newEpoch, long newLogEndOffset) {
        lock.lock();
        try {
            // 清空所有未完成的批次
            if (currentBatch != null) {
                currentBatch.close();
                currentBatch = null;
            }
            completedBatches.clear();

            this.leaderEpoch = newEpoch;
            this.logEndOffset = newLogEndOffset;
        } finally {
            lock.unlock();
        }
    }

    // ===== 嵌套类 =====
    public static class CompletedBatch<T> {
        public final long baseOffset;
        public final int epoch;
        public final List<T> records;
        public final MemoryRecords data;
        public final long appendTimestamp;
    }
}
```

### 5.2 批次构建器

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `BatchBuilder<T>` | raft.internals | ~200 | `leaderEpoch: int`<br>`baseOffset: long`<br>`createdTimeMs: long`<br>`records: List<T>`<br>`buffer: ByteBuffer`<br>`recordsBuilder: MemoryRecordsBuilder` | `append()`<br>`build()`<br>`isFull()`<br>`close()` | 批次构建器 |

### 5.3 内存管理

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `BatchMemoryPool` | raft.internals | ~150 | `bufferSize: int`<br>`maxBuffers: int`<br>`freeBuffers: BlockingQueue<ByteBuffer>` | `allocate()`<br>`release()` | 内存池 |

### 5.4 批次读取器

| 类名 | 位置 | 行数 | 核心方法 | 说明 |
|------|------|---------|------|
| `MemoryBatchReader<T>` | raft.internals | ~100 | `hasNext()`<br>`next()` | 内存批次读取 |
| `RecordsBatchReader<T>` | raft.internals | ~120 | `hasNext()`<br>`next()` | Records批次读取 |

**实现顺序：**
```
1. BatchMemoryPool        ← 最底层，先实现
2. BatchBuilder<T>        ← 依赖内存池
3. BatchAccumulator<T>    ← 依赖BatchBuilder，最复杂
4. MemoryBatchReader<T>   ← 读取器
5. RecordsBatchReader<T>  ← 读取器
```

---

## 阶段6: 状态机实现 ⭐⭐⭐ 最核心最复杂

**原则：** 实现所有Raft状态，这是整个系统的核心

**预计时间：** 7-10天

### 6.1 状态实现顺序（从简单到复杂）

```
1. UnattachedState      ← 最简单（1天）
2. ResignedState        ← 简单（1天）
3. VotedState          ← 中等（1天）
4. FollowerState       ← 中等（1-2天）
5. ProspectiveState    ← 复杂（1-2天）
6. CandidateState      ← 复杂（2天）
7. LeaderState<T>      ← 最复杂（3-4天）⭐⭐⭐
```

### 6.2 状态类详细表

#### 6.2.1 UnattachedState（最简单，从这里开始）

| 项目 | 内容 |
|------|------|
| **类名** | `UnattachedState` |
| **位置** | org.apache.kafka.raft |
| **行数** | ~100 |
| **核心属性** | `epoch: int`<br>`voters: VoterSet`<br>`highWatermark: Optional<LogOffsetMetadata>`<br>`electionTimeoutMs: long` |
| **核心方法** | `epoch(): int`<br>`election(): ElectionState`<br>`electionTimeoutMs(long): long`<br>`canGrantVote(ReplicaKey, boolean): boolean`<br>`highWatermark(): Optional<LogOffsetMetadata>` |
| **状态转移** | → ProspectiveState（选举超时）<br>→ FollowerState（发现Leader）<br>→ VotedState（投票） |

**完整实现模板：**
```java
public class UnattachedState implements EpochState {
    private final int epoch;
    private final VoterSet voters;
    private final Optional<LogOffsetMetadata> highWatermark;
    private final long electionTimeoutMs;

    public UnattachedState(
        int epoch,
        VoterSet voters,
        Optional<LogOffsetMetadata> highWatermark,
        long electionTimeoutMs
    ) {
        this.epoch = epoch;
        this.voters = voters;
        this.highWatermark = highWatermark;
        this.electionTimeoutMs = electionTimeoutMs;
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withUnknownLeader(epoch, voters.voterIds());
    }

    @Override
    public long electionTimeoutMs(long currentTimeMs) {
        return electionTimeoutMs;
    }

    @Override
    public boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate) {
        return voters.isVoter(candidateKey) && isLogUpToDate;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public String name() {
        return "Unattached";
    }
}
```

#### 6.2.2 FollowerState

| 项目 | 内容 |
|------|------|
| **类名** | `FollowerState` |
| **行数** | ~200 |
| **核心属性** | `epoch: int`<br>`leaderId: int`<br>`voters: VoterSet`<br>`highWatermark: Optional<LogOffsetMetadata>`<br>`fetchTimeoutMs: long`<br>`lastFetchTimestampMs: long`<br>`lastCaughtUpTimestampMs: long` |
| **核心方法** | `epoch(): int`<br>`leaderId(): int`<br>`election(): ElectionState`<br>`electionTimeoutMs(long): long`<br>`canGrantVote(...): boolean`<br>`updateFetchTimestamp(long): void`<br>`hasFetchTimeoutExpired(long): boolean` |
| **状态转移** | → CandidateState（Fetch超时）<br>→ VotedState（更高epoch的投票请求） |

**核心字段说明：**
```java
public class FollowerState implements EpochState {
    private final int epoch;                    // 当前epoch
    private final int leaderId;                 // Leader的ID
    private final VoterSet voters;              // 投票者集合
    private final Optional<LogOffsetMetadata> highWatermark;  // 高水位
    private final long fetchTimeoutMs;          // Fetch超时时间

    // 时间戳（用于检测超时）
    private long lastFetchTimestampMs;          // 最后一次Fetch时间
    private long lastCaughtUpTimestampMs;       // 最后一次赶上Leader的时间
}
```

#### 6.2.3 CandidateState

| 项目 | 内容 |
|------|------|
| **类名** | `CandidateState` |
| **行数** | ~300 |
| **核心属性** | `localId: int`<br>`epoch: int`<br>`voters: VoterSet`<br>`grantingVoters: Map<Integer, Boolean>`<br>`rejectingVoters: Map<Integer, String>`<br>`unrecordedVoters: Set<Integer>`<br>`electionTimeoutMs: long` |
| **核心方法** | `recordGrantedVote(int): boolean`<br>`recordRejectedVote(int, String): boolean`<br>`isVoteGranted(): boolean`<br>`isVoteRejected(): boolean`<br>`unrecordedVoters(): Set<Integer>` |
| **状态转移** | → LeaderState（获得多数派投票）<br>→ FollowerState（被多数派拒绝或发现更高epoch）<br>→ UnattachedState（选举超时） |

**核心算法：投票追踪**
```java
public class CandidateState implements EpochState {
    private final int localId;
    private final int epoch;
    private final VoterSet voters;

    // 投票追踪
    private final Map<Integer, Boolean> grantingVoters = new HashMap<>();
    private final Map<Integer, String> rejectingVoters = new HashMap<>();
    private final Set<Integer> unrecordedVoters;

    public CandidateState(...) {
        // 初始化
        this.unrecordedVoters = new HashSet<>(voters.voterIds());

        // 给自己投票
        recordGrantedVote(localId);
    }

    // 记录同意投票
    public boolean recordGrantedVote(int voterId) {
        if (!voters.isVoter(voterId)) return false;

        grantingVoters.put(voterId, true);
        unrecordedVoters.remove(voterId);
        rejectingVoters.remove(voterId);
        return true;
    }

    // 检查是否获得多数派
    public boolean isVoteGranted() {
        return grantingVoters.size() >= voters.majority();
    }

    // 检查是否被多数派拒绝
    public boolean isVoteRejected() {
        return rejectingVoters.size() > voters.voterIds().size() - voters.majority();
    }
}
```

#### 6.2.4 LeaderState<T>（最复杂，核心中的核心）⭐⭐⭐

| 项目 | 内容 |
|------|------|
| **类名** | `LeaderState<T>` |
| **行数** | ~1050 |
| **核心属性** | `localId: int`<br>`epoch: int`<br>`epochStartOffset: long`<br>`voters: VoterSet`<br>`highWatermark: Optional<LogOffsetMetadata>`<br>`voterStates: Map<Integer, ReplicaState>`<br>`observerStates: Map<Integer, ReplicaState>`<br>`accumulator: BatchAccumulator<T>`<br>`fetchTimeoutMs: int` |
| **核心方法** | `updateReplicaState(int, long, LogOffsetMetadata): boolean`<br>`updateHighWatermark(): void`<br>`followersThatNeedFetch(long): Set<Integer>`<br>`isQuorumHealthy(long): boolean`<br>`nextFetchOffset(int): long`<br>`addVoter(VoterNode): void`<br>`removeVoter(int): void` |

**LeaderState核心结构：**

```java
public class LeaderState<T> implements EpochState {
    // ===== 基本信息 =====
    private final int localId;
    private final int epoch;
    private final long epochStartOffset;  // 这个epoch开始时的偏移量

    // ===== 投票者管理 =====
    private final VoterSet voters;

    // ===== 高水位（核心）=====
    private Optional<LogOffsetMetadata> highWatermark;

    // ===== 副本状态追踪（核心）=====
    private final Map<Integer, ReplicaState> voterStates;      // 投票者
    private final Map<Integer, ReplicaState> observerStates;   // 观察者

    // ===== 批处理累积器 =====
    private final BatchAccumulator<T> accumulator;

    // ===== 超时配置 =====
    private final int fetchTimeoutMs;
    private final int checkQuorumTimeoutMs;

    // ===== 嵌套类：ReplicaState =====
    static class ReplicaState {
        final int replicaId;

        // 下一个要发送的偏移量
        long nextOffset;

        // 已确认复制的最大偏移量（核心）
        Optional<LogOffsetMetadata> matchOffset;

        // 时间戳
        long lastFetchTimestampMs;
        long lastCaughtUpTimestampMs;

        // 是否有未完成的Fetch请求
        boolean hasInflightFetch;

        // 更新匹配偏移量
        boolean updateMatchOffset(LogOffsetMetadata offset) {
            if (this.matchOffset.isPresent()) {
                LogOffsetMetadata current = this.matchOffset.get();
                if (offset.offset() < current.offset()) {
                    return false;  // 不能后退
                }
            }

            this.matchOffset = Optional.of(offset);
            this.nextOffset = offset.offset() + 1;
            return true;
        }
    }

    // ===== 核心方法1：更新副本状态 =====
    public boolean updateReplicaState(
        int replicaId,
        long fetchTimeMs,
        LogOffsetMetadata fetchOffset
    ) {
        ReplicaState state = getReplicaState(replicaId);
        if (state == null) return false;

        state.lastFetchTimestampMs = fetchTimeMs;
        state.hasInflightFetch = false;

        if (state.updateMatchOffset(fetchOffset)) {
            // 更新高水位
            updateHighWatermark();
            return true;
        }

        return false;
    }

    // ===== 核心方法2：计算高水位（最重要的算法）=====
    private void updateHighWatermark() {
        // 1. 收集所有副本的匹配偏移量
        List<Long> matchOffsets = new ArrayList<>();

        // Leader自己的偏移量
        matchOffsets.add(accumulator.logEndOffset());

        // 所有Follower的匹配偏移量
        for (ReplicaState state : voterStates.values()) {
            if (state.matchOffset.isPresent()) {
                matchOffsets.add(state.matchOffset.get().offset());
            }
        }

        // 2. 排序
        Collections.sort(matchOffsets);

        // 3. 取多数派位置
        // 例如：5个节点，多数派=3，索引=2（从0开始）
        int majorityIndex = voters.majority() - 1;

        if (matchOffsets.size() > majorityIndex) {
            long newHighWatermark = matchOffsets.get(majorityIndex);

            // 4. 更新高水位（只能前进，不能后退）
            Optional<LogOffsetMetadata> currentHW = highWatermark;
            if (!currentHW.isPresent() ||
                newHighWatermark > currentHW.get().offset()) {

                highWatermark = Optional.of(
                    new LogOffsetMetadata(newHighWatermark)
                );
            }
        }
    }

    // ===== 核心方法3：检查哪些Follower需要Fetch =====
    public Set<Integer> followersThatNeedFetch(long currentTimeMs) {
        Set<Integer> needsFetch = new HashSet<>();

        for (Map.Entry<Integer, ReplicaState> entry : voterStates.entrySet()) {
            ReplicaState state = entry.getValue();

            // 如果没有未完成的请求，且已经超时
            if (!state.hasInflightFetch &&
                currentTimeMs - state.lastFetchTimestampMs >= fetchTimeoutMs) {
                needsFetch.add(entry.getKey());
            }
        }

        return needsFetch;
    }

    // ===== 核心方法4：检查Quorum健康状态 =====
    public boolean isQuorumHealthy(long currentTimeMs) {
        int healthyFollowers = 0;

        for (ReplicaState state : voterStates.values()) {
            if (currentTimeMs - state.lastFetchTimestampMs < checkQuorumTimeoutMs) {
                healthyFollowers++;
            }
        }

        // Leader自己 + 健康的Follower >= 多数派
        return (1 + healthyFollowers) >= voters.majority();
    }

    // ===== 核心方法5：获取Follower的下一个Fetch偏移量 =====
    public long nextFetchOffset(int followerId) {
        ReplicaState state = getReplicaState(followerId);
        return state != null ? state.nextOffset : epochStartOffset;
    }
}
```

**高水位计算示例：**
```
假设：5个节点，多数派=3

各节点偏移量：
Leader(1): 100
Follower(2): 95
Follower(3): 90
Follower(4): 85
Follower(5): 80

排序：[100, 95, 90, 85, 80]

多数派索引 = 3 - 1 = 2
高水位 = matchOffsets[2] = 90

解释：偏移量90已被至少3个节点确认（节点1,2,3）
```

#### 6.2.5 其他状态类

| 类名 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|---------|---------|------|
| `ProspectiveState` | ~200 | `localId: int`<br>`epoch: int`<br>`voters: VoterSet`<br>`grantingVoters: Set<Integer>`<br>`rejectingVoters: Set<Integer>` | `recordGrantedVote()`<br>`isVoteGranted()`<br>`canGrantVote()` | Pre-Vote状态 |
| `VotedState` | ~150 | `epoch: int`<br>`votedId: int`<br>`voters: VoterSet`<br>`highWatermark: Optional<LogOffsetMetadata>` | `votedId()`<br>`canGrantVote()` | 已投票状态 |
| `ResignedState` | ~100 | `epoch: int`<br>`voters: VoterSet`<br>`preferredSuccessors: List<Integer>`<br>`highWatermark: Optional<LogOffsetMetadata>` | `preferredSuccessors()`<br>`canGrantVote()` | 辞职状态 |

---

## 阶段7: 状态管理器

**原则：** 协调所有状态转移

**预计时间：** 2-3天

### 7.1 QuorumState（状态管理器，核心中的核心）

| 项目 | 内容 |
|------|------|
| **类名** | `QuorumState` |
| **行数** | ~800 |
| **核心属性** | `localId: int`<br>`store: QuorumStateStore`<br>`voters: VoterSet`<br>`state: EpochState`<br>`electionTimeoutMs: int`<br>`fetchTimeoutMs: int` |
| **核心方法** | `transitionToUnattached(int)`<br>`transitionToFollower(int, int)`<br>`transitionToCandidate()`<br>`transitionToLeader(long, BatchAccumulator<T>)`<br>`transitionToVoted(int, int)`<br>`transitionToResigned(List<Integer>)`<br>`currentState(): EpochState`<br>`isLeader(): boolean`<br>`isFollower(): boolean` |

**QuorumState完整实现：**

```java
public class QuorumState {
    // ===== 基本信息 =====
    private final int localId;
    private final QuorumStateStore store;
    private VoterSet voters;

    // ===== 当前状态（核心）=====
    private EpochState state;

    // ===== 配置 =====
    private final int electionTimeoutMs;
    private final int fetchTimeoutMs;
    private final int retryBackoffMs;
    private final Random random;

    // ===== 核心方法：状态转移 =====

    // 1. 转移到Unattached
    public void transitionToUnattached(int epoch) {
        EpochState prevState = state;

        state = new UnattachedState(
            epoch,
            voters,
            prevState.highWatermark(),
            randomElectionTimeoutMs()
        );

        // 持久化状态
        store.writeElectionState(ElectionState.withUnknownLeader(epoch, voters.voterIds()));
    }

    // 2. 转移到Follower
    public void transitionToFollower(
        int epoch,
        int leaderId,
        long fetchTimeoutMs
    ) {
        EpochState prevState = state;

        state = new FollowerState(
            epoch,
            leaderId,
            voters,
            prevState.highWatermark(),
            fetchTimeoutMs
        );

        // 持久化状态
        store.writeElectionState(ElectionState.withElectedLeader(epoch, leaderId, voters.voterIds()));
    }

    // 3. 转移到Candidate
    public void transitionToCandidate() {
        EpochState prevState = state;
        int newEpoch = prevState.epoch() + 1;

        state = new CandidateState(
            localId,
            newEpoch,
            voters,
            prevState.highWatermark(),
            retryBackoffMs,
            randomElectionTimeoutMs()
        );

        // 持久化状态（给自己投票）
        store.writeElectionState(ElectionState.withVotedCandidate(newEpoch, localId, voters.voterIds()));
    }

    // 4. 转移到Leader
    public <T> LeaderState<T> transitionToLeader(
        long epochStartOffset,
        BatchAccumulator<T> accumulator
    ) {
        EpochState prevState = state;

        // 获取投票给自己的节点集合
        Set<Integer> grantingVoters;
        if (prevState instanceof CandidateState) {
            CandidateState candidate = (CandidateState) prevState;
            grantingVoters = candidate.grantingVoters();
        } else {
            grantingVoters = Collections.singleton(localId);
        }

        LeaderState<T> leaderState = new LeaderState<>(
            localId,
            prevState.epoch(),
            epochStartOffset,
            voters,
            grantingVoters,
            accumulator,
            fetchTimeoutMs
        );

        state = leaderState;

        // 持久化状态
        store.writeElectionState(ElectionState.withElectedLeader(
            prevState.epoch(), localId, voters.voterIds()
        ));

        return leaderState;
    }

    // 5. 转移到Voted
    public void transitionToVoted(int epoch, int candidateId) {
        EpochState prevState = state;

        state = new VotedState(
            epoch,
            candidateId,
            voters,
            prevState.highWatermark()
        );

        // 持久化状态
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, candidateId, voters.voterIds()));
    }

    // 6. 转移到Resigned
    public void transitionToResigned(List<Integer> preferredSuccessors) {
        EpochState prevState = state;

        state = new ResignedState(
            prevState.epoch(),
            voters,
            prevState.highWatermark(),
            preferredSuccessors
        );
    }

    // ===== 查询方法 =====

    public EpochState currentState() {
        return state;
    }

    public int epoch() {
        return state.epoch();
    }

    public boolean isLeader() {
        return state instanceof LeaderState;
    }

    public boolean isFollower() {
        return state instanceof FollowerState;
    }

    public boolean isCandidate() {
        return state instanceof CandidateState;
    }

    public Optional<LogOffsetMetadata> highWatermark() {
        return state.highWatermark();
    }

    // ===== 工具方法 =====

    private long randomElectionTimeoutMs() {
        return electionTimeoutMs + random.nextInt(electionTimeoutMs);
    }
}
```

---

## 阶段8: Voter管理

**预计时间：** 2-3天

### 8.1 Voter相关类

| 类名 | 位置 | 行数 | 核心属性 | 核心方法 | 说明 |
|------|------|------|---------|---------|------|
| `VoterSet` | raft | ~300 | `voters: Map<Integer, VoterNode>` | `voterIds()`<br>`isVoter(int)`<br>`majority()`<br>`addVoter()`<br>`removeVoter()` | 投票者集合 |
| `DynamicVoter` | raft | ~100 | `voterNode: VoterNode`<br>`isAdding: boolean` | `isAdding()`<br>`voterNode()` | 动态投票者 |
| `DynamicVoters` | raft | ~150 | `staticVoters: VoterSet`<br>`addingVoters: Map<Integer, VoterNode>`<br>`removingVoters: Set<Integer>` | `voterSetAtOffset(long)`<br>`isOnlyVoterInStandby(int)` | 动态投票者集合 |

---

## 阶段9: 高级功能

**预计时间：** 2-3天

### 9.1 投票者变更处理

| 类名 | 位置 | 行数 | 核心方法 | 说明 |
|------|------|------|---------|------|
| `AddVoterHandler` | raft.internals | ~200 | `handleRequest()`<br>`onAppendResult()` | 添加投票者 |
| `RemoveVoterHandler` | raft.internals | ~200 | `handleRequest()`<br>`onAppendResult()` | 移除投票者 |
| `UpdateVoterHandler` | raft.internals | ~150 | `handleRequest()` | 更新投票者 |
| `VoterSetHistory` | raft.internals | ~200 | `valueAtOrBefore(long)`<br>`addAt(long, VoterSet)` | 投票者历史 |

---

## 阶段10: 快照管理

**预计时间：** 2-3天

### 10.1 快照核心类

| 类名 | 位置 | 行数 | 核心方法 | 说明 |
|------|------|------|---------|------|
| `SnapshotReader<T>` | snapshot | ~50 | `snapshotId()`<br>`iterator()` | 快照读取器接口 |
| `SnapshotWriter<T>` | snapshot | ~50 | `append()`<br>`freeze()` | 快照写入器接口 |
| `FileRawSnapshotReader` | snapshot | ~150 | `read()` | 文件读取实现 |
| `FileRawSnapshotWriter` | snapshot | ~200 | `append()`<br>`freeze()` | 文件写入实现 |
| `RecordsSnapshotReader<T>` | snapshot | ~100 | `iterator()` | 记录快照读取 |
| `RecordsSnapshotWriter<T>` | snapshot | ~120 | `append()` | 记录快照写入 |
| `SnapshotPath` | snapshot | ~100 | `parse()`<br>`build()` | 快照路径管理 |
| `Snapshots` | snapshot | ~150 | `deleteOldSnapshots()` | 快照工具类 |

---

## 阶段11: 核心引擎 KafkaRaftClient<T> ⭐⭐⭐

**原则：** 整合所有模块，实现完整的Raft客户端

**预计时间：** 7-10天

### 11.1 KafkaRaftClient核心结构

| 项目 | 内容 |
|------|------|
| **类名** | `KafkaRaftClient<T>` |
| **行数** | ~4141 |
| **核心属性** | 50+ 个字段（见下表） |
| **核心方法** | 100+ 个方法（见下表） |

### 11.2 KafkaRaftClient核心字段表

| 分类 | 字段名 | 类型 | 说明 |
|------|--------|------|------|
| **标识** | `localId` | `int` | 本节点ID |
| | `clusterId` | `String` | 集群ID |
| **状态管理** | `quorum` | `QuorumState` | Quorum状态管理器 ⭐ |
| | `requestManager` | `RequestManager` | 请求管理器 |
| **日志** | `log` | `ReplicatedLog` | 复制日志 ⭐ |
| | `accumulator` | `BatchAccumulator<T>` | 批处理累积器 ⭐ |
| **网络** | `channel` | `NetworkChannel` | 网络通道 ⭐ |
| | `messageQueue` | `RaftMessageQueue` | 消息队列 |
| **监听器** | `listeners` | `List<RaftClient.Listener<T>>` | 事件监听器 |
| **超时** | `fetchPurgatory` | `FuturePurgatory<Long>` | Fetch超时管理 |
| | `appendPurgatory` | `FuturePurgatory<Long>` | Append超时管理 |
| **配置** | `electionTimeoutMs` | `int` | 选举超时 |
| | `fetchTimeoutMs` | `int` | Fetch超时 |
| | `appendLingerMs` | `int` | 批处理延迟 |
| **指标** | `metrics` | `KafkaRaftMetrics` | 性能指标 |
| **时间** | `time` | `Time` | 时间服务 |
| **线程** | `executor` | `ExecutorService` | 执行器 |

### 11.3 KafkaRaftClient核心方法表

#### 11.3.1 初始化和生命周期

| 方法签名 | 说明 | 实现要点 |
|---------|------|---------|
| `initialize()` | 初始化客户端 | 1. 加载持久化状态<br>2. 恢复日志<br>3. 初始化状态机 |
| `shutdown(int timeoutMs)` | 关闭客户端 | 1. 停止事件循环<br>2. 关闭网络<br>3. 刷新日志 |
| `register(Listener<T>)` | 注册监听器 | 添加到listeners列表 |

#### 11.3.2 事件循环（最核心）

| 方法签名 | 说明 | 实现要点 |
|---------|------|---------|
| `poll()` | 事件循环主方法 ⭐⭐⭐ | 1. 处理网络消息<br>2. 检查超时<br>3. Leader逻辑<br>4. Candidate逻辑<br>5. Follower逻辑 |

**poll()方法完整实现：**

```java
@Override
public void poll() {
    long currentTimeMs = time.milliseconds();

    try {
        // ===== 1. 处理网络消息 =====
        List<RaftMessage> messages = channel.poll(100);
        for (RaftMessage message : messages) {
            handleInboundMessage(message, currentTimeMs);
        }

        // ===== 2. 处理超时 =====
        maybeFireElectionTimeout(currentTimeMs);
        maybeFireFetchTimeout(currentTimeMs);

        // ===== 3. Leader特定逻辑 =====
        if (quorum.isLeader()) {
            LeaderState<T> leaderState = (LeaderState<T>) quorum.currentState();

            // 3.1 追加批次到日志
            maybeAppendBatches(leaderState, currentTimeMs);

            // 3.2 发送Fetch请求到Follower（心跳+日志复制）
            Set<Integer> needsFetch = leaderState.followersThatNeedFetch(currentTimeMs);
            for (int followerId : needsFetch) {
                sendFetchRequest(followerId, currentTimeMs);
            }

            // 3.3 检查Quorum健康
            if (!leaderState.isQuorumHealthy(currentTimeMs)) {
                log.warn("Quorum is unhealthy, may need to resign");
            }
        }

        // ===== 4. Candidate特定逻辑 =====
        else if (quorum.isCandidate()) {
            CandidateState candidate = (CandidateState) quorum.currentState();

            // 重发Vote请求到未响应的节点
            for (int voterId : candidate.unrecordedVoters()) {
                sendVoteRequest(voterId, currentTimeMs);
            }
        }

        // ===== 5. Follower特定逻辑 =====
        else if (quorum.isFollower()) {
            FollowerState follower = (FollowerState) quorum.currentState();

            // 发送Fetch请求到Leader
            if (shouldSendFetch(follower, currentTimeMs)) {
                sendFetchRequest(follower.leaderId(), currentTimeMs);
            }
        }

        // ===== 6. 通知监听器已提交的记录 =====
        maybeNotifyListeners();

    } catch (Exception e) {
        log.error("Unexpected error in poll", e);
    }
}
```

#### 11.3.3 消息处理（核心）

| 方法签名 | 说明 | 调用时机 |
|---------|------|---------|
| `handleInboundMessage(RaftMessage, long)` | 处理入站消息分发器 | poll()中 |
| `handleVoteRequest(RaftRequest.Inbound<VoteRequest>)` | 处理Vote请求 | 收到Vote请求 |
| `handleVoteResponse(RaftResponse.Inbound<VoteResponse>)` | 处理Vote响应 | 收到Vote响应 |
| `handleBeginQuorumEpochRequest(...)` | 处理BeginQuorumEpoch | Leader通知新epoch |
| `handleFetchRequest(RaftRequest.Inbound<FetchRequest>)` | 处理Fetch请求 | Follower发起Fetch |
| `handleFetchResponse(RaftResponse.Inbound<FetchResponse>)` | 处理Fetch响应 | Leader响应Fetch |

**消息处理实现示例：**

```java
// 处理Vote请求
private VoteResponse handleVoteRequest(
    RaftRequest.Inbound<VoteRequest> request,
    long currentTimeMs
) {
    VoteRequest voteRequest = request.data;
    int candidateId = voteRequest.candidateId();
    int candidateEpoch = voteRequest.candidateEpoch();

    // 1. 检查epoch
    if (candidateEpoch < quorum.epoch()) {
        return buildVoteResponse(Errors.FENCED_LEADER_EPOCH, false);
    }

    // 2. 检查日志是否最新
    OffsetAndEpoch lastEpochEndOffset = log.endOffsetForEpoch(
        voteRequest.lastOffsetEpoch()
    );

    boolean isLogUpToDate = isLogUpToDate(
        lastEpochEndOffset,
        voteRequest.lastOffset()
    );

    // 3. 询问当前状态是否可以投票
    boolean canGrantVote = quorum.currentState().canGrantVote(
        ReplicaKey.of(candidateId, voteRequest.candidateDirectoryId()),
        isLogUpToDate
    );

    if (canGrantVote) {
        // 投票并转移到Voted状态
        quorum.transitionToVoted(candidateEpoch, candidateId);
        return buildVoteResponse(Errors.NONE, true);
    } else {
        return buildVoteResponse(Errors.NONE, false);
    }
}

// 处理Vote响应
private void handleVoteResponse(
    RaftResponse.Inbound<VoteResponse> response,
    long currentTimeMs
) {
    if (!quorum.isCandidate()) {
        return;  // 只有Candidate才处理Vote响应
    }

    CandidateState candidate = (CandidateState) quorum.currentState();
    VoteResponse voteResponse = response.data;
    int responderId = response.sourceId();

    if (voteResponse.voteGranted()) {
        // 记录同意投票
        candidate.recordGrantedVote(responderId);

        // 检查是否获得多数派
        if (candidate.isVoteGranted()) {
            becomeLeader(currentTimeMs);
        }
    } else {
        // 记录拒绝投票
        candidate.recordRejectedVote(responderId, "Vote rejected");

        // 检查是否被多数派拒绝
        if (candidate.isVoteRejected()) {
            transitionToFollower(currentTimeMs);
        }
    }
}
```

#### 11.3.4 状态转移方法

| 方法签名 | 说明 | 调用时机 |
|---------|------|---------|
| `transitionToUnattached(int epoch)` | 转移到Unattached | 初始化或异常 |
| `transitionToFollower(int epoch, int leaderId)` | 转移到Follower | 发现Leader |
| `transitionToCandidate(long currentTimeMs)` | 转移到Candidate | 选举超时 |
| `transitionToLeader(long currentTimeMs)` | 转移到Leader | 赢得选举 |
| `transitionToVoted(int epoch, int candidateId)` | 转移到Voted | 投票后 |

#### 11.3.5 Leader相关方法

| 方法签名 | 说明 | 实现要点 |
|---------|------|---------|
| `scheduleAppend(int epoch, List<T> records)` | 调度追加记录 | 1. 检查是否是Leader<br>2. 调用accumulator.append() |
| `maybeAppendBatches(LeaderState<T>, long)` | 追加批次到日志 | 1. 排空accumulator<br>2. 追加到log<br>3. 更新高水位 |
| `sendFetchRequest(int followerId, long)` | 发送Fetch请求 | 1. 构建FetchRequest<br>2. channel.send() |
| `becomeLeader(long currentTimeMs)` | 成为Leader | 1. 转移状态<br>2. 追加LeaderChange记录<br>3. 通知监听器 |

**maybeAppendBatches实现：**

```java
private void maybeAppendBatches(
    LeaderState<T> leaderState,
    long currentTimeMs
) {
    // 1. 从accumulator排空已完成的批次
    List<BatchAccumulator.CompletedBatch<T>> batches = accumulator.drain();

    if (batches.isEmpty()) {
        return;
    }

    // 2. 追加到日志
    for (BatchAccumulator.CompletedBatch<T> batch : batches) {
        LogAppendInfo info = log.appendAsLeader(
            batch.data,
            leaderState.epoch()
        );

        // 3. 记录待确认的批次
        leaderState.addPendingBatch(batch);
    }

    // 4. 刷新日志（可选）
    log.flush(false);
}
```

#### 11.3.6 Follower相关方法

| 方法签名 | 说明 | 实现要点 |
|---------|------|---------|
| `handleFetchRequestAsFollower(...)` | 处理Fetch请求（作为Follower） | 不应该收到Fetch请求 |
| `handleFetchResponseAsFollower(...)` | 处理Fetch响应（作为Follower） | 1. 追加日志<br>2. 更新高水位<br>3. 重置超时 |

#### 11.3.7 选举相关方法

| 方法签名 | 说明 | 实现要点 |
|---------|------|---------|
| `maybeFireElectionTimeout(long)` | 检查选举超时 | 1. 检查超时<br>2. 发起选举 |
| `initiateElection(long)` | 发起选举 | 1. 递增epoch<br>2. 转移到Candidate<br>3. 发送Vote请求 |
| `isLogUpToDate(...)` | 检查日志是否最新 | Raft日志比较算法 |

#### 11.3.8 工具方法

| 方法签名 | 说明 |
|---------|------|
| `buildVoteRequest()` | 构建Vote请求 |
| `buildFetchRequest(int followerId)` | 构建Fetch请求 |
| `buildVoteResponse(Errors, boolean)` | 构建Vote响应 |
| `buildFetchResponse(...)` | 构建Fetch响应 |
| `notifyListeners()` | 通知监听器 |

---

## 📊 完整实现顺序总结表

| 阶段 | 天数 | 核心类数量 | 关键类 | 说明 |
|------|------|-----------|--------|------|
| 阶段1 | 2-3 | 12 | OffsetAndEpoch, LogOffsetMetadata | 基础数据结构，无依赖 |
| 阶段2 | 1-2 | 6 | RaftClient, EpochState | 核心接口定义 |
| 阶段3 | 2-3 | 3 | QuorumConfig, FileQuorumStateStore | 配置和存储 |
| 阶段4 | 3-4 | 8 | KafkaNetworkChannel, RequestManager | 网络和消息 |
| 阶段5 | 3-4 | 5 | BatchAccumulator, BatchBuilder | 批处理系统 ⭐ |
| 阶段6 | 7-10 | 7 | LeaderState, CandidateState | 状态机 ⭐⭐⭐ |
| 阶段7 | 2-3 | 2 | QuorumState, KafkaRaftMetrics | 状态管理器 |
| 阶段8 | 2-3 | 3 | VoterSet, DynamicVoters | Voter管理 |
| 阶段9 | 2-3 | 6 | AddVoterHandler, VoterSetHistory | 高级功能 |
| 阶段10 | 2-3 | 11 | FileRawSnapshotWriter/Reader | 快照管理 |
| 阶段11 | 7-10 | 1 | KafkaRaftClient ⭐⭐⭐ | 核心引擎 |
| **总计** | **25-35** | **84** | - | - |

---

## 🎯 关键实现建议

### 1. 实现顺序严格遵守
- 必须按照11个阶段的顺序实现
- 每个阶段完成后进行充分测试
- 不要跳过任何阶段

### 2. 三个最关键的实现点

1. **BatchAccumulator<T>（阶段5）** ⭐
   - 性能的关键
   - 需要精确的内存管理和线程安全
   - 预计3-4天

2. **LeaderState<T>（阶段6）** ⭐⭐⭐
   - 最复杂的状态类（1050行）
   - 高水位计算算法是核心
   - 预计3-4天

3. **KafkaRaftClient<T>（阶段11）** ⭐⭐⭐
   - 整合所有模块
   - poll()方法是整个系统的心脏
   - 预计7-10天

### 3. 测试策略

每个阶段完成后：
- [ ] 单元测试覆盖率>80%
- [ ] 所有公共方法有测试
- [ ] 边界情况有覆盖
- [ ] 集成测试通过

---

## 📝 实现检查清单

### 阶段1-5基础模块
- [ ] 所有基础数据结构实现并测试通过
- [ ] 所有接口定义完整
- [ ] 配置和存储可用
- [ ] 网络通信可用
- [ ] 批处理系统完整实现

### 阶段6-7核心模块
- [ ] 7个状态类全部实现
- [ ] 状态转移逻辑正确
- [ ] QuorumState协调正确
- [ ] 高水位计算正确

### 阶段8-10高级模块
- [ ] Voter管理完整
- [ ] 成员变更可用
- [ ] 快照读写正确

### 阶段11集成
- [ ] KafkaRaftClient完整实现
- [ ] 事件循环工作正常
- [ ] 所有消息处理正确
- [ ] 3节点集群测试通过
- [ ] 5节点集群测试通过
- [ ] 故障恢复测试通过

---

祝你实现顺利！🚀
