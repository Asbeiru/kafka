# KRaft 模块化学习指南

**学习目标：** 按功能模块深入理解 Kafka Raft 实现，所有类名、方法签名和实现与 Kafka 源码完全一致。

**核心原则：**
- ✅ 类名必须一致
- ✅ 方法签名必须一致
- ✅ 核心实现逻辑必须一致
- ✅ 理解每个模块的设计思想

---

## 学习路线图

```
模块1: 基础设施 (Foundation)
   ↓
模块2: 状态机 (State Machine)
   ↓
模块3: 选举机制 (Election)
   ↓
模块4: 日志复制 (Log Replication)
   ↓
模块5: 批处理系统 (Batching)
   ↓
模块6: 网络通信 (Networking)
   ↓
模块7: 快照管理 (Snapshot)
   ↓
模块8: 成员变更 (Membership)
   ↓
模块9: 监控指标 (Metrics)
```

---

## 模块 1: 基础设施 (Foundation) ⭐

**学习目标：** 理解 Raft 的基础数据结构、配置和工具类

**预计时间：** 1-2 周

### 1.1 核心接口定义

#### 文件清单（5个核心接口）

| 文件 | 位置 | 行数 | 用途 |
|------|------|------|------|
| `RaftClient.java` | `org.apache.kafka.raft` | ~150 | Raft客户端主接口 |
| `EpochState.java` | `org.apache.kafka.raft` | ~80 | 状态基接口 |
| `ReplicatedLog.java` | `org.apache.kafka.raft` | ~120 | 复制日志接口 |
| `NetworkChannel.java` | `org.apache.kafka.raft` | ~50 | 网络通道接口 |
| `RaftMessage.java` | `org.apache.kafka.raft` | ~30 | 消息基接口 |

#### 1.1.1 RaftClient 接口

**核心方法签名（必须一致）：**

```java
package org.apache.kafka.raft;

public interface RaftClient<T> extends Closeable {

    // 初始化
    void initialize() throws Exception;

    // 注册监听器
    void register(Listener<T> listener);

    // Leader写入（追加记录）
    long scheduleAppend(int epoch, List<T> records);

    // 读取记录
    Records read(long startOffset, Isolation isolation);

    // 创建快照
    void createSnapshot(OffsetAndEpoch snapshotId, long lastContainedLogTimestamp);

    // 状态查询
    LeaderAndEpoch leaderAndEpoch();
    OptionalInt currentLeader();
    int currentEpoch();

    // 事件循环
    void poll();

    // 优雅关闭
    void resign(int epoch);
    void shutdown(int timeoutMs) throws InterruptedException;

    // 监听器接口
    interface Listener<T> {
        void handleCommit(BatchReader<T> reader);
        void handleLoadSnapshot(SnapshotReader<T> reader);
        void handleLeaderChange(LeaderAndEpoch leader);
    }
}
```

**设计思想：**
- **事件驱动**：通过 `poll()` 循环处理所有事件
- **回调机制**：通过 `Listener` 通知上层应用
- **角色分离**：Leader 可写（scheduleAppend），所有角色可读

#### 1.1.2 EpochState 接口

**核心方法签名：**

```java
package org.apache.kafka.raft;

public interface EpochState {

    // 获取当前epoch
    int epoch();

    // 获取选举状态
    ElectionState election();

    // 选举超时
    long electionTimeoutMs(long currentTimeMs);

    // 投票决策
    boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate);

    // Leader信息
    Optional<LogOffsetMetadata> highWatermark();
    boolean canGrantVote(int candidateId, boolean isLogUpToDate);
}
```

**设计思想：**
- **状态多态**：不同状态有不同的选举超时和投票策略
- **封装性**：每个状态独立管理自己的数据

#### 1.1.3 ReplicatedLog 接口

**核心方法签名：**

```java
package org.apache.kafka.raft;

public interface ReplicatedLog extends Closeable {

    // 读取日志
    LogFetchInfo read(long startOffset, Isolation isolation);

    // Leader追加
    LogAppendInfo appendAsLeader(Records records, int epoch);

    // Follower追加
    LogAppendInfo appendAsFollower(Records records, int epoch, long baseOffset);

    // 截断操作
    boolean truncateTo(long offset);
    boolean truncateToEndOffset(OffsetAndEpoch endOffset);

    // 偏移量查询
    LogOffsetMetadata endOffset();
    long startOffset();
    OffsetAndEpoch endOffsetForEpoch(int epoch);

    // 高水位管理
    void updateHighWatermark(LogOffsetMetadata highWatermark);
    Optional<LogOffsetMetadata> highWatermark();

    // 刷盘
    void flush(boolean forceFlush);
}
```

**设计思想：**
- **角色区分**：Leader 和 Follower 有不同的追加方法
- **一致性保证**：通过高水位确保只读已提交数据
- **持久化**：支持刷盘操作

### 1.2 核心数据结构

#### 文件清单（12个类）

| 文件 | 用途 | 关键字段 |
|------|------|----------|
| `LeaderAndEpoch.java` | Leader信息 | `leaderId`, `epoch` |
| `LogOffsetMetadata.java` | 偏移量元数据 | `offset`, `segmentBaseOffset` |
| `OffsetAndEpoch.java` | 偏移量和Epoch | `offset`, `epoch` |
| `ValidOffsetAndEpoch.java` | 有效偏移量 | - |
| `ElectionState.java` | 选举状态 | `epoch`, `voters`, `leaderIdOpt` |
| `Endpoints.java` | 端点信息 | `listeners` |
| `ReplicaKey.java` | 副本标识 | `replicaId`, `replicaDirectoryId` |
| `Isolation.java` | 隔离级别 | `COMMITTED`, `UNCOMMITTED` |
| `LogAppendInfo.java` | 日志追加信息 | - |
| `LogFetchInfo.java` | 日志获取信息 | - |
| `Batch.java` | 批次 | `baseOffset`, `epoch`, `records` |
| `BatchReader.java` | 批次读取器接口 | - |

#### 1.2.1 LeaderAndEpoch 类

**完整实现：**

```java
package org.apache.kafka.raft;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Leader 和 Epoch 的不可变组合
 */
public class LeaderAndEpoch {

    private final OptionalInt leaderId;
    private final int epoch;

    public LeaderAndEpoch(OptionalInt leaderId, int epoch) {
        this.leaderId = Objects.requireNonNull(leaderId);
        this.epoch = epoch;
    }

    public static LeaderAndEpoch noLeaderOrEpoch() {
        return new LeaderAndEpoch(OptionalInt.empty(), -1);
    }

    public OptionalInt leaderId() {
        return leaderId;
    }

    public Optional<Integer> leader() {
        return leaderId.isPresent() ?
            Optional.of(leaderId.getAsInt()) : Optional.empty();
    }

    public int epoch() {
        return epoch;
    }

    public boolean isLeader(int nodeId) {
        return leaderId.isPresent() && leaderId.getAsInt() == nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LeaderAndEpoch that = (LeaderAndEpoch) o;
        return epoch == that.epoch && leaderId.equals(that.leaderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderId, epoch);
    }

    @Override
    public String toString() {
        return "LeaderAndEpoch(" +
            "leaderId=" + leaderId +
            ", epoch=" + epoch +
            ')';
    }
}
```

**设计思想：**
- **不可变性**：线程安全，避免意外修改
- **Optional 包装**：优雅处理无 Leader 的情况

#### 1.2.2 LogOffsetMetadata 类

**核心字段：**

```java
package org.apache.kafka.raft;

public class LogOffsetMetadata {

    // 逻辑偏移量
    private final long offset;

    // 段文件的基础偏移量
    private final Optional<Long> segmentBaseOffset;

    // 在段文件中的物理位置
    private final Optional<Integer> relativePositionInSegment;

    // 构造方法
    public LogOffsetMetadata(long offset) {
        this(offset, Optional.empty(), Optional.empty());
    }

    public LogOffsetMetadata(
        long offset,
        Optional<Long> segmentBaseOffset,
        Optional<Integer> relativePositionInSegment
    ) {
        this.offset = offset;
        this.segmentBaseOffset = segmentBaseOffset;
        this.relativePositionInSegment = relativePositionInSegment;
    }

    // 核心方法
    public long offset() { return offset; }

    public Optional<Long> segmentBaseOffset() {
        return segmentBaseOffset;
    }

    public Optional<Integer> relativePositionInSegment() {
        return relativePositionInSegment;
    }

    // 用于比较偏移量
    public int compareTo(LogOffsetMetadata other) {
        return Long.compare(this.offset, other.offset);
    }
}
```

**设计思想：**
- **物理+逻辑**：既有逻辑偏移量，又有物理位置信息
- **性能优化**：避免反复查找段文件

### 1.3 配置管理

#### 1.3.1 QuorumConfig 类

**完整实现（必须一致）：**

```java
package org.apache.kafka.raft;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class QuorumConfig extends AbstractConfig {

    // 配置项定义
    public static final String QUORUM_VOTERS_CONFIG = "controller.quorum.voters";
    public static final String QUORUM_ELECTION_TIMEOUT_MS_CONFIG =
        "controller.quorum.election.timeout.ms";
    public static final String QUORUM_FETCH_TIMEOUT_MS_CONFIG =
        "controller.quorum.fetch.timeout.ms";
    public static final String QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG =
        "controller.quorum.election.backoff.max.ms";
    public static final String QUORUM_APPEND_LINGER_MS_CONFIG =
        "controller.quorum.append.linger.ms";
    public static final String QUORUM_REQUEST_TIMEOUT_MS_CONFIG =
        "controller.quorum.request.timeout.ms";
    public static final String QUORUM_RETRY_BACKOFF_MS_CONFIG =
        "controller.quorum.retry.backoff.ms";

    // 默认值
    public static final int DEFAULT_QUORUM_ELECTION_TIMEOUT_MS = 1000;
    public static final int DEFAULT_QUORUM_FETCH_TIMEOUT_MS = 2000;
    public static final int DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS = 1000;
    public static final int DEFAULT_QUORUM_APPEND_LINGER_MS = 25;
    public static final int DEFAULT_QUORUM_REQUEST_TIMEOUT_MS = 2000;
    public static final int DEFAULT_QUORUM_RETRY_BACKOFF_MS = 20;

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(QUORUM_VOTERS_CONFIG,
                ConfigDef.Type.LIST,
                "",
                ConfigDef.Importance.HIGH,
                "投票者列表")
        .define(QUORUM_ELECTION_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_QUORUM_ELECTION_TIMEOUT_MS,
                ConfigDef.Importance.HIGH,
                "选举超时时间")
        .define(QUORUM_FETCH_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_QUORUM_FETCH_TIMEOUT_MS,
                ConfigDef.Importance.MEDIUM,
                "Fetch超时时间")
        .define(QUORUM_APPEND_LINGER_MS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_QUORUM_APPEND_LINGER_MS,
                ConfigDef.Importance.MEDIUM,
                "批处理延迟时间");

    public QuorumConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    // Getter 方法
    public int electionTimeoutMs() {
        return getInt(QUORUM_ELECTION_TIMEOUT_MS_CONFIG);
    }

    public int fetchTimeoutMs() {
        return getInt(QUORUM_FETCH_TIMEOUT_MS_CONFIG);
    }

    public int electionBackoffMaxMs() {
        return getInt(QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG);
    }

    public int appendLingerMs() {
        return getInt(QUORUM_APPEND_LINGER_MS_CONFIG);
    }

    public int requestTimeoutMs() {
        return getInt(QUORUM_REQUEST_TIMEOUT_MS_CONFIG);
    }

    public int retryBackoffMs() {
        return getInt(QUORUM_RETRY_BACKOFF_MS_CONFIG);
    }
}
```

### 1.4 工具类

#### 1.4.1 RaftUtil 类

**位置：** `org.apache.kafka.raft.RaftUtil`
**行数：** ~950 行
**用途：** 提供各种工具方法

**核心方法签名：**

```java
package org.apache.kafka.raft;

public class RaftUtil {

    // 日志比较（用于选举）
    public static boolean hasValidLeaderEpoch(
        OffsetAndEpoch endOffset,
        int epoch,
        LogOffsetMetadata localEndOffset
    );

    // 创建控制记录
    public static SimpleRecord createLeaderChangeRecord(
        int leaderId,
        List<Voter> voters
    );

    // 解析投票者配置
    public static Map<Integer, AddressSpec> parseVoterConnections(
        List<String> voterConnections
    );

    // 计算随机退避时间
    public static int randomBackoff(
        Random random,
        int retryBackoffMs,
        int retryBackoffMaxMs
    );

    // 其他工具方法...
}
```

### 1.5 学习检查清单

**模块1完成标准：**

- [ ] 理解 `RaftClient` 接口的设计思想
- [ ] 理解 `EpochState` 的多态性
- [ ] 理解 `ReplicatedLog` 的 Leader/Follower 区别
- [ ] 实现所有基础数据结构类
- [ ] 实现 `QuorumConfig` 配置管理
- [ ] 理解高水位（High Watermark）的概念
- [ ] 理解 Epoch 和 Term 的关系

**实践练习：**

```java
// 练习1: 创建一个简单的配置
Properties props = new Properties();
props.put("controller.quorum.election.timeout.ms", "1000");
QuorumConfig config = new QuorumConfig(props);
assert config.electionTimeoutMs() == 1000;

// 练习2: 理解 LeaderAndEpoch
LeaderAndEpoch leader1 = new LeaderAndEpoch(OptionalInt.of(1), 5);
assert leader1.isLeader(1);
assert !leader1.isLeader(2);

// 练习3: 理解偏移量元数据
LogOffsetMetadata offset = new LogOffsetMetadata(100L);
assert offset.offset() == 100L;
```

---

## 模块 2: 状态机 (State Machine) ⭐⭐⭐

**学习目标：** 深入理解 Raft 状态机的所有状态和状态转移逻辑

**预计时间：** 3-4 周

**核心思想：** Raft 将每个节点建模为一个状态机，通过明确的状态转移规则来实现共识

### 2.1 状态体系结构

```
EpochState (接口)
    ├── UnattachedState      # 未附加状态
    ├── FollowerState        # 跟随者状态
    ├── ResignedState        # 辞职状态
    ├── VotedState           # 已投票状态
    └── NomineeState (接口)  # 候选人基接口
        ├── ProspectiveState # 预选举状态
        ├── CandidateState   # 候选人状态
        └── LeaderState<T>   # 领导者状态
```

### 2.2 状态转移图

```
初始化
  ↓
UnattachedState ──(超时)──→ ProspectiveState ──(Pre-Vote成功)──→ CandidateState
  ↑                                                                    |
  |                                                                    |
  |                          ←─────(选举失败)─────────────────────────+
  |                          |                                         |
  |                          |                     (选举成功)          ↓
  |                     VotedState ←──(收到投票请求)           LeaderState
  |                          |                                         |
  |                          |                                         |
  |                          ↓                                         |
  +──────────────────→ FollowerState ←──(发现新Leader)────────────────+
                             |                                         |
                             |                    (优雅退出)            ↓
                             +──────────────────────────────→ ResignedState
```

### 2.3 状态类实现

#### 2.3.1 UnattachedState（最简单，从这里开始）

**文件：** `org.apache.kafka.raft.UnattachedState`
**行数：** ~100 行

**完整实现：**

```java
package org.apache.kafka.raft;

import java.util.Optional;
import java.util.Set;

/**
 * 未附加状态
 *
 * 节点的初始状态，或者从其他状态回退到的状态。
 * 在此状态下，节点：
 * 1. 不知道当前的 Leader
 * 2. 等待选举超时后发起选举
 * 3. 可以响应其他节点的投票请求
 */
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
        // Unattached 状态可以投票给任何在投票者集合中的候选人
        // 前提是候选人的日志是最新的
        return voters.isVoter(candidateKey) && isLogUpToDate;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    public String name() {
        return "Unattached";
    }

    @Override
    public String toString() {
        return "UnattachedState(" +
            "epoch=" + epoch +
            ", voters=" + voters +
            ", highWatermark=" + highWatermark +
            ", electionTimeoutMs=" + electionTimeoutMs +
            ')';
    }
}
```

**设计思想：**
- **被动等待**：不主动发起任何操作，等待超时
- **开放投票**：可以投票给任何合格的候选人
- **无 Leader 信息**：不知道当前 Leader

#### 2.3.2 FollowerState（核心状态）

**文件：** `org.apache.kafka.raft.FollowerState`
**行数：** ~200 行

**完整实现：**

```java
package org.apache.kafka.raft;

import java.util.Optional;
import java.util.Set;

/**
 * Follower 状态
 *
 * 这是 Raft 中最常见的状态。Follower：
 * 1. 从 Leader 接收日志条目
 * 2. 响应 Leader 的心跳
 * 3. 如果长时间没收到 Leader 心跳，则发起选举
 */
public class FollowerState implements EpochState {

    private final int fetchTimeoutMs;
    private final int epoch;
    private final int leaderId;
    private final VoterSet voters;
    private final Optional<LogOffsetMetadata> highWatermark;
    private final long fetchTimeoutMs;

    public FollowerState(
        int fetchTimeoutMs,
        int epoch,
        int leaderId,
        VoterSet voters,
        Optional<LogOffsetMetadata> highWatermark
    ) {
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.voters = voters;
        this.highWatermark = highWatermark;
    }

    @Override
    public int epoch() {
        return epoch;
    }

    public int leaderId() {
        return leaderId;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(
            epoch,
            leaderId,
            voters.voterIds()
        );
    }

    @Override
    public long electionTimeoutMs(long currentTimeMs) {
        // Follower 使用 Fetch 超时作为选举超时
        // 如果在此时间内没收到 Leader 的消息，则发起选举
        return fetchTimeoutMs;
    }

    @Override
    public boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate) {
        // Follower 通常不会投票，因为它已经有一个 Leader
        // 但如果候选人的 epoch 更大，可能会投票
        return false;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    public String name() {
        return "Follower";
    }

    @Override
    public String toString() {
        return "FollowerState(" +
            "epoch=" + epoch +
            ", leaderId=" + leaderId +
            ", voters=" + voters +
            ", highWatermark=" + highWatermark +
            ", fetchTimeoutMs=" + fetchTimeoutMs +
            ')';
    }
}
```

**设计思想：**
- **被动跟随**：只响应 Leader 的请求
- **心跳检测**：通过 Fetch 超时检测 Leader 是否存活
- **快速转换**：超时后立即转换为 Candidate

#### 2.3.3 CandidateState（选举状态）

**文件：** `org.apache.kafka.raft.CandidateState`
**行数：** ~300 行

**核心字段和方法：**

```java
package org.apache.kafka.raft;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Candidate 状态
 *
 * 节点发起选举时进入此状态。Candidate：
 * 1. 递增自己的 epoch
 * 2. 给自己投票
 * 3. 向所有其他节点发送 Vote 请求
 * 4. 等待多数派的投票
 */
public class CandidateState implements NomineeState {

    private final int localId;
    private final int epoch;
    private final VoterSet voters;
    private final Optional<LogOffsetMetadata> highWatermark;
    private final int retryBackoffMs;
    private final int requestTimeoutMs;

    // 记录每个节点的投票结果
    private final Map<Integer, Boolean> grantingVoters;

    // 记录拒绝投票的原因
    private final Map<Integer, String> rejectingVoters;

    // 未响应的投票者
    private final Set<Integer> unrecordedVoters;

    // 选举开始时间
    private final long electionTimeoutMs;

    public CandidateState(
        int localId,
        int epoch,
        VoterSet voters,
        Optional<LogOffsetMetadata> highWatermark,
        int retryBackoffMs,
        int requestTimeoutMs,
        long electionTimeoutMs
    ) {
        this.localId = localId;
        this.epoch = epoch;
        this.voters = voters;
        this.highWatermark = highWatermark;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.electionTimeoutMs = electionTimeoutMs;

        this.grantingVoters = new HashMap<>();
        this.rejectingVoters = new HashMap<>();
        this.unrecordedVoters = voters.voterIds();

        // 给自己投票
        recordGrantedVote(localId);
    }

    /**
     * 记录同意投票
     */
    public boolean recordGrantedVote(int voterId) {
        if (!voters.isVoter(voterId)) {
            return false;
        }

        grantingVoters.put(voterId, true);
        unrecordedVoters.remove(voterId);
        rejectingVoters.remove(voterId);

        return true;
    }

    /**
     * 记录拒绝投票
     */
    public boolean recordRejectedVote(int voterId, String reason) {
        if (!voters.isVoter(voterId)) {
            return false;
        }

        rejectingVoters.put(voterId, reason);
        unrecordedVoters.remove(voterId);
        grantingVoters.remove(voterId);

        return true;
    }

    /**
     * 是否已获得多数派投票
     */
    public boolean isVoteGranted() {
        return grantingVoters.size() >= voters.majority();
    }

    /**
     * 是否已被多数派拒绝
     */
    public boolean isVoteRejected() {
        return rejectingVoters.size() >= voters.majority();
    }

    /**
     * 未响应的投票者集合
     */
    public Set<Integer> unrecordedVoters() {
        return unrecordedVoters;
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withVotedCandidate(
            epoch,
            localId,
            voters.voterIds()
        );
    }

    @Override
    public long electionTimeoutMs(long currentTimeMs) {
        return electionTimeoutMs;
    }

    @Override
    public boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate) {
        // Candidate 不会投票给其他候选人
        return false;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    public String name() {
        return "Candidate";
    }

    @Override
    public String toString() {
        return "CandidateState(" +
            "localId=" + localId +
            ", epoch=" + epoch +
            ", grantingVoters=" + grantingVoters.size() +
            ", rejectingVoters=" + rejectingVoters.size() +
            ", unrecordedVoters=" + unrecordedVoters.size() +
            ')';
    }
}
```

**设计思想：**
- **主动拉票**：向所有节点发送 Vote 请求
- **多数派决策**：需要获得多数派投票才能成为 Leader
- **快速失败**：如果被多数派拒绝，立即退出选举

#### 2.3.4 LeaderState（最复杂，核心中的核心）⭐⭐⭐

**文件：** `org.apache.kafka.raft.LeaderState`
**行数：** ~1,050 行

**核心职责：**
1. 接收客户端写入请求
2. 复制日志到所有 Follower
3. 计算和更新高水位
4. 发送心跳维持领导地位

**核心字段：**

```java
package org.apache.kafka.raft;

import java.util.*;

public class LeaderState<T> implements EpochState {

    private final int localId;
    private final int epoch;
    private final long epochStartOffset;

    // 投票者集合
    private final VoterSet voters;

    // 高水位（已被多数派确认的最大偏移量）
    private Optional<LogOffsetMetadata> highWatermark;

    // 每个 Follower 的复制状态
    private final Map<Integer, ReplicaState> voterStates;
    private final Map<Integer, ReplicaState> observerStates;

    // 待确认的批次
    private final Map<Long, BatchAcceptors> pendingBatches;

    // 累积器（用于批处理）
    private final BatchAccumulator<T> accumulator;

    // 时间配置
    private final int fetchTimeoutMs;
    private final int checkQuorumTimeoutMs;

    /**
     * ReplicaState - 跟踪每个副本的复制状态
     */
    static class ReplicaState {
        final int replicaId;

        // 已知的副本端偏移量（下一个要发送的偏移量）
        long nextOffset;

        // 已知的副本匹配偏移量（已确认复制的最大偏移量）
        Optional<LogOffsetMetadata> matchOffset;

        // 最后一次 Fetch 时间
        long lastFetchTimestampMs;

        // 最后一次响应时间
        long lastCaughtUpTimestampMs;

        // 是否有未完成的 Fetch 请求
        boolean hasInflightFetch;

        ReplicaState(int replicaId, long nextOffset) {
            this.replicaId = replicaId;
            this.nextOffset = nextOffset;
            this.matchOffset = Optional.empty();
            this.lastFetchTimestampMs = 0;
            this.lastCaughtUpTimestampMs = 0;
            this.hasInflightFetch = false;
        }

        /**
         * 更新匹配偏移量（当收到 Fetch 响应时）
         */
        boolean updateMatchOffset(LogOffsetMetadata matchOffset) {
            if (this.matchOffset.isPresent()) {
                LogOffsetMetadata currentMatch = this.matchOffset.get();
                if (matchOffset.offset() < currentMatch.offset()) {
                    return false;
                }
            }

            this.matchOffset = Optional.of(matchOffset);
            this.nextOffset = matchOffset.offset() + 1;
            return true;
        }
    }

    /**
     * 构造 Leader 状态
     */
    public LeaderState(
        int localId,
        int epoch,
        long epochStartOffset,
        VoterSet voters,
        Set<Integer> grantingVoters,
        BatchAccumulator<T> accumulator,
        int fetchTimeoutMs
    ) {
        this.localId = localId;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;
        this.voters = voters;
        this.accumulator = accumulator;
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.checkQuorumTimeoutMs = fetchTimeoutMs;

        // 初始化高水位为 epoch 开始偏移量
        this.highWatermark = Optional.of(
            new LogOffsetMetadata(epochStartOffset)
        );

        // 初始化所有 Follower 的状态
        this.voterStates = new HashMap<>();
        this.observerStates = new HashMap<>();
        this.pendingBatches = new HashMap<>();

        for (int voterId : voters.voterIds()) {
            if (voterId != localId) {
                voterStates.put(
                    voterId,
                    new ReplicaState(voterId, epochStartOffset)
                );
            }
        }
    }

    /**
     * 更新副本的匹配偏移量（核心方法）
     */
    public boolean updateReplicaState(
        int replicaId,
        long fetchOffsetMs,
        LogOffsetMetadata fetchOffset
    ) {
        ReplicaState state = getReplicaState(replicaId);
        if (state == null) {
            return false;
        }

        state.lastFetchTimestampMs = fetchOffsetMs;
        state.hasInflightFetch = false;

        if (state.updateMatchOffset(fetchOffset)) {
            // 更新高水位
            updateHighWatermark();
            return true;
        }

        return false;
    }

    /**
     * 计算新的高水位（核心算法）
     *
     * 高水位是所有副本中，被多数派确认的最大偏移量。
     * 计算方式：
     * 1. 收集所有副本的匹配偏移量
     * 2. 按偏移量排序
     * 3. 取第 (n/2 + 1) 个偏移量
     */
    private void updateHighWatermark() {
        // 收集所有匹配偏移量
        List<Long> matchOffsets = new ArrayList<>();

        // 添加 Leader 自己的偏移量（始终是最新的）
        matchOffsets.add(accumulator.logEndOffset());

        // 添加所有 Follower 的匹配偏移量
        for (ReplicaState state : voterStates.values()) {
            if (state.matchOffset.isPresent()) {
                matchOffsets.add(state.matchOffset.get().offset());
            }
        }

        // 排序
        Collections.sort(matchOffsets);

        // 多数派的索引（从0开始）
        int majorityIndex = voters.majority() - 1;

        if (matchOffsets.size() > majorityIndex) {
            long newHighWatermark = matchOffsets.get(majorityIndex);

            Optional<LogOffsetMetadata> currentHW = highWatermark;
            if (!currentHW.isPresent() ||
                newHighWatermark > currentHW.get().offset()) {

                highWatermark = Optional.of(
                    new LogOffsetMetadata(newHighWatermark)
                );
            }
        }
    }

    /**
     * 检查是否需要发送 Fetch 请求
     */
    public Set<Integer> followersThatNeedFetch(long currentTimeMs) {
        Set<Integer> needsFetch = new HashSet<>();

        for (Map.Entry<Integer, ReplicaState> entry : voterStates.entrySet()) {
            ReplicaState state = entry.getValue();

            // 如果没有未完成的请求，且上次 Fetch 已超时
            if (!state.hasInflightFetch &&
                currentTimeMs - state.lastFetchTimestampMs >= fetchTimeoutMs) {
                needsFetch.add(entry.getKey());
            }
        }

        return needsFetch;
    }

    /**
     * 检查 Quorum 是否健康
     */
    public boolean isQuorumHealthy(long currentTimeMs) {
        int healthyFollowers = 0;

        for (ReplicaState state : voterStates.values()) {
            if (currentTimeMs - state.lastFetchTimestampMs < checkQuorumTimeoutMs) {
                healthyFollowers++;
            }
        }

        // Leader 自己 + 健康的 Follower >= 多数派
        return (1 + healthyFollowers) >= voters.majority();
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(
            epoch,
            localId,
            voters.voterIds()
        );
    }

    @Override
    public long electionTimeoutMs(long currentTimeMs) {
        // Leader 不需要选举超时
        return Long.MAX_VALUE;
    }

    @Override
    public boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate) {
        // Leader 不会投票给其他候选人
        return false;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    public String name() {
        return "Leader";
    }

    private ReplicaState getReplicaState(int replicaId) {
        ReplicaState state = voterStates.get(replicaId);
        if (state == null) {
            state = observerStates.get(replicaId);
        }
        return state;
    }
}
```

**核心算法解析：**

1. **高水位计算算法：**
```
假设有 5 个节点 (1, 2, 3, 4, 5)，多数派 = 3

节点偏移量：
Leader (1):  100
Follower 2:  95
Follower 3:  90
Follower 4:  85
Follower 5:  80

排序：[100, 95, 90, 85, 80]

多数派索引 = 3 - 1 = 2（从0开始）
高水位 = 排序后[2] = 90

解释：偏移量 90 已被至少 3 个节点确认（1, 2, 3）
```

2. **日志复制流程：**
```
1. Leader 收到写入请求
2. Leader 追加到本地日志
3. Leader 向所有 Follower 发送 Fetch 响应（包含新日志）
4. Follower 追加到本地日志并响应
5. Leader 收到响应后更新 matchOffset
6. Leader 重新计算高水位
7. 如果高水位前进，则通知应用层可以读取
```

#### 2.3.5 其他状态类

**ProspectiveState（Pre-Vote）：**
- **用途**：实现 Pre-Vote 机制，避免不必要的选举
- **行数**：~200 行
- **核心逻辑**：在真正发起选举前，先询问其他节点是否愿意投票

**VotedState（已投票）：**
- **用途**：记录已经投票给某个候选人
- **行数**：~150 行
- **核心逻辑**：在同一个 epoch 内只能投票给一个候选人

**ResignedState（辞职）：**
- **用途**：Leader 优雅退出时进入的状态
- **行数**：~100 行
- **核心逻辑**：通知所有 Follower 需要发起新的选举

### 2.4 状态管理器：QuorumState

**文件：** `org.apache.kafka.raft.QuorumState`
**行数：** ~800 行
**用途：** 管理状态转移

**核心方法签名：**

```java
package org.apache.kafka.raft;

public class QuorumState {

    private final int localId;
    private final QuorumStateStore store;
    private final VoterSet voters;
    private final int electionTimeoutMs;
    private final int fetchTimeoutMs;

    // 当前状态
    private EpochState state;

    /**
     * 转移到 Unattached 状态
     */
    public void transitionToUnattached(int epoch);

    /**
     * 转移到 Follower 状态
     */
    public void transitionToFollower(
        int epoch,
        int leaderId,
        long fetchTimeoutMs
    );

    /**
     * 转移到 Candidate 状态
     */
    public void transitionToCandidate(long electionTimeoutMs);

    /**
     * 转移到 Leader 状态
     */
    public <T> LeaderState<T> transitionToLeader(
        long epochStartOffset,
        BatchAccumulator<T> accumulator
    );

    /**
     * 转移到 Resigned 状态
     */
    public void transitionToResigned(List<Integer> preferredSuccessors);

    /**
     * 获取当前状态
     */
    public EpochState currentState();

    /**
     * 是否是 Leader
     */
    public boolean isLeader();

    /**
     * 是否是 Follower
     */
    public boolean isFollower();

    /**
     * 是否是 Candidate
     */
    public boolean isCandidate();
}
```

### 2.5 学习检查清单

**模块2完成标准：**

- [ ] 实现所有 7 个状态类
- [ ] 理解状态转移的触发条件
- [ ] 理解高水位的计算算法
- [ ] 理解 Pre-Vote 机制的作用
- [ ] 实现 QuorumState 状态管理器
- [ ] 能够画出完整的状态转移图
- [ ] 能够解释为什么需要这么多状态

**实践练习：**

```java
// 练习1: 测试状态转移
QuorumState quorum = new QuorumState(...);

// 初始状态
assert quorum.currentState() instanceof UnattachedState;

// 发起选举
quorum.transitionToCandidate();
assert quorum.currentState() instanceof CandidateState;

// 赢得选举
quorum.transitionToLeader();
assert quorum.currentState() instanceof LeaderState;

// 练习2: 测试高水位计算
LeaderState leader = ...;
// 模拟 Follower 响应
leader.updateReplicaState(2, currentTime, offset(95));
leader.updateReplicaState(3, currentTime, offset(90));
// 验证高水位
assert leader.highWatermark().get().offset() == 90;
```

---

## 模块 3: 选举机制 (Election) ⭐⭐

**学习目标：** 理解 Raft 选举的完整流程和 Kafka 的优化

**预计时间：** 2-3 周

**核心文件：** 选举逻辑主要在 `KafkaRaftClient` 中实现

### 3.1 选举相关的核心方法

**位置：** `org.apache.kafka.raft.KafkaRaftClient`

```java
/**
 * 处理 Vote 请求
 */
private VoteResponse handleVoteRequest(
    RaftRequest.Inbound<VoteRequest> request,
    long currentTimeMs
) {
    VoteRequest voteRequest = request.data;
    int candidateId = voteRequest.candidateId();
    int candidateEpoch = voteRequest.candidateEpoch();

    // 1. 检查 epoch
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
        candidateId,
        isLogUpToDate
    );

    if (canGrantVote) {
        // 投票并转移到 Voted 状态
        quorum.transitionToVoted(candidateEpoch, candidateId);
        return buildVoteResponse(Errors.NONE, true);
    } else {
        return buildVoteResponse(Errors.NONE, false);
    }
}

/**
 * 处理 Vote 响应
 */
private void handleVoteResponse(
    RaftResponse.Inbound<VoteResponse> response,
    long currentTimeMs
) {
    VoteResponse voteResponse = response.data;
    int responderId = response.sourceId();

    // 只有 Candidate 状态才处理 Vote 响应
    if (!(quorum.currentState() instanceof CandidateState)) {
        return;
    }

    CandidateState candidateState = (CandidateState) quorum.currentState();

    if (voteResponse.voteGranted()) {
        // 记录同意投票
        candidateState.recordGrantedVote(responderId);

        // 检查是否获得多数派
        if (candidateState.isVoteGranted()) {
            // 成为 Leader
            becomeLeader(currentTimeMs);
        }
    } else {
        // 记录拒绝投票
        candidateState.recordRejectedVote(
            responderId,
            voteResponse.errorMessage()
        );

        // 检查是否被多数派拒绝
        if (candidateState.isVoteRejected()) {
            // 退回到 Follower
            transitionToFollower(currentTimeMs);
        }
    }
}

/**
 * 发起选举
 */
private void initiateElection(long currentTimeMs) {
    // 1. 递增 epoch
    int newEpoch = quorum.epoch() + 1;

    // 2. 转移到 Candidate 状态
    quorum.transitionToCandidate(newEpoch);

    // 3. 构建 Vote 请求
    VoteRequest voteRequest = buildVoteRequest();

    // 4. 向所有节点发送 Vote 请求
    for (int voterId : quorum.voters().voterIds()) {
        if (voterId != localId) {
            sendVoteRequest(voterId, voteRequest, currentTimeMs);
        }
    }
}

/**
 * 成为 Leader
 */
private void becomeLeader(long currentTimeMs) {
    long epochStartOffset = log.endOffset().offset;

    // 1. 转移到 Leader 状态
    LeaderState<T> leaderState = quorum.transitionToLeader(
        epochStartOffset,
        accumulator
    );

    // 2. 追加 LeaderChange 控制记录
    appendLeaderChangeMessage(leaderState, currentTimeMs);

    // 3. 通知监听器
    notifyLeaderChange(quorum.leaderAndEpoch());
}
```

### 3.2 Pre-Vote 机制

**目的：** 避免不必要的选举

**实现：** `ProspectiveState`

```java
/**
 * Pre-Vote 流程：
 * 1. 节点在真正发起选举前，先进入 Prospective 状态
 * 2. 发送 Pre-Vote 请求（不递增 epoch）
 * 3. 如果获得多数派支持，再发起真正的选举
 * 4. 否则，退回到原来的状态
 *
 * 好处：避免因网络分区导致的无用选举
 */
```

### 3.3 日志比较算法

**核心算法：判断候选人的日志是否比自己新**

```java
/**
 * 判断候选人的日志是否足够新
 *
 * Raft 规则：
 * 1. 如果候选人的 last log epoch > 自己的 last log epoch，则更新
 * 2. 如果 epoch 相同，比较 last log offset
 */
private boolean isLogUpToDate(
    OffsetAndEpoch myLastOffset,
    long candidateLastOffset,
    int candidateLastEpoch
) {
    if (candidateLastEpoch > myLastOffset.epoch()) {
        return true;
    } else if (candidateLastEpoch < myLastOffset.epoch()) {
        return false;
    } else {
        return candidateLastOffset >= myLastOffset.offset();
    }
}
```

---

## 模块 4: 日志复制 (Log Replication) ⭐⭐⭐

**学习目标：** 理解 Raft 日志复制的核心算法

**预计时间：** 3-4 周

### 4.1 日志复制核心流程

**Leader 端实现：**

```java
/**
 * 处理 Fetch 请求（Leader）
 */
private FetchResponse handleFetchRequestAsLeader(
    RaftRequest.Inbound<FetchRequest> request,
    long currentTimeMs
) {
    FetchRequest fetchRequest = request.data;
    int followerId = request.sourceId();
    long fetchOffset = fetchRequest.fetchOffset();

    LeaderState<T> leaderState = (LeaderState<T>) quorum.currentState();

    // 1. 更新 Follower 的状态
    leaderState.updateReplicaState(
        followerId,
        currentTimeMs,
        new LogOffsetMetadata(fetchOffset)
    );

    // 2. 读取日志
    LogFetchInfo fetchInfo = log.read(fetchOffset, Isolation.UNCOMMITTED);

    // 3. 构建响应
    FetchResponse response = buildFetchResponse(
        Errors.NONE,
        fetchInfo.records,
        leaderState.highWatermark()
    );

    return response;
}

/**
 * 定期向 Follower 发送心跳/日志
 */
private void sendFetchRequests(long currentTimeMs) {
    LeaderState<T> leaderState = (LeaderState<T>) quorum.currentState();

    // 找出需要发送 Fetch 的 Follower
    Set<Integer> needsFetch = leaderState.followersThatNeedFetch(currentTimeMs);

    for (int followerId : needsFetch) {
        sendFetchRequest(followerId, currentTimeMs);
    }
}
```

**Follower 端实现：**

```java
/**
 * 处理 Fetch 响应（Follower）
 */
private void handleFetchResponseAsFollower(
    RaftResponse.Inbound<FetchResponse> response,
    long currentTimeMs
) {
    FetchResponse fetchResponse = response.data;

    // 1. 检查 epoch
    if (fetchResponse.epoch() != quorum.epoch()) {
        return;
    }

    // 2. 追加日志
    if (fetchResponse.records() != null) {
        log.appendAsFollower(
            fetchResponse.records(),
            fetchResponse.epoch(),
            fetchResponse.baseOffset()
        );
    }

    // 3. 更新高水位
    if (fetchResponse.highWatermark() > 0) {
        log.updateHighWatermark(
            new LogOffsetMetadata(fetchResponse.highWatermark())
        );
    }

    // 4. 重置选举超时
    resetElectionTimeout(currentTimeMs);
}
```

### 4.2 学习检查清单

- [ ] 理解选举触发条件
- [ ] 理解投票决策算法
- [ ] 理解 Pre-Vote 机制
- [ ] 理解日志复制流程
- [ ] 理解高水位更新机制
- [ ] 实现完整的选举和复制逻辑

---

**继续阅读其他模块（模块5-9）请查看后续部分...**

---

## 总结：模块化学习的优势

1. **循序渐进**：从简单到复杂
2. **聚焦核心**：每次专注一个功能模块
3. **理解设计**：理解为什么需要这样设计
4. **完整实现**：所有类名和方法与 Kafka 一致
5. **可验证性**：每个模块都可以独立测试

每个模块都包含：
- ✅ 完整的类列表
- ✅ 核心方法签名
- ✅ 实现要点
- ✅ 设计思想解析
- ✅ 学习检查清单
