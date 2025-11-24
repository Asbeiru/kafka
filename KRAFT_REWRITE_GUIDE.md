# KRaft 重写完整实现指南

## 项目概述

本指南提供了从零开始重写 Apache Kafka KRaft 模块的完整实现思路。基于对原始实现的深入研究，我们将采用循序渐进的方式，分阶段实现一个完整的 Raft 共识协议。

---

## 第一部分：实现路线图

### 阶段 0: 前置准备 (1-2周)

#### 0.1 理论学习
- [ ] 深入学习 Raft 论文 (In Search of an Understandable Consensus Algorithm)
- [ ] 理解 PreVote 机制 (防止无效选举)
- [ ] 研究 Kafka 的日志模型和复制机制
- [ ] 理解 Leader Lease 和 Check Quorum 机制

#### 0.2 技术选型
```
决策点：
1. 编程语言: Java/Kotlin/Scala/Go/Rust?
   建议: Java (与原实现一致) 或 Go (更适合分布式系统)

2. 网络库: Netty/Java NIO/gRPC?
   建议: Netty (高性能) 或 gRPC (简化协议)

3. 序列化: Protocol Buffers/Avro/自定义?
   建议: Protocol Buffers (性能+可维护性)

4. 日志存储: 自实现/RocksDB/LevelDB?
   建议: 先自实现简单版本，后期可优化

5. 测试框架: JUnit/TestNG + Mockito?
   建议: JUnit 5 + AssertJ + Testcontainers
```

#### 0.3 项目结构设计
```
kraft-rewrite/
├── kraft-api/              # 公共接口定义
│   ├── RaftClient
│   ├── RaftServer
│   └── StateMachine
├── kraft-core/             # 核心实现
│   ├── state/              # 状态机模块
│   ├── log/                # 日志复制模块
│   ├── election/           # 选举模块
│   ├── replication/        # 复制模块
│   └── snapshot/           # 快照模块
├── kraft-network/          # 网络通信
│   ├── protocol/           # 协议定义
│   └── transport/          # 传输层
├── kraft-storage/          # 存储层
│   ├── log/                # 日志存储
│   └── snapshot/           # 快照存储
├── kraft-utils/            # 工具类
└── kraft-tests/            # 集成测试
    ├── unit/
    ├── integration/
    └── chaos/              # 混沌测试
```

---

### 阶段 1: 基础设施 (2-3周)

#### 1.1 定义核心数据结构

**文件**: `kraft-api/src/main/java/raft/api/RaftNode.java`
```java
public interface RaftNode {
    int nodeId();
    String host();
    int port();
    Optional<String> directoryId();  // 用于 JBOD 支持
}
```

**文件**: `kraft-core/src/main/java/raft/core/LogEntry.java`
```java
public class LogEntry {
    private final long offset;        // 日志偏移量
    private final int epoch;          // Leader 纪元
    private final long timestamp;     // 时间戳
    private final ByteBuffer data;    // 数据
    private final EntryType type;     // 类型: DATA/CONTROL

    // 控制记录类型
    public enum EntryType {
        DATA,                  // 普通数据
        LEADER_CHANGE,         // Leader 变更
        VOTER_CHANGE,          // 投票者变更
        SNAPSHOT_MARKER        // 快照标记
    }
}
```

**文件**: `kraft-core/src/main/java/raft/core/OffsetAndEpoch.java`
```java
public class OffsetAndEpoch {
    private final long offset;
    private final int epoch;

    // 用于日志截断、快照标识等
}
```

#### 1.2 实现基础日志存储

**文件**: `kraft-storage/src/main/java/raft/storage/Log.java`
```java
public interface Log extends AutoCloseable {
    // 基本操作
    long append(LogEntry entry);
    long append(List<LogEntry> entries);
    Optional<LogEntry> read(long offset);
    List<LogEntry> read(long startOffset, int maxEntries);

    // 日志管理
    void truncateTo(long offset);        // 截断到指定位置
    void truncateFrom(long offset);      // 从指定位置开始截断

    // 元数据
    long startOffset();                  // 日志起始偏移量
    long endOffset();                    // 日志结束偏移量（下一条记录的位置）
    int lastEpoch();                     // 最后一条记录的 epoch

    // Epoch 管理
    OffsetAndEpoch endOffsetForEpoch(int epoch);  // 获取 epoch 的结束位置
    void initializeLeaderEpoch(int epoch);        // 初始化新的 leader epoch

    // 持久化
    void flush();
}
```

**实现提示**:
1. 使用 **Segment** 模型（类似 Kafka Log Segment）
2. 每个 Segment 包含:
   - `.log` 文件: 存储实际数据
   - `.index` 文件: 偏移量索引
   - `.timeindex` 文件: 时间戳索引（可选）
3. 实现 **LogSegment** 和 **LogManager**
4. 考虑 mmap 或 FileChannel 的性能权衡

#### 1.3 实现持久化状态存储

**文件**: `kraft-storage/src/main/java/raft/storage/StateStore.java`
```java
public interface StateStore {
    // 选举状态持久化
    void saveElectionState(ElectionState state);
    Optional<ElectionState> readElectionState();

    // 投票状态
    void saveVote(int epoch, int votedFor);
    Optional<Vote> readVote();
}

public class ElectionState {
    private final int epoch;                    // 当前 epoch
    private final OptionalInt votedFor;         // 投票给谁
    private final OptionalInt leaderId;         // 当前 leader
    private final long lastUpdateTimestamp;     // 最后更新时间
}
```

**实现建议**:
- 使用简单的 Properties 文件或 JSON
- 每次更新都要 fsync
- 考虑使用 WAL (Write-Ahead Log) 模式

#### 1.4 实现网络协议

**文件**: `kraft-network/src/main/proto/raft.proto`
```protobuf
syntax = "proto3";

package raft.protocol;

// 投票请求
message VoteRequest {
    int32 epoch = 1;              // 候选者 epoch
    int32 candidate_id = 2;       // 候选者 ID
    int64 last_log_offset = 3;    // 候选者最后日志偏移量
    int32 last_log_epoch = 4;     // 候选者最后日志 epoch
    bool pre_vote = 5;            // 是否是 PreVote
}

message VoteResponse {
    int32 epoch = 1;              // 响应者当前 epoch
    bool vote_granted = 2;        // 是否投票
    optional int32 leader_id = 3; // 如果知道 leader，返回 leader ID
}

// 心跳/数据复制请求
message AppendEntriesRequest {
    int32 epoch = 1;              // Leader epoch
    int32 leader_id = 2;          // Leader ID
    int64 prev_log_offset = 3;    // 前一条日志偏移量
    int32 prev_log_epoch = 4;     // 前一条日志 epoch
    repeated LogEntryProto entries = 5;  // 日志条目
    int64 leader_commit = 6;      // Leader 已提交的偏移量
}

message AppendEntriesResponse {
    int32 epoch = 1;              // 响应者当前 epoch
    bool success = 2;             // 是否成功
    int64 last_log_offset = 3;    // 响应者最后日志偏移量
    optional int64 diverging_offset = 4;  // 分歧点偏移量（用于快速回退）
    optional int32 diverging_epoch = 5;   // 分歧点 epoch
}

// Kafka 风格的 Fetch 请求（Follower 拉取）
message FetchRequest {
    int32 replica_id = 1;         // 请求者 ID
    int32 replica_epoch = 2;      // 请求者当前 epoch
    int64 fetch_offset = 3;       // 要拉取的起始偏移量
    int32 last_fetched_epoch = 4; // 上一次拉取的 epoch
    int32 max_bytes = 5;          // 最大字节数
}

message FetchResponse {
    int32 epoch = 1;              // Leader 当前 epoch
    int32 leader_id = 2;          // Leader ID
    int64 high_watermark = 3;     // 高水位
    repeated LogEntryProto entries = 4;  // 日志条目
    optional SnapshotInfo snapshot = 5;   // 如果需要快照
    optional OffsetAndEpochProto diverging = 6;  // 分歧点
}

// 快照传输
message FetchSnapshotRequest {
    int32 replica_id = 1;
    int64 snapshot_offset = 2;
    int32 snapshot_epoch = 3;
    int64 position = 4;           // 快照文件的读取位置
    int32 max_bytes = 5;
}

message FetchSnapshotResponse {
    int64 snapshot_size = 1;
    int64 position = 2;
    bytes data = 3;
}
```

---

### 阶段 2: 状态机实现 (3-4周)

#### 2.1 定义状态接口

**文件**: `kraft-core/src/main/java/raft/core/state/State.java`
```java
public interface State {
    StateType type();
    int epoch();

    // 状态名称，用于日志
    default String name() {
        return type().name() + "(epoch=" + epoch() + ")";
    }
}

public enum StateType {
    UNATTACHED,    // 未附加
    PROSPECTIVE,   // 预选举
    CANDIDATE,     // 候选者
    LEADER,        // 领导者
    FOLLOWER,      // 跟随者
    RESIGNED       // 已辞职
}
```

#### 2.2 实现各个状态

**文件**: `kraft-core/src/main/java/raft/core/state/UnattachedState.java`
```java
public class UnattachedState implements State {
    private final int epoch;
    private final OptionalInt lastKnownLeader;
    private final long electionTimeoutMs;
    private final long electionDeadline;  // 选举超时时间点

    public UnattachedState(int epoch, long currentTime, long electionTimeoutMs) {
        this.epoch = epoch;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionDeadline = currentTime + electionTimeoutMs;
        this.lastKnownLeader = OptionalInt.empty();
    }

    public boolean hasElectionTimeoutExpired(long currentTime) {
        return currentTime >= electionDeadline;
    }

    @Override
    public StateType type() {
        return StateType.UNATTACHED;
    }

    @Override
    public int epoch() {
        return epoch;
    }
}
```

**文件**: `kraft-core/src/main/java/raft/core/state/ProspectiveState.java`
```java
public class ProspectiveState implements State {
    private final int epoch;
    private final Set<Integer> voters;           // 所有投票者
    private final Set<Integer> grantingVoters;   // PreVote 中支持的投票者
    private final long electionDeadline;

    public boolean recordGrantedVote(int voterId) {
        return grantingVoters.add(voterId);
    }

    public boolean hasPreVoteMajority() {
        return grantingVoters.size() >= (voters.size() / 2 + 1);
    }

    public boolean hasElectionTimeoutExpired(long currentTime) {
        return currentTime >= electionDeadline;
    }
}
```

**文件**: `kraft-core/src/main/java/raft/core/state/CandidateState.java`
```java
public class CandidateState implements State {
    private final int epoch;
    private final int candidateId;
    private final Set<Integer> voters;
    private final Set<Integer> grantingVoters;   // 正式投票中支持的投票者
    private final long electionDeadline;

    public CandidateState(int epoch, int candidateId, Set<Integer> voters,
                          long currentTime, long electionTimeoutMs) {
        this.epoch = epoch;
        this.candidateId = candidateId;
        this.voters = voters;
        this.grantingVoters = new HashSet<>();
        this.grantingVoters.add(candidateId);  // 给自己投票
        this.electionDeadline = currentTime + electionTimeoutMs;
    }

    public boolean recordGrantedVote(int voterId) {
        return grantingVoters.add(voterId);
    }

    public boolean hasMajority() {
        return grantingVoters.size() >= (voters.size() / 2 + 1);
    }
}
```

**文件**: `kraft-core/src/main/java/raft/core/state/LeaderState.java`
```java
public class LeaderState implements State {
    private final int epoch;
    private final int leaderId;
    private final long epochStartOffset;
    private final Set<Integer> voters;

    // 每个 Follower 的复制状态
    private final Map<Integer, ReplicaState> replicaStates;

    // 高水位
    private long highWatermark;

    // Check Quorum 相关
    private final Set<Integer> activeFollowers;  // 活跃的 followers
    private final long checkQuorumDeadline;

    public LeaderState(int epoch, int leaderId, long epochStartOffset,
                       Set<Integer> voters, long currentTime, long checkQuorumTimeoutMs) {
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.epochStartOffset = epochStartOffset;
        this.voters = voters;
        this.highWatermark = epochStartOffset;
        this.replicaStates = new HashMap<>();
        this.activeFollowers = new HashSet<>();
        this.checkQuorumDeadline = currentTime + checkQuorumTimeoutMs;

        // 初始化所有 replica 状态
        for (int voterId : voters) {
            if (voterId != leaderId) {
                replicaStates.put(voterId, new ReplicaState(epochStartOffset));
            }
        }
    }

    // 更新 Follower 的复制进度
    public void updateReplicaState(int followerId, long matchOffset) {
        ReplicaState state = replicaStates.get(followerId);
        if (state != null) {
            state.updateMatchOffset(matchOffset);
            activeFollowers.add(followerId);
        }
    }

    // 计算并更新高水位
    public boolean maybeUpdateHighWatermark() {
        // 收集所有 replica 的 match offset（包括 leader 自己）
        List<Long> matchOffsets = new ArrayList<>();
        matchOffsets.add(epochStartOffset);  // Leader 至少有 epochStartOffset

        for (ReplicaState state : replicaStates.values()) {
            matchOffsets.add(state.matchOffset());
        }

        // 排序并找到中位数（多数派的最小值）
        matchOffsets.sort(Long::compareTo);
        int quorumIndex = voters.size() / 2;
        long newHighWatermark = matchOffsets.get(quorumIndex);

        if (newHighWatermark > highWatermark) {
            highWatermark = newHighWatermark;
            return true;
        }
        return false;
    }

    // 检查是否仍有多数派支持
    public boolean hasCheckQuorumFailed(long currentTime) {
        if (currentTime < checkQuorumDeadline) {
            return false;
        }

        // 检查活跃的 followers 是否达到多数派
        int activeCount = activeFollowers.size() + 1;  // +1 是 leader 自己
        return activeCount < (voters.size() / 2 + 1);
    }

    public void resetCheckQuorum(long currentTime, long checkQuorumTimeoutMs) {
        activeFollowers.clear();
        // checkQuorumDeadline = currentTime + checkQuorumTimeoutMs;
    }
}

class ReplicaState {
    private long matchOffset;   // 已确认复制的最高偏移量
    private long nextOffset;    // 下一条要发送的偏移量

    public ReplicaState(long startOffset) {
        this.matchOffset = startOffset - 1;
        this.nextOffset = startOffset;
    }

    public void updateMatchOffset(long offset) {
        this.matchOffset = Math.max(this.matchOffset, offset);
        this.nextOffset = Math.max(this.nextOffset, offset + 1);
    }

    public long matchOffset() { return matchOffset; }
    public long nextOffset() { return nextOffset; }
}
```

**文件**: `kraft-core/src/main/java/raft/core/state/FollowerState.java`
```java
public class FollowerState implements State {
    private final int epoch;
    private final int leaderId;
    private final long fetchDeadline;  // Fetch 超时时间点

    public FollowerState(int epoch, int leaderId, long currentTime, long fetchTimeoutMs) {
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.fetchDeadline = currentTime + fetchTimeoutMs;
    }

    public boolean hasFetchTimeoutExpired(long currentTime) {
        return currentTime >= fetchDeadline;
    }

    public FollowerState resetFetchTimeout(long currentTime, long fetchTimeoutMs) {
        return new FollowerState(epoch, leaderId, currentTime, fetchTimeoutMs);
    }
}
```

#### 2.3 实现状态机控制器

**文件**: `kraft-core/src/main/java/raft/core/state/StateMachine.java`
```java
public class StateMachine {
    private final int nodeId;
    private final StateStore stateStore;
    private final Time time;
    private final Random random;
    private final int baseElectionTimeoutMs;
    private final int fetchTimeoutMs;

    private volatile State currentState;

    public StateMachine(int nodeId, StateStore stateStore,
                        int electionTimeoutMs, int fetchTimeoutMs) {
        this.nodeId = nodeId;
        this.stateStore = stateStore;
        this.time = Time.SYSTEM;
        this.random = new Random();
        this.baseElectionTimeoutMs = electionTimeoutMs;
        this.fetchTimeoutMs = fetchTimeoutMs;
    }

    public void initialize(int currentEpoch) {
        // 从持久化状态恢复或初始化为 Unattached
        ElectionState stored = stateStore.readElectionState().orElse(
            new ElectionState(currentEpoch, OptionalInt.empty(), OptionalInt.empty())
        );

        this.currentState = new UnattachedState(
            stored.epoch(),
            time.milliseconds(),
            randomElectionTimeout()
        );
    }

    // 状态转换方法
    public void transitionToProspective(int epoch, Set<Integer> voters) {
        long currentTime = time.milliseconds();
        this.currentState = new ProspectiveState(
            epoch, nodeId, voters, currentTime, randomElectionTimeout()
        );
        logger.info("Transitioned to {}", currentState.name());
    }

    public void transitionToCandidate(int epoch, Set<Integer> voters) {
        long currentTime = time.milliseconds();
        this.currentState = new CandidateState(
            epoch, nodeId, voters, currentTime, randomElectionTimeout()
        );

        // 持久化投票状态
        stateStore.saveVote(epoch, nodeId);
        logger.info("Transitioned to {}", currentState.name());
    }

    public void transitionToLeader(int epoch, long epochStartOffset, Set<Integer> voters) {
        long currentTime = time.milliseconds();
        long checkQuorumTimeout = (long) (fetchTimeoutMs * 1.5);

        this.currentState = new LeaderState(
            epoch, nodeId, epochStartOffset, voters, currentTime, checkQuorumTimeout
        );

        // 持久化 leader 状态
        stateStore.saveElectionState(new ElectionState(epoch, OptionalInt.of(nodeId), OptionalInt.of(nodeId)));
        logger.info("Transitioned to {}", currentState.name());
    }

    public void transitionToFollower(int epoch, int leaderId) {
        long currentTime = time.milliseconds();
        this.currentState = new FollowerState(epoch, leaderId, currentTime, fetchTimeoutMs);

        // 持久化 follower 状态
        stateStore.saveElectionState(new ElectionState(epoch, OptionalInt.empty(), OptionalInt.of(leaderId)));
        logger.info("Transitioned to {}", currentState.name());
    }

    public void transitionToUnattached(int epoch) {
        long currentTime = time.milliseconds();
        this.currentState = new UnattachedState(epoch, currentTime, randomElectionTimeout());

        stateStore.saveElectionState(new ElectionState(epoch, OptionalInt.empty(), OptionalInt.empty()));
        logger.info("Transitioned to {}", currentState.name());
    }

    // 随机选举超时（防止同时选举）
    private long randomElectionTimeout() {
        return baseElectionTimeoutMs + random.nextInt(baseElectionTimeoutMs);
    }

    public State currentState() {
        return currentState;
    }

    public int currentEpoch() {
        return currentState.epoch();
    }

    public StateType currentStateType() {
        return currentState.type();
    }
}
```

---

### 阶段 3: 选举实现 (2-3周)

#### 3.1 PreVote 机制

**文件**: `kraft-core/src/main/java/raft/core/election/ElectionManager.java`
```java
public class ElectionManager {
    private final StateMachine stateMachine;
    private final Log log;
    private final NetworkClient networkClient;
    private final Set<Integer> voters;

    // 发起 PreVote
    public void startPreVote() {
        State current = stateMachine.currentState();
        if (!(current instanceof UnattachedState)) {
            logger.warn("Cannot start PreVote from state: {}", current.name());
            return;
        }

        int newEpoch = current.epoch() + 1;
        stateMachine.transitionToProspective(newEpoch, voters);

        // 发送 PreVote 请求给所有投票者
        VoteRequest request = VoteRequest.newBuilder()
            .setEpoch(newEpoch)
            .setCandidateId(stateMachine.nodeId())
            .setLastLogOffset(log.endOffset())
            .setLastLogEpoch(log.lastEpoch())
            .setPreVote(true)
            .build();

        for (int voterId : voters) {
            if (voterId != stateMachine.nodeId()) {
                networkClient.sendVoteRequest(voterId, request);
            }
        }
    }

    // 处理 PreVote 响应
    public void handlePreVoteResponse(int fromNode, VoteResponse response) {
        State current = stateMachine.currentState();
        if (!(current instanceof ProspectiveState)) {
            return;
        }

        ProspectiveState prospective = (ProspectiveState) current;

        // 检查 epoch
        if (response.getEpoch() > prospective.epoch()) {
            // 发现更高的 epoch，转为 Unattached
            stateMachine.transitionToUnattached(response.getEpoch());
            return;
        }

        if (response.getVoteGranted()) {
            prospective.recordGrantedVote(fromNode);

            // 检查是否获得多数派 PreVote
            if (prospective.hasPreVoteMajority()) {
                // 转为 Candidate 并发起正式投票
                startElection();
            }
        }
    }

    // 发起正式选举
    public void startElection() {
        State current = stateMachine.currentState();
        int newEpoch = current.epoch();

        stateMachine.transitionToCandidate(newEpoch, voters);

        // 发送正式投票请求
        VoteRequest request = VoteRequest.newBuilder()
            .setEpoch(newEpoch)
            .setCandidateId(stateMachine.nodeId())
            .setLastLogOffset(log.endOffset())
            .setLastLogEpoch(log.lastEpoch())
            .setPreVote(false)
            .build();

        for (int voterId : voters) {
            if (voterId != stateMachine.nodeId()) {
                networkClient.sendVoteRequest(voterId, request);
            }
        }
    }

    // 处理投票响应
    public void handleVoteResponse(int fromNode, VoteResponse response) {
        State current = stateMachine.currentState();
        if (!(current instanceof CandidateState)) {
            return;
        }

        CandidateState candidate = (CandidateState) current;

        if (response.getEpoch() > candidate.epoch()) {
            stateMachine.transitionToUnattached(response.getEpoch());
            return;
        }

        if (response.getVoteGranted()) {
            candidate.recordGrantedVote(fromNode);

            // 检查是否获得多数派投票
            if (candidate.hasMajority()) {
                becomeLeader();
            }
        }
    }

    // 成为 Leader
    private void becomeLeader() {
        long epochStartOffset = log.endOffset();

        // 初始化 leader epoch
        log.initializeLeaderEpoch(stateMachine.currentEpoch());

        // 写入 LeaderChange 控制记录
        LogEntry leaderChangeRecord = createLeaderChangeRecord(
            stateMachine.currentEpoch(),
            voters
        );
        log.append(leaderChangeRecord);

        // 转换为 Leader 状态
        stateMachine.transitionToLeader(
            stateMachine.currentEpoch(),
            epochStartOffset,
            voters
        );
    }
}
```

#### 3.2 投票决策逻辑

**文件**: `kraft-core/src/main/java/raft/core/election/VoteGranter.java`
```java
public class VoteGranter {
    private final StateMachine stateMachine;
    private final StateStore stateStore;
    private final Log log;

    public VoteResponse handleVoteRequest(VoteRequest request) {
        int localEpoch = stateMachine.currentEpoch();

        // 1. 如果请求的 epoch 更旧，拒绝
        if (request.getEpoch() < localEpoch) {
            return VoteResponse.newBuilder()
                .setEpoch(localEpoch)
                .setVoteGranted(false)
                .build();
        }

        // 2. 如果请求的 epoch 更新，更新本地 epoch
        if (request.getEpoch() > localEpoch) {
            stateMachine.transitionToUnattached(request.getEpoch());
            localEpoch = request.getEpoch();
        }

        // 3. 检查是否已经投票
        Optional<Vote> existingVote = stateStore.readVote();
        if (existingVote.isPresent() &&
            existingVote.get().epoch() == request.getEpoch() &&
            existingVote.get().votedFor() != request.getCandidateId()) {
            // 已经在这个 epoch 投给了别人
            return VoteResponse.newBuilder()
                .setEpoch(localEpoch)
                .setVoteGranted(false)
                .build();
        }

        // 4. 检查候选者的日志是否至少和自己一样新
        if (!isLogUpToDate(request.getLastLogEpoch(), request.getLastLogOffset())) {
            return VoteResponse.newBuilder()
                .setEpoch(localEpoch)
                .setVoteGranted(false)
                .build();
        }

        // 5. PreVote 不需要持久化
        if (!request.getPreVote()) {
            stateStore.saveVote(request.getEpoch(), request.getCandidateId());
        }

        return VoteResponse.newBuilder()
            .setEpoch(localEpoch)
            .setVoteGranted(true)
            .build();
    }

    // 检查候选者的日志是否至少和本地一样新
    private boolean isLogUpToDate(int candidateLastEpoch, long candidateLastOffset) {
        int localLastEpoch = log.lastEpoch();
        long localLastOffset = log.endOffset() - 1;

        // 先比较 epoch，epoch 大的更新
        if (candidateLastEpoch != localLastEpoch) {
            return candidateLastEpoch > localLastEpoch;
        }

        // epoch 相同，比较 offset
        return candidateLastOffset >= localLastOffset;
    }
}
```

---

### 阶段 4: 日志复制实现 (4-5周)

#### 4.1 Leader 端: 批量累积器

**文件**: `kraft-core/src/main/java/raft/core/replication/BatchAccumulator.java`
```java
public class BatchAccumulator<T> {
    private final int epoch;
    private final int maxBatchSize;
    private final int lingerMs;
    private final Serializer<T> serializer;

    private final Queue<CompletedBatch> completedBatches = new ConcurrentLinkedQueue<>();
    private BatchBuilder currentBatch;
    private long nextOffset;

    public CompletableFuture<Long> append(List<T> records) {
        CompletableFuture<Long> future = new CompletableFuture<>();

        synchronized (this) {
            // 检查当前批次是否需要创建或关闭
            if (currentBatch == null || !currentBatch.hasRoom(records)) {
                closeCurrentBatch();
                currentBatch = new BatchBuilder(nextOffset, epoch, maxBatchSize);
            }

            // 添加记录
            long lastOffset = currentBatch.append(records);
            nextOffset = lastOffset + 1;

            // 注册回调
            currentBatch.addCallback(future);

            // 检查是否需要立即刷新
            if (shouldFlush()) {
                closeCurrentBatch();
            }
        }

        return future;
    }

    private void closeCurrentBatch() {
        if (currentBatch != null) {
            completedBatches.add(currentBatch.build());
            currentBatch = null;
        }
    }

    public List<CompletedBatch> drain() {
        List<CompletedBatch> batches = new ArrayList<>();
        CompletedBatch batch;
        while ((batch = completedBatches.poll()) != null) {
            batches.add(batch);
        }
        return batches;
    }

    private boolean shouldFlush() {
        return currentBatch.size() >= maxBatchSize ||
               currentBatch.elapsedMs() >= lingerMs;
    }
}
```

#### 4.2 Follower 端: Fetch 机制

**文件**: `kraft-core/src/main/java/raft/core/replication/FetchManager.java`
```java
public class FetchManager {
    private final int replicaId;
    private final StateMachine stateMachine;
    private final Log log;
    private final NetworkClient networkClient;

    // Follower 定期发送 Fetch 请求
    public void sendFetchRequest() {
        State current = stateMachine.currentState();
        if (!(current instanceof FollowerState)) {
            return;
        }

        FollowerState follower = (FollowerState) current;

        FetchRequest request = FetchRequest.newBuilder()
            .setReplicaId(replicaId)
            .setReplicaEpoch(follower.epoch())
            .setFetchOffset(log.endOffset())
            .setLastFetchedEpoch(log.lastEpoch())
            .setMaxBytes(1024 * 1024)  // 1MB
            .build();

        networkClient.sendFetchRequest(follower.leaderId(), request);
    }

    // 处理 Fetch 响应
    public void handleFetchResponse(FetchResponse response) {
        State current = stateMachine.currentState();
        if (!(current instanceof FollowerState)) {
            return;
        }

        FollowerState follower = (FollowerState) current;

        // 检查 epoch
        if (response.getEpoch() > follower.epoch()) {
            stateMachine.transitionToUnattached(response.getEpoch());
            return;
        }

        // 检查是否需要快照
        if (response.hasSnapshot()) {
            handleSnapshotResponse(response.getSnapshot());
            return;
        }

        // 检查是否有分歧
        if (response.hasDiverging()) {
            handleDivergence(response.getDiverging());
            return;
        }

        // 追加日志
        if (!response.getEntriesList().isEmpty()) {
            for (LogEntryProto entryProto : response.getEntriesList()) {
                LogEntry entry = fromProto(entryProto);
                log.append(entry);
            }
        }

        // 更新高水位
        if (response.getHighWatermark() > log.highWatermark().offset()) {
            log.updateHighWatermark(response.getHighWatermark());
        }

        // 重置 fetch 超时
        stateMachine.transitionToFollower(
            follower.epoch(),
            follower.leaderId()
        );
    }

    private void handleDivergence(OffsetAndEpochProto diverging) {
        // 截断到分歧点
        long divergingOffset = diverging.getOffset();
        log.truncateTo(divergingOffset);

        logger.info("Truncated log to diverging offset: {}", divergingOffset);
    }
}
```

#### 4.3 Leader 端: 处理 Fetch 请求

**文件**: `kraft-core/src/main/java/raft/core/replication/ReplicationManager.java`
```java
public class ReplicationManager {
    private final StateMachine stateMachine;
    private final Log log;

    public FetchResponse handleFetchRequest(FetchRequest request) {
        State current = stateMachine.currentState();
        if (!(current instanceof LeaderState)) {
            // 不是 Leader，返回错误或重定向
            return FetchResponse.newBuilder()
                .setEpoch(current.epoch())
                .setLeaderId(-1)
                .build();
        }

        LeaderState leader = (LeaderState) current;
        int followerId = request.getReplicaId();
        long fetchOffset = request.getFetchOffset();
        int lastFetchedEpoch = request.getLastFetchedEpoch();

        // 1. 验证 Follower 的日志是否一致
        ValidOffsetAndEpoch validation = log.validateOffsetAndEpoch(
            fetchOffset,
            lastFetchedEpoch
        );

        // 2. 如果需要快照
        if (validation.kind() == ValidationKind.SNAPSHOT) {
            SnapshotInfo snapshotInfo = createSnapshotInfo(validation.offsetAndEpoch());
            return FetchResponse.newBuilder()
                .setEpoch(leader.epoch())
                .setLeaderId(leader.leaderId())
                .setSnapshot(snapshotInfo)
                .build();
        }

        // 3. 如果有分歧
        if (validation.kind() == ValidationKind.DIVERGING) {
            return FetchResponse.newBuilder()
                .setEpoch(leader.epoch())
                .setLeaderId(leader.leaderId())
                .setDiverging(toProto(validation.offsetAndEpoch()))
                .build();
        }

        // 4. 读取日志条目
        List<LogEntry> entries = log.read(
            fetchOffset,
            request.getMaxBytes()
        );

        // 5. 更新 Follower 状态
        if (!entries.isEmpty()) {
            long lastOffset = entries.get(entries.size() - 1).offset();
            leader.updateReplicaState(followerId, lastOffset);
        }

        // 6. 尝试更新高水位
        leader.maybeUpdateHighWatermark();

        // 7. 构造响应
        FetchResponse.Builder responseBuilder = FetchResponse.newBuilder()
            .setEpoch(leader.epoch())
            .setLeaderId(leader.leaderId())
            .setHighWatermark(leader.highWatermark());

        for (LogEntry entry : entries) {
            responseBuilder.addEntries(toProto(entry));
        }

        return responseBuilder.build();
    }
}
```

---

### 阶段 5: 快照实现 (2-3周)

#### 5.1 快照接口定义

**文件**: `kraft-storage/src/main/java/raft/storage/Snapshot.java`
```java
public interface SnapshotWriter extends AutoCloseable {
    // 写入数据
    void append(ByteBuffer data);

    // 完成快照（freeze）
    void freeze();

    // 元数据
    OffsetAndEpoch snapshotId();
    long size();
}

public interface SnapshotReader extends AutoCloseable {
    // 读取数据
    ByteBuffer read(long position, int maxBytes);

    // 元数据
    OffsetAndEpoch snapshotId();
    long size();
}
```

#### 5.2 快照管理器

**文件**: `kraft-core/src/main/java/raft/core/snapshot/SnapshotManager.java`
```java
public class SnapshotManager {
    private final File snapshotDir;
    private final Log log;

    // 创建快照
    public SnapshotWriter createSnapshot(OffsetAndEpoch snapshotId) {
        // 验证 snapshotId 是否有效
        if (snapshotId.offset() > log.highWatermark().offset()) {
            throw new IllegalArgumentException(
                "Snapshot offset " + snapshotId.offset() +
                " is greater than high watermark " + log.highWatermark()
            );
        }

        File snapshotFile = getSnapshotFile(snapshotId);
        return new FileSnapshotWriter(snapshotFile, snapshotId);
    }

    // 读取快照
    public Optional<SnapshotReader> readSnapshot(OffsetAndEpoch snapshotId) {
        File snapshotFile = getSnapshotFile(snapshotId);
        if (!snapshotFile.exists()) {
            return Optional.empty();
        }
        return Optional.of(new FileSnapshotReader(snapshotFile, snapshotId));
    }

    // 获取最新快照
    public Optional<SnapshotReader> latestSnapshot() {
        // 扫描快照目录，找到最新的快照
        File[] snapshotFiles = snapshotDir.listFiles(
            (dir, name) -> name.endsWith(".snapshot")
        );

        if (snapshotFiles == null || snapshotFiles.length == 0) {
            return Optional.empty();
        }

        // 解析文件名，找到最新的
        OffsetAndEpoch latest = null;
        for (File file : snapshotFiles) {
            OffsetAndEpoch id = parseSnapshotFileName(file.getName());
            if (latest == null || id.offset() > latest.offset()) {
                latest = id;
            }
        }

        return readSnapshot(latest);
    }

    // 删除旧快照
    public void deleteSnapshotsBefore(OffsetAndEpoch snapshotId) {
        File[] snapshotFiles = snapshotDir.listFiles(
            (dir, name) -> name.endsWith(".snapshot")
        );

        if (snapshotFiles == null) return;

        for (File file : snapshotFiles) {
            OffsetAndEpoch id = parseSnapshotFileName(file.getName());
            if (id.offset() < snapshotId.offset()) {
                file.delete();
                logger.info("Deleted old snapshot: {}", file.getName());
            }
        }
    }

    // 快照文件命名: offset-epoch.snapshot
    private File getSnapshotFile(OffsetAndEpoch snapshotId) {
        String fileName = String.format("%020d-%010d.snapshot",
            snapshotId.offset(), snapshotId.epoch());
        return new File(snapshotDir, fileName);
    }

    private OffsetAndEpoch parseSnapshotFileName(String fileName) {
        // 解析 "offset-epoch.snapshot" 格式
        String[] parts = fileName.replace(".snapshot", "").split("-");
        long offset = Long.parseLong(parts[0]);
        int epoch = Integer.parseInt(parts[1]);
        return new OffsetAndEpoch(offset, epoch);
    }
}
```

#### 5.3 快照传输

**文件**: `kraft-core/src/main/java/raft/core/snapshot/SnapshotTransfer.java`
```java
public class SnapshotTransfer {
    private static final int CHUNK_SIZE = 1024 * 1024;  // 1MB per chunk

    // Follower: 拉取快照
    public void fetchSnapshot(OffsetAndEpoch snapshotId, int leaderId) {
        SnapshotWriter writer = snapshotManager.createSnapshot(snapshotId);
        long position = 0;

        try {
            while (true) {
                FetchSnapshotRequest request = FetchSnapshotRequest.newBuilder()
                    .setReplicaId(nodeId)
                    .setSnapshotOffset(snapshotId.offset())
                    .setSnapshotEpoch(snapshotId.epoch())
                    .setPosition(position)
                    .setMaxBytes(CHUNK_SIZE)
                    .build();

                FetchSnapshotResponse response =
                    networkClient.sendFetchSnapshotRequest(leaderId, request).get();

                if (response.getData().isEmpty()) {
                    break;  // 传输完成
                }

                writer.append(response.getData().asReadOnlyByteBuffer());
                position = response.getPosition() + response.getData().size();
            }

            writer.freeze();
            logger.info("Successfully fetched snapshot: {}", snapshotId);

        } catch (Exception e) {
            writer.close();  // 失败时清理
            throw new RuntimeException("Failed to fetch snapshot", e);
        }
    }

    // Leader: 处理快照请求
    public FetchSnapshotResponse handleFetchSnapshotRequest(FetchSnapshotRequest request) {
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(
            request.getSnapshotOffset(),
            request.getSnapshotEpoch()
        );

        Optional<SnapshotReader> reader = snapshotManager.readSnapshot(snapshotId);
        if (reader.isEmpty()) {
            throw new IllegalStateException("Snapshot not found: " + snapshotId);
        }

        try (SnapshotReader r = reader.get()) {
            ByteBuffer data = r.read(request.getPosition(), request.getMaxBytes());

            return FetchSnapshotResponse.newBuilder()
                .setSnapshotSize(r.size())
                .setPosition(request.getPosition())
                .setData(ByteString.copyFrom(data))
                .build();
        }
    }
}
```

---

### 阶段 6: 高级特性 (3-4周)

#### 6.1 动态投票者变更

**文件**: `kraft-core/src/main/java/raft/core/membership/MembershipManager.java`
```java
public class MembershipManager {
    private final StateMachine stateMachine;
    private final Log log;

    // 添加投票者
    public CompletableFuture<Void> addVoter(int newVoterId, String host, int port) {
        State current = stateMachine.currentState();
        if (!(current instanceof LeaderState)) {
            return CompletableFuture.failedFuture(
                new NotLeaderException("Only leader can add voters")
            );
        }

        LeaderState leader = (LeaderState) current;
        Set<Integer> newVoters = new HashSet<>(leader.voters());
        newVoters.add(newVoterId);

        // 写入 VoterChange 控制记录
        VoterChangeRecord record = VoterChangeRecord.newBuilder()
            .setType(ChangeType.ADD)
            .setVoterId(newVoterId)
            .setHost(host)
            .setPort(port)
            .build();

        CompletableFuture<Long> appendFuture =
            log.append(createControlEntry(record, leader.epoch()));

        return appendFuture.thenAccept(offset -> {
            logger.info("Added voter {} at offset {}", newVoterId, offset);
        });
    }

    // 移除投票者（类似实现）
    public CompletableFuture<Void> removeVoter(int voterId) {
        // 实现类似...
    }
}
```

#### 6.2 动态配置

**文件**: `kraft-core/src/main/java/raft/core/config/DynamicConfig.java`
```java
public class DynamicConfig {
    private volatile Set<RaftNode> voters;
    private volatile Map<Integer, RaftNode> voterMap;

    // 从控制记录更新配置
    public void applyVoterChange(VoterChangeRecord record) {
        Set<RaftNode> newVoters = new HashSet<>(voters);

        switch (record.getType()) {
            case ADD:
                RaftNode newNode = new RaftNode(
                    record.getVoterId(),
                    record.getHost(),
                    record.getPort()
                );
                newVoters.add(newNode);
                break;

            case REMOVE:
                newVoters.removeIf(node -> node.nodeId() == record.getVoterId());
                break;
        }

        // 原子更新
        this.voters = Collections.unmodifiableSet(newVoters);
        this.voterMap = newVoters.stream()
            .collect(Collectors.toMap(RaftNode::nodeId, Function.identity()));

        logger.info("Updated voter set to: {}", voters);
    }
}
```

---

### 阶段 7: 核心引擎整合 (3-4周)

#### 7.1 主循环实现

**文件**: `kraft-core/src/main/java/raft/core/RaftEngine.java`
```java
public class RaftEngine implements Runnable {
    private final int nodeId;
    private final StateMachine stateMachine;
    private final Log log;
    private final ElectionManager electionManager;
    private final ReplicationManager replicationManager;
    private final FetchManager fetchManager;
    private final NetworkClient networkClient;
    private final BatchAccumulator batchAccumulator;

    private volatile boolean running = true;
    private final long pollIntervalMs = 100;

    @Override
    public void run() {
        while (running) {
            long startTime = System.currentTimeMillis();

            try {
                // 1. 处理网络事件
                processNetworkEvents();

                // 2. 检查状态超时
                checkStateTimeouts(startTime);

                // 3. 执行状态特定的任务
                executeStateTasks(startTime);

                // 4. 处理待提交的批次
                processPendingBatches();

                // 5. 睡眠到下一个周期
                sleepUntilNextPoll(startTime);

            } catch (Exception e) {
                logger.error("Error in Raft engine loop", e);
            }
        }
    }

    private void processNetworkEvents() {
        List<NetworkEvent> events = networkClient.poll();

        for (NetworkEvent event : events) {
            switch (event.type()) {
                case VOTE_REQUEST:
                    handleVoteRequest(event);
                    break;
                case VOTE_RESPONSE:
                    handleVoteResponse(event);
                    break;
                case FETCH_REQUEST:
                    handleFetchRequest(event);
                    break;
                case FETCH_RESPONSE:
                    handleFetchResponse(event);
                    break;
                // ... 其他消息类型
            }
        }
    }

    private void checkStateTimeouts(long currentTime) {
        State current = stateMachine.currentState();

        switch (current.type()) {
            case UNATTACHED:
                UnattachedState unattached = (UnattachedState) current;
                if (unattached.hasElectionTimeoutExpired(currentTime)) {
                    // 发起 PreVote
                    electionManager.startPreVote();
                }
                break;

            case PROSPECTIVE:
                ProspectiveState prospective = (ProspectiveState) current;
                if (prospective.hasElectionTimeoutExpired(currentTime)) {
                    // PreVote 超时，返回 Unattached
                    stateMachine.transitionToUnattached(prospective.epoch());
                }
                break;

            case CANDIDATE:
                CandidateState candidate = (CandidateState) current;
                if (candidate.hasElectionTimeoutExpired(currentTime)) {
                    // 选举超时，重新发起 PreVote
                    electionManager.startPreVote();
                }
                break;

            case FOLLOWER:
                FollowerState follower = (FollowerState) current;
                if (follower.hasFetchTimeoutExpired(currentTime)) {
                    // Fetch 超时，转为 Unattached 并发起选举
                    stateMachine.transitionToUnattached(follower.epoch());
                }
                break;

            case LEADER:
                LeaderState leader = (LeaderState) current;
                if (leader.hasCheckQuorumFailed(currentTime)) {
                    // Check Quorum 失败，辞职
                    logger.warn("Check quorum failed, stepping down as leader");
                    stateMachine.transitionToResigned(leader.epoch());
                }
                break;
        }
    }

    private void executeStateTasks(long currentTime) {
        State current = stateMachine.currentState();

        if (current instanceof LeaderState) {
            // Leader: 发送心跳，处理复制
            LeaderState leader = (LeaderState) current;

            // 刷新待发送的批次
            List<CompletedBatch> batches = batchAccumulator.drain();
            if (!batches.isEmpty()) {
                for (CompletedBatch batch : batches) {
                    log.append(batch.entries());
                }

                // 批次已写入日志，等待复制
            }

            // 检查是否需要发送心跳（实际上 Follower 会主动 Fetch）

        } else if (current instanceof FollowerState) {
            // Follower: 定期发送 Fetch 请求
            if (shouldSendFetch(currentTime)) {
                fetchManager.sendFetchRequest();
            }
        }
    }

    private void processPendingBatches() {
        // 处理等待提交的批次
        State current = stateMachine.currentState();
        if (current instanceof LeaderState) {
            LeaderState leader = (LeaderState) current;
            long oldHighWatermark = leader.highWatermark();

            if (leader.maybeUpdateHighWatermark()) {
                long newHighWatermark = leader.highWatermark();
                log.updateHighWatermark(newHighWatermark);

                // 通知应用层已提交的记录
                notifyCommitted(oldHighWatermark, newHighWatermark);
            }
        }
    }

    private void sleepUntilNextPoll(long startTime) {
        long elapsed = System.currentTimeMillis() - startTime;
        long remaining = pollIntervalMs - elapsed;

        if (remaining > 0) {
            try {
                Thread.sleep(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
```

#### 7.2 客户端 API 实现

**文件**: `kraft-api/src/main/java/raft/api/RaftClient.java`
```java
public class RaftClientImpl implements RaftClient {
    private final RaftEngine engine;
    private final BatchAccumulator batchAccumulator;
    private final StateMachine stateMachine;
    private final Log log;

    // 追加记录
    @Override
    public CompletableFuture<Long> append(byte[] data) {
        State current = stateMachine.currentState();
        if (!(current instanceof LeaderState)) {
            return CompletableFuture.failedFuture(
                new NotLeaderException("Not the leader")
            );
        }

        return batchAccumulator.append(List.of(data));
    }

    // 读取记录（需要等待提交）
    @Override
    public CompletableFuture<List<byte[]>> read(long offset, int maxRecords) {
        // 读取已提交的记录
        long highWatermark = log.highWatermark().offset();
        if (offset >= highWatermark) {
            return CompletableFuture.failedFuture(
                new OffsetOutOfRangeException("Offset " + offset +
                    " is beyond high watermark " + highWatermark)
            );
        }

        List<LogEntry> entries = log.read(offset, maxRecords);
        List<byte[]> data = entries.stream()
            .map(LogEntry::data)
            .map(ByteBuffer::array)
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(data);
    }

    // 获取 Leader 信息
    @Override
    public Optional<Integer> currentLeader() {
        State current = stateMachine.currentState();

        if (current instanceof LeaderState) {
            return Optional.of(((LeaderState) current).leaderId());
        } else if (current instanceof FollowerState) {
            return Optional.of(((FollowerState) current).leaderId());
        }

        return Optional.empty();
    }

    // 注册提交监听器
    @Override
    public void registerCommitListener(CommitListener listener) {
        engine.addCommitListener(listener);
    }
}
```

---

### 阶段 8: 测试和优化 (持续进行)

#### 8.1 单元测试

**文件**: `kraft-tests/src/test/java/raft/core/state/StateMachineTest.java`
```java
public class StateMachineTest {

    @Test
    public void testStateTransitions() {
        StateMachine sm = new StateMachine(1, mockStateStore, 5000, 3000);
        sm.initialize(0);

        // 初始状态应该是 Unattached
        assertEquals(StateType.UNATTACHED, sm.currentStateType());

        // 转换到 Prospective
        sm.transitionToProspective(1, Set.of(1, 2, 3));
        assertEquals(StateType.PROSPECTIVE, sm.currentStateType());
        assertEquals(1, sm.currentEpoch());

        // 转换到 Candidate
        sm.transitionToCandidate(1, Set.of(1, 2, 3));
        assertEquals(StateType.CANDIDATE, sm.currentStateType());

        // 转换到 Leader
        sm.transitionToLeader(1, 100L, Set.of(1, 2, 3));
        assertEquals(StateType.LEADER, sm.currentStateType());
    }

    @Test
    public void testElectionTimeout() {
        Time mockTime = new MockTime();
        StateMachine sm = new StateMachine(1, mockStateStore, 1000, 500);
        sm.initialize(0);

        UnattachedState unattached = (UnattachedState) sm.currentState();

        // 未超时
        assertFalse(unattached.hasElectionTimeoutExpired(mockTime.milliseconds()));

        // 超时
        mockTime.sleep(1500);
        assertTrue(unattached.hasElectionTimeoutExpired(mockTime.milliseconds()));
    }
}
```

#### 8.2 集成测试

**文件**: `kraft-tests/src/test/java/raft/integration/ThreeNodeClusterTest.java`
```java
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ThreeNodeClusterTest {
    private List<RaftNode> cluster;

    @BeforeAll
    public void setUp() {
        // 启动 3 节点集群
        cluster = Arrays.asList(
            createNode(1, 9001),
            createNode(2, 9002),
            createNode(3, 9003)
        );

        cluster.forEach(RaftNode::start);

        // 等待选举完成
        await().atMost(10, TimeUnit.SECONDS)
            .until(this::hasLeader);
    }

    @Test
    public void testLeaderElection() {
        // 应该有一个 Leader
        RaftNode leader = findLeader();
        assertNotNull(leader);

        // 其他节点应该是 Follower
        long followers = cluster.stream()
            .filter(node -> node.currentState().type() == StateType.FOLLOWER)
            .count();
        assertEquals(2, followers);
    }

    @Test
    public void testLogReplication() throws Exception {
        RaftNode leader = findLeader();

        // 写入数据
        byte[] data = "test-data".getBytes();
        Long offset = leader.client().append(data).get(5, TimeUnit.SECONDS);

        // 等待复制到所有节点
        await().atMost(5, TimeUnit.SECONDS)
            .until(() -> allNodesHaveOffset(offset));

        // 验证所有节点的数据一致
        for (RaftNode node : cluster) {
            List<byte[]> records = node.client().read(offset, 1).get();
            assertEquals(1, records.size());
            assertArrayEquals(data, records.get(0));
        }
    }

    @Test
    public void testLeaderFailover() throws Exception {
        RaftNode oldLeader = findLeader();
        int oldLeaderId = oldLeader.nodeId();

        // 停止 Leader
        oldLeader.shutdown();

        // 等待新 Leader 选举
        await().atMost(10, TimeUnit.SECONDS)
            .until(() -> {
                RaftNode newLeader = findLeader();
                return newLeader != null && newLeader.nodeId() != oldLeaderId;
            });

        RaftNode newLeader = findLeader();
        assertNotNull(newLeader);
        assertNotEquals(oldLeaderId, newLeader.nodeId());

        // 新 Leader 应该能处理写入
        byte[] data = "new-leader-data".getBytes();
        Long offset = newLeader.client().append(data).get(5, TimeUnit.SECONDS);
        assertNotNull(offset);
    }
}
```

#### 8.3 混沌测试

**文件**: `kraft-tests/src/test/java/raft/chaos/ChaosTest.java`
```java
public class ChaosTest {

    @Test
    public void testRandomPartitions() {
        // 随机网络分区测试
        FiveNodeCluster cluster = new FiveNodeCluster();
        Random random = new Random();

        // 运行 5 分钟
        long endTime = System.currentTimeMillis() + 300_000;

        while (System.currentTimeMillis() < endTime) {
            // 随机写入
            if (random.nextBoolean()) {
                tryWrite(cluster);
            }

            // 随机分区
            if (random.nextInt(100) < 5) {  // 5% 概率
                createRandomPartition(cluster);
                Thread.sleep(random.nextInt(5000));  // 持续 0-5 秒
                healPartition(cluster);
            }

            Thread.sleep(100);
        }

        // 验证最终一致性
        healAllPartitions(cluster);
        await().atMost(30, TimeUnit.SECONDS)
            .until(() -> cluster.isConsistent());
    }

    @Test
    public void testRandomNodeFailures() {
        // 随机节点故障测试
        // 类似实现...
    }
}
```

---

## 第二部分：实现要点和最佳实践

### 设计原则

1. **渐进式开发**
   - 先实现最小可用版本（MVP）
   - 逐步添加高级特性
   - 每个阶段都要有测试覆盖

2. **模块化设计**
   - 清晰的接口定义
   - 松耦合的模块
   - 易于测试和替换

3. **正确性优先**
   - 先保证正确性，再优化性能
   - 充分的单元测试和集成测试
   - 使用形式化方法验证（如 TLA+）

### 关键实现细节

#### 1. 持久化顺序
```
写入顺序（严格）:
1. 更新持久化状态（如投票记录）
2. fsync
3. 更新内存状态
4. 发送网络消息
```

#### 2. 日志截断时机
```
只在以下情况截断日志:
- Follower 发现与 Leader 有分歧
- 新 Leader 当选后写入 LeaderChange 记录前
```

#### 3. 高水位更新
```
Leader 高水位更新条件:
- 多数派已复制
- 该条目是当前 epoch 的（不能提交旧 epoch 的日志）
```

#### 4. PreVote 的必要性
```
防止无效选举的场景:
- 网络分区恢复后，少数派节点 epoch 很高
- 没有 PreVote 会导致无谓的 epoch 增长
```

### 性能优化建议

1. **批处理**
   - 累积多个写入请求为一个批次
   - 批次大小和延迟的权衡

2. **Pipeline 复制**
   - 不等待前一个批次完成就发送下一个
   - 需要谨慎处理失败场景

3. **并行读取**
   - Follower 可以提供读服务（需要检查 Lease）
   - 或使用 ReadIndex 机制

4. **零拷贝**
   - 使用 FileChannel.transferTo
   - DirectByteBuffer

### 调试和监控

1. **日志输出**
   - 每次状态转换都要日志
   - 记录关键决策点（投票、截断等）

2. **指标收集**
   - 当前状态
   - 当前 epoch
   - 高水位
   - 复制延迟
   - 选举次数

3. **可视化**
   - 使用 Prometheus + Grafana
   - 日志时间线可视化

---

## 第三部分：学习资源

### 必读论文
1. Raft 原始论文
2. Raft PhD 论文（更详细）
3. Kafka 的 KIP-500（移除 ZooKeeper）

### 参考实现
1. etcd/raft (Go)
2. hashicorp/raft (Go)
3. Apache Kafka KRaft (Java)
4. TiKV/raft-rs (Rust)

### 测试工具
1. Jepsen - 分布式系统测试
2. Chaos Mesh - 混沌工程
3. TLA+ - 形式化验证

---

## 附录：项目里程碑

### MVP (最小可用产品) - 8 周
- [ ] 基础日志存储
- [ ] Leader 选举（PreVote + Vote）
- [ ] 简单的日志复制
- [ ] 基本的持久化
- [ ] 3 节点集群测试通过

### V1.0 - 16 周
- [ ] 完整的快照机制
- [ ] Check Quorum
- [ ] 完善的错误处理
- [ ] 性能测试通过
- [ ] 混沌测试通过

### V2.0 - 24 周
- [ ] 动态投票者变更
- [ ] Observer 支持
- [ ] Pipeline 复制
- [ ] 零拷贝优化
- [ ] 生产环境就绪

---

这是一个完整的实现指南。建议从 MVP 开始，逐步推进。每个阶段都要有充分的测试，确保正确性。

祝你实现顺利！
