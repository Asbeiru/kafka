# KRaft 重写项目执行计划

> **项目目标**: 基于 Apache Kafka KRaft 的核心思想，从零实现一个生产级的 Raft 共识协议
>
> **项目周期**: 20 周（5 个月）
>
> **实施方式**: 10 个 Sprint，每个 Sprint 2 周

---

## 目录

1. [项目概述](#项目概述)
2. [核心架构设计](#核心架构设计)
3. [技术栈选择](#技术栈选择)
4. [项目结构](#项目结构)
5. [Sprint 计划](#sprint-计划)
6. [核心类设计参考](#核心类设计参考)
7. [开发规范](#开发规范)
8. [测试策略](#测试策略)
9. [交付标准](#交付标准)

---

## 项目概述

### 项目范围

#### **包含的功能**
- ✅ Leader 选举（PreVote + Vote）
- ✅ 日志复制（Follower Fetch 模式）
- ✅ 日志持久化和恢复
- ✅ 快照机制
- ✅ Check Quorum（Leader 健康检查）
- ✅ 动态投票者变更
- ✅ Observer 支持
- ✅ 完整的错误处理和恢复
- ✅ 生产级监控指标

#### **不包含的功能**（未来扩展）
- ❌ 读优化（ReadIndex/Lease Read）
- ❌ Multi-Raft（多个 Raft Group）
- ❌ 日志压缩优化（增量快照）
- ❌ JBOD 支持

### 成功标准

1. **功能完整性**: 通过 100+ 单元测试，30+ 集成测试
2. **正确性**: 通过 Jepsen 混沌测试
3. **性能**:
   - 写入延迟 < 10ms (P99)
   - 吞吐量 > 10K writes/sec
   - Leader 选举时间 < 3 秒
4. **可靠性**: 任意单节点故障恢复时间 < 5 秒
5. **可维护性**: 代码覆盖率 > 85%

---

## 核心架构设计

### 分层架构

```
┌─────────────────────────────────────────────────────────┐
│                  Application Layer                       │
│            (User State Machine Implementation)          │
└─────────────────────────────────────────────────────────┘
                          ↓ ↑
                    RaftClient API
                          ↓ ↑
┌─────────────────────────────────────────────────────────┐
│                   Raft Core Engine                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ State Machine│  │   Election   │  │  Replication │  │
│  │   Manager    │  │   Manager    │  │   Manager    │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   Snapshot   │  │  Membership  │  │    Metrics   │  │
│  │   Manager    │  │   Manager    │  │   Collector  │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓ ↑
┌─────────────────────────────────────────────────────────┐
│                  Storage Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Replicated   │  │    State     │  │   Snapshot   │  │
│  │     Log      │  │    Store     │  │    Store     │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓ ↑
┌─────────────────────────────────────────────────────────┐
│                  Network Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   gRPC       │  │   Protocol   │  │  Connection  │  │
│  │   Server     │  │   Handlers   │  │   Manager    │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### 核心模块依赖关系

```
RaftNode (Main Entry)
    ├── RaftEngine (Event Loop)
    │   ├── QuorumState (State Machine)
    │   │   ├── UnattachedState
    │   │   ├── ProspectiveState
    │   │   ├── CandidateState
    │   │   ├── LeaderState
    │   │   ├── FollowerState
    │   │   └── ResignedState
    │   ├── ElectionManager
    │   ├── ReplicationManager
    │   │   ├── BatchAccumulator
    │   │   └── FetchManager
    │   ├── SnapshotManager
    │   └── MembershipManager
    ├── ReplicatedLog
    │   ├── LogSegment
    │   └── LogCleaner
    ├── QuorumStateStore
    └── NetworkChannel
        ├── RaftServer (gRPC)
        └── RaftClient (gRPC)
```

---

## 技术栈选择

### 编程语言和框架

```yaml
语言: Java 17
构建工具: Maven 3.9+
网络框架: gRPC 1.59+
序列化: Protocol Buffers 3.24+
日志: SLF4J + Logback
测试框架:
  - JUnit 5
  - AssertJ
  - Mockito
  - Testcontainers
  - Awaitility
代码质量:
  - Checkstyle
  - SpotBugs
  - JaCoCo (Coverage)
```

### 开发环境要求

```
JDK: 17+
Maven: 3.9+
IDE: IntelliJ IDEA (推荐) / Eclipse
Git: 2.x+
Docker: 20.x+ (用于集成测试)
```

---

## 项目结构

### 模块划分

```
mini-raft/
├── pom.xml                          # 父 POM
├── README.md
├── LICENSE
├── .gitignore
│
├── raft-common/                     # 公共模块
│   ├── pom.xml
│   └── src/main/java/io/github/mini/raft/common/
│       ├── model/                   # 数据模型
│       │   ├── LogEntry.java
│       │   ├── OffsetAndEpoch.java
│       │   └── ReplicaKey.java
│       ├── exception/               # 异常定义
│       │   ├── NotLeaderException.java
│       │   └── RaftException.java
│       └── utils/                   # 工具类
│           ├── Time.java
│           └── Timer.java
│
├── raft-api/                        # API 模块
│   ├── pom.xml
│   └── src/main/java/io/github/mini/raft/api/
│       ├── RaftClient.java          # 客户端接口
│       ├── RaftNode.java            # 节点接口
│       ├── StateMachine.java        # 应用状态机接口
│       ├── BatchReader.java
│       └── Listener.java
│
├── raft-protocol/                   # 协议模块
│   ├── pom.xml
│   └── src/main/proto/
│       ├── raft.proto              # Raft 协议定义
│       └── raft_service.proto      # gRPC 服务定义
│
├── raft-core/                       # 核心实现
│   ├── pom.xml
│   └── src/main/java/io/github/mini/raft/core/
│       ├── RaftEngine.java         # 核心引擎
│       ├── RaftNodeImpl.java       # 节点实现
│       │
│       ├── state/                  # 状态机
│       │   ├── State.java
│       │   ├── QuorumState.java
│       │   ├── UnattachedState.java
│       │   ├── ProspectiveState.java
│       │   ├── CandidateState.java
│       │   ├── LeaderState.java
│       │   ├── FollowerState.java
│       │   └── ResignedState.java
│       │
│       ├── election/               # 选举
│       │   ├── ElectionManager.java
│       │   ├── VoteGranter.java
│       │   └── ElectionState.java
│       │
│       ├── replication/            # 复制
│       │   ├── ReplicationManager.java
│       │   ├── FetchManager.java
│       │   ├── BatchAccumulator.java
│       │   ├── BatchBuilder.java
│       │   └── ReplicaState.java
│       │
│       ├── snapshot/               # 快照
│       │   ├── SnapshotManager.java
│       │   ├── SnapshotWriter.java
│       │   └── SnapshotReader.java
│       │
│       └── membership/             # 成员变更
│           ├── MembershipManager.java
│           ├── VoterSet.java
│           └── VoterSetHistory.java
│
├── raft-storage/                    # 存储模块
│   ├── pom.xml
│   └── src/main/java/io/github/mini/raft/storage/
│       ├── log/
│       │   ├── ReplicatedLog.java
│       │   ├── LogSegment.java
│       │   ├── LogManager.java
│       │   ├── LogIndex.java
│       │   └── LogCleaner.java
│       ├── state/
│       │   ├── QuorumStateStore.java
│       │   └── FileQuorumStateStore.java
│       └── snapshot/
│           ├── SnapshotStore.java
│           └── FileSnapshotStore.java
│
├── raft-network/                    # 网络模块
│   ├── pom.xml
│   └── src/main/java/io/github/mini/raft/network/
│       ├── NetworkChannel.java
│       ├── GrpcNetworkChannel.java
│       ├── RaftServiceImpl.java
│       ├── ConnectionManager.java
│       └── RequestRouter.java
│
├── raft-metrics/                    # 指标模块
│   ├── pom.xml
│   └── src/main/java/io/github/mini/raft/metrics/
│       ├── RaftMetrics.java
│       └── MetricsCollector.java
│
└── raft-tests/                      # 测试模块
    ├── pom.xml
    └── src/test/java/io/github/mini/raft/tests/
        ├── unit/                    # 单元测试
        ├── integration/             # 集成测试
        │   ├── ThreeNodeClusterTest.java
        │   ├── FiveNodeClusterTest.java
        │   └── PartitionTest.java
        └── chaos/                   # 混沌测试
            └── JepsenTest.java
```

---

## Sprint 计划

### Sprint 0: 项目初始化 (Week 1-2)

#### **目标**: 搭建项目骨架和开发环境

#### **任务清单**

| 任务 | 负责模块 | 估时 | 优先级 |
|------|---------|------|--------|
| 创建 Maven 多模块项目 | - | 2h | P0 |
| 配置 Protocol Buffers 插件 | raft-protocol | 2h | P0 |
| 定义 Raft 协议消息 | raft-protocol | 4h | P0 |
| 实现通用数据结构 | raft-common | 4h | P0 |
| 配置 CI/CD | - | 3h | P0 |
| 编写开发文档 | - | 3h | P1 |

#### **交付物**
- [x] 完整的项目结构
- [x] Protocol Buffers 协议定义
- [x] 基础数据模型
- [x] CI/CD 配置
- [x] 开发环境文档

#### **验收标准**
```bash
# 项目可以编译通过
mvn clean compile

# 测试框架就位
mvn test

# 代码风格检查通过
mvn checkstyle:check
```

---

### Sprint 1: 日志存储实现 (Week 3-4)

#### **目标**: 实现可靠的日志存储层

#### **参考 Kafka KRaft 类**
```java
// 参考这些 Kafka 类:
org.apache.kafka.raft.ReplicatedLog
org.apache.kafka.storage.internals.log.LogSegment
org.apache.kafka.storage.internals.log.OffsetIndex
```

#### **核心类实现**

**1. ReplicatedLog 接口**
```java
package io.github.mini.raft.storage.log;

public interface ReplicatedLog extends AutoCloseable {
    // 追加日志
    LogAppendInfo appendAsLeader(Records records, int epoch);
    LogAppendInfo appendAsFollower(Records records, int epoch);

    // 读取日志
    LogFetchInfo read(long startOffset, int maxBytes);

    // 截断日志
    void truncateTo(long offset);

    // 元数据
    long startOffset();
    long endOffset();
    int lastEpoch();
    OffsetAndEpoch endOffsetForEpoch(int epoch);

    // 高水位
    LogOffsetMetadata highWatermark();
    void updateHighWatermark(LogOffsetMetadata offsetMetadata);

    // 初始化 Leader Epoch
    void initializeLeaderEpoch(int epoch);
}
```

**2. LogSegment 实现**
```java
package io.github.mini.raft.storage.log;

/**
 * 日志段，类似 Kafka 的 LogSegment
 *
 * 文件格式:
 * - 00000000000000000000.log   (日志文件)
 * - 00000000000000000000.index (偏移量索引)
 *
 * 参考: org.apache.kafka.storage.internals.log.LogSegment
 */
public class LogSegment implements AutoCloseable {
    private final File logFile;
    private final File indexFile;
    private final long baseOffset;
    private final FileChannel logChannel;
    private final OffsetIndex offsetIndex;

    // 追加记录
    public void append(long offset, int epoch, ByteBuffer records) {
        // 1. 写入日志文件
        // 2. 更新索引
        // 3. fsync
    }

    // 读取记录
    public Records read(long startOffset, int maxBytes) {
        // 1. 从索引找到物理位置
        // 2. 从日志文件读取
    }

    // 恢复日志（启动时）
    public void recover() {
        // 1. 扫描日志文件
        // 2. 重建索引
        // 3. 检查完整性
    }
}
```

**3. OffsetIndex 实现**
```java
package io.github.mini.raft.storage.log;

/**
 * 偏移量索引，用于快速查找
 *
 * 文件格式（每条索引 8 字节）:
 * - 相对偏移量 (4 bytes)
 * - 物理位置 (4 bytes)
 *
 * 参考: org.apache.kafka.storage.internals.log.OffsetIndex
 */
public class OffsetIndex {
    private final MappedByteBuffer mmap;

    public void append(long offset, int position) {
        // 写入 mmap
    }

    public IndexEntry lookup(long offset) {
        // 二分查找
    }
}
```

#### **任务清单**

| 任务 | 文件 | 估时 | 优先级 |
|------|------|------|--------|
| 实现 LogEntry 数据结构 | LogEntry.java | 2h | P0 |
| 实现 OffsetIndex | OffsetIndex.java | 6h | P0 |
| 实现 LogSegment | LogSegment.java | 12h | P0 |
| 实现 LogManager | LogManager.java | 8h | P0 |
| 实现 ReplicatedLog | ReplicatedLogImpl.java | 8h | P0 |
| 单元测试 - 日志追加 | LogSegmentTest.java | 4h | P0 |
| 单元测试 - 日志读取 | LogSegmentTest.java | 3h | P0 |
| 单元测试 - 日志截断 | LogSegmentTest.java | 3h | P0 |
| 单元测试 - 崩溃恢复 | LogRecoveryTest.java | 4h | P0 |

#### **交付物**
- [x] 完整的日志存储实现
- [x] 索引机制
- [x] 日志恢复逻辑
- [x] 15+ 单元测试

#### **验收标准**
```java
@Test
public void testLogAppendAndRead() {
    ReplicatedLog log = new ReplicatedLogImpl(logDir);

    // 追加
    LogAppendInfo info = log.appendAsLeader(records, 1);
    assertEquals(0, info.firstOffset());

    // 读取
    LogFetchInfo fetch = log.read(0, 1024);
    assertEquals(records, fetch.records());
}

@Test
public void testLogTruncate() {
    // 追加 10 条记录
    for (int i = 0; i < 10; i++) {
        log.appendAsLeader(records, 1);
    }

    // 截断到 offset 5
    log.truncateTo(5);

    assertEquals(5, log.endOffset());
}

@Test
public void testCrashRecovery() {
    // 追加记录
    log.appendAsLeader(records, 1);

    // 模拟崩溃（不关闭）
    log = null;

    // 重新打开
    log = new ReplicatedLogImpl(logDir);

    // 验证数据完整性
    assertEquals(1, log.endOffset());
}
```

---

### Sprint 2: 持久化状态 + 状态机基础 (Week 5-6)

#### **目标**: 实现选举状态持久化和基础状态机框架

#### **参考 Kafka KRaft 类**
```java
// 参考:
org.apache.kafka.raft.QuorumState
org.apache.kafka.raft.QuorumStateStore
org.apache.kafka.raft.FileQuorumStateStore
org.apache.kafka.raft.EpochState
```

#### **核心类实现**

**1. QuorumStateStore 接口**
```java
package io.github.mini.raft.storage.state;

/**
 * 持久化选举状态
 *
 * 文件格式 (quorum-state):
 * {
 *   "epoch": 5,
 *   "votedFor": 1,
 *   "leaderId": 1,
 *   "timestamp": 1234567890
 * }
 *
 * 参考: org.apache.kafka.raft.QuorumStateStore
 */
public interface QuorumStateStore {
    void writeElectionState(ElectionState state);
    Optional<ElectionState> readElectionState();
    void clear();
}
```

**2. State 接口和实现**
```java
package io.github.mini.raft.core.state;

/**
 * 状态接口
 *
 * 参考: org.apache.kafka.raft.EpochState
 */
public interface State {
    StateType type();
    int epoch();
    String name();
}

public enum StateType {
    UNATTACHED,
    PROSPECTIVE,
    CANDIDATE,
    LEADER,
    FOLLOWER,
    RESIGNED
}
```

**3. QuorumState 状态管理器**
```java
package io.github.mini.raft.core.state;

/**
 * 状态机控制器
 *
 * 参考: org.apache.kafka.raft.QuorumState
 */
public class QuorumState {
    private final int localId;
    private final QuorumStateStore store;
    private final Time time;
    private final int electionTimeoutMs;
    private final int fetchTimeoutMs;

    private volatile State state;

    public void initialize(OffsetAndEpoch logEndOffsetAndEpoch) {
        // 从持久化状态恢复
        ElectionState election = store.readElectionState()
            .orElse(new ElectionState(0, OptionalInt.empty()));

        // 初始化为合适的状态
        if (election.hasVoted()) {
            // 恢复为 Candidate 或 Follower
        } else {
            this.state = new UnattachedState(
                election.epoch(),
                time.milliseconds(),
                randomElectionTimeout()
            );
        }
    }

    // 状态转换方法
    public void transitionToProspective(int epoch, Set<Integer> voters) { }
    public void transitionToCandidate(int epoch, Set<Integer> voters) { }
    public void transitionToLeader(int epoch, long epochStartOffset, Set<Integer> voters) { }
    public void transitionToFollower(int epoch, int leaderId) { }
    public void transitionToUnattached(int epoch) { }
}
```

**4. 各个状态实现**
```java
// UnattachedState.java
public class UnattachedState implements State {
    private final int epoch;
    private final long electionDeadline;

    public boolean hasElectionTimeoutExpired(long currentTime) {
        return currentTime >= electionDeadline;
    }
}

// ProspectiveState.java - PreVote 状态
public class ProspectiveState implements State {
    private final Set<Integer> grantingVoters = new HashSet<>();

    public boolean recordGrantedVote(int voterId) {
        return grantingVoters.add(voterId);
    }

    public boolean hasPreVoteMajority(int totalVoters) {
        return grantingVoters.size() > totalVoters / 2;
    }
}

// CandidateState.java
public class CandidateState implements State {
    private final Set<Integer> grantingVoters = new HashSet<>();

    public boolean hasMajority(int totalVoters) {
        return grantingVoters.size() > totalVoters / 2;
    }
}

// LeaderState.java
public class LeaderState implements State {
    private final Map<Integer, ReplicaState> replicaStates = new HashMap<>();
    private long highWatermark;

    public boolean maybeUpdateHighWatermark() {
        // 计算多数派的 match offset
    }
}

// FollowerState.java
public class FollowerState implements State {
    private final int leaderId;
    private final long fetchDeadline;

    public boolean hasFetchTimeoutExpired(long currentTime) {
        return currentTime >= fetchDeadline;
    }
}
```

#### **任务清单**

| 任务 | 文件 | 估时 | 优先级 |
|------|------|------|--------|
| 实现 ElectionState 数据结构 | ElectionState.java | 2h | P0 |
| 实现 QuorumStateStore 接口 | QuorumStateStore.java | 2h | P0 |
| 实现 FileQuorumStateStore | FileQuorumStateStore.java | 6h | P0 |
| 实现 State 接口 | State.java | 1h | P0 |
| 实现 UnattachedState | UnattachedState.java | 3h | P0 |
| 实现 ProspectiveState | ProspectiveState.java | 4h | P0 |
| 实现 CandidateState | CandidateState.java | 4h | P0 |
| 实现 LeaderState | LeaderState.java | 8h | P0 |
| 实现 FollowerState | FollowerState.java | 3h | P0 |
| 实现 ResignedState | ResignedState.java | 2h | P1 |
| 实现 QuorumState 控制器 | QuorumState.java | 8h | P0 |
| 单元测试 - 状态转换 | QuorumStateTest.java | 6h | P0 |
| 单元测试 - 状态持久化 | QuorumStateStoreTest.java | 4h | P0 |

#### **交付物**
- [x] 完整的状态机框架
- [x] 6 种状态实现
- [x] 状态持久化
- [x] 20+ 单元测试

#### **验收标准**
```java
@Test
public void testStateTransitions() {
    QuorumState state = new QuorumState(1, store, 5000, 3000);
    state.initialize(new OffsetAndEpoch(0, 0));

    // 初始状态
    assertEquals(StateType.UNATTACHED, state.currentState().type());

    // Unattached -> Prospective
    state.transitionToProspective(1, Set.of(1, 2, 3));
    assertEquals(StateType.PROSPECTIVE, state.currentState().type());

    // Prospective -> Candidate
    state.transitionToCandidate(1, Set.of(1, 2, 3));
    assertEquals(StateType.CANDIDATE, state.currentState().type());

    // Candidate -> Leader
    state.transitionToLeader(1, 0, Set.of(1, 2, 3));
    assertEquals(StateType.LEADER, state.currentState().type());
}

@Test
public void testStatePersistence() {
    QuorumState state = new QuorumState(1, store, 5000, 3000);
    state.transitionToCandidate(5, Set.of(1, 2, 3));

    // 验证持久化
    ElectionState stored = store.readElectionState().get();
    assertEquals(5, stored.epoch());
    assertEquals(1, stored.votedFor().getAsInt());
}
```

---

### Sprint 3: 网络通信实现 (Week 7-8)

#### **目标**: 实现基于 gRPC 的网络通信层

#### **参考 Kafka KRaft 类**
```java
// 参考:
org.apache.kafka.raft.NetworkChannel
org.apache.kafka.raft.KafkaNetworkChannel
```

#### **Protocol Buffers 定义**

**文件: raft.proto**
```protobuf
syntax = "proto3";

package miniraft;

option java_package = "io.github.mini.raft.protocol";
option java_outer_classname = "RaftProto";

// 投票请求
message VoteRequest {
  int32 epoch = 1;
  int32 candidate_id = 2;
  int64 last_log_offset = 3;
  int32 last_log_epoch = 4;
  bool pre_vote = 5;  // true 表示 PreVote
}

message VoteResponse {
  int32 epoch = 1;
  bool vote_granted = 2;
  optional int32 leader_id = 3;
}

// Fetch 请求（Follower 拉取）
message FetchRequest {
  int32 replica_id = 1;
  int32 replica_epoch = 2;
  int64 fetch_offset = 3;
  int32 last_fetched_epoch = 4;
  int32 max_bytes = 5;
}

message FetchResponse {
  int32 epoch = 1;
  int32 leader_id = 2;
  int64 high_watermark = 3;
  repeated LogEntryProto entries = 4;
  optional SnapshotIdProto snapshot = 5;
  optional OffsetAndEpochProto diverging = 6;
}

// 日志条目
message LogEntryProto {
  int64 offset = 1;
  int32 epoch = 2;
  int64 timestamp = 3;
  bytes data = 4;
  EntryType type = 5;
}

enum EntryType {
  DATA = 0;
  LEADER_CHANGE = 1;
  VOTER_CHANGE = 2;
}

message OffsetAndEpochProto {
  int64 offset = 1;
  int32 epoch = 2;
}

message SnapshotIdProto {
  int64 offset = 1;
  int32 epoch = 2;
}
```

**文件: raft_service.proto**
```protobuf
syntax = "proto3";

package miniraft;

import "raft.proto";

option java_package = "io.github.mini.raft.protocol";

service RaftService {
  // 投票 RPC
  rpc Vote(VoteRequest) returns (VoteResponse);

  // Fetch RPC
  rpc Fetch(FetchRequest) returns (FetchResponse);

  // Fetch 快照 RPC
  rpc FetchSnapshot(FetchSnapshotRequest) returns (stream FetchSnapshotResponse);
}

message FetchSnapshotRequest {
  int32 replica_id = 1;
  int64 snapshot_offset = 2;
  int32 snapshot_epoch = 3;
  int64 position = 4;
  int32 max_bytes = 5;
}

message FetchSnapshotResponse {
  int64 snapshot_size = 1;
  int64 position = 2;
  bytes data = 3;
}
```

#### **核心类实现**

**1. NetworkChannel 接口**
```java
package io.github.mini.raft.network;

/**
 * 网络通信抽象
 *
 * 参考: org.apache.kafka.raft.NetworkChannel
 */
public interface NetworkChannel {
    // 发送请求
    CompletableFuture<VoteResponse> sendVoteRequest(
        int targetId, VoteRequest request);

    CompletableFuture<FetchResponse> sendFetchRequest(
        int targetId, FetchRequest request);

    // 启动/关闭
    void start();
    void shutdown();
}
```

**2. GrpcNetworkChannel 实现**
```java
package io.github.mini.raft.network;

/**
 * 基于 gRPC 的网络通信实现
 */
public class GrpcNetworkChannel implements NetworkChannel {
    private final Map<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<Integer, RaftServiceStub> stubs = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<VoteResponse> sendVoteRequest(
            int targetId, VoteRequest request) {
        RaftServiceStub stub = getOrCreateStub(targetId);

        CompletableFuture<VoteResponse> future = new CompletableFuture<>();

        stub.vote(request, new StreamObserver<VoteResponse>() {
            @Override
            public void onNext(VoteResponse response) {
                future.complete(response);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() { }
        });

        return future;
    }

    private RaftServiceStub getOrCreateStub(int nodeId) {
        return stubs.computeIfAbsent(nodeId, id -> {
            String address = getNodeAddress(id);
            ManagedChannel channel = ManagedChannelBuilder
                .forTarget(address)
                .usePlaintext()
                .build();
            channels.put(id, channel);
            return RaftServiceGrpc.newStub(channel);
        });
    }
}
```

**3. RaftServiceImpl 服务端实现**
```java
package io.github.mini.raft.network;

/**
 * gRPC 服务端实现
 */
public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftEngine engine;

    @Override
    public void vote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
        try {
            VoteResponse response = engine.handleVoteRequest(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void fetch(FetchRequest request, StreamObserver<FetchResponse> responseObserver) {
        try {
            FetchResponse response = engine.handleFetchRequest(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
```

#### **任务清单**

| 任务 | 文件 | 估时 | 优先级 |
|------|------|------|--------|
| 编写 Protocol Buffers 定义 | raft.proto | 4h | P0 |
| 编写 gRPC 服务定义 | raft_service.proto | 2h | P0 |
| 实现 NetworkChannel 接口 | NetworkChannel.java | 2h | P0 |
| 实现 GrpcNetworkChannel | GrpcNetworkChannel.java | 8h | P0 |
| 实现 RaftServiceImpl | RaftServiceImpl.java | 6h | P0 |
| 实现连接管理器 | ConnectionManager.java | 4h | P1 |
| 实现请求路由 | RequestRouter.java | 3h | P1 |
| 单元测试 - 网络通信 | NetworkChannelTest.java | 6h | P0 |
| 集成测试 - 两节点通信 | TwoNodeTest.java | 4h | P0 |

#### **交付物**
- [x] 完整的 gRPC 通信层
- [x] 连接管理
- [x] 10+ 测试

#### **验收标准**
```java
@Test
public void testVoteRequest() {
    // 启动两个节点
    RaftNode node1 = startNode(1, 9001);
    RaftNode node2 = startNode(2, 9002);

    // Node1 发送投票请求给 Node2
    VoteRequest request = VoteRequest.newBuilder()
        .setEpoch(1)
        .setCandidateId(1)
        .setPreVote(true)
        .build();

    VoteResponse response = node1.network()
        .sendVoteRequest(2, request)
        .get(5, TimeUnit.SECONDS);

    assertTrue(response.getVoteGranted());
}
```

---

### Sprint 4: 选举实现 (Week 9-10)

#### **目标**: 实现完整的 Leader 选举流程

#### **参考 Kafka KRaft 类**
```java
// 参考:
org.apache.kafka.raft.KafkaRaftClient (选举部分)
// 特别关注 handleVoteRequest, handleVoteResponse 方法
```

#### **核心类实现**

**1. ElectionManager**
```java
package io.github.mini.raft.core.election;

/**
 * 选举管理器
 */
public class ElectionManager {
    private final int nodeId;
    private final QuorumState quorumState;
    private final ReplicatedLog log;
    private final NetworkChannel network;
    private final Set<Integer> voters;

    /**
     * 发起 PreVote
     *
     * 参考: KafkaRaftClient.pollUnattached()
     */
    public void startPreVote() {
        State current = quorumState.currentState();
        if (current.type() != StateType.UNATTACHED) {
            logger.warn("Cannot start PreVote from state: {}", current.type());
            return;
        }

        int newEpoch = current.epoch() + 1;
        quorumState.transitionToProspective(newEpoch, voters);

        // 发送 PreVote 请求
        VoteRequest request = VoteRequest.newBuilder()
            .setEpoch(newEpoch)
            .setCandidateId(nodeId)
            .setLastLogOffset(log.endOffset() - 1)
            .setLastLogEpoch(log.lastEpoch())
            .setPreVote(true)
            .build();

        for (int voterId : voters) {
            if (voterId != nodeId) {
                network.sendVoteRequest(voterId, request)
                    .thenAccept(response -> handlePreVoteResponse(voterId, response));
            }
        }

        logger.info("Node {} started PreVote for epoch {}", nodeId, newEpoch);
    }

    /**
     * 处理 PreVote 响应
     */
    public void handlePreVoteResponse(int fromNode, VoteResponse response) {
        State current = quorumState.currentState();
        if (current.type() != StateType.PROSPECTIVE) {
            return;
        }

        ProspectiveState prospective = (ProspectiveState) current;

        // 检查 epoch
        if (response.getEpoch() > prospective.epoch()) {
            quorumState.transitionToUnattached(response.getEpoch());
            return;
        }

        if (response.getVoteGranted()) {
            prospective.recordGrantedVote(fromNode);

            // 检查是否获得多数派
            if (prospective.hasPreVoteMajority(voters.size())) {
                startElection();
            }
        }
    }

    /**
     * 发起正式选举
     *
     * 参考: KafkaRaftClient.pollCandidate()
     */
    public void startElection() {
        State current = quorumState.currentState();
        int newEpoch = current.epoch();

        quorumState.transitionToCandidate(newEpoch, voters);

        // 发送正式投票请求
        VoteRequest request = VoteRequest.newBuilder()
            .setEpoch(newEpoch)
            .setCandidateId(nodeId)
            .setLastLogOffset(log.endOffset() - 1)
            .setLastLogEpoch(log.lastEpoch())
            .setPreVote(false)
            .build();

        for (int voterId : voters) {
            if (voterId != nodeId) {
                network.sendVoteRequest(voterId, request)
                    .thenAccept(response -> handleVoteResponse(fromNode, response));
            }
        }

        logger.info("Node {} started election for epoch {}", nodeId, newEpoch);
    }

    /**
     * 处理投票响应
     */
    public void handleVoteResponse(int fromNode, VoteResponse response) {
        State current = quorumState.currentState();
        if (current.type() != StateType.CANDIDATE) {
            return;
        }

        CandidateState candidate = (CandidateState) current;

        if (response.getEpoch() > candidate.epoch()) {
            quorumState.transitionToUnattached(response.getEpoch());
            return;
        }

        if (response.getVoteGranted()) {
            candidate.recordGrantedVote(fromNode);

            if (candidate.hasMajority(voters.size())) {
                becomeLeader();
            }
        }
    }

    /**
     * 成为 Leader
     */
    private void becomeLeader() {
        long epochStartOffset = log.endOffset();
        int epoch = quorumState.currentEpoch();

        // 初始化 Leader Epoch
        log.initializeLeaderEpoch(epoch);

        // 写入 LeaderChange 记录
        writeLeaderChangeRecord(epoch, voters);

        // 转换为 Leader 状态
        quorumState.transitionToLeader(epoch, epochStartOffset, voters);

        logger.info("Node {} became leader for epoch {}", nodeId, epoch);
    }
}
```

**2. VoteGranter**
```java
package io.github.mini.raft.core.election;

/**
 * 投票决策器
 *
 * 参考: KafkaRaftClient.handleVoteRequest()
 */
public class VoteGranter {
    private final QuorumState quorumState;
    private final QuorumStateStore stateStore;
    private final ReplicatedLog log;

    public VoteResponse handleVoteRequest(VoteRequest request) {
        int localEpoch = quorumState.currentEpoch();

        // 1. 拒绝旧的 epoch
        if (request.getEpoch() < localEpoch) {
            logger.info("Rejecting vote for old epoch: {} < {}",
                request.getEpoch(), localEpoch);
            return rejectVote(localEpoch);
        }

        // 2. 发现更高的 epoch
        if (request.getEpoch() > localEpoch) {
            quorumState.transitionToUnattached(request.getEpoch());
            localEpoch = request.getEpoch();
        }

        // 3. 检查是否已经投票
        if (!request.getPreVote()) {
            Optional<ElectionState> existingVote = stateStore.readElectionState();
            if (existingVote.isPresent() &&
                existingVote.get().epoch() == request.getEpoch() &&
                existingVote.get().votedFor().isPresent() &&
                existingVote.get().votedFor().getAsInt() != request.getCandidateId()) {
                logger.info("Already voted for {} in epoch {}",
                    existingVote.get().votedFor().getAsInt(), request.getEpoch());
                return rejectVote(localEpoch);
            }
        }

        // 4. 检查日志是否足够新
        if (!isLogUpToDate(request)) {
            logger.info("Rejecting vote: candidate log not up-to-date");
            return rejectVote(localEpoch);
        }

        // 5. 投票
        if (!request.getPreVote()) {
            ElectionState newState = new ElectionState(
                request.getEpoch(),
                OptionalInt.of(request.getCandidateId()),
                OptionalInt.empty()
            );
            stateStore.writeElectionState(newState);
        }

        logger.info("Granted {} vote for {} in epoch {}",
            request.getPreVote() ? "pre" : "",
            request.getCandidateId(),
            request.getEpoch());

        return VoteResponse.newBuilder()
            .setEpoch(localEpoch)
            .setVoteGranted(true)
            .build();
    }

    private boolean isLogUpToDate(VoteRequest request) {
        int localLastEpoch = log.lastEpoch();
        long localLastOffset = log.endOffset() - 1;

        // 先比较 epoch
        if (request.getLastLogEpoch() != localLastEpoch) {
            return request.getLastLogEpoch() > localLastEpoch;
        }

        // epoch 相同，比较 offset
        return request.getLastLogOffset() >= localLastOffset;
    }

    private VoteResponse rejectVote(int epoch) {
        return VoteResponse.newBuilder()
            .setEpoch(epoch)
            .setVoteGranted(false)
            .build();
    }
}
```

#### **任务清单**

| 任务 | 文件 | 估时 | 优先级 |
|------|------|------|--------|
| 实现 ElectionManager | ElectionManager.java | 10h | P0 |
| 实现 VoteGranter | VoteGranter.java | 6h | P0 |
| 实现 LeaderChange 记录 | LeaderChangeRecord.java | 3h | P0 |
| 单元测试 - PreVote | ElectionManagerTest.java | 4h | P0 |
| 单元测试 - 投票决策 | VoteGranterTest.java | 4h | P0 |
| 集成测试 - 3 节点选举 | ThreeNodeElectionTest.java | 6h | P0 |
| 集成测试 - 5 节点选举 | FiveNodeElectionTest.java | 4h | P0 |
| 集成测试 - 网络分区 | PartitionElectionTest.java | 6h | P0 |

#### **交付物**
- [x] 完整的选举实现
- [x] PreVote 机制
- [x] 20+ 测试

#### **验收标准**
```java
@Test
public void testThreeNodeElection() {
    // 启动 3 节点集群
    List<RaftNode> cluster = startCluster(1, 2, 3);

    // 触发 Node1 选举超时
    cluster.get(0).triggerElectionTimeout();

    // 等待选举完成
    await().atMost(10, SECONDS).until(() -> hasLeader(cluster));

    // 验证有一个 Leader
    long leaderCount = cluster.stream()
        .filter(node -> node.state().type() == StateType.LEADER)
        .count();
    assertEquals(1, leaderCount);

    // 验证其他节点是 Follower
    long followerCount = cluster.stream()
        .filter(node -> node.state().type() == StateType.FOLLOWER)
        .count();
    assertEquals(2, followerCount);
}

@Test
public void testPreVotePreventsSplit() {
    // 创建 3 节点集群
    List<RaftNode> cluster = startCluster(1, 2, 3);

    // Node1 成为 Leader
    electLeader(cluster, 1);

    // 网络分区：Node3 被隔离
    partition(cluster, Set.of(3), Set.of(1, 2));

    // Node3 会尝试选举，但 PreVote 应该失败
    cluster.get(2).triggerElectionTimeout();

    // Node3 不应该增加 epoch
    await().during(5, SECONDS).until(() ->
        cluster.get(2).currentEpoch() == cluster.get(0).currentEpoch()
    );
}
```

---

### Sprint 5: 日志复制实现 (Week 11-12)

#### **目标**: 实现 Leader 到 Follower 的日志复制

#### **参考 Kafka KRaft 类**
```java
// 参考:
org.apache.kafka.raft.internals.BatchAccumulator
org.apache.kafka.raft.KafkaRaftClient.handleFetchRequest()
org.apache.kafka.raft.KafkaRaftClient.handleFetchResponse()
```

#### **核心类实现**

**1. BatchAccumulator**
```java
package io.github.mini.raft.core.replication;

/**
 * 批次累积器
 *
 * 参考: org.apache.kafka.raft.internals.BatchAccumulator
 */
public class BatchAccumulator<T> {
    private final int epoch;
    private final int maxBatchSize;
    private final int lingerMs;
    private final Serializer<T> serializer;

    private final Queue<CompletedBatch> completedBatches = new ConcurrentLinkedQueue<>();
    private BatchBuilder currentBatch;
    private long nextOffset;

    /**
     * 追加记录
     */
    public CompletableFuture<Long> append(int epoch, List<T> records) {
        if (epoch != this.epoch) {
            return CompletableFuture.failedFuture(
                new NotLeaderException("Wrong epoch: " + epoch)
            );
        }

        CompletableFuture<Long> future = new CompletableFuture<>();

        synchronized (this) {
            if (currentBatch == null || !currentBatch.hasRoom(records)) {
                closeCurrentBatch();
                currentBatch = new BatchBuilder(nextOffset, epoch, maxBatchSize, serializer);
            }

            long lastOffset = currentBatch.append(records);
            nextOffset = lastOffset + 1;
            currentBatch.addCallback(future);

            if (shouldFlush()) {
                closeCurrentBatch();
            }
        }

        return future;
    }

    /**
     * 排空已完成的批次
     */
    public List<CompletedBatch> drain() {
        List<CompletedBatch> batches = new ArrayList<>();
        CompletedBatch batch;
        while ((batch = completedBatches.poll()) != null) {
            batches.add(batch);
        }
        return batches;
    }

    private void closeCurrentBatch() {
        if (currentBatch != null) {
            completedBatches.add(currentBatch.build());
            currentBatch = null;
        }
    }

    private boolean shouldFlush() {
        return currentBatch.sizeInBytes() >= maxBatchSize ||
               currentBatch.elapsedMs() >= lingerMs;
    }
}
```

**2. ReplicationManager (Leader 端)**
```java
package io.github.mini.raft.core.replication;

/**
 * 复制管理器（Leader 端）
 *
 * 参考: KafkaRaftClient.handleFetchRequest()
 */
public class ReplicationManager {
    private final QuorumState quorumState;
    private final ReplicatedLog log;

    /**
     * 处理 Follower 的 Fetch 请求
     */
    public FetchResponse handleFetchRequest(FetchRequest request) {
        State current = quorumState.currentState();
        if (current.type() != StateType.LEADER) {
            // 不是 Leader，返回错误
            return buildNotLeaderResponse(current.epoch());
        }

        LeaderState leader = (LeaderState) current;
        int followerId = request.getReplicaId();
        long fetchOffset = request.getFetchOffset();
        int lastFetchedEpoch = request.getLastFetchedEpoch();

        // 1. 验证 Follower 的日志位置
        ValidOffsetAndEpoch validation = log.validateOffsetAndEpoch(
            fetchOffset, lastFetchedEpoch
        );

        // 2. 如果需要快照
        if (validation.kind() == ValidationKind.SNAPSHOT) {
            return buildSnapshotResponse(
                leader.epoch(),
                leader.nodeId(),
                validation.offsetAndEpoch()
            );
        }

        // 3. 如果有分歧
        if (validation.kind() == ValidationKind.DIVERGING) {
            return buildDivergingResponse(
                leader.epoch(),
                leader.nodeId(),
                validation.offsetAndEpoch()
            );
        }

        // 4. 读取日志
        List<LogEntry> entries = log.read(fetchOffset, request.getMaxBytes());

        // 5. 更新 Follower 的复制状态
        if (!entries.isEmpty()) {
            long lastOffset = entries.get(entries.size() - 1).offset();
            leader.updateReplicaState(followerId, lastOffset);
        }

        // 6. 更新高水位
        if (leader.maybeUpdateHighWatermark()) {
            log.updateHighWatermark(leader.highWatermark());
        }

        // 7. 构造响应
        return buildSuccessResponse(
            leader.epoch(),
            leader.nodeId(),
            leader.highWatermark(),
            entries
        );
    }
}
```

**3. FetchManager (Follower 端)**
```java
package io.github.mini.raft.core.replication;

/**
 * Fetch 管理器（Follower 端）
 *
 * 参考: KafkaRaftClient.pollFollower()
 */
public class FetchManager {
    private final int nodeId;
    private final QuorumState quorumState;
    private final ReplicatedLog log;
    private final NetworkChannel network;

    private long lastFetchTime = 0;
    private final int fetchIntervalMs = 50;

    /**
     * 发送 Fetch 请求
     */
    public void maybeSendFetch(long currentTime) {
        State current = quorumState.currentState();
        if (current.type() != StateType.FOLLOWER) {
            return;
        }

        if (currentTime - lastFetchTime < fetchIntervalMs) {
            return;
        }

        FollowerState follower = (FollowerState) current;

        FetchRequest request = FetchRequest.newBuilder()
            .setReplicaId(nodeId)
            .setReplicaEpoch(follower.epoch())
            .setFetchOffset(log.endOffset())
            .setLastFetchedEpoch(log.lastEpoch())
            .setMaxBytes(1024 * 1024)  // 1MB
            .build();

        network.sendFetchRequest(follower.leaderId(), request)
            .thenAccept(this::handleFetchResponse);

        lastFetchTime = currentTime;
    }

    /**
     * 处理 Fetch 响应
     */
    public void handleFetchResponse(FetchResponse response) {
        State current = quorumState.currentState();
        if (current.type() != StateType.FOLLOWER) {
            return;
        }

        FollowerState follower = (FollowerState) current;

        // 检查 epoch
        if (response.getEpoch() > follower.epoch()) {
            quorumState.transitionToUnattached(response.getEpoch());
            return;
        }

        // 处理快照
        if (response.hasSnapshot()) {
            handleSnapshotResponse(response.getSnapshot());
            return;
        }

        // 处理分歧
        if (response.hasDiverging()) {
            long divergingOffset = response.getDiverging().getOffset();
            log.truncateTo(divergingOffset);
            logger.info("Truncated log to diverging offset: {}", divergingOffset);
            return;
        }

        // 追加日志
        if (response.getEntriesCount() > 0) {
            for (LogEntryProto entryProto : response.getEntriesList()) {
                LogEntry entry = fromProto(entryProto);
                log.appendAsFollower(entry, follower.epoch());
            }
        }

        // 更新高水位
        if (response.getHighWatermark() > log.highWatermark().offset()) {
            log.updateHighWatermark(response.getHighWatermark());
        }

        // 重置 fetch 超时
        quorumState.transitionToFollower(follower.epoch(), follower.leaderId());
    }
}
```

#### **任务清单**

| 任务 | 文件 | 估时 | 优先级 |
|------|------|------|--------|
| 实现 BatchBuilder | BatchBuilder.java | 4h | P0 |
| 实现 BatchAccumulator | BatchAccumulator.java | 8h | P0 |
| 实现 ReplicaState | ReplicaState.java | 3h | P0 |
| 实现 ReplicationManager | ReplicationManager.java | 10h | P0 |
| 实现 FetchManager | FetchManager.java | 8h | P0 |
| 实现日志验证逻辑 | LogValidator.java | 4h | P0 |
| 单元测试 - BatchAccumulator | BatchAccumulatorTest.java | 4h | P0 |
| 单元测试 - 复制逻辑 | ReplicationManagerTest.java | 6h | P0 |
| 集成测试 - 日志复制 | ReplicationTest.java | 8h | P0 |
| 集成测试 - Leader 故障转移 | FailoverTest.java | 6h | P0 |

#### **交付物**
- [x] 完整的日志复制实现
- [x] 批处理机制
- [x] 25+ 测试

#### **验收标准**
```java
@Test
public void testLogReplication() {
    List<RaftNode> cluster = startCluster(1, 2, 3);
    RaftNode leader = electLeader(cluster, 1);

    // Leader 写入数据
    byte[] data = "test-data".getBytes();
    Long offset = leader.client().append(data).get(5, SECONDS);

    // 等待复制到所有节点
    await().atMost(5, SECONDS).until(() ->
        allNodesHaveOffset(cluster, offset)
    );

    // 验证所有节点数据一致
    for (RaftNode node : cluster) {
        LogEntry entry = node.log().read(offset);
        assertArrayEquals(data, entry.data());
    }
}

@Test
public void testLeaderFailover() {
    List<RaftNode> cluster = startCluster(1, 2, 3);
    RaftNode oldLeader = electLeader(cluster, 1);

    // 写入一些数据
    for (int i = 0; i < 10; i++) {
        oldLeader.client().append(("data-" + i).getBytes()).get();
    }

    // 停止 Leader
    int oldLeaderId = oldLeader.nodeId();
    oldLeader.shutdown();
    cluster.remove(oldLeader);

    // 等待新 Leader
    await().atMost(10, SECONDS).until(() ->
        hasLeader(cluster) && getLeader(cluster).nodeId() != oldLeaderId
    );

    // 新 Leader 可以继续写入
    RaftNode newLeader = getLeader(cluster);
    Long offset = newLeader.client().append("new-data".getBytes()).get(5, SECONDS);
    assertNotNull(offset);
}
```

---

### Sprint 6-10: 后续 Sprint 概要

由于篇幅限制，这里简要列出后续 Sprint 的目标：

#### **Sprint 6: 快照实现** (Week 13-14)
- 快照创建和冻结
- 快照传输
- 日志压缩
- 快照恢复

#### **Sprint 7: Check Quorum + 性能优化** (Week 15-16)
- Check Quorum 实现
- Pipeline 复制
- 零拷贝优化
- 性能基准测试

#### **Sprint 8: 动态成员变更** (Week 17-18)
- VoterSet 管理
- AddVoter/RemoveVoter
- 配置变更日志
- 单步变更验证

#### **Sprint 9: Observer + 监控** (Week 19-20)
- Observer 状态机
- Observer 复制
- 指标收集
- 监控面板

#### **Sprint 10: 生产就绪** (Week 21-22)
- 混沌测试
- 性能调优
- 文档完善
- 部署指南

---

## 核心类设计参考

### 从 Kafka KRaft 映射到我们的实现

| Kafka KRaft 类 | 我们的实现类 | 说明 |
|---------------|------------|------|
| `KafkaRaftClient` | `RaftEngine` | 核心引擎 |
| `QuorumState` | `QuorumState` | 状态机控制器 |
| `EpochState` | `State` | 状态接口 |
| `LeaderState` | `LeaderState` | Leader 状态 |
| `FollowerState` | `FollowerState` | Follower 状态 |
| `CandidateState` | `CandidateState` | Candidate 状态 |
| `UnattachedState` | `UnattachedState` | Unattached 状态 |
| `ReplicatedLog` | `ReplicatedLog` | 日志接口 |
| `KafkaMetadataLog` | `ReplicatedLogImpl` | 日志实现 |
| `BatchAccumulator` | `BatchAccumulator` | 批次累积器 |
| `NetworkChannel` | `NetworkChannel` | 网络通道 |
| `KafkaNetworkChannel` | `GrpcNetworkChannel` | 网络实现 |
| `QuorumStateStore` | `QuorumStateStore` | 状态持久化 |
| `FileQuorumStateStore` | `FileQuorumStateStore` | 文件持久化 |
| `VoterSet` | `VoterSet` | 投票者集合 |

---

## 开发规范

### 代码风格

```xml
<!-- checkstyle.xml -->
<module name="Checker">
    <module name="LineLength">
        <property name="max" value="120"/>
    </module>
    <module name="TreeWalker">
        <module name="Indentation">
            <property name="basicOffset" value="4"/>
        </module>
    </module>
</module>
```

### 命名规范

```java
// 类名：大驼峰
public class ReplicatedLog { }

// 方法名：小驼峰
public void appendEntry() { }

// 常量：全大写下划线
public static final int MAX_BATCH_SIZE = 1024;

// 包名：全小写
package io.github.mini.raft.core;
```

### 日志规范

```java
// 使用 SLF4J
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftEngine {
    private static final Logger logger = LoggerFactory.getLogger(RaftEngine.class);

    public void transitionToLeader() {
        logger.info("Node {} transitioning to Leader in epoch {}", nodeId, epoch);
    }

    public void handleError(Exception e) {
        logger.error("Failed to process request", e);
    }
}
```

### 异常处理

```java
// 定义自定义异常
public class RaftException extends RuntimeException {
    public RaftException(String message) {
        super(message);
    }
}

public class NotLeaderException extends RaftException {
    private final OptionalInt leaderId;

    public NotLeaderException(String message, OptionalInt leaderId) {
        super(message);
        this.leaderId = leaderId;
    }
}
```

---

## 测试策略

### 测试金字塔

```
        /\
       /  \       E2E/Chaos Tests (5%)
      /----\
     /      \     Integration Tests (25%)
    /--------\
   /          \   Unit Tests (70%)
  /____________\
```

### 单元测试

```java
@Test
public void testStateTransition() {
    // Arrange
    QuorumState state = new QuorumState(...);

    // Act
    state.transitionToCandidate(1, voters);

    // Assert
    assertEquals(StateType.CANDIDATE, state.currentState().type());
}
```

### 集成测试

```java
@TestInstance(Lifecycle.PER_CLASS)
public class ThreeNodeClusterTest {
    private List<RaftNode> cluster;

    @BeforeAll
    public void setUp() {
        cluster = startCluster(1, 2, 3);
    }

    @AfterAll
    public void tearDown() {
        cluster.forEach(RaftNode::shutdown);
    }

    @Test
    public void testElection() {
        await().until(() -> hasLeader(cluster));
    }
}
```

### 混沌测试

```java
@Test
public void testRandomPartitions() {
    FiveNodeCluster cluster = new FiveNodeCluster();
    ChaosMonkey monkey = new ChaosMonkey(cluster);

    // 运行 5 分钟
    monkey.runFor(Duration.ofMinutes(5))
        .withRandomPartitions()
        .withRandomNodeFailures()
        .withRandomMessageDelays();

    // 验证一致性
    assertTrue(cluster.isConsistent());
}
```

---

## 交付标准

### 每个 Sprint 的定义完成 (DoD)

- [ ] 所有计划的功能已实现
- [ ] 单元测试覆盖率 > 85%
- [ ] 所有测试通过
- [ ] 代码审查完成
- [ ] 文档已更新
- [ ] 无 P0/P1 Bug

### 项目完成标准

- [ ] 所有功能实现完成
- [ ] 通过 100+ 单元测试
- [ ] 通过 30+ 集成测试
- [ ] 通过混沌测试（Jepsen）
- [ ] 性能达标
- [ ] 代码覆盖率 > 85%
- [ ] 文档完整
- [ ] 部署指南完成

---

## 下一步

现在你已经有了完整的项目执行计划。我准备好开始实施了！

**我建议的启动方式**：

1. **Sprint 0**: 我先帮你搭建项目骨架
2. **Sprint 1**: 然后实现日志存储
3. **逐步推进**: 每完成一个 Sprint，Review 并调整计划

你准备好开始了吗？我们从 Sprint 0 开始！
