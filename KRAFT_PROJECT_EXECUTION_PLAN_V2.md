# KRaft 重写项目执行计划 V2

> **项目目标**: 基于 Apache Kafka KRaft 的核心思想，从零实现一个生产级的 Raft 共识协议
>
> **类名规范**: 所有类名与 Kafka KRaft 保持完全一致，方便参考源码
>
> **项目周期**: 20 周（5 个月）
>
> **实施方式**: 10 个 Sprint，每个 Sprint 2 周

---

## 目录

1. [项目概述](#项目概述)
2. [核心架构设计](#核心架构设计)
3. [类名映射规范](#类名映射规范)
4. [项目结构](#项目结构)
5. [Sprint 计划](#sprint-计划)
6. [开发规范](#开发规范)
7. [测试策略](#测试策略)

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
│                 KafkaRaftClient (Core)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ QuorumState  │  │   Election   │  │  Replication │  │
│  │   Manager    │  │   Logic      │  │   Logic      │  │
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
│  │ Replicated   │  │  Quorum      │  │   Snapshot   │  │
│  │     Log      │  │ StateStore   │  │    Store     │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓ ↑
┌─────────────────────────────────────────────────────────┐
│                  Network Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   Network    │  │   Protocol   │  │  Connection  │  │
│  │   Channel    │  │   Handlers   │  │   Manager    │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### 核心模块依赖关系

```
KafkaRaftClient (Main Entry)
    ├── QuorumState (State Machine Manager)
    │   ├── UnattachedState
    │   ├── ProspectiveState
    │   ├── CandidateState
    │   ├── LeaderState
    │   ├── FollowerState
    │   └── ResignedState
    ├── ReplicatedLog (Log Interface)
    │   └── KafkaMetadataLog (Implementation)
    ├── QuorumStateStore
    │   └── FileQuorumStateStore
    ├── NetworkChannel
    │   └── GrpcNetworkChannel (Our gRPC impl)
    ├── BatchAccumulator (Leader side)
    ├── VoterSet (Membership)
    └── Snapshots (Snapshot manager)
```

---

## 类名映射规范

> **重要原则**: 所有类名与 Apache Kafka KRaft 保持完全一致

### 核心类对照表

| Kafka KRaft 类 | 我们的类 | Kafka 源码位置 |
|---------------|---------|---------------|
| `RaftClient` | `RaftClient` | org.apache.kafka.raft.RaftClient |
| `KafkaRaftClient` | `KafkaRaftClient` | org.apache.kafka.raft.KafkaRaftClient |
| `QuorumState` | `QuorumState` | org.apache.kafka.raft.QuorumState |
| `EpochState` | `EpochState` | org.apache.kafka.raft.EpochState |
| `LeaderState` | `LeaderState` | org.apache.kafka.raft.LeaderState |
| `FollowerState` | `FollowerState` | org.apache.kafka.raft.FollowerState |
| `CandidateState` | `CandidateState` | org.apache.kafka.raft.CandidateState |
| `UnattachedState` | `UnattachedState` | org.apache.kafka.raft.UnattachedState |
| `ProspectiveState` | `ProspectiveState` | org.apache.kafka.raft.ProspectiveState |
| `ResignedState` | `ResignedState` | org.apache.kafka.raft.ResignedState |
| `ReplicatedLog` | `ReplicatedLog` | org.apache.kafka.raft.ReplicatedLog |
| `BatchAccumulator` | `BatchAccumulator` | org.apache.kafka.raft.internals.BatchAccumulator |
| `BatchBuilder` | `BatchBuilder` | org.apache.kafka.raft.internals.BatchBuilder |
| `NetworkChannel` | `NetworkChannel` | org.apache.kafka.raft.NetworkChannel |
| `QuorumStateStore` | `QuorumStateStore` | org.apache.kafka.raft.QuorumStateStore |
| `FileQuorumStateStore` | `FileQuorumStateStore` | org.apache.kafka.raft.FileQuorumStateStore |
| `VoterSet` | `VoterSet` | org.apache.kafka.raft.VoterSet |
| `Endpoints` | `Endpoints` | org.apache.kafka.raft.Endpoints |

详细映射请查看: **KRAFT_CLASS_NAMES_MAPPING.md**

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
│   └── src/main/java/io/github/miniraft/raft/common/
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
│   └── src/main/java/io/github/miniraft/raft/api/
│       ├── RaftClient.java          # 客户端接口（对应Kafka）
│       ├── RaftNode.java
│       ├── StateMachine.java
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
│   └── src/main/java/io/github/miniraft/raft/core/
│       ├── KafkaRaftClient.java    # ✅ 核心引擎（对应Kafka）
│       ├── QuorumState.java        # ✅ 状态管理器（对应Kafka）
│       ├── VoterSet.java           # ✅ 投票者集合（对应Kafka）
│       ├── Endpoints.java          # ✅ 端点集合（对应Kafka）
│       │
│       ├── state/                  # 状态机
│       │   ├── EpochState.java     # ✅ 状态接口（对应Kafka）
│       │   ├── UnattachedState.java # ✅（对应Kafka）
│       │   ├── ProspectiveState.java # ✅（对应Kafka）
│       │   ├── CandidateState.java # ✅（对应Kafka）
│       │   ├── LeaderState.java    # ✅（对应Kafka）
│       │   ├── FollowerState.java  # ✅（对应Kafka）
│       │   └── ResignedState.java  # ✅（对应Kafka）
│       │
│       └── internals/              # 内部实现
│           ├── BatchAccumulator.java # ✅（对应Kafka）
│           ├── BatchBuilder.java   # ✅（对应Kafka）
│           ├── BatchMemoryPool.java # ✅（对应Kafka）
│           ├── KafkaRaftMetrics.java # ✅（对应Kafka）
│           ├── RecordsIterator.java # ✅（对应Kafka）
│           ├── VoterSetHistory.java # ✅（对应Kafka）
│           ├── FuturePurgatory.java # ✅（对应Kafka）
│           └── KRaftControlRecordStateMachine.java # ✅（对应Kafka）
│
├── raft-storage/                    # 存储模块
│   ├── pom.xml
│   └── src/main/java/io/github/miniraft/raft/storage/
│       ├── log/
│       │   ├── ReplicatedLog.java  # ✅ 接口（对应Kafka）
│       │   ├── LogSegment.java
│       │   ├── LogManager.java
│       │   ├── LogIndex.java
│       │   └── KafkaMetadataLog.java # ✅（对应Kafka，我们用Java）
│       ├── state/
│       │   ├── QuorumStateStore.java # ✅（对应Kafka）
│       │   └── FileQuorumStateStore.java # ✅（对应Kafka）
│       └── snapshot/
│           └── (Kafka的snapshot包中的类)
│
├── raft-network/                    # 网络模块
│   ├── pom.xml
│   └── src/main/java/io/github/miniraft/raft/network/
│       ├── NetworkChannel.java     # ✅ 接口（对应Kafka）
│       ├── GrpcNetworkChannel.java # 我们的gRPC实现
│       ├── RaftServiceImpl.java
│       └── ConnectionManager.java
│
├── raft-snapshot/                   # 快照模块
│   ├── pom.xml
│   └── src/main/java/io/github/miniraft/raft/snapshot/
│       ├── SnapshotWriter.java     # ✅（对应Kafka）
│       ├── SnapshotReader.java     # ✅（对应Kafka）
│       ├── RawSnapshotWriter.java  # ✅（对应Kafka）
│       ├── RawSnapshotReader.java  # ✅（对应Kafka）
│       ├── FileRawSnapshotWriter.java # ✅（对应Kafka）
│       ├── FileRawSnapshotReader.java # ✅（对应Kafka）
│       └── Snapshots.java          # ✅（对应Kafka）
│
└── raft-tests/                      # 测试模块
    ├── pom.xml
    └── src/test/java/io/github/miniraft/raft/tests/
        ├── unit/                    # 单元测试
        ├── integration/             # 集成测试
        └── chaos/                   # 混沌测试
```

---

## Sprint 计划

### Sprint 0: 项目初始化 (Week 1-2)

#### **目标**: 搭建项目骨架和开发环境

#### **任务清单**

| 任务 | 涉及类 | 估时 | 优先级 |
|------|-------|------|--------|
| 创建 Maven 多模块项目 | - | 2h | P0 |
| 配置 Protocol Buffers 插件 | raft-protocol | 2h | P0 |
| 定义 Raft 协议消息 | raft.proto | 4h | P0 |
| 实现通用数据结构 | LogEntry, OffsetAndEpoch, ReplicaKey | 4h | P0 |
| 配置 CI/CD | - | 3h | P0 |
| 编写开发文档 | README.md | 3h | P1 |

#### **交付物**
- [x] 完整的项目结构
- [x] Protocol Buffers 协议定义
- [x] 基础数据模型
- [x] CI/CD 配置
- [x] 开发环境文档

#### **验收标准**
```bash
mvn clean compile    # 编译成功
mvn test            # 测试通过
mvn checkstyle:check # 代码风格检查通过
```

---

### Sprint 1: 日志存储实现 (Week 3-4)

#### **目标**: 实现可靠的日志存储层

#### **参考 Kafka KRaft 类**
```java
// 主要参考:
org.apache.kafka.raft.ReplicatedLog (接口)
org.apache.kafka.storage.internals.log.LogSegment
org.apache.kafka.storage.internals.log.OffsetIndex
```

#### **核心类实现**

**1. ReplicatedLog 接口** ✅ 对应 Kafka

```java
package io.github.miniraft.raft.storage.log;

/**
 * 复制日志接口
 *
 * 参考: org.apache.kafka.raft.ReplicatedLog
 */
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
package io.github.miniraft.raft.storage.log;

/**
 * 日志段，类似 Kafka 的 LogSegment
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
        // 实现...
    }

    // 读取记录
    public Records read(long startOffset, int maxBytes) {
        // 实现...
    }

    // 恢复日志
    public void recover() {
        // 实现...
    }
}
```

#### **任务清单**

| 任务 | 文件 | Kafka参考 | 估时 |
|------|------|----------|------|
| 实现 LogEntry | LogEntry.java | - | 2h |
| 实现 OffsetIndex | OffsetIndex.java | OffsetIndex | 6h |
| 实现 LogSegment | LogSegment.java | LogSegment | 12h |
| 实现 LogManager | LogManager.java | - | 8h |
| 实现 ReplicatedLog | ReplicatedLogImpl.java | ReplicatedLog | 8h |
| 单元测试 | LogSegmentTest.java | - | 10h |

---

### Sprint 2: 持久化状态 + 状态机基础 (Week 5-6)

#### **目标**: 实现选举状态持久化和基础状态机框架

#### **参考 Kafka KRaft 类**
```java
// 主要参考:
org.apache.kafka.raft.QuorumState
org.apache.kafka.raft.QuorumStateStore
org.apache.kafka.raft.FileQuorumStateStore
org.apache.kafka.raft.EpochState (状态接口)
org.apache.kafka.raft.*State (各种状态实现)
```

#### **核心类实现**

**1. QuorumStateStore 接口** ✅ 对应 Kafka

```java
package io.github.miniraft.raft.storage.state;

/**
 * 持久化选举状态
 *
 * 参考: org.apache.kafka.raft.QuorumStateStore
 */
public interface QuorumStateStore {
    void writeElectionState(ElectionState state);
    Optional<ElectionState> readElectionState();
    void clear();
}
```

**2. EpochState 接口** ✅ 对应 Kafka

```java
package io.github.miniraft.raft.core.state;

/**
 * 状态接口
 *
 * 参考: org.apache.kafka.raft.EpochState
 */
public interface EpochState {
    int epoch();
    ElectionState election();

    // 其他方法...
}
```

**3. QuorumState 状态管理器** ✅ 对应 Kafka

```java
package io.github.miniraft.raft.core;

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

    private volatile EpochState state;

    // 状态转换方法
    public void transitionToUnattached(int epoch) {
        durableTransitionTo(new UnattachedState(...));
    }

    public void transitionToCandidate(int epoch) {
        durableTransitionTo(new CandidateState(...));
    }

    // ... 其他转换方法
}
```

**4. 各个状态实现** ✅ 全部对应 Kafka

```java
// UnattachedState.java - 对应 org.apache.kafka.raft.UnattachedState
public class UnattachedState implements EpochState {
    private final int epoch;
    private final long electionDeadline;
    // ...
}

// ProspectiveState.java - 对应 org.apache.kafka.raft.ProspectiveState
public class ProspectiveState implements EpochState {
    private final Set<Integer> grantingVoters = new HashSet<>();
    // ...
}

// CandidateState.java - 对应 org.apache.kafka.raft.CandidateState
public class CandidateState implements EpochState {
    // ...
}

// LeaderState.java - 对应 org.apache.kafka.raft.LeaderState
public class LeaderState implements EpochState {
    private final Map<Integer, ReplicaState> voterStates;
    private long highWatermark;
    // ...
}

// FollowerState.java - 对应 org.apache.kafka.raft.FollowerState
public class FollowerState implements EpochState {
    private final int leaderId;
    private final long fetchDeadline;
    // ...
}

// ResignedState.java - 对应 org.apache.kafka.raft.ResignedState
public class ResignedState implements EpochState {
    // ...
}
```

#### **任务清单**

| 任务 | 文件 | Kafka参考 | 估时 |
|------|------|----------|------|
| 实现 ElectionState | ElectionState.java | ElectionState | 2h |
| 实现 QuorumStateStore | QuorumStateStore.java | QuorumStateStore | 2h |
| 实现 FileQuorumStateStore | FileQuorumStateStore.java | FileQuorumStateStore | 6h |
| 实现 EpochState 接口 | EpochState.java | EpochState | 1h |
| 实现 UnattachedState | UnattachedState.java | UnattachedState | 3h |
| 实现 ProspectiveState | ProspectiveState.java | ProspectiveState | 4h |
| 实现 CandidateState | CandidateState.java | CandidateState | 4h |
| 实现 LeaderState | LeaderState.java | LeaderState | 8h |
| 实现 FollowerState | FollowerState.java | FollowerState | 3h |
| 实现 ResignedState | ResignedState.java | ResignedState | 2h |
| 实现 QuorumState | QuorumState.java | QuorumState | 8h |
| 单元测试 | 各种Test.java | - | 10h |

---

### Sprint 3: 网络通信实现 (Week 7-8)

#### **目标**: 实现基于 gRPC 的网络通信层

#### **参考 Kafka KRaft 类**
```java
// 主要参考:
org.apache.kafka.raft.NetworkChannel (接口)
org.apache.kafka.raft.KafkaNetworkChannel (Kafka的实现)
// 我们用 GrpcNetworkChannel 代替 KafkaNetworkChannel
```

#### **核心类实现**

**1. NetworkChannel 接口** ✅ 对应 Kafka

```java
package io.github.miniraft.raft.network;

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

**2. GrpcNetworkChannel 实现** (我们的实现，类似 KafkaNetworkChannel)

```java
package io.github.miniraft.raft.network;

/**
 * 基于 gRPC 的网络通信实现
 *
 * 类似: org.apache.kafka.raft.KafkaNetworkChannel
 * 但我们用 gRPC 而不是 Kafka NetworkClient
 */
public class GrpcNetworkChannel implements NetworkChannel {
    private final Map<Integer, ManagedChannel> channels;
    private final Map<Integer, RaftServiceStub> stubs;

    @Override
    public CompletableFuture<VoteResponse> sendVoteRequest(...) {
        // 使用 gRPC 发送
    }
}
```

#### **任务清单**

| 任务 | 文件 | Kafka参考 | 估时 |
|------|------|----------|------|
| 编写 Protocol Buffers | raft.proto | - | 4h |
| 实现 NetworkChannel | NetworkChannel.java | NetworkChannel | 2h |
| 实现 GrpcNetworkChannel | GrpcNetworkChannel.java | KafkaNetworkChannel | 8h |
| 实现 RaftServiceImpl | RaftServiceImpl.java | - | 6h |
| 单元测试 | - | - | 10h |

---

### Sprint 4: 选举实现 (Week 9-10)

#### **目标**: 实现完整的 Leader 选举流程

#### **参考 Kafka KRaft 类**
```java
// 主要参考 KafkaRaftClient 中的选举逻辑:
org.apache.kafka.raft.KafkaRaftClient
  - handleVoteRequest()
  - handleVoteResponse()
  - pollUnattached()
  - pollProspective()
  - pollCandidate()
```

#### **核心实现**

选举逻辑主要在 **KafkaRaftClient** 中实现，我们会创建辅助方法来组织代码。

```java
package io.github.miniraft.raft.core;

/**
 * KafkaRaft 客户端实现
 *
 * 参考: org.apache.kafka.raft.KafkaRaftClient
 */
public class KafkaRaftClient<T> implements RaftClient<T> {
    private final QuorumState quorumState;
    private final ReplicatedLog log;
    private final NetworkChannel channel;

    // 主循环
    public void poll() {
        // 根据当前状态分发
        EpochState state = quorumState.currentState();

        if (state instanceof UnattachedState) {
            pollUnattached((UnattachedState) state);
        } else if (state instanceof ProspectiveState) {
            pollProspective((ProspectiveState) state);
        } else if (state instanceof CandidateState) {
            pollCandidate((CandidateState) state);
        } else if (state instanceof LeaderState) {
            pollLeader((LeaderState) state);
        } else if (state instanceof FollowerState) {
            pollFollower((FollowerState) state);
        }
    }

    // 处理 Unattached 状态
    private void pollUnattached(UnattachedState state) {
        // 检查选举超时
        if (state.hasElectionTimeoutExpired()) {
            // 发起 PreVote
            startPreVote();
        }
    }

    // 发起 PreVote
    private void startPreVote() {
        // 转换到 Prospective 状态
        // 发送 PreVote 请求
        // 参考 Kafka 实现
    }

    // 处理投票请求
    public VoteResponse handleVoteRequest(VoteRequest request) {
        // 参考 Kafka KafkaRaftClient.handleVoteRequest()
    }

    // 处理投票响应
    public void handleVoteResponse(VoteResponse response) {
        // 参考 Kafka KafkaRaftClient.handleVoteResponse()
    }
}
```

#### **任务清单**

| 任务 | 涉及方法 | Kafka参考 | 估时 |
|------|---------|----------|------|
| 实现 pollUnattached | pollUnattached() | KafkaRaftClient | 4h |
| 实现 PreVote 逻辑 | startPreVote(), pollProspective() | KafkaRaftClient | 6h |
| 实现正式选举 | startElection(), pollCandidate() | KafkaRaftClient | 6h |
| 实现投票决策 | handleVoteRequest() | KafkaRaftClient | 6h |
| 实现成为 Leader | becomeLeader() | KafkaRaftClient | 4h |
| 单元测试 | - | - | 8h |
| 集成测试 | ThreeNodeElectionTest | - | 6h |

---

### Sprint 5: 日志复制实现 (Week 11-12)

#### **目标**: 实现 Leader 到 Follower 的日志复制

#### **参考 Kafka KRaft 类**
```java
// 主要参考:
org.apache.kafka.raft.internals.BatchAccumulator
org.apache.kafka.raft.KafkaRaftClient
  - handleFetchRequest()
  - handleFetchResponse()
  - pollLeader()
  - pollFollower()
```

#### **核心类实现**

**1. BatchAccumulator** ✅ 对应 Kafka

```java
package io.github.miniraft.raft.core.internals;

/**
 * 批次累积器
 *
 * 参考: org.apache.kafka.raft.internals.BatchAccumulator
 */
public class BatchAccumulator<T> {
    private final int epoch;
    private final int maxBatchSize;
    private final int lingerMs;

    private final Queue<CompletedBatch> completedBatches;
    private BatchBuilder currentBatch;

    public CompletableFuture<Long> append(int epoch, List<T> records) {
        // 参考 Kafka 实现
    }

    public List<CompletedBatch> drain() {
        // 参考 Kafka 实现
    }
}
```

**2. KafkaRaftClient 中的复制逻辑**

```java
/**
 * 处理 Fetch 请求 (Leader 端)
 *
 * 参考: org.apache.kafka.raft.KafkaRaftClient.handleFetchRequest()
 */
public FetchResponse handleFetchRequest(FetchRequest request) {
    // 1. 验证日志
    // 2. 读取日志
    // 3. 更新 replica 状态
    // 4. 更新高水位
}

/**
 * 处理 Fetch 响应 (Follower 端)
 *
 * 参考: org.apache.kafka.raft.KafkaRaftClient.handleFetchResponse()
 */
public void handleFetchResponse(FetchResponse response) {
    // 1. 处理快照
    // 2. 处理分歧
    // 3. 追加日志
    // 4. 更新高水位
}
```

#### **任务清单**

| 任务 | 涉及类/方法 | Kafka参考 | 估时 |
|------|-----------|----------|------|
| 实现 BatchBuilder | BatchBuilder.java | BatchBuilder | 4h |
| 实现 BatchAccumulator | BatchAccumulator.java | BatchAccumulator | 8h |
| 实现 ReplicaState | ReplicaState.java | LeaderState内部 | 3h |
| 实现 handleFetchRequest | handleFetchRequest() | KafkaRaftClient | 10h |
| 实现 handleFetchResponse | handleFetchResponse() | KafkaRaftClient | 8h |
| 实现 pollLeader | pollLeader() | KafkaRaftClient | 6h |
| 实现 pollFollower | pollFollower() | KafkaRaftClient | 6h |
| 单元测试 | - | - | 8h |
| 集成测试 | ReplicationTest | - | 8h |

---

### Sprint 6-10: 后续 Sprint 概要

#### **Sprint 6: 快照实现** (Week 13-14)
- 实现 SnapshotWriter/SnapshotReader ✅ 对应 Kafka
- 实现 RawSnapshotWriter/RawSnapshotReader ✅ 对应 Kafka
- 实现 FileRawSnapshotWriter/FileRawSnapshotReader ✅ 对应 Kafka
- 快照传输和恢复

#### **Sprint 7: Check Quorum + 性能优化** (Week 15-16)
- LeaderState 中的 Check Quorum 逻辑 ✅ 对应 Kafka
- Pipeline 复制
- 性能基准测试

#### **Sprint 8: 动态成员变更** (Week 17-18)
- VoterSet 管理 ✅ 对应 Kafka
- VoterSetHistory ✅ 对应 Kafka
- AddVoter/RemoveVoter/UpdateVoter

#### **Sprint 9: Observer + 监控** (Week 19-20)
- Observer 状态机
- KafkaRaftMetrics ✅ 对应 Kafka
- 监控面板

#### **Sprint 10: 生产就绪** (Week 21-22)
- 混沌测试
- 文档完善
- 部署指南

---

## 开发规范

### 代码风格

```java
// 包名
package io.github.miniraft.raft.core;

// 类名：与 Kafka 完全一致
public class KafkaRaftClient<T> implements RaftClient<T> { }

// 方法名：参考 Kafka
public void handleVoteRequest() { }

// 常量：全大写下划线
public static final int MAX_BATCH_SIZE = 1024;
```

### 注释规范

```java
/**
 * KafkaRaft 客户端实现
 *
 * <p>这个类是 Raft 协议的核心实现，负责：
 * <ul>
 *   <li>Leader 选举</li>
 *   <li>日志复制</li>
 *   <li>状态管理</li>
 * </ul>
 *
 * <p><b>参考 Kafka 源码</b>:
 * {@code org.apache.kafka.raft.KafkaRaftClient}
 *
 * @param <T> 记录类型
 * @see RaftClient
 * @see QuorumState
 */
public class KafkaRaftClient<T> implements RaftClient<T> {
    // ...
}
```

### 引用 Kafka 源码

```java
/**
 * 处理投票请求
 *
 * <p>投票决策逻辑：
 * <ol>
 *   <li>检查 epoch</li>
 *   <li>检查是否已投票</li>
 *   <li>检查日志是否足够新</li>
 *   <li>决定是否投票</li>
 * </ol>
 *
 * <p><b>Kafka 参考</b>:
 * {@code org.apache.kafka.raft.KafkaRaftClient.handleVoteRequest()}
 * 第 2156-2234 行
 *
 * @param request 投票请求
 * @return 投票响应
 */
public VoteResponse handleVoteRequest(VoteRequest request) {
    // 实现...
}
```

---

## 测试策略

### 单元测试

```java
/**
 * 测试类名也与 Kafka 保持一致
 */
public class QuorumStateTest {

    @Test
    public void testStateTransition() {
        // Arrange
        QuorumState state = new QuorumState(...);

        // Act
        state.transitionToCandidate(1, voters);

        // Assert
        assertEquals(StateType.CANDIDATE,
            state.currentState().election().voteState());
    }
}
```

---

## 总结

### 关键变更

1. **RaftEngine** → **KafkaRaftClient** ✅
2. **State** → **EpochState** ✅
3. 所有其他核心类名与 Kafka 完全一致 ✅

### 好处

1. 📖 **容易对照**: 可以直接在 Kafka 源码中搜索同名类
2. 🔍 **便于调试**: 堆栈跟踪更直观
3. 📚 **方便学习**: 文档和代码可以直接引用 Kafka
4. 🤝 **团队协作**: 其他开发者更容易理解

### 参考文档

- **类名映射详情**: 查看 `KRAFT_CLASS_NAMES_MAPPING.md`
- **Kafka 源码**: https://github.com/apache/kafka/tree/trunk/raft

---

**准备好开始 Sprint 0 了吗？** 🚀
