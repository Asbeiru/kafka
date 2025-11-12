# Kafka KRaft 重写实现计划 - 完整指南

## 项目概览

**目标**：用 Spring Boot + Maven 项目重写 Kafka KRaft 的选举模块，保持所有类和方法与 Kafka 完全一致，目的是深入理解 KRaft 的架构和实现细节。

**预计时间**：
- Phase 0 - 基础设施：2-3 天
- Phase 1 - 选举流程：3-5 天
- Phase 2 - 日志复制：4-6 天
- Phase 3 - 配置更改：2-3 天
- **总计**：2-3 周

**学习价值**：通过完整实现，你将深度理解：
- Raft 共识算法在真实系统中的应用
- 状态机和事件驱动编程
- 分布式系统的设计模式
- Kafka 的架构理念

---

## 第 1 部分：依赖关系分析

### 核心依赖关系图

```
┌─────────────────────────────────────────────────────────────┐
│ 层级 7：应用协调器（KafkaRaftClient）                        │
│ 依赖：所有下面的模块                                          │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│ 层级 6：高级特性                                             │
│ ├─ BatchAccumulator（日志批处理）                           │
│ ├─ SnapshotManager（快照管理）                              │
│ ├─ VoterSet（选民管理）                                     │
│ 依赖：层级 5 的 RPC、日志、状态管理                         │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│ 层级 5：具体状态实现（需要选举基础）                         │
│ ├─ LeaderState（管理副本、高水位）                          │
│ ├─ FollowerState（管理 Fetch、快照拉取）                    │
│ ├─ CandidateState（收集投票）                               │
│ ├─ ProspectiveState（PreVote）                              │
│ ├─ UnattachedState（未连接）                                │
│ ├─ ResignedState（已辞职）                                  │
│ 依赖：EpochState、RPC 消息、QuorumState                     │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│ 层级 4：选举核心（★ 第一个重点实现区域）                    │
│ ├─ QuorumState（状态机管理）                                │
│ ├─ EpochState（状态接口）                                   │
│ ├─ ElectionState（选举状态持久化）                          │
│ ├─ VoteRequest/VoteResponse（投票 RPC）                     │
│ ├─ BeginQuorumEpochRequest（开始 epoch）                    │
│ 依赖：RPC 基础、存储、计时器                                │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│ 层级 3：日志和副本（★ 第二个重点实现区域）                  │
│ ├─ ReplicatedLog（日志管理接口）                            │
│ ├─ LogSegment（日志段）                                     │
│ ├─ FetchRequest/FetchResponse（日志拉取 RPC）              │
│ ├─ LogOffsetMetadata（日志元数据）                          │
│ 依赖：RPC 基础、存储、状态管理                              │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│ 层级 2：RPC 和消息（基础）                                   │
│ ├─ RaftRequest/RaftResponse（RPC 包装）                     │
│ ├─ 各种 Request/Response 类                                 │
│ ├─ RequestManager（请求队列管理）                           │
│ 依赖：序列化、网络基础                                      │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│ 层级 1：基础设施（★ 优先实现）                              │
│ ├─ Timer（计时器）                                          │
│ ├─ NetworkChannel（网络通信）                               │
│ ├─ MemoryPool（内存管理）                                   │
│ ├─ KRaftMetrics（监控指标）                                 │
│ ├─ ReplicaKey（副本标识）                                   │
│ ├─ Endpoints（网络端点）                                    │
│ 依赖：无（或仅依赖标准库）                                  │
└─────────────────────────────────────────────────────────────┘
```

### 实现顺序的关键发现

**你的初步想法（选举 → 日志复制 → 配置更改）是对的，但需要细化**：

选举无法单独实现，因为它需要 RPC 基础和状态管理。因此真实的实现顺序应该是：

```
Layer 1: 基础设施
    ↓
Layer 2: RPC 和消息框架
    ↓
Layer 4: 选举核心（包括 QuorumState、ElectionState）
    ↓
Layer 3: 日志管理（ReplicatedLog）
    ↓
Layer 5: 具体状态实现（LeaderState、FollowerState 等）
    ↓
Layer 6: 高级特性（日志复制、快照等）
    ↓
Layer 7: 应用协调器（KafkaRaftClient）
```

---

## 第 2 部分：分阶段实现计划

### Phase 0：基础设施搭建（2-3 天）

这个阶段是基础，后续所有代码都依赖这些类。

#### 0.1 创建 Maven 项目结构

```bash
raft-implementation/
├── pom.xml
├── src/
│   ├── main/
│   │   └── java/
│   │       └── org/apache/kafka/
│   │           ├── raft/                          # 核心 Raft 包
│   │           │   ├── api/                       # 公共 API
│   │           │   ├── client/                    # 客户端实现
│   │           │   ├── internals/                 # 内部实现
│   │           │   ├── state/                     # 状态机
│   │           │   ├── rpc/                       # RPC 消息
│   │           │   ├── log/                       # 日志管理
│   │           │   └── network/                   # 网络通信
│   │           └── common/
│   │               ├── record/                    # 记录和序列化
│   │               └── config/                    # 配置
│   │
│   ├── test/
│   │   └── java/...                               # 单元测试
│   └── resources/
│       └── application.properties                 # Spring Boot 配置
│
└── README.md
```

#### 0.2 pom.xml 依赖配置

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.apache.kafka.raft</groupId>
    <artifactId>kafka-raft-implementation</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <name>Kafka Raft Implementation</name>
    <description>Educational reimplementation of Kafka KRaft</description>

    <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <spring-boot.version>2.7.0</spring-boot.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <!-- Spring Boot -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
        </dependency>

        <!-- Testing -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Utilities -->
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
            <version>31.1-jre</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>${spring-boot.version}</version>
            </plugin>
        </plugins>
    </build>
</project>
```

#### 0.3 需要实现的基础类（按优先级）

| # | 类名 | 文件 | 说明 | 预计代码行数 |
|---|------|------|------|-----------|
| 1 | `Timer` | `internals/Timer.java` | 计时器基类 | 50 |
| 2 | `ReplicaKey` | `common/ReplicaKey.java` | 副本身份（ID + Directory ID） | 100 |
| 3 | `Endpoints` | `common/Endpoints.java` | 网络端点管理（监听器） | 80 |
| 4 | `LogOffsetMetadata` | `log/LogOffsetMetadata.java` | 日志偏移量 + epoch 元数据 | 100 |
| 5 | `MemoryPool` | `internals/MemoryPool.java` | 内存池实现 | 150 |
| 6 | `NetworkChannel` | `network/NetworkChannel.java` | 网络通信接口 | 120 |
| 7 | `KRaftMetrics` | `internals/KRaftMetrics.java` | 监控指标 | 200 |

**优先级解释**：
- Timer、ReplicaKey、Endpoints、LogOffsetMetadata 是最基础的，几乎被所有其他类使用
- MemoryPool 在日志批处理时需要
- NetworkChannel 是 RPC 的基础
- KRaftMetrics 可以稍后添加

#### 0.4 第一个具体类：Timer（示例）

```java
// org/apache/kafka/raft/internals/Timer.java

public class Timer {
    private static final long UNINITIALIZED_DEADLINE = Long.MAX_VALUE;

    private long deadlineMs = UNINITIALIZED_DEADLINE;

    /**
     * 重置计时器，从现在开始计时 delayMs 毫秒
     */
    public void reset(long delayMs, long currentTimeMs) {
        if (delayMs < 0) {
            throw new IllegalArgumentException("Negative delay: " + delayMs);
        }
        this.deadlineMs = currentTimeMs + delayMs;
    }

    /**
     * 计时器是否已过期
     */
    public boolean isExpired(long currentTimeMs) {
        return currentTimeMs >= deadlineMs;
    }

    /**
     * 距离过期还需多少毫秒（如果为负表示已过期）
     */
    public long remainingMs(long currentTimeMs) {
        return deadlineMs - currentTimeMs;
    }

    /**
     * 清除计时器
     */
    public void cancel() {
        this.deadlineMs = UNINITIALIZED_DEADLINE;
    }

    /**
     * 计时器是否已初始化
     */
    public boolean isInitialized() {
        return deadlineMs != UNINITIALIZED_DEADLINE;
    }
}
```

### Phase 1：选举流程实现（3-5 天）

**核心目标**：完整实现 Leader 选举、投票决策、状态转换

#### 1.1 先实现：RPC 消息框架和状态管理

| # | 类名 | 所在包 | 依赖 | 代码行数 |
|---|------|-------|------|---------|
| 1 | `RaftRequest` | `rpc` | NetworkChannel | 100 |
| 2 | `RaftResponse` | `rpc` | NetworkChannel | 100 |
| 3 | `VoteRequest` | `rpc` | RaftRequest | 150 |
| 4 | `VoteResponse` | `rpc` | RaftResponse | 80 |
| 5 | `PreVoteRequest` | `rpc` | RaftRequest | 150 |
| 6 | `PreVoteResponse` | `rpc` | RaftResponse | 80 |
| 7 | `BeginQuorumEpochRequest` | `rpc` | RaftRequest | 100 |
| 8 | `BeginQuorumEpochResponse` | `rpc` | RaftResponse | 60 |
| 9 | `EpochState` | `state` | 无 | 250 |
| 10 | `ElectionState` | `state` | LogOffsetMetadata | 200 |
| 11 | `QuorumState` | `state` | EpochState, ElectionState | **400-500** |

**重点**：`QuorumState` 是核心类，管理所有状态转换和投票决策。应该认真实现。

#### 1.2 再实现：具体状态实现

| # | 类名 | 代码行数 | 难度 | 关键职责 |
|---|------|---------|------|---------|
| 1 | `UnattachedState` | 150 | ⭐ | 基础状态，等待选举超时 |
| 2 | `ProspectiveState` | 180 | ⭐⭐ | PreVote 逻辑，投票收集 |
| 3 | `CandidateState` | 200 | ⭐⭐ | 正式选举，投票收集 |
| 4 | `FollowerState` | 200 | ⭐⭐ | 跟踪 Leader，管理连接 |
| 5 | `LeaderState` | **500-600** | ⭐⭐⭐ | 最复杂，副本跟踪、高水位 |
| 6 | `ResignedState` | 100 | ⭐ | 优雅辞职状态 |

**实现顺序建议**：
1. 先实现 `UnattachedState`（最简单，用于理解状态接口）
2. 再实现 `ProspectiveState` 和 `CandidateState`（PreVote 和正式选举）
3. 最后实现 `FollowerState` 和 `LeaderState`（包含更复杂的逻辑）

#### 1.3 Phase 1 的关键实现点

**QuorumState.canGrantVote() 的实现**：
```java
public boolean canGrantVote(
    ReplicaKey candidate,
    int candidateEpoch,
    LogOffsetMetadata lastLogOffsetMetadata
) {
    // 检查 epoch
    if (candidateEpoch < election.epoch()) {
        return false;  // 过期的请求
    }

    // 检查是否已投票
    if (election.votedFor().isPresent() &&
        !election.votedFor().get().equals(candidate)) {
        return false;  // 已投票给其他候选人
    }

    // 检查日志是否足够新
    return isLogUpToDate(candidate, lastLogOffsetMetadata);
}

private boolean isLogUpToDate(
    ReplicaKey candidate,
    LogOffsetMetadata candidateLastLog
) {
    LogOffsetMetadata myLastLog = lastLogOffsetMetadata();

    // 候选人的最后一个日志 epoch 更大
    if (candidateLastLog.epoch > myLastLog.epoch) {
        return true;
    }

    // 相同 epoch，检查 offset
    if (candidateLastLog.epoch == myLastLog.epoch) {
        return candidateLastLog.offset >= myLastLog.offset;
    }

    return false;
}
```

#### 1.4 Phase 1 完成条件

- ✅ 能够进行完整的 Leader 选举
- ✅ 多个节点能够协调选出唯一的 Leader
- ✅ 能够通过 PreVote 检测日志的新旧程度
- ✅ 故障恢复时能够重新选举
- ✅ 单元测试覆盖主要场景

---

### Phase 2：日志复制实现（4-6 天）

**核心目标**：实现日志存储、Fetch 拉取、日志一致性

#### 2.1 先实现：日志存储层

| # | 类名 | 代码行数 | 说明 |
|---|------|---------|------|
| 1 | `RecordBatch` | 150 | 日志批次格式 |
| 2 | `LogSegment` | 300 | 单个日志文件（索引 + 数据） |
| 3 | `LogSegmentCollection` | 250 | 日志段集合管理 |
| 4 | `ReplicatedLog` | **400-500** | 日志管理核心接口 |
| 5 | `ValidOffsetAndEpoch` | 150 | 日志验证结果 |
| 6 | `ReplicaState` | 200 | 单个副本的复制状态 |

**关键复杂类**：`ReplicatedLog` 需要实现：
- 日志追加（Leader 和 Follower）
- 日志读取和验证
- 日志截断（处理分叉）
- 快照集成
- Epoch 缓存

#### 2.2 再实现：Fetch 请求处理

| # | 类名 | 代码行数 | 说明 |
|---|------|---------|------|
| 1 | `FetchRequest` | 200 | Follower 发起的拉取请求 |
| 2 | `FetchResponse` | 200 | Leader 的拉取响应 |
| 3 | `FetchSnapshot*` | 300 | 快照拉取 RPC |
| 4 | `RequestManager` | 250 | 待处理请求队列管理 |

#### 2.3 最后更新：LeaderState 和 FollowerState

- `LeaderState`：
  - 初始化 `nextIndex[]` 和 `matchIndex[]`
  - 实现高水位计算算法
  - 实现 Check Quorum 机制

- `FollowerState`：
  - 发送 Fetch 请求
  - 处理响应
  - 处理日志分叉

#### 2.4 Phase 2 完成条件

- ✅ 日志能够正确存储和读取
- ✅ Leader 能够正确计算高水位
- ✅ Follower 能够拉取日志并应用
- ✅ 能够正确处理日志分叉和截断
- ✅ 快照集成工作正常

---

### Phase 3：配置更改和高级特性（2-3 天）

**核心目标**：实现动态成员变更（AddVoter/RemoveVoter）

#### 3.1 需要实现的类

| # | 类名 | 代码行数 | 说明 |
|---|------|---------|------|
| 1 | `VotersRecord` | 150 | 控制记录：选民变更 |
| 2 | `AddVoterHandler` | 200 | 添加选民逻辑 |
| 3 | `RemoveVoterHandler` | 150 | 移除选民逻辑 |
| 4 | `KRaftControlRecordStateMachine` | 300 | 处理控制记录 |

#### 3.2 Phase 3 完成条件

- ✅ 支持运行时添加/移除选民
- ✅ 新选民可以追上日志后加入
- ✅ 保证一致性（不能同时进行多个变更）

---

## 第 3 部分：详细的优先级清单

### 第 1 周：基础 + 选举

**Week 1, Day 1-2：基础设施**
- [ ] 创建 Maven 项目
- [ ] 实现 `Timer`
- [ ] 实现 `ReplicaKey`, `Endpoints`, `LogOffsetMetadata`
- [ ] 实现 `MemoryPool`（可选，先跳过）
- [ ] 实现 `NetworkChannel` 接口

**Week 1, Day 3-4：RPC 框架**
- [ ] 实现 `VoteRequest`, `VoteResponse`
- [ ] 实现 `BeginQuorumEpochRequest`, `Response`
- [ ] 实现 `RaftRequest`, `RaftResponse` 基类
- [ ] 实现 `RequestManager`

**Week 1, Day 5-7：状态管理和选举**
- [ ] 实现 `EpochState` 接口
- [ ] 实现 `ElectionState`
- [ ] 实现 `QuorumState`（最关键！）
- [ ] 实现 `UnattachedState`
- [ ] 实现 `ProspectiveState` 和 `CandidateState`
- [ ] 编写选举测试

### 第 2 周：日志复制

**Week 2, Day 1-2：日志存储**
- [ ] 实现 `LogOffsetMetadata`（深化）
- [ ] 实现 `RecordBatch`
- [ ] 实现 `LogSegment`
- [ ] 实现 `LogSegmentCollection`

**Week 2, Day 3-4：日志管理**
- [ ] 实现 `ReplicatedLog` 接口
- [ ] 实现 `ValidOffsetAndEpoch`
- [ ] 实现日志追加逻辑
- [ ] 实现日志验证和截断

**Week 2, Day 5-7：Fetch 和副本管理**
- [ ] 实现 `FetchRequest`, `FetchResponse`
- [ ] 实现 `ReplicaState`
- [ ] 完善 `LeaderState`（副本跟踪 + 高水位）
- [ ] 完善 `FollowerState`（Fetch 逻辑）

### 第 3 周：集成和高级

**Week 3, Day 1-3：协调器**
- [ ] 实现 `KafkaRaftClient` 核心逻辑
- [ ] 连接选举和日志复制
- [ ] 实现 `poll()` 主循环
- [ ] 编写集成测试

**Week 3, Day 4-5：配置更改和优化**
- [ ] 实现动态成员变更
- [ ] 添加快照支持
- [ ] 性能优化

**Week 3, Day 6-7：总结和文档**
- [ ] 编写详细文档
- [ ] 性能基准测试
- [ ] 代码审查和重构

---

## 第 4 部分：每个阶段的代码框架

### Phase 0：Timer 实现示例

```java
// org/apache/kafka/raft/internals/Timer.java

package org.apache.kafka.raft.internals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简单的计时器实现
 * 用于跟踪各种超时事件（选举超时、Fetch 超时等）
 */
public class Timer {
    private static final Logger log = LoggerFactory.getLogger(Timer.class);
    private static final long UNINITIALIZED_DEADLINE = Long.MAX_VALUE;

    private volatile long deadlineMs = UNINITIALIZED_DEADLINE;
    private final String name;  // 用于日志

    public Timer(String name) {
        this.name = name;
    }

    /**
     * 重置计时器，从现在开始计时 delayMs 毫秒
     */
    public synchronized void reset(long delayMs, long currentTimeMs) {
        if (delayMs < 0) {
            throw new IllegalArgumentException("Negative delay: " + delayMs);
        }
        long newDeadline = currentTimeMs + delayMs;
        log.debug("{}: Reset timer to {} ms", name, delayMs);
        this.deadlineMs = newDeadline;
    }

    /**
     * 计时器是否已过期
     */
    public synchronized boolean isExpired(long currentTimeMs) {
        return currentTimeMs >= deadlineMs;
    }

    /**
     * 距离过期还需多少毫秒（如果为负表示已过期）
     */
    public synchronized long remainingMs(long currentTimeMs) {
        return deadlineMs - currentTimeMs;
    }

    /**
     * 清除计时器
     */
    public synchronized void cancel() {
        log.debug("{}: Cancel timer", name);
        this.deadlineMs = UNINITIALIZED_DEADLINE;
    }

    /**
     * 计时器是否已初始化
     */
    public synchronized boolean isInitialized() {
        return deadlineMs != UNINITIALIZED_DEADLINE;
    }

    @Override
    public String toString() {
        return String.format("Timer(%s, deadline=%d)", name, deadlineMs);
    }
}
```

### Phase 0：ReplicaKey 示例

```java
// org/apache/kafka/raft/common/ReplicaKey.java

package org.apache.kafka.raft.common;

import java.util.Objects;

/**
 * 副本身份标识
 * 包含 ID 和 Directory ID，唯一标识一个副本
 */
public class ReplicaKey {
    private final int id;
    private final String directoryId;

    public ReplicaKey(int id, String directoryId) {
        this.id = id;
        this.directoryId = Objects.requireNonNull(directoryId);
    }

    public int id() {
        return id;
    }

    public String directoryId() {
        return directoryId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaKey that = (ReplicaKey) o;
        return id == that.id &&
               Objects.equals(directoryId, that.directoryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, directoryId);
    }

    @Override
    public String toString() {
        return String.format("ReplicaKey(%d, %s)", id, directoryId);
    }
}
```

### Phase 1：VoteRequest 示例

```java
// org/apache/kafka/raft/rpc/VoteRequest.java

package org.apache.kafka.raft.rpc;

import org.apache.kafka.raft.common.ReplicaKey;
import org.apache.kafka.raft.log.LogOffsetMetadata;

/**
 * 投票请求 RPC
 * Candidate 向其他节点请求投票
 */
public class VoteRequest extends RaftRequest {
    private final int epoch;
    private final ReplicaKey candidateId;
    private final LogOffsetMetadata lastLogOffsetMetadata;
    private final boolean isPreVote;

    public VoteRequest(
        int epoch,
        ReplicaKey candidateId,
        LogOffsetMetadata lastLogOffsetMetadata,
        boolean isPreVote
    ) {
        this.epoch = epoch;
        this.candidateId = candidateId;
        this.lastLogOffsetMetadata = lastLogOffsetMetadata;
        this.isPreVote = isPreVote;
    }

    public int epoch() {
        return epoch;
    }

    public ReplicaKey candidateId() {
        return candidateId;
    }

    public LogOffsetMetadata lastLogOffsetMetadata() {
        return lastLogOffsetMetadata;
    }

    public boolean isPreVote() {
        return isPreVote;
    }

    @Override
    public String toString() {
        return String.format(
            "VoteRequest(epoch=%d, candidate=%s, lastLog=%s, preVote=%b)",
            epoch, candidateId, lastLogOffsetMetadata, isPreVote
        );
    }
}
```

### Phase 1：QuorumState 核心方法示例

```java
// org/apache/kafka/raft/state/QuorumState.java

package org.apache.kafka.raft.state;

import org.apache.kafka.raft.common.ReplicaKey;
import org.apache.kafka.raft.log.LogOffsetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quorum 状态管理器
 * 管理节点状态转换、投票决策、选举状态持久化
 */
public class QuorumState {
    private static final Logger log = LoggerFactory.getLogger(QuorumState.class);

    private int localId;
    private ElectionState election;  // 选举状态（持久化）
    private EpochState state;         // 当前状态（LeaderState、FollowerState 等）
    private LogOffsetMetadata lastLogOffsetMetadata;

    public QuorumState(int localId, ElectionState initialElection) {
        this.localId = localId;
        this.election = initialElection;
        this.state = new UnattachedState(initialElection);
    }

    /**
     * 检查是否可以投票
     * 投票限制是保证选举安全的关键机制
     */
    public boolean canGrantVote(
        ReplicaKey candidate,
        int candidateEpoch,
        LogOffsetMetadata candidateLastLog
    ) {
        // 检查 epoch 是否有效
        if (candidateEpoch < election.epoch()) {
            log.debug("Rejecting vote for {} with epoch {}: too old",
                candidate, candidateEpoch);
            return false;
        }

        // 如果候选人的 epoch 更新，需要先更新本地 epoch 并重置 votedFor
        if (candidateEpoch > election.epoch()) {
            updateElectionState(new ElectionState(
                candidateEpoch,
                null  // 重置 votedFor
            ));
        }

        // 检查是否已投票
        if (election.votedFor().isPresent() &&
            !election.votedFor().get().equals(candidate)) {
            log.debug("Already voted for {}, rejecting {}",
                election.votedFor().get(), candidate);
            return false;
        }

        // 检查日志是否足够新
        if (!isLogUpToDate(candidateLastLog)) {
            log.debug("Candidate {} log is not up-to-date", candidate);
            return false;
        }

        return true;
    }

    /**
     * 判断候选人的日志是否至少和本地日志一样新
     * 这是选举限制的核心实现
     */
    private boolean isLogUpToDate(LogOffsetMetadata candidateLastLog) {
        LogOffsetMetadata myLastLog = lastLogOffsetMetadata;

        // 候选人的最后一个日志 epoch 更大
        if (candidateLastLog.epoch > myLastLog.epoch) {
            return true;
        }

        // 相同 epoch，检查 offset
        if (candidateLastLog.epoch == myLastLog.epoch) {
            return candidateLastLog.offset >= myLastLog.offset;
        }

        return false;
    }

    /**
     * 状态转换：变为 Leader
     */
    public synchronized void transitionToLeader(long epochStartOffset) {
        log.info("Transitioning to Leader at epoch {}", election.epoch());
        this.state = new LeaderState(
            election.epoch(),
            epochStartOffset,
            null  // voters（需要从其他地方获取）
        );
    }

    /**
     * 状态转换：变为 Follower
     */
    public synchronized void transitionToFollower(
        ReplicaKey leaderId,
        int newEpoch
    ) {
        // 更新 epoch
        if (newEpoch > election.epoch()) {
            updateElectionState(new ElectionState(
                newEpoch,
                null  // 重置 votedFor
            ));
        }

        log.info("Transitioning to Follower, leader={}, epoch={}",
            leaderId, newEpoch);
        this.state = new FollowerState(leaderId);
    }

    // ... 更多方法

    private void updateElectionState(ElectionState newState) {
        // TODO: 持久化到磁盘
        this.election = newState;
    }
}
```

---

## 第 5 部分：学习和参考资源

### 代码对照表

实现时可对照以下 Kafka 源文件：

| 你要实现的类 | Kafka 源文件位置 | 行数 |
|-----------|----------------|------|
| Timer | `/raft/src/main/java/org/apache/kafka/raft/internals/Timer.java` | ~50 |
| ReplicaKey | `/raft/src/main/java/org/apache/kafka/raft/ReplicaKey.java` | ~100 |
| QuorumState | `/raft/src/main/java/org/apache/kafka/raft/QuorumState.java` | 934 |
| LeaderState | `/raft/src/main/java/org/apache/kafka/raft/LeaderState.java` | 1154 |
| FollowerState | `/raft/src/main/java/org/apache/kafka/raft/FollowerState.java` | 288 |
| ReplicatedLog | `/raft/src/main/java/org/apache/kafka/raft/ReplicatedLog.java` | 接口 |
| KafkaRaftClient | `/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java` | 4141 |

### 学习资源

| 资源 | 说明 | 用途 |
|------|------|------|
| `/home/user/kafka/Raft_Theory_Guide.md` | Raft 理论教学 | 理解共识算法 |
| `/home/user/kafka/KRaft_Implementation_Analysis.md` | KRaft 实现详解 | 了解设计决策 |
| `/home/user/kafka/raft/README.md` | KRaft 文档 | 系统层面理解 |
| `/home/user/kafka/raft/src/test/` | Kafka 的测试用例 | 验证实现正确性 |

### 测试策略

为每个关键类编写单元测试：

```java
// 测试 QuorumState 的选举限制
@Test
public void testCanGrantVoteWhenCandidateLogIsUpToDate() {
    QuorumState state = new QuorumState(1, new ElectionState(5, null));
    state.setLastLogOffsetMetadata(new LogOffsetMetadata(100, 5));

    // 候选人日志更新
    boolean canVote = state.canGrantVote(
        new ReplicaKey(2, "dir2"),
        6,  // 更高的 epoch
        new LogOffsetMetadata(50, 6)  // 更新的 epoch
    );

    assertTrue(canVote);
}

@Test
public void testCannotGrantVoteWhenCandidateLogIsOlder() {
    QuorumState state = new QuorumState(1, new ElectionState(5, null));
    state.setLastLogOffsetMetadata(new LogOffsetMetadata(100, 5));

    // 候选人日志较旧
    boolean canVote = state.canGrantVote(
        new ReplicaKey(2, "dir2"),
        5,
        new LogOffsetMetadata(50, 4)  // 更旧的 epoch
    );

    assertFalse(canVote);
}
```

---

## 第 6 部分：常见陷阱和注意事项

### ❌ 常见错误

1. **忽视持久化**
   - ❌ 只在内存中维护 ElectionState
   - ✅ 必须持久化到磁盘（可以先用文件，生产用数据库）

2. **忽视并发**
   - ❌ 状态管理不使用锁
   - ✅ 使用 `synchronized` 或更高级的并发工具

3. **日志验证逻辑错误**
   - ❌ 只比较 offset，忽视 epoch
   - ✅ 必须同时检查 epoch 和 offset

4. **高水位计算错误**
   - ❌ 取最大值（会导致数据丢失）
   - ✅ 取多数派的最小值（中位数）

5. **状态转换不完整**
   - ❌ 缺少必要的清理工作
   - ✅ 每次状态转换都要重置必要的字段

### ✅ 最佳实践

1. **每个类都有日志**
   ```java
   private static final Logger log = LoggerFactory.getLogger(MyClass.class);
   ```

2. **编写详细的单元测试**
   - 正常路径（happy path）
   - 边界情况（boundary conditions）
   - 错误场景（error scenarios）

3. **定期对比 Kafka 源码**
   - 每完成一个类，对比原始实现
   - 检查是否遗漏了关键逻辑

4. **保持代码结构一致**
   - 包名、类名、方法名尽量与 Kafka 一致
   - 便于学习和后续参考

5. **循序渐进地增加复杂性**
   - 先实现简单情况（单节点）
   - 再实现复杂情况（多节点、故障恢复）

---

## 总结：实现顺序优先级清单

```
✅ 应该先做：基础设施（Timer、ReplicaKey 等）
  ↓
✅ 再做：RPC 框架（请求/响应）
  ↓
✅ 再做：状态管理（QuorumState、ElectionState）
  ↓
✅ 再做：具体状态（LeaderState、FollowerState 等）
  ↓
✅ 再做：日志存储（ReplicatedLog、LogSegment）
  ↓
✅ 再做：日志复制（Fetch、副本同步）
  ↓
✅ 最后：配置更改和高级特性
```

### 为什么这个顺序？

1. **基础设施优先**：所有其他代码都依赖它们
2. **自下而上**：从 RPC 到状态机到具体实现
3. **增量验证**：每个阶段都可以测试和验证
4. **并发实现**：后续阶段可以并行进行

---

**关键建议**：

1. **先实现选举是对的**，这是基础
2. **但要为选举打好基础**，包括 RPC、状态管理
3. **日志复制依赖选举的结果**，所以选举必须先完成
4. **配置更改是可选的**，可以放在最后

祝学习顺利！这将是深入理解 Kafka 架构的最好方式。
