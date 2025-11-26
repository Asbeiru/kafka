# KRaft 学习项目规划指南

## 项目概述

本指南帮助你创建一个独立的 Maven 项目来深入学习 Kafka 的 Raft 实现（KRaft）。通过严格参照 Kafka 的 KRaft 实现，重写核心模块以深入理解 Raft 协议和 Kafka 的实现细节。

**代码规模参考：**
- Kafka KRaft 核心实现：约 18,500 行 Java 代码
- 46 个核心类 + 27 个内部类 + 11 个快照类
- 50+ 单元测试类

---

## 一、项目结构建议

### 推荐的 Maven 多模块项目结构

```
kraft-learning/
├── pom.xml                          # 父 POM
├── README.md
├── docs/                            # 学习笔记和文档
│   ├── raft-protocol.md             # Raft 协议笔记
│   ├── kafka-raft-design.md         # Kafka Raft 设计分析
│   └── implementation-notes.md      # 实现要点
│
├── kraft-common/                    # 模块1：公共基础
│   ├── pom.xml
│   └── src/main/java/org/apache/kafka/
│       ├── common/                  # 公共工具和数据结构
│       │   ├── utils/
│       │   ├── record/
│       │   ├── protocol/
│       │   └── errors/
│       └── server/common/           # 服务器公共组件
│
├── kraft-raft/                      # 模块2：核心 Raft 实现 ⭐⭐⭐
│   ├── pom.xml
│   └── src/
│       ├── main/
│       │   ├── java/org/apache/kafka/
│       │   │   ├── raft/            # 主包（46个核心类）
│       │   │   │   ├── RaftClient.java
│       │   │   │   ├── KafkaRaftClient.java (2,500行 - 最核心)
│       │   │   │   ├── QuorumState.java (800行)
│       │   │   │   ├── LeaderState.java (1,050行)
│       │   │   │   ├── FollowerState.java
│       │   │   │   ├── CandidateState.java
│       │   │   │   ├── ProspectiveState.java
│       │   │   │   ├── UnattachedState.java
│       │   │   │   ├── ResignedState.java
│       │   │   │   ├── EpochState.java
│       │   │   │   ├── ReplicatedLog.java
│       │   │   │   ├── NetworkChannel.java
│       │   │   │   ├── KafkaNetworkChannel.java
│       │   │   │   ├── VoterSet.java
│       │   │   │   ├── QuorumConfig.java
│       │   │   │   ├── RaftUtil.java (950行)
│       │   │   │   └── ... (其他42个类)
│       │   │   │
│       │   │   ├── raft/internals/  # 内部包（27个类）
│       │   │   │   ├── BatchAccumulator.java
│       │   │   │   ├── BatchBuilder.java
│       │   │   │   ├── BatchMemoryPool.java
│       │   │   │   ├── RequestSender.java
│       │   │   │   ├── AddVoterHandler.java
│       │   │   │   ├── RemoveVoterHandler.java
│       │   │   │   ├── VoterSetHistory.java
│       │   │   │   ├── KafkaRaftMetrics.java (800行)
│       │   │   │   ├── BlockingMessageQueue.java
│       │   │   │   ├── FuturePurgatory.java
│       │   │   │   └── ... (其他17个类)
│       │   │   │
│       │   │   └── snapshot/        # 快照包（11个类）
│       │   │       ├── SnapshotReader.java
│       │   │       ├── SnapshotWriter.java
│       │   │       ├── RawSnapshotReader.java
│       │   │       ├── RawSnapshotWriter.java
│       │   │       ├── FileRawSnapshotReader.java
│       │   │       ├── FileRawSnapshotWriter.java
│       │   │       ├── RecordsSnapshotReader.java
│       │   │       ├── RecordsSnapshotWriter.java
│       │   │       ├── SnapshotPath.java
│       │   │       └── Snapshots.java
│       │   │
│       │   └── resources/
│       │       └── common/message/
│       │           └── QuorumStateData.json  # 状态存储格式
│       │
│       └── test/
│           └── java/org/apache/kafka/raft/
│               ├── KafkaRaftClientTest.java
│               ├── QuorumStateTest.java
│               ├── LeaderStateTest.java
│               ├── FollowerStateTest.java
│               └── ... (50+ 测试类)
│
├── kraft-storage/                   # 模块3：日志存储
│   ├── pom.xml
│   └── src/main/java/org/apache/kafka/
│       └── storage/
│           ├── internals/
│           │   ├── log/             # UnifiedLog 简化实现
│           │   └── epoch/           # Leader Epoch 管理
│           └── StorageInterface.java
│
├── kraft-network/                   # 模块4：网络通信（可选）
│   ├── pom.xml
│   └── src/main/java/org/apache/kafka/
│       └── network/
│           ├── Selector.java
│           ├── KafkaChannel.java
│           └── NetworkClient.java
│
└── kraft-example/                   # 模块5：示例和测试
    ├── pom.xml
    └── src/main/java/
        └── example/
            ├── SimpleRaftCluster.java     # 简单集群示例
            ├── ThreeNodeExample.java      # 3节点集群
            └── LeaderElectionDemo.java    # 选举演示
```

---

## 二、分阶段学习路线

### 第一阶段：基础设施（1-2周）

**目标：** 搭建项目骨架，实现基础设施

**实现内容：**
1. **kraft-common 模块**
   - 公共异常类（RaftException, NotLeaderException等）
   - 基础数据结构（LogOffsetMetadata, ValidOffsetAndEpoch）
   - 序列化/反序列化（Serde接口）
   - 工具类

2. **核心接口定义**
   ```java
   // kraft-raft/src/main/java/org/apache/kafka/raft/

   // 1. 客户端接口
   RaftClient.java

   // 2. 状态接口
   EpochState.java

   // 3. 日志接口
   ReplicatedLog.java

   // 4. 网络接口
   NetworkChannel.java
   ```

**关键文件清单：**
- [ ] `RaftClient.java` - 核心客户端接口
- [ ] `EpochState.java` - 状态基接口
- [ ] `ReplicatedLog.java` - 复制日志接口
- [ ] `NetworkChannel.java` - 网络通道接口
- [ ] `QuorumConfig.java` - 配置类

**学习要点：**
- 理解 Raft 客户端的核心 API 设计
- 理解状态抽象和接口设计
- 理解日志接口的抽象层次

---

### 第二阶段：状态机实现（2-3周）⭐ 核心

**目标：** 实现 Raft 状态机的所有状态

**状态转移图：**
```
Unattached --> Prospective --> Candidate --> Leader
    ^            ^              ^            │
    │            │              │            │
    └── Follower ────────────────────────────┘
                                             │
    Resigned <────────────────────────────────
```

**实现顺序：**

1. **基础状态类（第1周）**
   ```java
   // 1. 实现简单状态
   UnattachedState.java       // 未附加状态（最简单）
   FollowerState.java         // Follower 状态
   ResignedState.java         // 辞职状态

   // 2. 选举相关状态
   NomineeState.java          // 候选人基接口
   ProspectiveState.java      // 预选举状态（Pre-Vote）
   CandidateState.java        // 候选人状态
   ```

2. **Leader 状态（第2周）**
   ```java
   LeaderState.java          // Leader 状态（1,050行 - 最复杂）
   ```

   **学习重点：**
   - 日志复制逻辑
   - 心跳机制
   - 高水位（High Watermark）管理
   - Follower 追踪

3. **状态管理器（第3周）**
   ```java
   QuorumState.java          // Quorum 状态管理（800行）
   ```

   **学习重点：**
   - 状态转移逻辑
   - 任期（Term/Epoch）管理
   - 投票管理

**关键文件清单：**
- [ ] `UnattachedState.java`
- [ ] `FollowerState.java`
- [ ] `ResignedState.java`
- [ ] `NomineeState.java`
- [ ] `ProspectiveState.java`
- [ ] `CandidateState.java`
- [ ] `LeaderState.java` ⭐⭐⭐
- [ ] `QuorumState.java` ⭐⭐⭐

**调试技巧：**
- 为每个状态添加详细日志
- 实现状态转移的可视化输出
- 编写状态转移的单元测试

---

### 第三阶段：核心客户端实现（3-4周）⭐⭐⭐ 最核心

**目标：** 实现 KafkaRaftClient（2,500行代码）

**实现步骤：**

1. **客户端框架（第1周）**
   ```java
   KafkaRaftClient.java      // 核心客户端
   KafkaRaftClientDriver.java // 驱动程序
   ```

   **先实现的方法：**
   - 初始化和配置
   - 状态查询（currentLeader, currentEpoch）
   - 基本的事件循环（poll）

2. **日志操作（第2周）**
   ```java
   // 在 KafkaRaftClient 中实现：
   - append()           // 追加记录
   - read()             // 读取记录
   - handleFetch()      // 处理 Fetch 请求
   - handleAppend()     // 处理追加
   ```

3. **选举逻辑（第3周）**
   ```java
   // 在 KafkaRaftClient 中实现：
   - handleVote()              // 处理投票请求
   - handleBeginQuorumEpoch()  // 处理新 Epoch
   - handleEndQuorumEpoch()    // 处理 Epoch 结束
   - maybeTransition()         // 状态转移
   ```

4. **网络通信（第4周）**
   ```java
   KafkaNetworkChannel.java    // 网络通道实现
   RequestManager.java         // 请求管理
   ```

**关键文件清单：**
- [ ] `KafkaRaftClient.java` ⭐⭐⭐ (最核心，2,500行)
- [ ] `KafkaRaftClientDriver.java`
- [ ] `KafkaNetworkChannel.java`
- [ ] `RequestManager.java` (400行)

**学习要点：**
- Raft 协议的完整流程
- 异步事件处理模型
- 网络请求的发送和响应处理
- 错误处理和重试机制

---

### 第四阶段：批处理和内部组件（2-3周）

**目标：** 实现高效的批处理和内部工具

**实现内容：**

1. **批处理系统（第1周）**
   ```java
   // kraft-raft/src/main/java/org/apache/kafka/raft/internals/

   BatchAccumulator.java      // 批次累积器（350行）
   BatchBuilder.java          // 批次构建器
   BatchMemoryPool.java       // 内存池
   MemoryBatchReader.java     // 批次读取器
   RecordsBatchReader.java    // 记录批次读取器
   ```

2. **请求处理（第2周）**
   ```java
   RequestSender.java         // 请求发送器接口
   DefaultRequestSender.java  // 默认实现
   ```

3. **数据结构和工具（第3周）**
   ```java
   BlockingMessageQueue.java  // 消息队列
   FuturePurgatory.java       // Future 管理
   ThresholdPurgatory.java    // 阈值清洁站
   RecordsIterator.java       // 记录迭代器
   ```

**关键文件清单：**
- [ ] `BatchAccumulator.java` (350行)
- [ ] `BatchBuilder.java`
- [ ] `BatchMemoryPool.java`
- [ ] `BlockingMessageQueue.java`
- [ ] `FuturePurgatory.java`
- [ ] `RequestSender.java`

---

### 第五阶段：动态成员变更（1-2周）

**目标：** 实现投票者的动态增删改

**实现内容：**

```java
// kraft-raft/src/main/java/org/apache/kafka/raft/internals/

AddVoterHandler.java         // 添加投票者
RemoveVoterHandler.java      // 移除投票者
UpdateVoterHandler.java      // 更新投票者
VoterSetHistory.java         // 投票者历史记录

// kraft-raft/src/main/java/org/apache/kafka/raft/

VoterSet.java                // 投票者集合
DynamicVoter.java            // 动态投票者
DynamicVoters.java           // 动态投票者集合
```

**关键文件清单：**
- [ ] `VoterSet.java`
- [ ] `AddVoterHandler.java`
- [ ] `RemoveVoterHandler.java`
- [ ] `UpdateVoterHandler.java`
- [ ] `VoterSetHistory.java`

**学习要点：**
- Raft 成员变更协议
- 多数派的动态计算
- 配置变更的两阶段提交

---

### 第六阶段：快照实现（1-2周）

**目标：** 实现快照的读写和管理

**实现内容：**

```java
// kraft-raft/src/main/java/org/apache/kafka/snapshot/

SnapshotReader.java            // 快照读取器接口
SnapshotWriter.java            // 快照写入器接口
RawSnapshotReader.java         // 原始快照读取器
RawSnapshotWriter.java         // 原始快照写入器
FileRawSnapshotReader.java     // 文件读取实现
FileRawSnapshotWriter.java     // 文件写入实现
RecordsSnapshotReader.java     // 记录快照读取器
RecordsSnapshotWriter.java     // 记录快照写入器
SnapshotPath.java              // 快照路径管理
Snapshots.java                 // 快照工具类
NotifyingRawSnapshotWriter.java // 通知写入器
```

**关键文件清单：**
- [ ] `SnapshotReader.java`
- [ ] `SnapshotWriter.java`
- [ ] `FileRawSnapshotReader.java`
- [ ] `FileRawSnapshotWriter.java`
- [ ] `RecordsSnapshotReader.java`
- [ ] `RecordsSnapshotWriter.java`
- [ ] `SnapshotPath.java`
- [ ] `Snapshots.java`

**学习要点：**
- 快照的存储格式
- 增量快照加载
- 快照与日志的一致性

---

### 第七阶段：监控和度量（1周）

**目标：** 实现性能监控和指标收集

**实现内容：**

```java
// kraft-raft/src/main/java/org/apache/kafka/raft/internals/

KafkaRaftMetrics.java         // Raft 指标（800行）

// kraft-raft/src/main/java/org/apache/kafka/raft/

ExternalKRaftMetrics.java     // 外部指标接口
```

**指标类型：**
- 选举次数和时长
- 日志追加速率
- 网络请求延迟
- 状态转移统计

---

### 第八阶段：存储实现（2-3周）

**目标：** 实现日志存储（kraft-storage 模块）

**实现内容：**

```java
// kraft-storage/src/main/java/org/apache/kafka/storage/

MetadataLog.java              // 元数据日志（实现 ReplicatedLog）
```

**简化实现建议：**
- 使用文件系统存储日志段
- 实现基本的日志段管理
- 支持日志截断和清理
- 不需要实现完整的 UnifiedLog

**学习要点：**
- 日志段（Log Segment）的组织
- 索引结构（offset index, time index）
- 日志清理策略

---

### 第九阶段：集成测试和示例（1-2周）

**目标：** 实现可运行的集群示例

**实现内容：**

```java
// kraft-example/src/main/java/example/

SimpleRaftCluster.java        // 简单集群
ThreeNodeExample.java         // 3节点集群
LeaderElectionDemo.java       // 选举演示
FailoverTest.java             // 故障转移测试
SnapshotDemo.java             # 快照演示
```

**示例场景：**
1. 启动3节点集群
2. 写入数据并观察复制
3. 模拟 Leader 宕机
4. 观察选举和恢复
5. 测试快照生成和加载

---

## 三、Maven 配置示例

### 父 POM (kraft-learning/pom.xml)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.apache.kafka.learning</groupId>
    <artifactId>kraft-learning</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>KRaft Learning Project</name>
    <description>深入学习 Kafka Raft (KRaft) 实现</description>

    <modules>
        <module>kraft-common</module>
        <module>kraft-raft</module>
        <module>kraft-storage</module>
        <module>kraft-network</module>
        <module>kraft-example</module>
    </modules>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

        <!-- Kafka版本 - 与源码保持一致 -->
        <kafka.version>4.0.0-SNAPSHOT</kafka.version>

        <!-- 依赖版本 -->
        <slf4j.version>2.0.9</slf4j.version>
        <jackson.version>2.17.2</jackson.version>
        <junit.version>5.10.1</junit.version>
        <mockito.version>5.8.0</mockito.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <!-- SLF4J -->
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>${slf4j.version}</version>
            </dependency>

            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-simple</artifactId>
                <version>${slf4j.version}</version>
            </dependency>

            <!-- Jackson -->
            <dependency>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-databind</artifactId>
                <version>${jackson.version}</version>
            </dependency>

            <!-- JUnit 5 -->
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit.version}</version>
                <scope>test</scope>
            </dependency>

            <!-- Mockito -->
            <dependency>
                <groupId>org.mockito</groupId>
                <artifactId>mockito-core</artifactId>
                <version>${mockito.version}</version>
                <scope>test</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.11.0</version>
                <configuration>
                    <source>17</source>
                    <target>17</target>
                </configuration>
            </plugin>

            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.2.3</version>
            </plugin>
        </plugins>
    </build>
</project>
```

### kraft-raft 模块 POM

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.apache.kafka.learning</groupId>
        <artifactId>kraft-learning</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>kraft-raft</artifactId>
    <name>KRaft Raft Implementation</name>

    <dependencies>
        <!-- 内部依赖 -->
        <dependency>
            <groupId>org.apache.kafka.learning</groupId>
            <artifactId>kraft-common</artifactId>
            <version>${project.version}</version>
        </dependency>

        <!-- 外部依赖 -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>

        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>

        <!-- 测试依赖 -->
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

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

---

## 四、核心类实现清单

### 必须实现的 84 个类（按优先级）

#### 优先级 P0（核心，必须先实现）- 15个类

**kraft-raft 主包：**
- [ ] `RaftClient.java` - 客户端接口
- [ ] `EpochState.java` - 状态接口
- [ ] `ReplicatedLog.java` - 日志接口
- [ ] `NetworkChannel.java` - 网络接口
- [ ] `QuorumConfig.java` - 配置类
- [ ] `QuorumState.java` - 状态管理 ⭐⭐⭐
- [ ] `KafkaRaftClient.java` - 核心客户端 ⭐⭐⭐
- [ ] `LeaderState.java` - Leader 状态 ⭐⭐
- [ ] `FollowerState.java` - Follower 状态 ⭐⭐
- [ ] `CandidateState.java` - Candidate 状态
- [ ] `UnattachedState.java` - Unattached 状态
- [ ] `ResignedState.java` - Resigned 状态
- [ ] `ProspectiveState.java` - Prospective 状态
- [ ] `NomineeState.java` - Nominee 接口
- [ ] `RaftUtil.java` - 工具类

#### 优先级 P1（重要功能）- 25个类

**kraft-raft 主包：**
- [ ] `KafkaRaftClientDriver.java`
- [ ] `KafkaNetworkChannel.java`
- [ ] `RequestManager.java`
- [ ] `VoterSet.java`
- [ ] `LeaderAndEpoch.java`
- [ ] `ElectionState.java`
- [ ] `LogAppendInfo.java`
- [ ] `LogFetchInfo.java`
- [ ] `LogOffsetMetadata.java`
- [ ] `Batch.java`
- [ ] `BatchReader.java`
- [ ] `Isolation.java`
- [ ] `ValidOffsetAndEpoch.java`
- [ ] `Endpoints.java`
- [ ] `ControlRecord.java`

**kraft-raft/internals 包：**
- [ ] `BatchAccumulator.java`
- [ ] `BatchBuilder.java`
- [ ] `BatchMemoryPool.java`
- [ ] `MemoryBatchReader.java`
- [ ] `RecordsBatchReader.java`
- [ ] `RequestSender.java`
- [ ] `DefaultRequestSender.java`
- [ ] `KafkaRaftMetrics.java`
- [ ] `BlockingMessageQueue.java`
- [ ] `FuturePurgatory.java`

#### 优先级 P2（高级功能）- 22个类

**kraft-raft 主包：**
- [ ] `DynamicVoter.java`
- [ ] `DynamicVoters.java`
- [ ] `ReplicaKey.java`
- [ ] `RaftManager.java`
- [ ] `QuorumStateStore.java`
- [ ] `FileQuorumStateStore.java`
- [ ] `MetadataLogConfig.java`
- [ ] `ExpirationService.java`
- [ ] `TimingWheelExpirationService.java`
- [ ] `ExternalKRaftMetrics.java`
- [ ] `OffsetMetadata.java`

**kraft-raft/internals 包：**
- [ ] `AddVoterHandler.java`
- [ ] `RemoveVoterHandler.java`
- [ ] `UpdateVoterHandler.java`
- [ ] `VoterSetHistory.java`
- [ ] `LogHistory.java`
- [ ] `TreeMapLogHistory.java`
- [ ] `EpochElection.java`
- [ ] `ThresholdPurgatory.java`
- [ ] `CloseListener.java`
- [ ] `KRaftControlRecordStateMachine.java`
- [ ] `KRaftVersionUpgrade.java`

#### 优先级 P3（快照功能）- 11个类

**kraft-raft/snapshot 包：**
- [ ] `SnapshotReader.java`
- [ ] `SnapshotWriter.java`
- [ ] `RawSnapshotReader.java`
- [ ] `RawSnapshotWriter.java`
- [ ] `FileRawSnapshotReader.java`
- [ ] `FileRawSnapshotWriter.java`
- [ ] `NotifyingRawSnapshotWriter.java`
- [ ] `RecordsSnapshotReader.java`
- [ ] `RecordsSnapshotWriter.java`
- [ ] `SnapshotPath.java`
- [ ] `Snapshots.java`

#### 优先级 P4（辅助工具）- 11个类

**kraft-raft/internals 包：**
- [ ] `IdentitySerde.java`
- [ ] `StringSerde.java`
- [ ] `RecordsIterator.java`
- [ ] `RequestSendResult.java`

**kraft-raft 主包：**
- [ ] `RaftRequest.java`
- [ ] `RaftResponse.java`
- [ ] `RaftMessage.java`
- [ ] `RaftMessageQueue.java`

**错误处理：**
- [ ] `RaftException.java`
- [ ] `NotLeaderException.java`
- [ ] `BufferAllocationException.java`

**总计：84 个核心类**

---

## 五、学习建议和技巧

### 1. 代码阅读策略

**从接口开始：**
```
第1天：RaftClient 接口
第2天：EpochState 接口体系
第3天：ReplicatedLog 接口
第4天：NetworkChannel 接口
第5天：开始实现简单状态类
```

**逐步深入：**
1. 先看接口定义，理解抽象
2. 再看简单实现（UnattachedState, FollowerState）
3. 然后看复杂实现（LeaderState, QuorumState）
4. 最后看核心引擎（KafkaRaftClient）

### 2. 调试技巧

**添加详细日志：**
```java
private static final Logger log = LoggerFactory.getLogger(KafkaRaftClient.class);

public void poll() {
    log.debug("Polling in state: {}, epoch: {}, leader: {}",
              currentState(), currentEpoch(), currentLeader());
    // ...
}
```

**状态可视化：**
```java
public String toString() {
    return String.format("QuorumState{state=%s, epoch=%d, leader=%s, voted=%s}",
                         state.name(), epoch, leaderId, votedId);
}
```

### 3. 单元测试策略

**状态测试：**
```java
@Test
void testFollowerToCandidate() {
    // 测试 Follower -> Candidate 转移
}

@Test
void testCandidateWinsElection() {
    // 测试选举成功
}
```

**集成测试：**
```java
@Test
void testThreeNodeClusterElection() {
    // 启动3节点集群，测试选举
}
```

### 4. 参考资料

**必读论文：**
1. Raft 论文：[In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
2. Kafka KRaft KIP：
   - [KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500)
   - [KIP-595: A Raft Protocol for the Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595)
   - [KIP-853: KRaft Controller Membership Changes](https://cwiki.apache.org/confluence/display/KAFKA/KIP-853)

**在线资源：**
- Raft 可视化：https://raft.github.io/
- Kafka 文档：https://kafka.apache.org/documentation/

### 5. 代码复制策略

**可以直接复制的代码：**
- 数据结构（如 LogOffsetMetadata）
- 工具方法（如 RaftUtil）
- 配置类（如 QuorumConfig）
- 常量定义

**必须手写的代码：**
- 核心状态机逻辑（QuorumState, LeaderState）
- 客户端实现（KafkaRaftClient）
- 选举和复制逻辑

**建议：边复制边添加注释**
```java
/**
 * 高水位（High Watermark）：所有副本都已复制的最大偏移量
 *
 * 在 Raft 中，只有已经被多数派确认的日志条目才能被应用到状态机。
 * 高水位就是这个"已确认的最大偏移量"。
 *
 * Leader 通过跟踪每个 Follower 的复制进度来计算高水位：
 * highWatermark = min(follower1.fetchOffset, follower2.fetchOffset, ...)
 */
private long highWatermark;
```

---

## 六、常见问题

### Q1: 是否需要实现完整的网络层？

**答：** 可以简化。建议：
- 第一阶段：使用内存队列模拟网络通信
- 第二阶段：使用 Netty 或 Java NIO 实现真实网络
- 参考 Kafka 的实现但不需要完全一致

### Q2: 日志存储是否需要实现完整的 UnifiedLog？

**答：** 不需要。建议：
- 使用简单的文件存储
- 实现基本的日志段管理
- 重点理解日志复制逻辑，而非存储优化

### Q3: 是否需要实现所有 Kafka 消息协议？

**答：** 不需要。核心消息即可：
- Vote/VoteResponse
- Fetch/FetchResponse
- BeginQuorumEpoch/EndQuorumEpoch
- FetchSnapshot（如果实现快照）

### Q4: 如何验证实现的正确性？

**答：**
1. 单元测试覆盖关键逻辑
2. 集成测试验证集群行为
3. 对比 Kafka 的测试用例
4. 使用 Jepsen 式的混沌测试（高级）

### Q5: 预计学习时间？

**答：**
- **快速版**（核心功能）：2-3 个月，每天2小时
- **完整版**（包括高级功能）：4-6 个月
- **精通版**（包括性能优化）：6-12 个月

---

## 七、项目启动命令

### 创建项目骨架

```bash
# 创建项目根目录
mkdir kraft-learning
cd kraft-learning

# 创建父 POM
touch pom.xml

# 创建模块
mkdir -p kraft-common/src/{main,test}/java
mkdir -p kraft-raft/src/{main,test}/java
mkdir -p kraft-storage/src/{main,test}/java
mkdir -p kraft-network/src/{main,test}/java
mkdir -p kraft-example/src/main/java

# 创建包结构
mkdir -p kraft-raft/src/main/java/org/apache/kafka/raft
mkdir -p kraft-raft/src/main/java/org/apache/kafka/raft/internals
mkdir -p kraft-raft/src/main/java/org/apache/kafka/snapshot
mkdir -p kraft-raft/src/test/java/org/apache/kafka/raft

# 创建文档目录
mkdir docs

# 初始化 Git
git init
```

### 编译和测试

```bash
# 编译所有模块
mvn clean compile

# 运行测试
mvn test

# 打包
mvn package

# 运行示例
cd kraft-example
mvn exec:java -Dexec.mainClass="example.ThreeNodeExample"
```

---

## 八、下一步行动

### 立即开始（今天）

1. **创建项目骨架**
   ```bash
   mkdir kraft-learning && cd kraft-learning
   # 按照上面的命令创建结构
   ```

2. **复制核心接口**
   - 从 Kafka 源码复制这4个接口到你的项目：
     - `RaftClient.java`
     - `EpochState.java`
     - `ReplicatedLog.java`
     - `NetworkChannel.java`

3. **阅读 Raft 论文**
   - 下载并打印 Raft 论文
   - 重点阅读第5章（Raft 共识算法）

### 第一周目标

- [ ] 搭建完整的项目结构
- [ ] 实现 5 个基础接口
- [ ] 实现 UnattachedState（最简单的状态）
- [ ] 编写第一个单元测试
- [ ] 阅读完 Raft 论文

### 第一个月目标

- [ ] 实现所有状态类（7个）
- [ ] 实现 QuorumState
- [ ] 理解状态转移逻辑
- [ ] 能够解释 Raft 选举过程

---

## 九、学习检查清单

### Raft 协议理解

- [ ] 理解 Leader Election
- [ ] 理解 Log Replication
- [ ] 理解 Safety
- [ ] 理解成员变更
- [ ] 理解日志压缩

### Kafka KRaft 特性

- [ ] 理解 Pre-Vote 机制
- [ ] 理解 Epoch 和 Term 的区别
- [ ] 理解高水位（High Watermark）
- [ ] 理解 Leader Epoch
- [ ] 理解动态成员变更

### 代码实现

- [ ] 能独立实现一个简单的 Raft 集群
- [ ] 能解释每个状态的职责
- [ ] 能调试选举问题
- [ ] 能分析日志复制延迟
- [ ] 能优化批处理性能

---

## 十、总结

通过这个学习项目，你将：

1. **深入理解 Raft 协议**
   - 选举、复制、安全性
   - 成员变更、日志压缩

2. **掌握 Kafka KRaft 实现**
   - 84 个核心类的实现细节
   - Kafka 特有的优化和设计

3. **提升工程能力**
   - 分布式系统设计
   - 状态机实现
   - 网络编程
   - 测试和调试

**最重要的建议：**
- 不要急于求成，扎实理解每一行代码
- 多写注释，解释"为什么"这样实现
- 多画图，可视化状态转移和消息流
- 多测试，覆盖各种边界情况

祝学习顺利！🚀
