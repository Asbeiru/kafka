# KRaft学习项目 - 依赖说明

## 问题描述

kraft-learning 是一个**学习项目**，目的是通过重新实现KRaft核心逻辑来深入理解Raft协议在Kafka中的应用。

但是，这个项目**依赖Kafka的基础设施类**，不能完全独立编译。

## 依赖的Kafka组件

### 1. kafka-clients (`org.apache.kafka.common.*`)

**包含的关键类：**
- `Node` - 表示Kafka集群中的节点
- `TopicPartition` - topic和partition的组合
- `Uuid` - Kafka使用的UUID
- `ListenerName` - 监听器名称
- `Records` - 记录集合
- `ApiKeys`, `ApiMessage`, `Errors` - 协议相关类
- `ConfigDef`, `AbstractConfig` - 配置框架

**为什么需要：**
这些是Kafka的基础数据结构和协议框架，KRaft实现依赖它们。

### 2. kafka-raft (`org.apache.kafka.raft.generated.*`)

**包含的关键类：**
- `QuorumStateData` - 从JSON schema生成的Quorum状态数据类
- `QuorumStateDataJsonConverter` - JSON转换器

**生成的协议类** (`org.apache.kafka.common.message.*`):
- `VoteRequestData`, `VoteResponseData`
- `FetchRequestData`, `FetchResponseData`
- `BeginQuorumEpochRequestData/ResponseData`
- `EndQuorumEpochRequestData/ResponseData`
- `FetchSnapshotRequestData/ResponseData`
- `DescribeQuorumRequestData/ResponseData`
- `AddRaftVoterRequestData/ResponseData`
- `RemoveRaftVoterRequestData/ResponseData`
- `UpdateRaftVoterRequestData/ResponseData`
- `VotersRecord` - 投票者记录

**为什么需要：**
这些类是从协议schema文件（`.json`）自动生成的，包含所有Raft协议消息的定义。

### 3. kafka-server-common (`org.apache.kafka.server.common.*`)

**包含的关键类：**
- `OffsetAndEpoch` - offset和epoch的组合
- `KRaftVersion` - KRaft协议版本

**为什么需要：**
服务器端通用类，被Raft和其他服务器组件共享。

### 4. kafka-storage (`org.apache.kafka.snapshot.*`)

**包含的关键类：**
- `RawSnapshotReader` - 快照读取器接口
- `RawSnapshotWriter` - 快照写入器接口

**为什么需要：**
KRaft的快照机制依赖这些接口。

## 已实现的kraft-learning类

我们在kraft-learning中**重新实现了KRaft的核心逻辑类**：

### 阶段1：基础数据结构（12个类）
- OffsetMetadata, LogOffsetMetadata
- LeaderAndEpoch, ValidOffsetAndEpoch
- ElectionState, Endpoints, ReplicaKey
- Batch<T>, LogAppendInfo, LogFetchInfo
- Isolation (enum)

### 阶段2：核心接口（6个接口）
- RaftMessage, BatchReader<T>, NetworkChannel
- EpochState, RaftClient<T>, ReplicatedLog

### 阶段3：配置和存储（4个类）
- QuorumStateStore, FileQuorumStateStore
- QuorumConfig, RaftUtil

## Maven依赖配置

### 父pom.xml (kraft-learning/pom.xml)

```xml
<properties>
    <kafka.version>4.0.0-SNAPSHOT</kafka.version>
</properties>

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-raft</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-server-common</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-storage</artifactId>
            <version>${kafka.version}</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

### 子模块pom.xml (kraft-raft/pom.xml)

```xml
<dependencies>
    <!-- Kafka Dependencies -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
    </dependency>

    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-raft</artifactId>
    </dependency>

    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-server-common</artifactId>
    </dependency>

    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-storage</artifactId>
    </dependency>
</dependencies>
```

## 如何编译kraft-learning

### 前提条件

1. **先编译Kafka主项目**（在 /home/user/kafka 目录）：
   ```bash
   cd /home/user/kafka
   ./gradlew clean build -x test publishToMavenLocal
   ```

   这会将Kafka的jar包安装到本地Maven仓库（~/.m2/repository）。

2. **然后编译kraft-learning**：
   ```bash
   cd /home/user/kafka/kraft-learning
   mvn clean compile
   ```

### 编译输出

kraft-learning项目会：
- 引用Kafka本地Maven仓库中的基础类
- 编译我们重新实现的KRaft核心逻辑
- 验证我们的实现与Kafka API的兼容性

## 学习方法

### 推荐的学习流程

1. **阅读kraft-learning中的实现**
   - 每个类都有详细的中文注释
   - 包含使用场景和示例代码
   - 解释设计决策和权衡

2. **对比Kafka源码**
   - kraft-learning: `/home/user/kafka/kraft-learning/kraft-raft/src/main/java/org/apache/kafka/raft/`
   - Kafka源码: `/home/user/kafka/raft/src/main/java/org/apache/kafka/raft/`

3. **运行测试**
   - 编写单元测试验证理解
   - 使用kraft-example模块创建实际示例

### 为什么不完全独立？

**选择依赖Kafka而不是重新实现所有类的原因：**

1. **聚焦核心**
   - 重点学习Raft协议的实现逻辑
   - 而不是重新实现配置框架、网络层、协议生成器等

2. **保持兼容**
   - 我们的实现使用Kafka相同的接口
   - 可以直接对比和测试

3. **实用性**
   - 可以在Kafka生态系统中使用
   - 理解真实的集成方式

4. **避免重复工作**
   - 协议类从schema自动生成（1000+行代码）
   - 配置框架、网络层是通用基础设施

## 依赖类的作用

### 基础数据结构类（来自kafka-clients）

```java
// 这些是Kafka的通用基础类
Node node = new Node(1, "localhost", 9092);
TopicPartition tp = new TopicPartition("__cluster_metadata", 0);
Uuid topicId = Uuid.randomUuid();
ListenerName listener = new ListenerName("PLAINTEXT");
```

### 协议消息类（来自kafka-raft）

```java
// 这些类由Kafka从JSON schema自动生成
VoteRequestData request = new VoteRequestData()
    .setClusterId(clusterId)
    .setTopics(topics);

// 我们的RaftUtil提供工厂方法简化创建
VoteRequestData request = RaftUtil.singletonVoteRequest(
    topicPartition, clusterId, epoch, ...
);
```

### 生成的Quorum状态类

```java
// QuorumStateData是从schema生成的
QuorumStateData data = new QuorumStateData()
    .setLeaderId(1)
    .setLeaderEpoch(5)
    .setVotedId(1);

// 我们的ElectionState封装了这个复杂的结构
ElectionState state = new ElectionState(epoch, votedId, leaderId, voters);
state.toQuorumStateData(version); // 转换为Kafka格式
```

## 常见问题

### Q: 为什么不把所有类都重新实现？

A: 本项目是**学习项目**，目标是：
- 深入理解KRaft的核心算法和实现
- 而不是重建整个Kafka
- 依赖Kafka基础设施可以让我们专注于Raft逻辑

### Q: 如何验证我们的实现正确？

A: 可以通过以下方式：
1. 单元测试（使用真实的Kafka协议类）
2. 与Kafka源码对比（相同的接口签名）
3. 编译检查（确保API兼容性）

### Q: 可以独立运行吗？

A: 可以部分独立运行：
- 阅读代码和注释：不需要编译
- 编译验证：需要Kafka依赖
- 运行测试：需要完整的Kafka基础设施

### Q: 如何处理版本不匹配？

A: 确保kafka.version与/home/user/kafka的Kafka版本一致：
```xml
<properties>
    <kafka.version>4.0.0-SNAPSHOT</kafka.version>
</properties>
```

## 总结

kraft-learning项目的价值在于：
- ✅ **详细的中文注释**：每个类都有深入的说明
- ✅ **核心逻辑实现**：重新实现了KRaft的关键类
- ✅ **学习示例**：包含丰富的使用场景和代码示例
- ✅ **设计讲解**：解释为什么这样设计，有什么权衡

依赖Kafka基础设施：
- ✅ **专注核心**：不被基础设施分散注意力
- ✅ **保持兼容**：使用真实的Kafka API
- ✅ **实用性强**：可以在真实环境中验证

这是一个理想的学习方法：**在理解核心的同时，利用现有的基础设施**。
