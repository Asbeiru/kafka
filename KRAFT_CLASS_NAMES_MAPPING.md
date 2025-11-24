# KRaft 类名对照表

> **重要**: 本项目所有类名将与 Apache Kafka KRaft 保持完全一致，方便参考源码

---

## 核心类名映射

### ✅ 完全一致的类名（直接使用）

| 类名 | Kafka 位置 | 用途 | 我们的包名 |
|------|-----------|------|-----------|
| `RaftClient` | org.apache.kafka.raft.RaftClient | 客户端接口 | io.github.miniraft.raft.api.RaftClient |
| `KafkaRaftClient` | org.apache.kafka.raft.KafkaRaftClient | 核心实现 | io.github.miniraft.raft.core.KafkaRaftClient |
| `QuorumState` | org.apache.kafka.raft.QuorumState | 状态管理器 | io.github.miniraft.raft.core.QuorumState |
| `EpochState` | org.apache.kafka.raft.EpochState | 状态接口 | io.github.miniraft.raft.core.state.EpochState |
| `LeaderState` | org.apache.kafka.raft.LeaderState | Leader 状态 | io.github.miniraft.raft.core.state.LeaderState |
| `FollowerState` | org.apache.kafka.raft.FollowerState | Follower 状态 | io.github.miniraft.raft.core.state.FollowerState |
| `CandidateState` | org.apache.kafka.raft.CandidateState | Candidate 状态 | io.github.miniraft.raft.core.state.CandidateState |
| `UnattachedState` | org.apache.kafka.raft.UnattachedState | Unattached 状态 | io.github.miniraft.raft.core.state.UnattachedState |
| `ProspectiveState` | org.apache.kafka.raft.ProspectiveState | Prospective 状态 | io.github.miniraft.raft.core.state.ProspectiveState |
| `ResignedState` | org.apache.kafka.raft.ResignedState | Resigned 状态 | io.github.miniraft.raft.core.state.ResignedState |
| `VotedState` | org.apache.kafka.raft.VotedState | Voted 状态 | io.github.miniraft.raft.core.state.VotedState |
| `ReplicatedLog` | org.apache.kafka.raft.ReplicatedLog | 日志接口 | io.github.miniraft.raft.storage.log.ReplicatedLog |
| `BatchAccumulator` | org.apache.kafka.raft.internals.BatchAccumulator | 批次累积器 | io.github.miniraft.raft.core.internals.BatchAccumulator |
| `BatchBuilder` | org.apache.kafka.raft.internals.BatchBuilder | 批次构建器 | io.github.miniraft.raft.core.internals.BatchBuilder |
| `NetworkChannel` | org.apache.kafka.raft.NetworkChannel | 网络接口 | io.github.miniraft.raft.network.NetworkChannel |
| `QuorumStateStore` | org.apache.kafka.raft.QuorumStateStore | 状态存储接口 | io.github.miniraft.raft.storage.state.QuorumStateStore |
| `FileQuorumStateStore` | org.apache.kafka.raft.FileQuorumStateStore | 文件存储实现 | io.github.miniraft.raft.storage.state.FileQuorumStateStore |
| `VoterSet` | org.apache.kafka.raft.VoterSet | 投票者集合 | io.github.miniraft.raft.core.VoterSet |
| `Endpoints` | org.apache.kafka.raft.Endpoints | 端点集合 | io.github.miniraft.raft.core.Endpoints |
| `ReplicaKey` | org.apache.kafka.raft.ReplicaKey | 副本键 | io.github.miniraft.raft.common.ReplicaKey |
| `OffsetAndEpoch` | org.apache.kafka.server.common.OffsetAndEpoch | 偏移量和纪元 | io.github.miniraft.raft.common.OffsetAndEpoch |

### 📝 内部类和辅助类

| 类名 | Kafka 位置 | 用途 | 我们的包名 |
|------|-----------|------|-----------|
| `BatchMemoryPool` | org.apache.kafka.raft.internals.BatchMemoryPool | 内存池 | io.github.miniraft.raft.core.internals.BatchMemoryPool |
| `RecordsIterator` | org.apache.kafka.raft.internals.RecordsIterator | 记录迭代器 | io.github.miniraft.raft.core.internals.RecordsIterator |
| `KRaftControlRecordStateMachine` | org.apache.kafka.raft.internals.KRaftControlRecordStateMachine | 控制记录状态机 | io.github.miniraft.raft.core.internals.KRaftControlRecordStateMachine |
| `VoterSetHistory` | org.apache.kafka.raft.internals.VoterSetHistory | 投票者历史 | io.github.miniraft.raft.core.internals.VoterSetHistory |
| `FuturePurgatory` | org.apache.kafka.raft.internals.FuturePurgatory | Future 管理 | io.github.miniraft.raft.core.internals.FuturePurgatory |
| `ThresholdPurgatory` | org.apache.kafka.raft.internals.ThresholdPurgatory | 阈值炼狱 | io.github.miniraft.raft.core.internals.ThresholdPurgatory |
| `KafkaRaftMetrics` | org.apache.kafka.raft.internals.KafkaRaftMetrics | 指标收集 | io.github.miniraft.raft.core.internals.KafkaRaftMetrics |

### 📦 快照相关类

| 类名 | Kafka 位置 | 用途 | 我们的包名 |
|------|-----------|------|-----------|
| `SnapshotWriter` | org.apache.kafka.snapshot.SnapshotWriter | 快照写入接口 | io.github.miniraft.raft.snapshot.SnapshotWriter |
| `SnapshotReader` | org.apache.kafka.snapshot.SnapshotReader | 快照读取接口 | io.github.miniraft.raft.snapshot.SnapshotReader |
| `RawSnapshotWriter` | org.apache.kafka.snapshot.RawSnapshotWriter | 原始快照写入 | io.github.miniraft.raft.snapshot.RawSnapshotWriter |
| `RawSnapshotReader` | org.apache.kafka.snapshot.RawSnapshotReader | 原始快照读取 | io.github.miniraft.raft.snapshot.RawSnapshotReader |
| `FileRawSnapshotWriter` | org.apache.kafka.snapshot.FileRawSnapshotWriter | 文件快照写入 | io.github.miniraft.raft.snapshot.FileRawSnapshotWriter |
| `FileRawSnapshotReader` | org.apache.kafka.snapshot.FileRawSnapshotReader | 文件快照读取 | io.github.miniraft.raft.snapshot.FileRawSnapshotReader |
| `RecordsSnapshotWriter` | org.apache.kafka.snapshot.RecordsSnapshotWriter | 记录快照写入 | io.github.miniraft.raft.snapshot.RecordsSnapshotWriter |
| `RecordsSnapshotReader` | org.apache.kafka.snapshot.RecordsSnapshotReader | 记录快照读取 | io.github.miniraft.raft.snapshot.RecordsSnapshotReader |
| `Snapshots` | org.apache.kafka.snapshot.Snapshots | 快照工具类 | io.github.miniraft.raft.snapshot.Snapshots |

### 🔧 我们需要实现但 Kafka 是 Scala 的类

| Kafka 类 (Scala) | 我们的类 (Java) | 说明 |
|-----------------|----------------|------|
| `KafkaMetadataLog` | `KafkaMetadataLog` | 日志实现（我们用Java重写） |
| `KafkaRaftManager` | `KafkaRaftManager` | Raft 管理器（我们用Java重写） |

### ⚠️ 需要调整的类名

| 我之前的命名 | 正确的 Kafka 命名 | 说明 |
|-------------|------------------|------|
| ~~`RaftEngine`~~ | `KafkaRaftClient` | 核心实现类 |
| ~~`State`~~ | `EpochState` | 状态接口 |
| ~~`GrpcNetworkChannel`~~ | `GrpcNetworkChannel` | 我们的gRPC实现（保留，类似Kafka的KafkaNetworkChannel） |

### 🆕 我们新增的类（Kafka 中不存在）

| 类名 | 用途 | 说明 |
|------|------|------|
| `GrpcNetworkChannel` | gRPC 网络实现 | 类似 Kafka 的 KafkaNetworkChannel，但用 gRPC |
| `RaftServiceImpl` | gRPC 服务实现 | 处理 gRPC 请求 |

---

## 包结构对应关系

### Kafka KRaft 包结构
```
org.apache.kafka.raft/
├── KafkaRaftClient.java
├── QuorumState.java
├── LeaderState.java
├── FollowerState.java
├── ...State.java
├── ReplicatedLog.java
├── VoterSet.java
├── NetworkChannel.java
├── internals/
│   ├── BatchAccumulator.java
│   ├── BatchBuilder.java
│   ├── KafkaRaftMetrics.java
│   └── ...
org.apache.kafka.snapshot/
├── SnapshotWriter.java
├── SnapshotReader.java
└── ...
```

### 我们的包结构（保持对应关系）
```
io.github.miniraft.raft/
├── api/
│   └── RaftClient.java
├── core/
│   ├── KafkaRaftClient.java          ← 对应 Kafka
│   ├── QuorumState.java              ← 对应 Kafka
│   ├── VoterSet.java                 ← 对应 Kafka
│   ├── Endpoints.java                ← 对应 Kafka
│   ├── state/
│   │   ├── EpochState.java           ← 对应 Kafka
│   │   ├── LeaderState.java          ← 对应 Kafka
│   │   ├── FollowerState.java        ← 对应 Kafka
│   │   └── ...State.java             ← 对应 Kafka
│   └── internals/
│       ├── BatchAccumulator.java     ← 对应 Kafka
│       ├── BatchBuilder.java         ← 对应 Kafka
│       ├── KafkaRaftMetrics.java     ← 对应 Kafka
│       └── ...
├── storage/
│   ├── log/
│   │   ├── ReplicatedLog.java        ← 对应 Kafka
│   │   └── KafkaMetadataLog.java     ← 对应 Kafka (我们用Java)
│   └── state/
│       ├── QuorumStateStore.java     ← 对应 Kafka
│       └── FileQuorumStateStore.java ← 对应 Kafka
├── network/
│   ├── NetworkChannel.java           ← 对应 Kafka
│   └── GrpcNetworkChannel.java       ← 我们的实现
└── snapshot/
    ├── SnapshotWriter.java           ← 对应 Kafka
    ├── SnapshotReader.java           ← 对应 Kafka
    ├── RawSnapshotWriter.java        ← 对应 Kafka
    └── ...                           ← 对应 Kafka
```

---

## 使用指南

### 参考 Kafka 源码时

当你在我们的代码中看到某个类，比如 `KafkaRaftClient`，可以直接在 Kafka 源码中搜索同名类：

```bash
# 在 Kafka 源码中查找
find . -name "KafkaRaftClient.java"
# 结果: ./raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java
```

### IDE 中对比学习

```java
// 我们的代码
package io.github.miniraft.raft.core;

public class KafkaRaftClient<T> implements RaftClient<T> {
    private final QuorumState quorumState;
    private final ReplicatedLog log;
    // ...
}

// Kafka 源码（完全相同的结构）
package org.apache.kafka.raft;

public final class KafkaRaftClient<T> implements RaftClient<T> {
    private final QuorumState quorumState;
    private final ReplicatedLog log;
    // ...
}
```

---

## 总结

- ✅ **核心类名**: 100% 与 Kafka 保持一致
- ✅ **包结构**: 对应关系清晰
- ✅ **接口定义**: 完全相同
- ✅ **内部类**: 命名一致
- ✅ **工具类**: 命名一致

**唯一的区别**:
- 我们用 `GrpcNetworkChannel` 代替 Kafka 的 `KafkaNetworkChannel`（因为底层实现不同）
- 我们的包名是 `io.github.miniraft.raft.*` 而不是 `org.apache.kafka.raft.*`

这样设计的好处：
1. 📖 **容易对照**: 直接搜索同名类
2. 🔍 **便于调试**: 堆栈跟踪更直观
3. 📚 **方便学习**: 文档和代码可以直接引用 Kafka
4. 🤝 **团队协作**: 其他开发者更容易理解
