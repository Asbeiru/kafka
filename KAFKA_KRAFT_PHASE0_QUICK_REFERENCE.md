# Phase 0 快速参考表

## 实现顺序和优先级

### Wave 1: 最基础的数据结构 (1-2 天)
```
依赖: 无或仅Java标准库

1. OffsetMetadata.java (21行)
   - 标记接口，无实现需要

2. ReplicaKey.java (82行)
   - public int id()
   - public Optional<Uuid> directoryId()
   - public int compareTo(ReplicaKey)
   - 需要equals、hashCode、toString
   - 包含工厂方法 of(int, Uuid)

3. LogOffsetMetadata.java (66行)
   - private final long offset
   - private final Optional<OffsetMetadata> metadata
   - 简单的getter和equals/hashCode

4. LeaderAndEpoch.java (32行)
   - Java record类型
   - public record LeaderAndEpoch(OptionalInt leaderId, int epoch)
   - 静态常量 UNKNOWN

5. ValidOffsetAndEpoch.java (82行)
   - enum Kind { DIVERGING, SNAPSHOT, VALID }
   - 多个工厂方法 diverging()、snapshot()、valid()

6. RaftException.java (39行)
   - extends KafkaException
   - 多个构造函数（String、String+Throwable、Throwable）
```

### Wave 2: RPC 框架基础 (2-3 天)
```
依赖: Wave 1, Kafka Common

1. RaftMessage.java (25行) - 接口
   - int correlationId()
   - ApiMessage data()

2. RaftRequest.java (115行)
   - abstract extends RaftMessage
   - 内部类：Inbound、Outbound
   - 字段：correlationId、data、createdTimeMs
   - Outbound额外字段：destination Node、CompletableFuture

3. RaftResponse.java (78行)
   - abstract extends RaftMessage
   - 内部类：Inbound、Outbound
   - Inbound额外字段：source Node

4. NetworkChannel.java (47行) - 接口
   - int newCorrelationId()
   - void send(RaftRequest.Outbound)
   - ListenerName listenerName()
   - void close() throws InterruptedException

5. RaftMessageQueue.java (58行) - 接口
   - RaftMessage poll(long timeoutMs)
   - void add(RaftMessage)
   - boolean isEmpty()
   - void wakeup()

6. BlockingMessageQueue.java (76行)
   - implements RaftMessageQueue
   - LinkedBlockingQueue<RaftMessage> queue
   - AtomicInteger size
   - 特殊的WAKEUP_MESSAGE marker
```

### Wave 3: 网络和端点管理 (1-2 天)
```
依赖: Wave 1-2, Kafka Common

1. Endpoints.java (302行)
   - Map<ListenerName, InetSocketAddress> endpoints
   - Optional<InetSocketAddress> address(ListenerName)
   - 多个from*()工厂方法
   - 多个to*()转换方法（到各种RPC格式）
   - 静态方法 empty()

2. KafkaNetworkChannel.java (209行)
   - implements NetworkChannel
   - static class SendThread extends InterBrokerSendThread
   - AtomicInteger correlationIdCounter
   - ListenerName listenerName
   - KafkaClient client
```

### Wave 4: 状态管理基础 (2-3 天)
```
依赖: Wave 1-3, Kafka Common

1. ElectionState.java (217行)
   - int epoch
   - OptionalInt leaderId
   - Optional<ReplicaKey> votedKey
   - Set<Integer> voters (已弃用)
   - 工厂方法：withVotedCandidate、withElectedLeader、withUnknownLeader
   - 转换：toQuorumStateData、fromQuorumStateData

2. EpochState.java (69行) - 接口
   - Optional<LogOffsetMetadata> highWatermark()
   - boolean canGrantVote(ReplicaKey, boolean, boolean)
   - ElectionState election()
   - int epoch()
   - Endpoints leaderEndpoints()
   - String name()
   - void close()

3. QuorumState.java (934行) - 最复杂
   - OptionalInt localId
   - Uuid localDirectoryId
   - Time time
   - QuorumStateStore store
   - KRaftControlRecordStateMachine partitionState
   - Endpoints localListeners
   - volatile EpochState state
   - void initialize(OffsetAndEpoch)
   - ElectionState readElectionState()
   - 多个grant和transition方法
```

### Wave 5: 支持服务 (2-3 天)
```
依赖: Wave 1-4, Kafka Common

1. RequestManager.java (384行)
   - Map<String, ConnectionState> connections
   - ArrayList<Node> bootstrapServers
   - int retryBackoffMs, requestTimeoutMs
   - Random random
   - boolean hasAnyInflightRequest(long)
   - Optional<Node> findReadyBootstrapServer(long)
   - long backoffBeforeAvailableBootstrapServer(long)
   - void onRequestSent/onResponseResult

2. QuorumConfig.java (325行)
   - 静态配置常量定义
   - extends AbstractConfig
   - 解析和验证配置值

3. ExpirationService.java (32行) - 接口
   - <T> CompletableFuture<T> failAfter(long)

4. TimingWheelExpirationService.java (80行)
   - implements ExpirationService
   - Timer timer
   - ExpiredOperationReaper (ShutdownableThread)
   - void shutdown()

5. BatchMemoryPool.java (148行)
   - implements MemoryPool (Kafka Common)
   - ReentrantLock lock
   - Deque<ByteBuffer> free
   - int maxRetainedBatches, batchSize
   - ByteBuffer tryAllocate(int)
   - void release(ByteBuffer)
   - void releaseRetained()
```

### Wave 6: 数据容器和指标 (1-2 天)
```
依赖: Wave 1-5, Kafka Common

1. Batch.java (224行)
   - 泛型类 Batch<T>
   - long baseOffset, epoch, lastOffset
   - long appendTimestamp
   - int sizeInBytes
   - List<T> records
   - List<ControlRecord> controlRecords
   - 工厂方法：data()、control()

2. VoterSet.java (518行)
   - Map<Integer, VoterNode> voters
   - Set<Node> voterNodes(Stream<Integer>, ListenerName)
   - boolean isVoter(ReplicaKey)
   - boolean isOnlyVoter(ReplicaKey)
   - Set<Integer> voterIds()
   - Set<ReplicaKey> voterKeys()
   - 多个from*/to*转换方法

3. KafkaRaftMetrics.java (289行)
   - Metrics metrics
   - 多个MetricName字段
   - Sensor commitTimeSensor, electionTimeSensor等
   - void initialize(QuorumState)

4. ExternalKRaftMetrics.java (26行) - 接口
   - void setIgnoredStaticVoters(boolean)
```

## 关键依赖关系

```
最低层 (0级):
  Java标准库 (Optional, Set, Map, List等)
  Kafka Common (Uuid, Node, ListenerName等)

→ 层级1数据结构
  ReplicaKey, OffsetMetadata, LogOffsetMetadata, LeaderAndEpoch等

→ 层级2RPC框架
  RaftMessage, RaftRequest, RaftResponse
  NetworkChannel, RaftMessageQueue

→ 层级3网络管理
  Endpoints, KafkaNetworkChannel, RequestManager

→ 层级4状态管理
  ElectionState, EpochState, QuorumState

→ 层级5支持服务
  ExpirationService, TimingWheelExpirationService
  BatchMemoryPool, QuorumConfig, VoterSet

→ 层级6高级特性
  KafkaRaftMetrics, ExternalKRaftMetrics
```

## 单元测试关键场景

### ReplicaKey Tests
```
- testReplicaKeyEquality
- testReplicaKeyComparable
- testReplicaKeyHashCode
- testReplicaKeyWithoutDirectoryId
- testReplicaKeyWithDirectoryId
```

### ElectionState Tests
```
- testElectionStateWithVotedCandidate
- testElectionStateWithElectedLeader
- testElectionStateWithUnknownLeader
- testElectionStateSerializeDeserialize
- testElectionStateIsLeader
- testElectionStateIsVotedCandidate
```

### QuorumState Tests
```
- testQuorumStateInitialization
- testQuorumStateStateTransitions
- testQuorumStateCanGrantVote
- testQuorumStateVoteRestrictions
- testQuorumStateHighWatermark
- testQuorumStateElectionStateStorage
```

### RequestManager Tests
```
- testRequestManagerConnectionTracking
- testRequestManagerBackoff
- testRequestManagerFindReadyServer
- testRequestManagerTimeoutHandling
```

### Endpoints Tests
```
- testEndpointsFromMultipleListeners
- testEndpointsConversionToRPCFormats
- testEndpointsEmpty
```

## 文件清单（总计约4500行）

| 类别 | 文件 | 行数 | 优先级 |
|------|------|------|--------|
| Wave 1 | 6个文件 | ~293 | P0 |
| Wave 2 | 6个文件 | ~409 | P1 |
| Wave 3 | 2个文件 | ~511 | P1 |
| Wave 4 | 3个文件 | ~1,220 | P2 |
| Wave 5 | 5个文件 | ~1,121 | P2 |
| Wave 6 | 4个文件 | ~949 | P3 |

## 常见陷阱

1. ReplicaKey directoryId 可以为 empty - 需要处理两种情况
2. Endpoints 需要支持多种监听器转换 - 工厂方法很多
3. QuorumState 需要持久化选举状态 - 与 QuorumStateStore 交互
4. ElectionState 向前向后兼容性 - version 0 和 version 1 格式不同
5. NetworkChannel 和 KafkaNetworkChannel 需要管理关联ID - 原子递增
6. RaftRequest/Response 有两种形式 - Inbound/Outbound 字段不同
7. TimingWheelExpirationService 需要后台线程管理时间轮 - 需要正确关闭

## 快速查询

**需要 Comparable?** → ReplicaKey
**需要 Optional?** → Endpoints, LogOffsetMetadata, ElectionState, LeaderAndEpoch
**需要同步?** → QuorumState, RequestManager, BatchMemoryPool
**需要工厂方法?** → ReplicaKey, Endpoints, ElectionState, ValidOffsetAndEpoch, Batch
**需要后台线程?** → TimingWheelExpirationService, KafkaNetworkChannel
**需要转换方法?** → Endpoints, ElectionState, VoterSet
**需要 Volatile?** → QuorumState.state, KafkaRaftMetrics各个字段
**需要接口?** → NetworkChannel, EpochState, ExpirationService, RaftMessageQueue, ExternalKRaftMetrics, OffsetMetadata, RaftMessage

