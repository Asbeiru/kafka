# Kafka KRaft Phase 1 - 选举流程 完整详细梳理

## 目录

1. [执行摘要](#执行摘要)
2. [Phase 1 完整类清单](#phase-1-完整类清单)
3. [类之间的关系](#类之间的关系)
4. [选举流程详解](#选举流程详解)
5. [关键概念理解](#关键概念理解)
6. [实现顺序和优先级](#实现顺序和优先级)
7. [代码框架示例](#代码框架示例)

---

## 执行摘要

### Phase 1 的定位

**Phase 1 = 完整的 Leader 选举实现**，包括：
- ✅ PreVote 机制（安全选举）
- ✅ 正式投票（获取 Leader 权力）
- ✅ BeginQuorumEpoch（宣告 epoch）
- ✅ EndQuorumEpoch（优雅辞职）
- ✅ 状态转换（6 种状态）
- ✅ 超时管理（选举超时、fetch 超时）

### 核心数字

| 指标 | 数值 |
|------|------|
| 总共类数 | 20+ 个 |
| RPC 消息对 | 3 对（Vote、BeginQuorumEpoch、EndQuorumEpoch） |
| 状态类 | 8 种（EpochState、6个具体状态、NomineeState） |
| 选举管理类 | 3 个（ElectionState、EpochElection、QuorumState） |
| 核心处理类 | 2 个（KafkaRaftClient、RaftUtil） |
| 预计代码行数 | 4,000-5,000 行 |
| 预计时间 | 3-5 周（包括测试） |

### 关键流程时间

```
从 Leader 失联到新 Leader 稳定：2-5 秒

T+0s   : Leader 失联
T+0-1.5s: Follower 检测超时
T+1-2s : PreVote 阶段（探测）
T+2-3s : 正式投票（Candidate 增加 epoch）
T+3-4s : BeginQuorumEpoch 广播（新 Leader 确认）
T+4-5s : 新 Leader 稳定运行
```

---

## Phase 1 完整类清单

### 分类 1：核心状态类（8 个）

#### 1. EpochState（接口，最重要）

**职责**：
- 定义所有 Raft 状态的通用接口
- 所有 6 种状态都实现此接口

**关键方法**：
```java
public interface EpochState {
    int epoch();                           // 当前 epoch
    Optional<Integer> leaderId();          // 已知的 Leader ID
    Optional<Endpoints> leaderEndpoints(); // Leader 端点

    boolean canGrantVote(
        ReplicaKey candidate,
        boolean isLogUpToDate,
        boolean isPreVote
    );                                     // 是否可以投票

    long remainingElectionTimeMs(long currentTimeMs);  // 剩余选举时间
}
```

**实现难度**：⭐ （纯接口定义）

---

#### 2. NomineeState（接口，可选）

**职责**：
- 为竞选者状态（Prospective 和 Candidate）定义通用接口
- 扩展 EpochState

**关键方法**：
```java
public interface NomineeState extends EpochState {
    void recordGrantedVote(ReplicaKey voter);   // 记录投票
    void recordRejectedVote(ReplicaKey voter);  // 记录拒绝

    boolean hasElectionTimeoutExpired(long currentTimeMs);  // 选举超时

    EpochElection epochElection();  // 获取投票统计
}
```

**实现难度**：⭐

---

#### 3. UnattachedState（最基础的具体状态）

**职责**：
- 表示节点未附加到任何 Leader
- 不知道 Leader 的网络端点
- 可能知道 Leader ID（但不知道端点）
- 可能已投票（votedKey）

**关键字段**：
```java
private final int epoch;
private final Optional<Integer> leaderId;
private final Optional<ReplicaKey> votedKey;
private final Timer electionTimer;
private final long randomElectionTimeoutMs;
```

**关键行为**：
- 选举超时 → 转为 Prospective（开始 PreVote）
- 收到更高 epoch → 保持 Unattached（新 epoch）
- 发现 Leader 端点 → 转为 Follower

**实现难度**：⭐⭐

---

#### 4. ProspectiveState（PreVote 阶段）

**职责**：
- 实现 PreVote 机制
- 不增加 epoch
- 发送 VoteRequest(PreVote=true)
- 收集投票响应

**关键字段**：
```java
private final int epoch;
private final EpochElection epochElection;  // 跟踪 PreVote 投票
private final Timer electionTimer;
private final long randomElectionTimeoutMs;
```

**关键行为**：
- 自动为自己投 PreVote 票
- 发送 PreVote 请求到所有投票者
- PreVote 成功（多数派）→ 转为 Candidate
- PreVote 失败 → 等待超时后转为 Unattached/Follower

**实现难度**：⭐⭐⭐

---

#### 5. CandidateState（正式投票阶段）

**职责**：
- 正式竞选 Leader
- epoch 已增加
- 发送 VoteRequest(PreVote=false)
- 收集投票响应

**关键字段**：
```java
private final int epoch;
private final EpochElection epochElection;  // 跟踪正式投票
private final Timer electionTimer;
private final long randomElectionTimeoutMs;
```

**关键行为**：
- epoch 比 ProspectiveState 的 epoch 多 1
- 自动为自己投正式票
- 发送正式 Vote 请求
- 正式投票成功（多数派）→ 转为 Leader
- 正式投票失败 → 等待超时后转为 ProspectiveState

**实现难度**：⭐⭐⭐

---

#### 6. FollowerState（跟随者状态）

**职责**：
- 跟随 Leader
- 发送 Fetch 请求拉取日志
- 检测 Leader 失联（Fetch 超时）

**关键字段**：
```java
private final ReplicaKey leaderId;
private final Optional<Endpoints> leaderEndpoints;
private final Timer fetchTimer;
private boolean hasFetchedFromLeader;  // 是否成功 Fetch 过一次
private Optional<OffsetAndEpoch> fetchingSnapshot;  // 正在拉取的快照
```

**关键行为**：
- 持续发送 Fetch 请求
- 收到成功响应 → 重置 fetchTimer
- fetchTimer 超时 → 转为 ProspectiveState（或 Unattached）
- 收到 BeginQuorumEpoch → 更新 Leader 端点

**实现难度**：⭐⭐（虽然逻辑简单，但涉及网络通信）

---

#### 7. LeaderState（领导者状态）

**职责**：
- 管理日志复制
- 发送 BeginQuorumEpoch 确认 epoch
- 计算高水位（highWatermark）
- 检测是否失去多数派（Check Quorum）

**关键字段**：
```java
private final int epoch;
private final long epochStartOffset;  // 此 epoch 的日志起始位置
private final Map<ReplicaKey, ReplicaState> voterStates;  // 所有投票者的状态
private final Map<ReplicaKey, ReplicaState> observerStates;  // 观察者状态
private long highWatermark;
private final Timer checkQuorumTimer;
private final Timer beginQuorumEpochTimer;
```

**关键行为**：
- 发送 BeginQuorumEpoch 到所有投票者
- 跟踪每个投票者的 Fetch 进度
- 计算多数派已复制的最小 offset = highWatermark
- Check Quorum：如果 checkQuorum 超时（未收到多数派 fetch）→ 转为 ResignedState

**实现难度**：⭐⭐⭐⭐（最复杂，需要理解高水位计算）

---

#### 8. ResignedState（已辞职状态）

**职责**：
- Leader 优雅辞职
- 发送 EndQuorumEpoch 到所有投票者
- 通知首选继任者
- 等待确认或超时

**关键字段**：
```java
private final int epoch;
private final ReplicaKey leaderId;
private final List<ReplicaKey> preferredSuccessors;  // 首选继任者（按优先级）
private final Set<ReplicaKey> unackedVoters;  // 未确认的投票者
private final Timer electionTimer;
```

**关键行为**：
- 发送 EndQuorumEpoch(preferredSuccessors=[...])
- preferredSuccessors 按日志新旧排序（新的优先）
- 等待投票者确认或选举超时
- 选举超时 → 转为 Unattached

**实现难度**：⭐⭐⭐

---

### 分类 2：选举管理类（3 个）

#### 9. ElectionState（选举状态持久化）

**职责**：
- 存储持久化的选举状态
- 包含：epoch、leaderId、votedKey、voters

**关键字段**：
```java
private final int epoch;                    // 当前 epoch
private final Optional<Integer> leaderId;   // 已知的 Leader ID
private final Optional<ReplicaKey> votedKey; // 投票给谁（正式投票时）
private final List<ReplicaKey> voters;      // 当前选民列表
```

**关键方法**：
```java
public boolean isLeader(int nodeId) { }
public boolean isVotedCandidate(ReplicaKey candidate) { }
public boolean hasVoted() { }
public ElectionState withVotedCandidate(int epoch, ReplicaKey candidate) { }
public ElectionState withElectedLeader(int epoch, int leaderId) { }
public ElectionState withUnknownLeader(int epoch) { }

// 序列化/反序列化
public QuorumStateData toQuorumStateData() { }
public static ElectionState fromQuorumStateData(QuorumStateData data) { }
```

**实现难度**：⭐⭐

---

#### 10. EpochElection（单次选举投票统计）

**职责**：
- 跟踪单次选举中的投票结果
- 维护每个投票者的投票状态

**关键字段**：
```java
private final VoterSet voterSet;
private final Map<Integer, EpochElectionState> voterStates;  // UNRECORDED/GRANTED/REJECTED
private final Logger log;

enum EpochElectionState {
    UNRECORDED,  // 未收到响应
    GRANTED,     // 授予投票
    REJECTED     // 拒绝投票
}
```

**关键方法**：
```java
public void recordVote(int voterId, boolean granted) { }
public boolean isVoteGranted() { }   // 多数派授予？
public boolean isVoteRejected() { }  // 多数派拒绝？
public int majoritySize() { }
```

**实现难度**：⭐⭐

---

#### 11. QuorumState（状态机管理器）

**职责**：
- 管理所有状态转换
- 维护当前状态和选举状态
- 判断投票决策

**关键字段**：
```java
private final Optional<Integer> localId;
private final String localDirectoryId;
private volatile EpochState state;  // 当前状态实例
private final QuorumStateStore store;  // 持久化存储
private ElectionState election;  // 选举状态（持久化的）
private final VoterSet voters;  // 当前选民集合
private LogOffsetMetadata logEndOffsetMetadata;  // 日志末端
private long highWatermark;  // 高水位标记
```

**关键方法**：
```java
// 查询方法
public int epoch() { }
public boolean isLeader() { }
public boolean isFollower() { }
public boolean isCandidate() { }
public Optional<Integer> leaderId() { }

// 投票决策（核心！）
public boolean canGrantVote(
    ReplicaKey candidate,
    boolean isLogUpToDate,
    boolean isPreVote
) { }

// 状态转换
public void transitionToLeader(long epochStartOffset) { }
public void transitionToFollower(ReplicaKey leaderId, int epoch, Endpoints endpoints) { }
public void transitionToCandidate(long currentTimeMs) { }
public void transitionToProspective(long currentTimeMs) { }
public void transitionToUnattached(int epoch, Optional<Integer> leaderId) { }
public void transitionToResigned() { }

// 持久化
public ElectionState election() { }
public void durableTransitionTo(EpochState newState) { }
```

**实现难度**：⭐⭐⭐⭐⭐ （最复杂，核心逻辑）

---

### 分类 3：RPC 消息类（3 对，6 个）

#### 12-13. VoteRequest / VoteResponse

**VoteRequest 字段**：
```
epoch: int32              // 候选人的 epoch（PreVote 时不增加）
candidateId: int32        // 候选人 ID
candidateDirectoryId: uuid // 候选人 directory ID
lastLogOffset: int64      // 候选人日志末端 offset
lastLogEpoch: int32       // 候选人日志末端 epoch
preVote: bool             // 是否为 PreVote（V2+）
voterDirectoryId: uuid    // 接收者 directory ID
```

**VoteResponse 字段**：
```
errorCode: int16          // 错误码
leaderId: int32           // 响应者知道的 Leader ID
leaderEpoch: int32        // Leader 的 epoch
voteGranted: bool         // 是否授予投票
```

**实现难度**：⭐ (RPC 自动生成，只需理解字段含义)

---

#### 14-15. BeginQuorumEpochRequest / BeginQuorumEpochResponse

**BeginQuorumEpochRequest 字段**：
```
epoch: int32              // 新的 epoch
leaderId: int32           // 新的 Leader ID
voterDirectoryId: uuid    // 接收者 directory ID
leaderEndpoints: []       // Leader 的网络端点
```

**BeginQuorumEpochResponse 字段**：
```
errorCode: int16          // 错误码
epoch: int32              // 响应者的 epoch（成功时应等于请求 epoch）
```

**实现难度**：⭐

---

#### 16-17. EndQuorumEpochRequest / EndQuorumEpochResponse

**EndQuorumEpochRequest 字段**：
```
epoch: int32              // 当前 epoch
leaderId: int32           // Leader ID
voterDirectoryId: uuid    // 接收者 directory ID
preferredSuccessors: []   // 首选继任者列表（按优先级，最新的优先）
leaderEndpoints: []       // Leader 端点
```

**EndQuorumEpochResponse 字段**：
```
errorCode: int16
epoch: int32
```

**实现难度**：⭐

---

### 分类 4：核心处理类（2 个）

#### 18. KafkaRaftClient（主协调器）

**职责**：
- 接收 RPC 请求并调用相应处理方法
- 管理状态转换
- 驱动选举、日志复制等流程
- 定期 poll()，发送和接收消息

**关键方法**：
```java
// RPC 处理
private BeginQuorumEpochResponseData handleVoteRequest(RaftRequest.Inbound request) { }
private BeginQuorumEpochResponseData handleVoteResponse(RaftResponse.Inbound response) { }
private BeginQuorumEpochResponseData handleBeginQuorumEpochRequest(RaftRequest.Inbound request) { }
private BeginQuorumEpochResponseData handleBeginQuorumEpochResponse(RaftResponse.Inbound response) { }
private BeginQuorumEpochResponseData handleEndQuorumEpochRequest(RaftRequest.Inbound request) { }
private BeginQuorumEpochResponseData handleEndQuorumEpochResponse(RaftResponse.Inbound response) { }

// 状态 poll（定期调用）
private void pollUnattached(long currentTimeMs) { }
private void pollProspective(long currentTimeMs) { }
private void pollCandidate(long currentTimeMs) { }
private void pollFollower(long currentTimeMs) { }
private void pollLeader(long currentTimeMs) { }
private void pollResigned(long currentTimeMs) { }

// 转换辅助
private void onBecomeLeader(long currentTimeMs) { }
private void onBecomeFollower() { }
private void maybeTransition(int leaderId, int epoch, Endpoints endpoints, long currentTimeMs) { }
```

**实现难度**：⭐⭐⭐⭐

---

#### 19. RaftUtil（工具类）

**职责**：
- 构建 RPC 消息
- 解析 RPC 响应
- 日志比较工具

**关键方法**：
```java
public static VoteRequestData buildVoteRequest(int epoch, int candidateId, String directoryId,
                                                long lastOffset, int lastEpoch, boolean preVote) { }
public static VoteResponseData buildVoteResponse(int epoch, int leaderId, boolean granted) { }
public static BeginQuorumEpochRequestData buildBeginQuorumEpochRequest(int epoch, int leaderId,
                                                                       List<Endpoint> endpoints) { }
// ... 更多工具方法
```

**实现难度**：⭐

---

### 分类 5：支持类（若干）

20. **ReplicaKey** - 副本唯一标识 (ID + DirectoryID) [来自 Phase 0]
21. **Endpoints** - 网络端点集合 [来自 Phase 0]
22. **LogOffsetMetadata** - 日志偏移量元数据 [来自 Phase 0]
23. **OffsetAndEpoch** - (offset, epoch) 对比较
24. **VoterSet** - 投票者集合 [来自 Phase 0]
25. **QuorumStateStore** / **FileQuorumStateStore** - 状态持久化
26. **Timer** - 超时管理 [来自 Phase 0]
27. **ReplicaState** - 单个副本的选举状态（投票记录）

---

## 类之间的关系

### 完整的依赖关系

```
KafkaRaftClient (主协调器)
    ├─ QuorumState (状态管理器)
    │   ├─ EpochState (当前状态接口)
    │   │   ├─ UnattachedState
    │   │   ├─ ProspectiveState ──→ NomineeState
    │   │   ├─ CandidateState ──→ NomineeState
    │   │   ├─ FollowerState
    │   │   ├─ LeaderState
    │   │   └─ ResignedState
    │   ├─ ElectionState (持久化)
    │   ├─ VoterSet (选民管理)
    │   ├─ QuorumStateStore (存储)
    │   └─ LogOffsetMetadata (日志元数据)
    │
    ├─ RPC 处理
    │   ├─ VoteRequest/Response
    │   ├─ BeginQuorumEpochRequest/Response
    │   └─ EndQuorumEpochRequest/Response
    │
    ├─ EpochElection (投票统计)
    │
    └─ RaftUtil (工具类)
```

### 关键依赖路径

**路径 1：选举流程**
```
KafkaRaftClient
  → handleVoteRequest()
    → QuorumState.canGrantVote()
      → [检查 epoch、votedKey、日志]
    → 返回 VoteResponse
```

**路径 2：状态转换**
```
KafkaRaftClient
  → pollProspective()
    → ProspectiveState.hasElectionTimeoutExpired()
    → QuorumState.transitionToCandidate()
      → CandidateState 创建
      → EpochElection 创建
      → 发送 VoteRequest
```

**路径 3：成为 Leader**
```
KafkaRaftClient
  → handleVoteResponse()
    → EpochElection.recordVote()
    → EpochElection.isVoteGranted() → true（多数派）
    → QuorumState.transitionToLeader()
      → LeaderState 创建
      → 写入 LeaderChangeMessage
      → 发送 BeginQuorumEpoch
```

---

## 选举流程详解

### 1. 完整的选举时间线

```
T0: 初始稳定状态
    ├─ Leader: Leader(epoch=5)
    ├─ Follower1: Follower(epoch=5, leader=Leader)
    └─ Follower2: Follower(epoch=5, leader=Leader)

T1: Leader 失联（网络故障）
    ├─ Leader: 离线
    ├─ Follower1: 继续 fetch（会失败）
    └─ Follower2: 继续 fetch（会失败）

T2: Follower 检测超时（~1.5s）
    ├─ Follower1: fetchTimer 超时
    │   └─ Follower → Unattached(epoch=5)
    │   └─ 启动 electionTimer
    │
    └─ Follower2: fetchTimer 超时
        └─ Follower → Unattached(epoch=5)
        └─ 启动 electionTimer

T3: 选举超时，Follower1 先超时（假设 Follower1 先）
    └─ Unattached → Prospective(epoch=5)
       ├─ 创建 EpochElection
       ├─ 自己投自己一票（PreVote）
       ├─ 发送 VoteRequest(PreVote=true, epoch=5) 到 Follower2
       └─ 重置 electionTimer

T4: Follower2 收到 PreVote 请求
    ├─ 检查日志新旧度：Follower1 的日志 >= Follower2 的日志？Yes
    ├─ 状态：Unattached，未知 Leader
    ├─ 决策：canGrantVote() = true
    ├─ 发送 VoteResponse(voteGranted=true)
    └─ 保持 Unattached（PreVote 不持久化）

T5: Follower1 收到 PreVote 响应
    ├─ recordGrantedVote(Follower2)
    ├─ epochElection.isVoteGranted()? Yes (2/2 多数派)
    ├─ Prospective → Candidate(epoch=6)
    ├─ epoch += 1（关键！）
    ├─ 持久化 votedKey=Follower1, epoch=6
    ├─ 创建新 EpochElection
    ├─ 自己投自己一票（正式投票）
    ├─ 发送 VoteRequest(PreVote=false, epoch=6) 到 Follower2
    └─ 重置 electionTimer

T6: Follower2 收到正式投票请求
    ├─ 检查：epoch=6 > 本地 epoch=5
    ├─ 状态：Unattached(epoch=5) → Unattached(epoch=6)
    ├─ 清除 votedKey（新 epoch）
    ├─ 检查日志新旧度：Yes
    ├─ 检查是否已投票：No
    ├─ 决策：canGrantVote() = true
    ├─ 持久化 votedKey=Follower1, epoch=6
    ├─ 发送 VoteResponse(voteGranted=true)
    └─ 状态：Unattached → Unattached（重置计时器）

T7: Follower1 收到正式投票响应
    ├─ recordGrantedVote(Follower2)
    ├─ epochElection.isVoteGranted()? Yes (2/2 多数派)
    ├─ Candidate → Leader(epoch=6)
    ├─ 创建 LeaderState
    ├─ epochStartOffset = log.endOffset()
    ├─ 写入 LeaderChangeMessage(epoch=6, leader=Follower1, voters=[1,2], grantingVoters=[1,2])
    ├─ 初始化 voterStates（Follower2 的状态）
    ├─ Follower2 状态: hasAcknowledgedLeader=false（未确认 BeginQuorumEpoch）
    ├─ 发送 BeginQuorumEpochRequest(epoch=6, leaderId=Follower1, endpoints=[...])
    └─ 重置 beginQuorumEpochTimer

T8: Follower2 收到 BeginQuorumEpoch
    ├─ 检查 epoch=6 = 本地 epoch=6
    ├─ 状态：Unattached → Follower(epoch=6, leader=Follower1)
    ├─ 保存 Leader 端点
    ├─ 发送 BeginQuorumEpochResponse(success)
    └─ 重置 fetchTimer

T9: Follower1 收到 BeginQuorumEpoch 响应
    ├─ voterStates[Follower2].hasAcknowledgedLeader = true
    └─ 选举完成！

T10+: 稳定运行
    ├─ Follower1: Leader(epoch=6)
    │   ├─ 定期发送心跳（空 Fetch 响应）
    │   ├─ 计算高水位
    │   └─ 管理日志复制
    │
    └─ Follower2: Follower(epoch=6, leader=Follower1)
        ├─ 定期发送 Fetch 请求
        ├─ 接收日志
        └─ 更新高水位
```

### 2. PreVote 的作用

**PreVote 流程**：
```
1. Prospective 节点发送 VoteRequest(PreVote=true, epoch=当前)
2. 接收者判断：
   ├─ 日志是否足够新？
   ├─ 是否已知有有效 Leader？
   └─ 是否已投过 PreVote？（不持久化，所以可重复）
3. 宽松的授予规则：
   ├─ Leader：拒绝（已有 Leader）
   ├─ Follower：视情况
   ├─ Unattached/Prospective：通常授予
   └─ Candidate：授予
4. PreVote 成功 → Candidate（增加 epoch）
   PreVote 失败 → Unattached 或 Follower
```

**为什么需要 PreVote？**
```
问题 1：网络分区
  ├─ 少数派的节点不知道 Leader 失联
  ├─ 持续重新选举，epoch 不断增加
  └─ 导致 epoch 膨胀

解决：PreVote 不增加 epoch
  ├─ 少数派的 PreVote 会被多数派拒绝
  ├─ 少数派发现无法赢得 PreVote，不进入 Candidate
  └─ epoch 保持稳定

问题 2：频繁的无效选举
  ├─ 每次 Candidate 失败都增加 epoch
  ├─ 可能导致短 epoch 现象

解决：PreVote 探测成功率
  ├─ 只有有机会赢的才增加 epoch
  ├─ 减少浪费的 epoch
```

### 3. 关键的投票决策规则

**所有状态的投票规则总表**：

```
┌─ VoteRequest(PreVote=true)
│
├─ Follower:
│   ├─ 如果已从 Leader fetch：拒绝
│   ├─ 如果日志足够新：授予
│   └─ 否则：拒绝
│
├─ Candidate:
│   ├─ 如果日志足够新：授予
│   └─ 否则：拒绝
│
├─ Prospective:
│   ├─ 如果日志足够新：授予
│   └─ 否则：拒绝
│
├─ Unattached:
│   ├─ 如果日志足够新：授予
│   └─ 否则：拒绝
│
├─ Leader：拒绝（已有 Leader）
│
└─ Resigned：
    ├─ 如果日志足够新：授予
    └─ 否则：拒绝

┌─ VoteRequest(PreVote=false, 正式投票)
│
├─ Follower：拒绝（遵从当前 Leader）
│
├─ Candidate：拒绝（同 epoch 不再投票）
│
├─ Prospective/Unattached:
│   ├─ 如果已投票：
│   │   ├─ 投给同一候选人：授予（幂等性）
│   │   └─ 投给其他人：拒绝
│   ├─ 如果未投票 + 日志足够新：授予（同时持久化 votedKey）
│   └─ 否则：拒绝
│
├─ Leader：拒绝
│
└─ Resigned：拒绝
```

**日志比较规则**：
```
候选人的日志 >= 接收者的日志 ？

判断标准（按优先级）：
1. lastLogEpoch（优先）
   ├─ 候选人.lastLogEpoch > 接收者.lastLogEpoch  → 候选人更新
   ├─ 候选人.lastLogEpoch < 接收者.lastLogEpoch  → 接收者更新
   └─ 候选人.lastLogEpoch == 接收者.lastLogEpoch  → 继续

2. lastLogOffset（其次）
   ├─ 候选人.lastLogOffset >= 接收者.lastLogOffset  → 候选人足够新
   └─ 候选人.lastLogOffset < 接收者.lastLogOffset  → 接收者更新
```

---

## 关键概念理解

### 1. epoch 的生命周期

```
初始化：epoch = 0

Prospective → Candidate：epoch += 1（只此一次）

示例：
  T0: 初始 epoch=0
  T1: 第一次选举 → Prospective(0) → Candidate(1) → Leader(1)
  T2: 第二次选举 → Prospective(1) → Candidate(2) → Leader(2)
  ...

重要性：
  ├─ 保证每个 epoch 最多一个 Leader
  ├─ 检测过期消息
  ├─ 日志的版本标记
  └─ 投票隔离（不同 epoch 的投票互不影响）
```

### 2. votedKey 的管理

```
votedKey 生命周期：

1. 初始：votedKey = null

2. 授予正式投票后：
   ├─ votedKey = candidateKey（持久化）
   └─ 不能改变（同 epoch 中）

3. 新 epoch：
   ├─ 接收到更高 epoch → votedKey = null（清除）
   └─ 准备投给新候选人

持久化：
  ├─ 每次改变 votedKey 时立即持久化
  ├─ 故障重启后能恢复投票决策
  └─ 防止重复投票

幂等性：
  ├─ 同一候选人的投票请求可重复
  ├─ 检查 votedKey == candidateKey
  └─ 如果匹配，再次授予（幂等）
```

### 3. 状态转换的完整规则

```
┌─────────────────────────────────────────────────┐
│ 所有可能的状态转换（用户操作触发）             │
├─────────────────────────────────────────────────┤
│                                                 │
│ Unattached:
│   ├─ electionTimer 超时 → Prospective
│   ├─ 收到更高 epoch → Unattached(newEpoch)
│   ├─ 发现 Leader + endpoints → Follower
│   └─ 授予投票 → Unattached(votedKey=...)
│
│ Prospective:
│   ├─ PreVote 成功(多数派) → Candidate
│   ├─ PreVote 失败(多数派拒) → Follower(或 Unattached)
│   ├─ electionTimer 超时 → Unattached(或 Follower)
│   ├─ 收到更高 epoch → Unattached(newEpoch)
│   └─ 发现 Leader → Follower
│
│ Candidate:
│   ├─ 正式投票成功(多数派) → Leader
│   ├─ 正式投票失败(多数派拒) → 等待超时
│   ├─ electionTimer 超时 → Prospective
│   ├─ 收到更高 epoch → Unattached(newEpoch)
│   └─ 发现 Leader → Follower
│
│ Follower:
│   ├─ fetchTimer 超时 → Prospective
│   ├─ 收到更高 epoch → Unattached(newEpoch)
│   └─ 发现更新 Leader(更高 epoch) → Follower(newLeader)
│
│ Leader:
│   ├─ checkQuorum 超时 → Resigned
│   ├─ 收到更高 epoch → Unattached(newEpoch)
│   ├─ 手动辞职 → Resigned
│   └─ 发现更新 Leader → Follower
│
│ Resigned:
│   ├─ electionTimer 超时 → Unattached
│   ├─ 收到更高 epoch → Unattached(newEpoch)
│   └─ 发现 Leader → Follower
│
└─────────────────────────────────────────────────┘
```

### 4. checkQuorum 机制（防脑裂）

```
目的：检测 Leader 是否失去多数派支持

工作流程：
  T0: Leader 启动 checkQuorumTimer
      ├─ 初始值：fetchTimeoutMs * 1.5
      └─ 重置条件：收到多数派 fetch 请求

  T1: checkQuorumTimer 运行中
      ├─ 每次收到 fetch 请求，更新对应 voter 的 lastFetchTimestamp
      └─ 检查：当前时间 - lastFetchTimestamp < checkQuorumTimeoutMs？

  T2: checkQuorumTimer 超时
      ├─ 统计在 checkQuorumTimeoutMs 内 fetch 过的 voter
      ├─ 如果数量 < 多数派：无法维持多数派
      └─ 动作：transitionToResigned()（自动辞职）

优势：
  ├─ 防止网络分区导致的两个 Leader
  ├─ Leader 主动检测并放弃权力
  ├─ 避免不一致的状态
  └─ 快速恢复：多数派可立即选举新 Leader
```

---

## 实现顺序和优先级

### 推荐的实现 Wave（共 4 个 Wave，3-5 周）

#### Wave 1：基本状态和接口（1 周）

**目标**：实现状态接口和最简单的状态

| 类 | 难度 | 优先级 |
|----|------|--------|
| EpochState (接口) | ⭐ | 最高 |
| NomineeState (接口) | ⭐ | 高 |
| UnattachedState | ⭐⭐ | 最高 |
| ElectionState | ⭐⭐ | 高 |
| EpochElection | ⭐⭐ | 高 |

**代码行数**：~400 行
**关键任务**：
- [ ] 定义状态接口的所有方法
- [ ] 实现 UnattachedState 的完整逻辑
- [ ] 实现 ElectionState 的序列化
- [ ] 实现 EpochElection 的投票统计

---

#### Wave 2：选举状态（1-1.5 周）

**目标**：实现选举的核心状态（PreVote 和正式投票）

| 类 | 难度 | 优先级 |
|----|------|--------|
| ProspectiveState | ⭐⭐⭐ | 最高 |
| CandidateState | ⭐⭐⭐ | 最高 |
| QuorumState (部分) | ⭐⭐⭐⭐ | 最高 |

**代码行数**：~800 行
**关键任务**：
- [ ] 实现 ProspectiveState 的 PreVote 逻辑
- [ ] 实现 CandidateState 的正式投票逻辑
- [ ] 实现 QuorumState.canGrantVote() （最关键！）
- [ ] 实现状态转换方法
- [ ] 完整的单元测试

---

#### Wave 3：Leader 和 Follower（1-1.5 周）

**目标**：实现 Leader 和 Follower 状态，以及 RPC 处理

| 类 | 难度 | 优先级 |
|----|------|--------|
| FollowerState | ⭐⭐ | 高 |
| LeaderState (基础) | ⭐⭐⭐⭐ | 高 |
| ResignedState | ⭐⭐⭐ | 中 |
| KafkaRaftClient (选举部分) | ⭐⭐⭐⭐ | 高 |

**代码行数**：~1,200 行
**关键任务**：
- [ ] 实现 FollowerState 的 fetch 逻辑
- [ ] 实现 LeaderState 的基础功能
- [ ] 实现 KafkaRaftClient 的 RPC 处理
- [ ] 实现 BeginQuorumEpoch 广播

---

#### Wave 4：集成和优化（1 周）

**目标**：完整的选举流程测试和优化

| 类 | 难度 | 优先级 |
|----|------|--------|
| QuorumState (完整) | ⭐⭐⭐⭐⭐ | 最高 |
| RaftUtil | ⭐ | 中 |
| 完整集成测试 | ⭐⭐⭐⭐ | 最高 |

**代码行数**：~400 行
**关键任务**：
- [ ] 完整的 QuorumState 逻辑
- [ ] 单线程事件驱动
- [ ] 3+ 节点的选举测试
- [ ] 网络分区、故障恢复测试
- [ ] 性能基准测试

---

## 代码框架示例

### 示例 1：EpochState 接口

```java
// org/apache/kafka/raft/state/EpochState.java

public interface EpochState {
    /**
     * 当前的 epoch（任期）
     */
    int epoch();

    /**
     * 当前已知的 Leader ID（可能为空）
     */
    Optional<Integer> leaderId();

    /**
     * Leader 的网络端点（可能为空）
     */
    Optional<Endpoints> leaderEndpoints();

    /**
     * 是否可以投票给指定的候选人
     *
     * @param candidate 候选人信息
     * @param isLogUpToDate 候选人的日志是否足够新
     * @param isPreVote 是否为 PreVote（不增加 epoch）
     * @return 是否可以投票
     */
    boolean canGrantVote(
        ReplicaKey candidate,
        boolean isLogUpToDate,
        boolean isPreVote
    );

    /**
     * 距离选举超时还有多少毫秒
     * 负数表示已超时
     */
    long remainingElectionTimeMs(long currentTimeMs);

    /**
     * 是否为 Leader 状态
     */
    boolean isLeader();

    /**
     * 是否为 Follower 状态
     */
    boolean isFollower();

    /**
     * 是否为 Candidate 状态
     */
    boolean isCandidate();

    /**
     * 是否为 Prospective 状态（PreVote 阶段）
     */
    boolean isProspective();

    /**
     * 是否为 Unattached 状态
     */
    boolean isUnattached();

    /**
     * 是否为 Resigned 状态（已辞职）
     */
    boolean isResigned();
}
```

### 示例 2：UnattachedState 实现（简化版）

```java
// org/apache/kafka/raft/state/UnattachedState.java

public class UnattachedState implements EpochState {
    private final int epoch;
    private final Optional<Integer> leaderId;
    private final Optional<ReplicaKey> votedKey;
    private final Timer electionTimer;
    private final long randomElectionTimeoutMs;
    private final Logger log;

    public UnattachedState(
        int epoch,
        Optional<Integer> leaderId,
        Optional<ReplicaKey> votedKey,
        long randomElectionTimeoutMs,
        long currentTimeMs,
        LogContext logContext
    ) {
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.votedKey = votedKey;
        this.randomElectionTimeoutMs = randomElectionTimeoutMs;
        this.log = logContext.logger(UnattachedState.class);

        // 初始化选举计时器
        this.electionTimer = new Timer("electionTimer");
        this.electionTimer.reset(randomElectionTimeoutMs, currentTimeMs);
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public Optional<Integer> leaderId() {
        return leaderId;
    }

    @Override
    public Optional<Endpoints> leaderEndpoints() {
        return Optional.empty();  // Unattached 不知道端点
    }

    @Override
    public boolean canGrantVote(
        ReplicaKey candidate,
        boolean isLogUpToDate,
        boolean isPreVote
    ) {
        if (isPreVote) {
            // PreVote：只检查日志
            return isLogUpToDate;
        } else {
            // 正式投票：检查日志和 votedKey
            if (votedKey.isPresent()) {
                ReplicaKey votedReplicaKey = votedKey.get();
                if (votedReplicaKey.id() == candidate.id()) {
                    // 重复投票请求，幂等处理
                    return votedReplicaKey.directoryId().equals(candidate.directoryId());
                }
                // 已投给其他人
                return false;
            }
            // 未投票，检查日志
            return isLogUpToDate;
        }
    }

    @Override
    public long remainingElectionTimeMs(long currentTimeMs) {
        return electionTimer.remainingMs(currentTimeMs);
    }

    @Override
    public boolean isUnattached() {
        return true;
    }

    @Override
    public boolean isLeader() {
        return false;
    }

    @Override
    public boolean isFollower() {
        return false;
    }

    @Override
    public boolean isCandidate() {
        return false;
    }

    @Override
    public boolean isProspective() {
        return false;
    }

    @Override
    public boolean isResigned() {
        return false;
    }

    /**
     * 检查选举超时
     */
    public boolean hasElectionTimeoutExpired(long currentTimeMs) {
        return electionTimer.isExpired(currentTimeMs);
    }
}
```

### 示例 3：VoteRequest 构建和处理

```java
// 在 KafkaRaftClient 中

private VoteRequestData buildVoteRequest(
    int epoch,
    ReplicaKey candidateKey,
    LogOffsetMetadata lastLogOffsetMetadata,
    boolean isPreVote
) {
    VoteRequestData request = new VoteRequestData()
        .setEpoch(epoch)
        .setReplicaId(candidateKey.id())
        .setReplicaDirectoryId(candidateKey.directoryId())
        .setLastLogOffset(lastLogOffsetMetadata.offset)
        .setLastLogEpoch(lastLogOffsetMetadata.epoch)
        .setPreVote(isPreVote);

    return request;
}

private VoteResponseData handleVoteRequest(
    RaftRequest.Inbound requestMetadata,
    long currentTimeMs
) {
    VoteRequestData request = (VoteRequestData) requestMetadata.data();

    // 1. 验证 epoch
    if (request.preVote() && request.lastLogEpoch() > quorum.lastFetchedEpoch()) {
        return buildVoteResponse(false, quorum.epoch());
    }
    if (!request.preVote() && request.lastLogEpoch() >= quorum.lastFetchedEpoch()) {
        return buildVoteResponse(false, quorum.epoch());
    }

    // 2. 判断是否超高 epoch
    if (request.epoch() > quorum.epoch()) {
        quorum.transitionToUnattached(request.epoch(), OptionalInt.empty());
    }

    // 3. 获取候选人信息
    ReplicaKey candidateKey = new ReplicaKey(
        request.replicaId(),
        request.replicaDirectoryId()
    );

    // 4. 检查日志是否足够新
    boolean isLogUpToDate =
        request.lastLogEpoch() > quorum.lastFetchedEpoch() ||
        (request.lastLogEpoch() == quorum.lastFetchedEpoch() &&
         request.lastLogOffset() >= quorum.logEndOffset());

    // 5. 调用状态的投票决策
    boolean voteGranted = quorum.canGrantVote(
        candidateKey,
        isLogUpToDate,
        request.preVote()
    );

    // 6. 如果授予正式投票，需要持久化
    if (voteGranted && !request.preVote()) {
        quorum.unattachedAddVotedState(request.epoch(), candidateKey);
    }

    // 7. 返回响应
    return new VoteResponseData()
        .setEpoch(quorum.epoch())
        .setVoteGranted(voteGranted)
        .setLeaderId(quorum.leaderId().orElse(-1));
}

private VoteResponseData buildVoteResponse(boolean voteGranted, int epoch) {
    return new VoteResponseData()
        .setEpoch(epoch)
        .setVoteGranted(voteGranted)
        .setLeaderId(quorum.leaderId().orElse(-1));
}
```

---

（本文档完整版保存在 GitHub：`PHASE1_DETAILED_BREAKDOWN.md`）

该文档包含了 Phase 1 的所有关键信息，用于指导 KRaft 选举流程的实现。
