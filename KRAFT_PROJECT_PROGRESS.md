# KRaft 学习项目进度跟踪文档

**文档用途：** 此文档供 Claude 助手追踪项目进度，快速恢复上下文并继续指导实现。

**最后更新时间：** 2025-11-26

---

## 📍 当前状态总览

### 项目阶段
- **当前阶段：** 📋 规划完成，准备开始实施
- **已完成阶段：** 0/11
- **已完成类：** 0/84
- **预计完成进度：** 0% (0/35天)

### 当前任务
```
状态：✋ 等待用户确认开始实施
下一步：创建学习项目骨架
```

---

## 📚 已创建的学习资源

### ✅ 已完成的文档

| 文档 | 状态 | 用途 |
|------|------|------|
| KRAFT_LEARNING_PROJECT_GUIDE.md | ✅ 完成 | 完整学习指南（按时间线） |
| KRAFT_MODULAR_LEARNING_GUIDE.md | ✅ 完成 | 模块化学习指南（模块1-4） |
| KRAFT_MODULAR_LEARNING_GUIDE_PART2.md | ✅ 完成 | 模块化学习指南（模块5-10） |
| KRAFT_IMPLEMENTATION_ORDER_TABLE.md | ✅ 完成 | **实现顺序表（主要参考）** ⭐⭐⭐ |
| kraft-interface-examples.md | ✅ 完成 | 核心接口详细示例 |
| kraft-learning-quickstart.sh | ✅ 完成 | 一键创建项目脚本 |

### 📋 项目结构规划

**学习项目位置：** `~/kraft-learning/` （待创建）

**Maven 模块结构：**
```
kraft-learning/
├── pom.xml                     # 父POM
├── kraft-common/               # 模块1：公共基础
├── kraft-raft/                 # 模块2：核心Raft实现 ⭐⭐⭐
├── kraft-storage/              # 模块3：日志存储
├── kraft-network/              # 模块4：网络通信（可选）
└── kraft-example/              # 模块5：示例和测试
```

---

## 🎯 11个实现阶段详细进度

### 阶段1：基础数据结构（2-3天）

**状态：** ⬜ 未开始
**预计时间：** 2-3天
**核心类数量：** 12个

#### 任务清单

| # | 类名 | 位置 | 行数 | 状态 | 完成日期 |
|---|------|------|------|------|---------|
| 1.1 | `OffsetAndEpoch` | raft | ~100 | ⬜ 未开始 | - |
| 1.2 | `LogOffsetMetadata` | raft | ~150 | ⬜ 未开始 | - |
| 1.3 | `ValidOffsetAndEpoch` | raft | ~80 | ⬜ 未开始 | - |
| 1.4 | `LeaderAndEpoch` | raft | ~100 | ⬜ 未开始 | - |
| 1.5 | `ElectionState` | raft | ~150 | ⬜ 未开始 | - |
| 1.6 | `Endpoints` | raft | ~120 | ⬜ 未开始 | - |
| 1.7 | `ReplicaKey` | raft | ~80 | ⬜ 未开始 | - |
| 1.8 | `Batch<T>` | raft | ~100 | ⬜ 未开始 | - |
| 1.9 | `LogAppendInfo` | raft | ~80 | ⬜ 未开始 | - |
| 1.10 | `LogFetchInfo` | raft | ~100 | ⬜ 未开始 | - |
| 1.11 | `Isolation` (enum) | raft | ~50 | ⬜ 未开始 | - |
| 1.12 | 单元测试 | test | - | ⬜ 未开始 | - |

**阶段1完成标准：**
- [ ] 所有12个类编译通过
- [ ] 所有equals/hashCode正确实现
- [ ] 所有toString可读性好
- [ ] 单元测试覆盖率>80%

**实现顺序：**
```
1. OffsetAndEpoch          ← 最简单，先实现
2. LogOffsetMetadata       ← 依赖OffsetAndEpoch
3. ValidOffsetAndEpoch     ← 依赖OffsetAndEpoch
4. LeaderAndEpoch
5. ElectionState
6. Endpoints
7. ReplicaKey
8. Batch<T>
9. LogAppendInfo
10. LogFetchInfo
11. Isolation
```

**当前子任务：** 无（阶段未开始）

---

### 阶段2：核心接口定义（1-2天）

**状态：** ⬜ 未开始
**预计时间：** 1-2天
**核心接口数量：** 6个

#### 任务清单

| # | 接口名 | 位置 | 核心方法数 | 状态 | 完成日期 |
|---|--------|------|-----------|------|---------|
| 2.1 | `RaftClient<T>` | raft | ~15 | ⬜ 未开始 | - |
| 2.2 | `EpochState` | raft | ~7 | ⬜ 未开始 | - |
| 2.3 | `ReplicatedLog` | raft | ~12 | ⬜ 未开始 | - |
| 2.4 | `NetworkChannel` | raft | ~5 | ⬜ 未开始 | - |
| 2.5 | `RaftMessage` | raft | ~3 | ⬜ 未开始 | - |
| 2.6 | `BatchReader<T>` | raft | ~4 | ⬜ 未开始 | - |

**阶段2完成标准：**
- [ ] 所有接口方法签名准确
- [ ] 所有javadoc完整
- [ ] 接口之间的依赖关系清晰

**当前子任务：** 无（阶段未开始）

---

### 阶段3：配置和存储（2-3天）

**状态：** ⬜ 未开始
**预计时间：** 2-3天
**核心类数量：** 3个

#### 任务清单

| # | 类名 | 位置 | 行数 | 状态 | 完成日期 |
|---|------|------|------|------|---------|
| 3.1 | `QuorumConfig` | raft | ~200 | ⬜ 未开始 | - |
| 3.2 | `QuorumStateStore` | raft | ~80 | ⬜ 未开始 | - |
| 3.3 | `FileQuorumStateStore` | raft | ~200 | ⬜ 未开始 | - |
| 3.4 | `RaftUtil` | raft | ~950 | ⬜ 未开始 | - |

**阶段3完成标准：**
- [ ] 配置类可以正确解析所有配置项
- [ ] 文件存储可以持久化和恢复状态
- [ ] 单元测试覆盖率>80%

**当前子任务：** 无（阶段未开始）

---

### 阶段4：网络和消息（3-4天）

**状态：** ⬜ 未开始
**预计时间：** 3-4天
**核心类数量：** 8个

#### 任务清单

| # | 类名 | 位置 | 行数 | 状态 | 完成日期 |
|---|------|------|------|------|---------|
| 4.1 | `RaftRequest` | raft | ~100 | ⬜ 未开始 | - |
| 4.2 | `RaftRequest.Outbound` | raft | ~80 | ⬜ 未开始 | - |
| 4.3 | `RaftRequest.Inbound` | raft | ~80 | ⬜ 未开始 | - |
| 4.4 | `RaftResponse` | raft | ~100 | ⬜ 未开始 | - |
| 4.5 | `RaftResponse.Outbound` | raft | ~80 | ⬜ 未开始 | - |
| 4.6 | `RaftResponse.Inbound` | raft | ~80 | ⬜ 未开始 | - |
| 4.7 | `KafkaNetworkChannel` | raft | ~400 | ⬜ 未开始 | - |
| 4.8 | `RequestManager` | raft | ~400 | ⬜ 未开始 | - |
| 4.9 | `BlockingMessageQueue` | raft.internals | ~150 | ⬜ 未开始 | - |

**阶段4完成标准：**
- [ ] 消息类可以正确序列化/反序列化
- [ ] RequestManager可以管理多个连接
- [ ] 超时和重试机制正常工作
- [ ] 单元测试覆盖率>80%

**当前子任务：** 无（阶段未开始）

---

### 阶段5：批处理系统 ⭐（3-4天）

**状态：** ⬜ 未开始
**预计时间：** 3-4天
**核心类数量：** 5个
**重要性：** ⭐ 性能关键

#### 任务清单

| # | 类名 | 位置 | 行数 | 状态 | 完成日期 |
|---|------|------|------|------|---------|
| 5.1 | `BatchMemoryPool` | raft.internals | ~150 | ⬜ 未开始 | - |
| 5.2 | `BatchBuilder<T>` | raft.internals | ~200 | ⬜ 未开始 | - |
| 5.3 | `BatchAccumulator<T>` | raft.internals | ~350 | ⬜ 未开始 | - |
| 5.4 | `MemoryBatchReader<T>` | raft.internals | ~100 | ⬜ 未开始 | - |
| 5.5 | `RecordsBatchReader<T>` | raft.internals | ~120 | ⬜ 未开始 | - |

**阶段5完成标准：**
- [ ] BatchAccumulator线程安全
- [ ] 批次完成判断逻辑正确（大小/时间阈值）
- [ ] 内存池正确分配和释放
- [ ] 性能测试：能处理高并发写入
- [ ] 单元测试覆盖率>80%

**实现顺序：**
```
1. BatchMemoryPool        ← 最底层
2. BatchBuilder<T>        ← 依赖内存池
3. BatchAccumulator<T>    ← 依赖BatchBuilder，最复杂
4. MemoryBatchReader<T>
5. RecordsBatchReader<T>
```

**当前子任务：** 无（阶段未开始）

**关键实现要点：**
- BatchAccumulator需要使用ReentrantLock保证线程安全
- 批次完成的双重阈值：大小和时间
- 内存池避免频繁分配

---

### 阶段6：状态机实现 ⭐⭐⭐（7-10天）

**状态：** ⬜ 未开始
**预计时间：** 7-10天
**核心类数量：** 7个
**重要性：** ⭐⭐⭐ 最核心最复杂

#### 任务清单

| # | 类名 | 位置 | 行数 | 复杂度 | 状态 | 完成日期 |
|---|------|------|------|--------|------|---------|
| 6.1 | `UnattachedState` | raft | ~100 | 简单 | ⬜ 未开始 | - |
| 6.2 | `ResignedState` | raft | ~100 | 简单 | ⬜ 未开始 | - |
| 6.3 | `VotedState` | raft | ~150 | 中等 | ⬜ 未开始 | - |
| 6.4 | `FollowerState` | raft | ~200 | 中等 | ⬜ 未开始 | - |
| 6.5 | `ProspectiveState` | raft | ~200 | 复杂 | ⬜ 未开始 | - |
| 6.6 | `CandidateState` | raft | ~300 | 复杂 | ⬜ 未开始 | - |
| 6.7 | `LeaderState<T>` | raft | ~1050 | 最复杂 ⭐⭐⭐ | ⬜ 未开始 | - |

**阶段6完成标准：**
- [ ] 所有7个状态类实现完整
- [ ] 状态转移逻辑正确
- [ ] LeaderState高水位计算正确
- [ ] Follower状态追踪正确
- [ ] 单元测试覆盖所有状态转移场景
- [ ] 单元测试覆盖率>80%

**实现顺序（从简单到复杂）：**
```
1. UnattachedState      ← 最简单（1天）
2. ResignedState        ← 简单（1天）
3. VotedState          ← 中等（1天）
4. FollowerState       ← 中等（1-2天）
5. ProspectiveState    ← 复杂（1-2天）
6. CandidateState      ← 复杂（2天）
7. LeaderState<T>      ← 最复杂（3-4天）⭐⭐⭐
```

**当前子任务：** 无（阶段未开始）

**关键实现要点：**
- UnattachedState是最简单的，从这里开始建立信心
- LeaderState包含高水位计算算法，是整个系统的核心
- CandidateState需要追踪投票结果

**LeaderState核心算法：**
```
高水位计算：
1. 收集所有副本的匹配偏移量
2. 排序
3. 取多数派位置（majority - 1）
4. 更新高水位（只能前进）
```

---

### 阶段7：状态管理器（2-3天）

**状态：** ⬜ 未开始
**预计时间：** 2-3天
**核心类数量：** 2个

#### 任务清单

| # | 类名 | 位置 | 行数 | 状态 | 完成日期 |
|---|------|------|------|------|---------|
| 7.1 | `QuorumState` | raft | ~800 | ⬜ 未开始 | - |
| 7.2 | `KafkaRaftMetrics` | raft.internals | ~800 | ⬜ 未开始 | - |

**阶段7完成标准：**
- [ ] QuorumState所有状态转移方法正确
- [ ] 状态持久化和恢复正确
- [ ] 指标收集完整
- [ ] 单元测试覆盖率>80%

**当前子任务：** 无（阶段未开始）

**关键实现要点：**
- QuorumState是所有状态的协调者
- 必须正确处理状态转移的原子性
- 需要持久化关键状态（epoch, votedId等）

---

### 阶段8：Voter管理（2-3天）

**状态：** ⬜ 未开始
**预计时间：** 2-3天
**核心类数量：** 3个

#### 任务清单

| # | 类名 | 位置 | 行数 | 状态 | 完成日期 |
|---|------|------|------|------|---------|
| 8.1 | `VoterSet` | raft | ~300 | ⬜ 未开始 | - |
| 8.2 | `DynamicVoter` | raft | ~100 | ⬜ 未开始 | - |
| 8.3 | `DynamicVoters` | raft | ~150 | ⬜ 未开始 | - |

**阶段8完成标准：**
- [ ] VoterSet不可变性保证
- [ ] 多数派计算正确
- [ ] 动态成员变更逻辑正确
- [ ] 单元测试覆盖率>80%

**当前子任务：** 无（阶段未开始）

---

### 阶段9：高级功能（2-3天）

**状态：** ⬜ 未开始
**预计时间：** 2-3天
**核心类数量：** 6个

#### 任务清单

| # | 类名 | 位置 | 行数 | 状态 | 完成日期 |
|---|------|------|------|------|---------|
| 9.1 | `AddVoterHandler` | raft.internals | ~200 | ⬜ 未开始 | - |
| 9.2 | `RemoveVoterHandler` | raft.internals | ~200 | ⬜ 未开始 | - |
| 9.3 | `UpdateVoterHandler` | raft.internals | ~150 | ⬜ 未开始 | - |
| 9.4 | `VoterSetHistory` | raft.internals | ~200 | ⬜ 未开始 | - |
| 9.5 | `FuturePurgatory` | raft.internals | ~200 | ⬜ 未开始 | - |
| 9.6 | `ThresholdPurgatory` | raft.internals | ~150 | ⬜ 未开始 | - |

**阶段9完成标准：**
- [ ] Voter变更处理器正确
- [ ] 历史记录管理正确
- [ ] 超时管理正确
- [ ] 单元测试覆盖率>80%

**当前子任务：** 无（阶段未开始）

---

### 阶段10：快照管理（2-3天）

**状态：** ⬜ 未开始
**预计时间：** 2-3天
**核心类数量：** 11个

#### 任务清单

| # | 类名 | 位置 | 行数 | 状态 | 完成日期 |
|---|------|------|------|------|---------|
| 10.1 | `SnapshotReader<T>` | snapshot | ~50 | ⬜ 未开始 | - |
| 10.2 | `SnapshotWriter<T>` | snapshot | ~50 | ⬜ 未开始 | - |
| 10.3 | `RawSnapshotReader` | snapshot | ~80 | ⬜ 未开始 | - |
| 10.4 | `RawSnapshotWriter` | snapshot | ~80 | ⬜ 未开始 | - |
| 10.5 | `FileRawSnapshotReader` | snapshot | ~150 | ⬜ 未开始 | - |
| 10.6 | `FileRawSnapshotWriter` | snapshot | ~200 | ⬜ 未开始 | - |
| 10.7 | `RecordsSnapshotReader<T>` | snapshot | ~100 | ⬜ 未开始 | - |
| 10.8 | `RecordsSnapshotWriter<T>` | snapshot | ~120 | ⬜ 未开始 | - |
| 10.9 | `NotifyingRawSnapshotWriter` | snapshot | ~100 | ⬜ 未开始 | - |
| 10.10 | `SnapshotPath` | snapshot | ~100 | ⬜ 未开始 | - |
| 10.11 | `Snapshots` | snapshot | ~150 | ⬜ 未开始 | - |

**阶段10完成标准：**
- [ ] 快照读写正确
- [ ] 快照文件命名正确
- [ ] 快照加载和应用正确
- [ ] 单元测试覆盖率>80%

**当前子任务：** 无（阶段未开始）

---

### 阶段11：核心引擎 KafkaRaftClient<T> ⭐⭐⭐（7-10天）

**状态：** ⬜ 未开始
**预计时间：** 7-10天
**核心类数量：** 1个
**重要性：** ⭐⭐⭐ 最核心

#### 任务清单

| # | 任务 | 方法/组件 | 复杂度 | 状态 | 完成日期 |
|---|------|----------|--------|------|---------|
| 11.1 | 基本框架 | 类定义+字段 | 中等 | ⬜ 未开始 | - |
| 11.2 | 初始化 | `initialize()` | 中等 | ⬜ 未开始 | - |
| 11.3 | 事件循环 | `poll()` | 最复杂 ⭐⭐⭐ | ⬜ 未开始 | - |
| 11.4 | Vote处理 | `handleVoteRequest/Response` | 复杂 | ⬜ 未开始 | - |
| 11.5 | Fetch处理 | `handleFetchRequest/Response` | 复杂 | ⬜ 未开始 | - |
| 11.6 | BeginQuorumEpoch处理 | `handleBeginQuorumEpoch*` | 中等 | ⬜ 未开始 | - |
| 11.7 | EndQuorumEpoch处理 | `handleEndQuorumEpoch*` | 中等 | ⬜ 未开始 | - |
| 11.8 | 状态转移 | `transitionTo*` 系列方法 | 复杂 | ⬜ 未开始 | - |
| 11.9 | Leader逻辑 | Leader特定方法 | 复杂 | ⬜ 未开始 | - |
| 11.10 | Follower逻辑 | Follower特定方法 | 中等 | ⬜ 未开始 | - |
| 11.11 | Candidate逻辑 | Candidate特定方法 | 中等 | ⬜ 未开始 | - |
| 11.12 | 监听器通知 | `notifyListeners()` | 简单 | ⬜ 未开始 | - |
| 11.13 | 集成测试 | 3节点集群 | - | ⬜ 未开始 | - |
| 11.14 | 集成测试 | 5节点集群 | - | ⬜ 未开始 | - |
| 11.15 | 故障测试 | 各种故障场景 | - | ⬜ 未开始 | - |

**阶段11完成标准：**
- [ ] KafkaRaftClient完整实现
- [ ] poll()事件循环工作正常
- [ ] 所有消息处理正确
- [ ] 状态转移正确
- [ ] 3节点集群测试通过
- [ ] 5节点集群测试通过
- [ ] 故障恢复测试通过
- [ ] 单元测试覆盖率>80%

**实现顺序：**
```
1. 基本框架（字段定义）
2. initialize()方法
3. 简单的poll()框架（只处理消息）
4. Vote请求/响应处理
5. 状态转移方法
6. Fetch请求/响应处理
7. Leader特定逻辑
8. Follower特定逻辑
9. Candidate特定逻辑
10. 完整的poll()方法（整合所有逻辑）
11. 监听器通知
12. 集成测试
```

**当前子任务：** 无（阶段未开始）

**关键实现要点：**
- poll()是整个系统的心脏
- 需要处理网络消息、超时、各角色特定逻辑
- 消息处理需要根据当前状态做不同响应

---

## 📊 总体进度统计

### 按阶段统计

| 阶段 | 预计天数 | 核心类数 | 状态 | 完成度 |
|------|---------|---------|------|--------|
| 阶段1：基础数据结构 | 2-3 | 12 | ⬜ 未开始 | 0% |
| 阶段2：核心接口 | 1-2 | 6 | ⬜ 未开始 | 0% |
| 阶段3：配置存储 | 2-3 | 3 | ⬜ 未开始 | 0% |
| 阶段4：网络消息 | 3-4 | 8 | ⬜ 未开始 | 0% |
| 阶段5：批处理 ⭐ | 3-4 | 5 | ⬜ 未开始 | 0% |
| 阶段6：状态机 ⭐⭐⭐ | 7-10 | 7 | ⬜ 未开始 | 0% |
| 阶段7：状态管理 | 2-3 | 2 | ⬜ 未开始 | 0% |
| 阶段8：Voter管理 | 2-3 | 3 | ⬜ 未开始 | 0% |
| 阶段9：高级功能 | 2-3 | 6 | ⬜ 未开始 | 0% |
| 阶段10：快照 | 2-3 | 11 | ⬜ 未开始 | 0% |
| 阶段11：引擎 ⭐⭐⭐ | 7-10 | 1 | ⬜ 未开始 | 0% |
| **总计** | **25-35** | **84** | - | **0%** |

### 按类型统计

| 类型 | 数量 | 已完成 | 进行中 | 未开始 |
|------|------|--------|--------|--------|
| 基础数据结构 | 12 | 0 | 0 | 12 |
| 接口定义 | 6 | 0 | 0 | 6 |
| 配置和存储 | 4 | 0 | 0 | 4 |
| 网络和消息 | 9 | 0 | 0 | 9 |
| 批处理系统 | 5 | 0 | 0 | 5 |
| 状态类 | 7 | 0 | 0 | 7 |
| 状态管理 | 2 | 0 | 0 | 2 |
| Voter管理 | 3 | 0 | 0 | 3 |
| 高级功能 | 6 | 0 | 0 | 6 |
| 快照管理 | 11 | 0 | 0 | 11 |
| 核心引擎 | 1 | 0 | 0 | 1 |
| **总计** | **84** | **0** | **0** | **84** |

---

## 🎯 下一步行动计划

### 立即执行（会话恢复后的第一步）

```
1. 询问用户是否准备开始实施
2. 如果是，执行快速启动脚本创建项目骨架
3. 开始阶段1的实现
```

### 阶段1第一天计划

**目标：** 实现前3个最简单的类

```
上午（2-3小时）：
1. 运行 kraft-learning-quickstart.sh 创建项目
2. 实现 OffsetAndEpoch.java
3. 编写 OffsetAndEpoch 的单元测试

下午（2-3小时）：
1. 实现 LogOffsetMetadata.java
2. 编写 LogOffsetMetadata 的单元测试
3. 实现 ValidOffsetAndEpoch.java
```

### 阶段1第二天计划

**目标：** 实现4-7号类

```
上午：LeaderAndEpoch + ElectionState
下午：Endpoints + ReplicaKey
```

### 阶段1第三天计划

**目标：** 完成剩余类和测试

```
上午：Batch<T> + LogAppendInfo + LogFetchInfo
下午：Isolation枚举 + 完善所有测试
```

---

## 📝 会话恢复检查清单

**当新会话开始时，Claude应该：**

1. **读取此文档** (`PROGRESS.md`)
2. **检查当前状态**
   - 查看"当前阶段"
   - 查看"当前任务"
   - 查看最后更新时间
3. **询问用户进度**
   - "上次我们完成到哪里了？"
   - "遇到什么问题了吗？"
4. **更新进度**
   - 根据用户反馈更新任务状态
   - 标记已完成的任务
5. **继续指导**
   - 从当前子任务继续
   - 提供下一步的详细指导

---

## 🔧 实施策略

### 代码获取策略

对于每个类，有两种方式：

1. **复制 Kafka 源码**
   ```bash
   # 从Kafka源码复制
   cp /home/user/kafka/raft/src/main/java/org/apache/kafka/raft/OffsetAndEpoch.java \
      ~/kraft-learning/kraft-raft/src/main/java/org/apache/kafka/raft/
   ```

2. **手动实现（更推荐）**
   - 参考 KRAFT_IMPLEMENTATION_ORDER_TABLE.md 中的代码示例
   - 边写边理解
   - 添加详细注释

### 测试策略

每个类实现后立即编写测试：

```java
// 示例：OffsetAndEpochTest.java
@Test
void testCompareTo() {
    OffsetAndEpoch oe1 = new OffsetAndEpoch(100, 5);
    OffsetAndEpoch oe2 = new OffsetAndEpoch(200, 5);
    assertTrue(oe1.compareTo(oe2) < 0);
}
```

### 调试策略

- 每个阶段完成后运行 `mvn test`
- 确保所有测试通过
- 使用 `mvn clean compile` 检查编译

---

## 📌 关键提醒

### 实现原则

1. **严格按照顺序**：不要跳过任何阶段
2. **测试驱动**：每个类都要有测试
3. **完全一致**：类名、方法名必须与Kafka一致
4. **深入理解**：不要只是复制代码，要理解为什么

### 三个最难的点

1. **BatchAccumulator<T>**（阶段5）
   - 线程安全
   - 内存管理
   - 性能优化

2. **LeaderState<T>**（阶段6）
   - 高水位计算
   - Follower追踪
   - 1050行代码

3. **KafkaRaftClient<T>**（阶段11）
   - poll()事件循环
   - 所有消息处理
   - 4141行代码

---

## 📚 参考文档快速链接

- 实现顺序详细规格：`KRAFT_IMPLEMENTATION_ORDER_TABLE.md`
- 模块化学习指南：`KRAFT_MODULAR_LEARNING_GUIDE.md` 和 `PART2.md`
- 接口示例：`kraft-interface-examples.md`
- 快速启动脚本：`kraft-learning-quickstart.sh`

---

## ✅ 更新日志

| 日期 | 更新内容 | 更新人 |
|------|---------|--------|
| 2025-11-26 | 创建初始进度跟踪文档 | Claude |

---

**下次会话开始时，Claude请先读取此文档，了解项目当前状态！**
