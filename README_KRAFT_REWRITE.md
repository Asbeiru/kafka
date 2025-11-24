# KRaft 重写项目 - 完整实施指南

## 🎯 项目概述

本项目旨在基于 Apache Kafka KRaft 的核心思想，从零实现一个生产级的 Raft 共识协议。

**项目周期**: 20 周（5 个月）
**实施方式**: 10 个 Sprint，每个 Sprint 2 周
**项目名称**: mini-raft

---

## 📚 完整文档体系

我已经为你准备了一套完整的项目文档，涵盖从理论学习到生产部署的全过程：

### 1. 学习和理解阶段

#### 📖 **KRAFT_REWRITE_GUIDE.md** (2882 行)
- **用途**: 详细的技术实现指南
- **内容**:
  - 8 个实现阶段的详细说明
  - 每个阶段的代码示例
  - 设计要点和最佳实践
  - 性能优化建议
  - 学习资源推荐

#### 📋 **KRAFT_IMPLEMENTATION_CHECKLIST.md**
- **用途**: 任务清单和里程碑追踪
- **内容**:
  - 每个阶段的详细任务勾选项
  - 测试覆盖率要求
  - 常见陷阱和调试技巧
  - 生产就绪标准

#### 🚀 **KRAFT_QUICK_START.md**
- **用途**: 快速上手指南
- **内容**:
  - 30 分钟理解核心概念
  - 可运行的代码示例
  - 第一个测试编写
  - 立即开始的步骤

### 2. 项目执行阶段

#### 📊 **KRAFT_PROJECT_EXECUTION_PLAN.md** (2565 行) ⭐
- **用途**: 项目执行的主文档，我们将严格按照这个计划实施
- **内容**:
  - **完整的架构设计**: 分层架构、模块依赖关系
  - **技术栈选择**: Java 17, gRPC, Protocol Buffers
  - **项目结构**: 7 个 Maven 模块的详细组织
  - **Sprint 0-5 详细计划**:
    - Sprint 0: 项目初始化
    - Sprint 1: 日志存储实现（ReplicatedLog, LogSegment, OffsetIndex）
    - Sprint 2: 状态机实现（6 种状态 + QuorumState）
    - Sprint 3: 网络通信（gRPC + Protocol Buffers）
    - Sprint 4: Leader 选举（PreVote + Vote 机制）
    - Sprint 5: 日志复制（Follower Fetch 模式）
  - **Sprint 6-10 概要**: 快照、Check Quorum、成员变更等
  - **每个任务的详细说明**:
    - 具体实现的类和方法
    - 参考的 Kafka 源码位置
    - 估时和优先级
    - 验收标准和测试用例

#### 🗺️ **KRAFT_CORE_CLASSES_MAPPING.md**
- **用途**: Kafka KRaft 核心类映射参考
- **内容**:
  - 每个 Kafka 类的详细分析
  - 对应到我们实现的映射关系
  - 关键代码片段和实现要点
  - 设计原则总结（持久化顺序、高水位更新等）

---

## 🏗️ 项目架构

### 模块结构

```
mini-raft/
├── raft-common/          # 公共模块（数据模型、异常、工具类）
├── raft-api/             # API 接口（RaftClient, RaftNode, StateMachine）
├── raft-protocol/        # 协议定义（Protocol Buffers）
├── raft-core/            # 核心实现（状态机、选举、复制）
├── raft-storage/         # 存储层（日志、状态、快照）
├── raft-network/         # 网络层（gRPC 通信）
├── raft-metrics/         # 监控指标
└── raft-tests/           # 测试（单元、集成、混沌）
```

### 核心类映射

| Kafka KRaft | mini-raft | 说明 |
|-------------|-----------|------|
| KafkaRaftClient | RaftEngine | 核心引擎 |
| QuorumState | QuorumState | 状态管理器 |
| LeaderState | LeaderState | Leader 状态 |
| ReplicatedLog | ReplicatedLog | 日志接口 |
| BatchAccumulator | BatchAccumulator | 批次累积器 |
| NetworkChannel | NetworkChannel | 网络抽象 |

---

## 🎯 实施计划

### Sprint 0: 项目初始化 (Week 1-2) 👈 **我们从这里开始！**

#### 目标
- ✅ 创建 Maven 多模块项目
- ✅ 配置 Protocol Buffers
- ✅ 定义基础数据结构
- ✅ 配置 CI/CD

#### 交付物
- [x] 完整的项目结构
- [x] 可编译的项目骨架
- [x] Protocol Buffers 协议定义
- [x] 基础测试框架

#### 验收标准
```bash
mvn clean compile    # 编译成功
mvn test            # 测试通过
mvn checkstyle:check # 代码风格检查通过
```

### Sprint 1: 日志存储 (Week 3-4)

#### 核心类
- `ReplicatedLog` - 日志接口
- `LogSegment` - 日志段实现
- `OffsetIndex` - 偏移量索引
- `LogManager` - 日志管理器

#### 参考 Kafka 类
```java
org.apache.kafka.raft.ReplicatedLog
org.apache.kafka.storage.internals.log.LogSegment
org.apache.kafka.storage.internals.log.OffsetIndex
```

#### 关键实现点
1. 日志段文件格式（.log + .index）
2. 使用 mmap 或 FileChannel
3. 日志恢复逻辑
4. 截断操作

### Sprint 2: 状态机 (Week 5-6)

#### 核心类
- `State` 接口
- `QuorumState` 状态管理器
- 6 种状态实现：
  - `UnattachedState`
  - `ProspectiveState` (PreVote)
  - `CandidateState`
  - `LeaderState`
  - `FollowerState`
  - `ResignedState`

#### 关键实现点
1. 状态转换逻辑
2. 状态持久化（先磁盘后内存）
3. 超时管理

### Sprint 3: 网络通信 (Week 7-8)

#### 核心类
- Protocol Buffers 定义（raft.proto）
- `NetworkChannel` 接口
- `GrpcNetworkChannel` 实现
- `RaftServiceImpl` gRPC 服务

#### 关键实现点
1. VoteRequest/Response
2. FetchRequest/Response
3. 连接管理
4. 请求超时

### Sprint 4: Leader 选举 (Week 9-10)

#### 核心类
- `ElectionManager` - 选举管理器
- `VoteGranter` - 投票决策器

#### 关键实现点
1. PreVote 机制（防止无效选举）
2. 投票决策逻辑（日志新旧比较）
3. 投票状态持久化
4. 成为 Leader 的流程

### Sprint 5: 日志复制 (Week 11-12)

#### 核心类
- `BatchAccumulator` - 批次累积器
- `ReplicationManager` - 复制管理器（Leader 端）
- `FetchManager` - Fetch 管理器（Follower 端）
- `ReplicaState` - 副本状态

#### 关键实现点
1. Follower 拉取模式
2. 日志验证和分歧检测
3. 高水位更新
4. 批处理优化

### Sprint 6-10: 高级特性

- **Sprint 6**: 快照机制
- **Sprint 7**: Check Quorum + 性能优化
- **Sprint 8**: 动态成员变更
- **Sprint 9**: Observer + 监控
- **Sprint 10**: 生产就绪

---

## 🚀 如何开始

### 方式一：按照完整计划实施（推荐）

```bash
# 1. 阅读执行计划
cat KRAFT_PROJECT_EXECUTION_PLAN.md

# 2. 阅读核心类映射
cat KRAFT_CORE_CLASSES_MAPPING.md

# 3. 准备好开始 Sprint 0
# 我会帮你逐步实现每个 Sprint 的所有代码
```

### 方式二：快速原型验证

```bash
# 1. 先看快速开始指南
cat KRAFT_QUICK_START.md

# 2. 一周内实现 MVP
# 用于概念验证或学习
```

---

## 📋 我的工作方式

当我们开始实施时，我会：

### 每个 Sprint 的工作流程

1. **Sprint 启动**
   - 回顾 Sprint 目标
   - 确认任务清单
   - 准备开发环境

2. **逐个实现任务**
   ```
   对于每个任务：
   - 创建对应的 Java 文件
   - 实现完整的类和方法
   - 添加必要的注释
   - 编写单元测试
   - 运行测试验证
   ```

3. **Sprint 评审**
   - 运行所有测试
   - 检查代码覆盖率
   - 验证交付标准
   - 提交代码到 Git

4. **Sprint 回顾**
   - 总结完成情况
   - 调整后续计划
   - 准备下一个 Sprint

### 代码质量保证

- ✅ 所有代码都有详细注释
- ✅ 每个类都有对应的单元测试
- ✅ 参考 Kafka 源码的设计模式
- ✅ 遵循 Java 编码规范
- ✅ 提供完整的 Javadoc

---

## 📊 项目里程碑

### MVP (8 周) - Sprint 0-3
- ✅ 基础设施完成
- ✅ 日志存储可用
- ✅ 状态机运行
- ✅ 网络通信正常

### Alpha (12 周) - Sprint 0-5
- ✅ Leader 选举成功
- ✅ 日志复制工作
- ✅ 3 节点集成测试通过

### Beta (16 周) - Sprint 0-7
- ✅ 快照机制完成
- ✅ 性能优化完成
- ✅ 混沌测试通过

### V1.0 (20 周) - Sprint 0-9
- ✅ 所有功能完成
- ✅ 生产就绪
- ✅ 文档完善

---

## 🎓 学习资源

### 必读论文
1. [Raft 论文](https://raft.github.io/raft.pdf) - 核心算法
2. [Raft PhD 论文](https://github.com/ongardie/dissertation) - 详细解释
3. [KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500) - Kafka KRaft 设计

### 参考实现
1. [Apache Kafka KRaft](https://github.com/apache/kafka/tree/trunk/raft) - 我们主要参考
2. [etcd/raft](https://github.com/etcd-io/raft) - Go 实现
3. [hashicorp/raft](https://github.com/hashicorp/raft) - Go 实现

### 工具
1. [Jepsen](https://github.com/jepsen-io/jepsen) - 分布式系统测试
2. [TLA+](https://lamport.azurewebsites.net/tla/tla.html) - 形式化验证

---

## ⚡ 关键设计原则

### 1. 持久化顺序

```java
// ✅ 正确：先磁盘，后内存，最后网络
void transitionToCandidate(int epoch) {
    store.writeElectionState(...);  // 1. 磁盘
    this.state = new CandidateState(...);  // 2. 内存
    sendVoteRequests();  // 3. 网络
}
```

### 2. 高水位更新

```java
// 只能提交当前 epoch 的日志
boolean updateHighWatermark() {
    long quorumOffset = calculateQuorumMatchOffset();

    // 必须是当前 epoch
    if (log.epochAtOffset(quorumOffset) != currentEpoch) {
        return false;
    }

    highWatermark = quorumOffset;
    return true;
}
```

### 3. 日志截断

```java
// 只在发现分歧时截断
if (validation.kind() == ValidationKind.DIVERGING) {
    log.truncateTo(validation.offset());
}
```

---

## 🤝 协作方式

### 你的角色
- 学习 Raft 协议和 Kafka KRaft 的设计
- 理解每个 Sprint 的目标和交付物
- 审查我实现的代码
- 提出问题和改进建议
- 运行测试验证功能

### 我的角色
- 按照执行计划逐步实现代码
- 确保代码质量和正确性
- 编写详细的注释和文档
- 提供单元测试和集成测试
- 解答你的问题

---

## 📞 下一步

我已经准备好开始实施了！我们可以：

### 选项 1: 立即开始 Sprint 0 🚀
```
我会为你：
1. 创建 Maven 多模块项目结构
2. 配置所有的 pom.xml
3. 定义 Protocol Buffers 协议
4. 实现基础数据模型
5. 配置测试框架
6. 编写第一个测试
```

### 选项 2: 先学习理解
```
你可以：
1. 阅读 Raft 论文
2. 研究 Kafka KRaft 源码
3. 理解执行计划中的架构设计
4. 准备开发环境
```

### 选项 3: 提问讨论
```
如果你有任何问题，比如：
- 为什么选择这种架构？
- 某个设计决策的原因？
- 如何理解某个 Kafka 类的实现？
- Sprint 计划是否需要调整？
```

---

**准备好了吗？告诉我你想从哪里开始！** 🎉

我建议直接开始 **Sprint 0**，让我们一起搭建项目骨架！
