/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft;

import java.io.Closeable;
import java.util.Optional;

/**
 * EpochState - Raft节点在特定epoch中的状态接口
 *
 * 这是一个核心的状态抽象接口，定义了Raft节点在某个epoch（任期）中的行为和状态。
 *
 * 【什么是EpochState？】
 *
 * 在Raft协议中，一个节点在不同时期会处于不同的角色：
 * - Unattached：未连接（刚启动或等待加入集群）
 * - Voted：已投票（投票给了某个候选者，等待选举结果）
 * - Follower：跟随者（正常工作，复制Leader的日志）
 * - Candidate：候选者（发起选举，试图成为Leader）
 * - Leader：领导者（处理写入请求，复制日志给Follower）
 * - Resigned：已辞职（Leader主动让出领导权）
 * - Prospective：准候选者（Pre-Vote阶段，测试是否能赢得选举）
 *
 * EpochState是所有这些状态的公共接口，定义了通用操作。
 *
 * 【接口设计哲学】
 *
 * 1. **不可变性**
 *    - election()返回的ElectionState是不可变的
 *    - epoch()返回的epoch是不可变的
 *    - 状态变更通过创建新的EpochState实例完成
 *
 * 2. **状态模式**
 *    <pre>
 *    // KRaft使用状态模式管理节点状态
 *    abstract class QuorumState {
 *        private EpochState state; // 当前状态
 *
 *        void transitionTo(EpochState newState) {
 *            this.state.close(); // 关闭旧状态
 *            this.state = newState; // 切换到新状态
 *        }
 *    }
 *    </pre>
 *
 * 3. **最小权限原则**
 *    - 每个状态只暴露其需要的操作
 *    - 例如：只有LeaderState有高水位计算
 *
 * 【状态转换图】
 *
 * <pre>
 * Unattached
 *   ↓ 收到VoteRequest
 * Voted (已投票)
 *   ↓ 选举超时或收到更高epoch
 * Candidate (发起选举)
 *   ↓ 赢得选举
 * Leader (当选Leader)
 *   ↓ 主动辞职
 * Resigned
 *   ↓ 新一轮选举
 * Unattached
 * </pre>
 *
 * 或者：
 * <pre>
 * Unattached
 *   ↓ 收到Leader心跳
 * Follower (跟随Leader)
 *   ↓ Leader超时
 * Candidate
 *   ...
 * </pre>
 *
 * 【实现类】
 *
 * KRaft中的7个EpochState实现类：
 *
 * 1. **UnattachedState** - 未连接状态
 *    - 刚启动或等待加入集群
 *    - 还没有投票，还没有Leader
 *
 * 2. **VotedState** - 已投票状态
 *    - 投票给了某个候选者
 *    - 等待选举结果
 *
 * 3. **FollowerState** - Follower状态
 *    - 跟随Leader
 *    - 复制Leader的日志
 *
 * 4. **CandidateState** - Candidate状态
 *    - 发起选举
 *    - 请求其他节点投票
 *
 * 5. **LeaderState** - Leader状态
 *    - 处理客户端写入
 *    - 复制日志给Follower
 *    - 计算高水位
 *
 * 6. **ResignedState** - 已辞职状态
 *    - Leader主动让出领导权
 *    - 等待新Leader选出
 *
 * 7. **ProspectiveState** - 准候选者状态（Pre-Vote）
 *    - 在真正发起选举前，先测试能否赢得选举
 *    - 避免频繁的无效选举
 *
 * 【典型使用模式】
 *
 * <pre>
 * // QuorumState管理当前状态
 * class QuorumState {
 *     private EpochState state;
 *
 *     // 查询当前epoch
 *     public int epoch() {
 *         return state.epoch();
 *     }
 *
 *     // 查询是否可以投票
 *     public boolean canGrantVote(ReplicaKey replicaKey, boolean isLogUpToDate) {
 *         return state.canGrantVote(replicaKey, isLogUpToDate, false);
 *     }
 *
 *     // 状态转换
 *     public void transitionToFollower(int epoch, int leaderId) {
 *         EpochState oldState = this.state;
 *         this.state = new FollowerState(epoch, leaderId, ...);
 *         oldState.close();
 *     }
 * }
 * </pre>
 *
 * 【与其他组件的关系】
 *
 * <pre>
 * QuorumState (状态管理器)
 *   └─→ EpochState (当前状态)
 *        ├─→ UnattachedState
 *        ├─→ VotedState
 *        ├─→ FollowerState
 *        ├─→ CandidateState
 *        ├─→ LeaderState (最复杂)
 *        ├─→ ResignedState
 *        └─→ ProspectiveState
 * </pre>
 *
 * 【线程安全性】
 *
 * - EpochState本身是不可变的（epoch、election都不变）
 * - 但内部可能有可变状态（如LeaderState的副本追踪）
 * - 状态转换必须在单线程中进行（通常在Raft主循环）
 *
 * @see UnattachedState 未连接状态
 * @see VotedState 已投票状态
 * @see FollowerState Follower状态
 * @see CandidateState Candidate状态
 * @see LeaderState Leader状态
 * @see ResignedState 已辞职状态
 * @see ProspectiveState 准候选者状态（Pre-Vote）
 */
public interface EpochState extends Closeable {

    /**
     * 获取高水位（High Watermark）
     *
     * 【什么是高水位？】
     *
     * 高水位 = 被多数派确认的最高offset
     * - 只有Leader才能计算高水位
     * - Follower不知道高水位（需要从Leader获取）
     *
     * 【默认实现】
     *
     * 大部分状态没有高水位的概念，默认返回Optional.empty()：
     * - UnattachedState：还没有Leader → 无高水位
     * - VotedState：等待选举 → 无高水位
     * - FollowerState：不计算高水位 → 无高水位
     * - CandidateState：还没当选 → 无高水位
     *
     * 只有**LeaderState**会覆盖此方法并返回实际的高水位。
     *
     * 【LeaderState的实现】
     *
     * <pre>
     * class LeaderState implements EpochState {
     *     private LogOffsetMetadata highWatermark;
     *
     *     @Override
     *     public Optional<LogOffsetMetadata> highWatermark() {
     *         return Optional.of(highWatermark);
     *     }
     * }
     * </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * EpochState state = quorumState.currentState();
     * Optional<LogOffsetMetadata> hwmOpt = state.highWatermark();
     *
     * if (hwmOpt.isPresent()) {
     *     // 只有Leader状态才会到这里
     *     LogOffsetMetadata hwm = hwmOpt.get();
     *     System.out.println("High watermark: " + hwm.offset());
     * } else {
     *     // 非Leader状态
     *     System.out.println("Not a leader, no high watermark");
     * }
     * </pre>
     *
     * 【为什么返回Optional？】
     *
     * - 表达"可能没有"的语义
     * - 避免使用null或特殊值（如-1）
     * - 类型安全，编译期检查
     *
     * @return 高水位元数据，如果当前状态不是Leader则返回Optional.empty()
     */
    default Optional<LogOffsetMetadata> highWatermark() {
        return Optional.empty();
    }

    /**
     * 决定是否向候选者投票
     *
     * Raft投票规则的核心实现。
     *
     * 【Raft投票规则】
     *
     * 一个节点可以投票给候选者，当且仅当：
     * 1. **同一epoch只能投一票**
     *    - 如果已经投给了别人，不能再投
     *    - 除非是Pre-Vote（非正式投票）
     *
     * 2. **候选者的日志至少和自己一样新**
     *    - 比较epoch：候选者的lastEpoch >= 自己的lastEpoch
     *    - 如果epoch相同，比较offset：候选者的lastOffset >= 自己的lastOffset
     *
     * 【不同状态的投票行为】
     *
     * 1. **UnattachedState** - 可以投票
     *    <pre>
     *    // 还没投过票，可以投
     *    if (isLogUpToDate) {
     *        return true;
     *    }
     *    </pre>
     *
     * 2. **VotedState** - 只能投给之前投过的候选者
     *    <pre>
     *    if (votedKey.equals(replicaKey) && isLogUpToDate) {
     *        return true; // 重复的投票请求
     *    }
     *    return false; // 已经投给了别人
     *    </pre>
     *
     * 3. **FollowerState** - 不投票
     *    <pre>
     *    return false; // 已经有Leader了，不参与选举
     *    </pre>
     *
     * 4. **CandidateState** - 只投给自己
     *    <pre>
     *    return replicaKey.equals(myKey);
     *    </pre>
     *
     * 5. **LeaderState** - 不投票
     *    <pre>
     *    return false; // 我已经是Leader了
     *    </pre>
     *
     * 【Pre-Vote vs 正式投票】
     *
     * **Pre-Vote（预投票）**：
     * - 非正式的投票，不改变状态
     * - 用于测试"如果发起选举，能赢吗？"
     * - 避免频繁的无效选举
     *
     * 流程：
     * <pre>
     * 1. Follower超时 → 先发Pre-Vote请求
     * 2. 如果Pre-Vote成功 → 真正发起选举（标准投票）
     * 3. 如果Pre-Vote失败 → 不发起选举（避免无效选举）
     * </pre>
     *
     * **标准投票**：
     * - 正式的投票，会改变状态
     * - 投票后进入VotedState
     * - 持久化到磁盘（防止重启后忘记投票）
     *
     * 【调用者的责任】
     *
     * 如果返回true（授予投票），调用者必须：
     * <pre>
     * if (state.canGrantVote(replicaKey, isLogUpToDate, isPreVote)) {
     *     if (!isPreVote) {
     *         // 标准投票：持久化投票记录
     *         quorumState.unattachedAddVotedState(epoch, replicaKey);
     *     }
     *     // 发送VoteResponse(granted=true)
     * }
     * </pre>
     *
     * 【为什么日志必须是最新的？】
     *
     * 保证Raft的安全性：
     * <pre>
     * 场景：
     * 节点A：[0, 1, 2, 3, 4]  epoch=5
     * 节点B：[0, 1, 2]        epoch=5
     * 节点C：[0, 1]           epoch=4
     *
     * 节点C发起选举 → 节点A、B不会投票（C的日志太旧）
     * → 节点C无法当选
     * → 保证了已提交的记录（0-2）不会丢失
     * </pre>
     *
     * 如果允许日志旧的节点当选：
     * - 新Leader的日志是[0, 1]
     * - 会覆盖掉节点A的[3, 4]
     * - 如果[3, 4]已经提交，就违反了安全性
     *
     * 【典型用法】
     *
     * <pre>
     * // 处理VoteRequest
     * boolean isLogUpToDate = candidate.lastEpoch > myLastEpoch ||
     *                        (candidate.lastEpoch == myLastEpoch &&
     *                         candidate.lastOffset >= myLastOffset);
     *
     * boolean canGrant = state.canGrantVote(
     *     candidateKey,
     *     isLogUpToDate,
     *     request.isPreVote()
     * );
     *
     * if (canGrant) {
     *     if (!request.isPreVote()) {
     *         // 持久化投票记录
     *         quorumState.transitionToVoted(epoch, candidateKey);
     *     }
     *     response.setVoteGranted(true);
     * } else {
     *     response.setVoteGranted(false);
     * }
     * </pre>
     *
     * @param replicaKey 候选者的ID和目录UUID
     * @param isLogUpToDate 候选者的日志是否至少和自己一样新
     * @param isPreVote 是否是Pre-Vote（非正式投票）
     * @return true表示可以授予投票，false表示不能投票
     */
    boolean canGrantVote(ReplicaKey replicaKey, boolean isLogUpToDate, boolean isPreVote);

    /**
     * 获取当前的选举状态
     *
     * 返回不可变的ElectionState对象，包含：
     * - epoch：当前任期
     * - leaderId：当前Leader的ID（可能为空）
     * - votedKey：投票给的候选者（可能为空）
     * - voters：投票者集合（已弃用）
     *
     * 【不可变性保证】
     *
     * 返回的ElectionState是不可变的：
     * - 所有字段都是final
     * - 线程安全
     * - 可以安全地保存引用
     *
     * <pre>
     * ElectionState election = state.election();
     * int epoch = election.epoch(); // OK
     * // election对象不会变，即使state切换了
     * </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * // 查询投票状态
     * ElectionState election = state.election();
     * if (election.hasVoted()) {
     *     ReplicaKey votedKey = election.votedKey();
     *     System.out.println("Voted for: " + votedKey);
     * }
     *
     * // 查询Leader
     * if (election.hasLeader()) {
     *     int leaderId = election.leaderId();
     *     System.out.println("Leader: " + leaderId);
     * }
     * </pre>
     *
     * @return 当前的不可变选举状态
     */
    ElectionState election();

    /**
     * 获取当前的epoch（任期）
     *
     * 【什么是epoch？】
     *
     * Epoch = 任期编号，单调递增：
     * - 每次选举开始，epoch递增
     * - 用于检测过期的Leader
     * - 用于日志冲突检测
     *
     * 【Epoch的作用】
     *
     * 1. **检测过期Leader**
     *    <pre>
     *    // Leader发送的请求
     *    if (request.epoch < currentEpoch) {
     *        // 过期的Leader，拒绝请求
     *        return;
     *    }
     *    </pre>
     *
     * 2. **检测日志冲突**
     *    <pre>
     *    // 检查prevEntry是否匹配
     *    if (log[prevOffset].epoch != request.prevEpoch) {
     *        // 日志冲突，需要截断
     *    }
     *    </pre>
     *
     * 3. **选举去重**
     *    <pre>
     *    if (request.epoch <= currentEpoch) {
     *        // 过期的选举请求，忽略
     *    }
     *    </pre>
     *
     * 【不可变性】
     *
     * 一个EpochState的epoch是不可变的：
     * - 创建时确定
     * - 永不改变
     * - epoch变化时，会创建新的EpochState
     *
     * 【Epoch递增时机】
     *
     * <pre>
     * UnattachedState(epoch=5)
     *   ↓ 选举超时，发起选举
     * CandidateState(epoch=6)  // epoch递增
     *   ↓ 赢得选举
     * LeaderState(epoch=6)     // epoch不变
     *   ↓ 辞职
     * ResignedState(epoch=6)   // epoch不变
     *   ↓ 新一轮选举
     * UnattachedState(epoch=7) // epoch递增
     * </pre>
     *
     * @return 当前epoch（不可变）
     */
    int epoch();

    /**
     * 返回已知的Leader端点信息
     *
     * 【什么是端点（Endpoints）？】
     *
     * 端点 = Leader的网络地址（可能有多个监听器）
     * 例如：
     * <pre>
     * {
     *   "INTERNAL": "192.168.1.10:9092",
     *   "EXTERNAL": "10.0.0.10:9093"
     * }
     * </pre>
     *
     * 【什么时候返回empty？】
     *
     * 1. **没有Leader**
     *    <pre>
     *    UnattachedState → 还没有Leader → Endpoints.empty()
     *    CandidateState  → 还没选出Leader → Endpoints.empty()
     *    </pre>
     *
     * 2. **Leader未知**
     *    <pre>
     *    VotedState → 知道epoch，但不知道谁是Leader → Endpoints.empty()
     *    </pre>
     *
     * 【什么时候有值？】
     *
     * 1. **FollowerState** - 知道Leader的地址
     *    <pre>
     *    Follower从Leader的心跳中获取端点信息
     *    </pre>
     *
     * 2. **LeaderState** - 自己是Leader
     *    <pre>
     *    返回自己的监听地址
     *    </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * // 查询Leader地址
     * Endpoints endpoints = state.leaderEndpoints();
     * if (!endpoints.isEmpty()) {
     *     Optional<InetSocketAddress> address = endpoints.address(
     *         ListenerName.normalised("INTERNAL")
     *     );
     *     if (address.isPresent()) {
     *         System.out.println("Leader at: " + address.get());
     *     }
     * }
     * </pre>
     *
     * 【为什么需要端点信息？】
     *
     * 客户端需要知道如何连接到Leader：
     * <pre>
     * // 客户端写入流程
     * 1. 查询Leader端点
     * 2. 连接到Leader
     * 3. 发送写入请求
     * 4. 如果Leader变了，重新查询端点
     * </pre>
     *
     * @return Leader的端点信息，如果Leader未知则返回Endpoints.empty()
     */
    Endpoints leaderEndpoints();

    /**
     * 返回用户友好的状态名称
     *
     * 用于日志、监控、调试。
     *
     * 【状态名称】
     *
     * 各个实现类返回的名称：
     * - UnattachedState → "Unattached"
     * - VotedState → "Voted"
     * - FollowerState → "Follower"
     * - CandidateState → "Candidate"
     * - LeaderState → "Leader"
     * - ResignedState → "Resigned"
     * - ProspectiveState → "Prospective"
     *
     * 【使用场景】
     *
     * <pre>
     * // 日志记录
     * logger.info("Transitioning to state: {}", state.name());
     *
     * // 监控指标
     * metrics.recordState(state.name());
     *
     * // 调试输出
     * System.out.println("Current state: " + state.name() +
     *                    ", epoch: " + state.epoch());
     * </pre>
     *
     * @return 状态的用户友好名称
     */
    String name();

    /**
     * 关闭状态并清理资源
     *
     * 【什么时候调用？】
     *
     * 状态转换时调用：
     * <pre>
     * void transitionTo(EpochState newState) {
     *     EpochState oldState = this.state;
     *     this.state = newState;
     *     oldState.close(); // 清理旧状态
     * }
     * </pre>
     *
     * 【为什么覆盖Closeable.close()？】
     *
     * Closeable.close()默认声明抛出IOException：
     * <pre>
     * void close() throws IOException;
     * </pre>
     *
     * 但EpochState的实现都不会抛出IOException，所以：
     * - 覆盖方法签名，去掉throws IOException
     * - 简化调用代码（不需要try-catch）
     *
     * 【各状态的清理工作】
     *
     * 大部分状态没有资源需要清理：
     * <pre>
     * @Override
     * public void close() {
     *     // 默认空实现
     * }
     * </pre>
     *
     * 只有LeaderState需要清理：
     * <pre>
     * class LeaderState implements EpochState {
     *     private BatchAccumulator<T> accumulator;
     *
     *     @Override
     *     public void close() {
     *         accumulator.close(); // 清理批次累积器
     *     }
     * }
     * </pre>
     *
     * 【设计说明】
     *
     * 注释说明：
     * "Since all subclasses implement the Closeable interface while none throw any IOException,
     *  this implementation is provided to eliminate the need for exception handling in the close operation."
     *
     * 翻译：
     * "由于所有子类都实现了Closeable接口但都不抛出IOException，
     *  提供此实现以消除close操作中的异常处理需求。"
     */
    @Override
    void close();
}
