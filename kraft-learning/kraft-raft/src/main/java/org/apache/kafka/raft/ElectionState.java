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

import org.apache.kafka.raft.generated.QuorumStateData;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ElectionState - 选举状态
 *
 * 封装了Raft选举过程中需要持久化到磁盘的状态信息。
 * 每次状态变更后都会写入磁盘，确保节点重启后能恢复选举状态。
 *
 * 【为什么需要持久化选举状态？】
 *
 * Raft协议的核心安全保证：在同一个epoch中，一个节点最多只能投一票。
 *
 * 问题场景（如果不持久化）：
 * <pre>
 * 1. 节点A在epoch=5时，投票给了节点B
 * 2. 节点A崩溃并重启
 * 3. 节点C也在epoch=5发起选举，向节点A请求投票
 * 4. 如果不持久化，节点A不记得已经投过票，可能再次投票给节点C
 * 5. 结果：在epoch=5中，节点A投了两票（违反Raft协议）
 * 6. 后果：可能有两个Leader同时存在（脑裂）
 * </pre>
 *
 * 解决方案：
 * - 每次投票前，先读取磁盘上的选举状态
 * - 投票后，立即持久化到磁盘
 * - 重启后，从磁盘恢复选举状态
 *
 * 【核心字段】
 *
 * 1. epoch - 选举周期（任期）
 *    - 单调递增的计数器
 *    - 每次选举开始时递增
 *    - 用于区分不同的选举周期
 *
 * 2. leaderId - 当前Leader的ID（可选）
 *    - OptionalInt.empty()：还没有Leader（正在选举中）
 *    - OptionalInt.of(id)：已经选出了Leader
 *
 * 3. votedKey - 投票给的候选者（可选）
 *    - Optional.empty()：还没有投票
 *    - Optional.of(key)：已经投票给某个候选者
 *    - 包含候选者的ID和目录UUID
 *
 * 4. voters - 投票者集合（已弃用）
 *    - 历史遗留字段，仅用于写入version 0格式
 *    - 新版本使用VoterSet管理投票者
 *
 * 【三种状态】
 *
 * ElectionState可以处于三种状态之一：
 *
 * 1. Unknown Leader（未知Leader）
 *    - epoch已知，但还不知道谁是Leader
 *    - leaderId = empty, votedKey = empty
 *    - 场景：选举刚开始，还没有投票
 *
 * 2. Voted Candidate（已投票）
 *    - 已经投票给某个候选者，但还不知道谁赢了
 *    - leaderId = empty, votedKey = present
 *    - 场景：收到了VoteRequest并投票，等待选举结果
 *
 * 3. Elected Leader（已选出Leader）
 *    - 已经知道谁是Leader了
 *    - leaderId = present, votedKey可能present也可能empty
 *    - 场景：收到了当选Leader的心跳或自己当选了
 *
 * 【状态转换】
 *
 * <pre>
 * Unknown Leader (epoch=N)
 *   ↓
 *   | 收到VoteRequest并投票
 *   ↓
 * Voted Candidate (epoch=N, votedKey=B)
 *   ↓
 *   | 收到当选Leader的心跳
 *   ↓
 * Elected Leader (epoch=N, leaderId=B)
 *   ↓
 *   | 选举超时，开始新选举
 *   ↓
 * Unknown Leader (epoch=N+1)
 * </pre>
 *
 * 【使用场景】
 *
 * 场景1：Follower收到投票请求
 * <pre>
 * // 检查是否已经投过票
 * ElectionState state = loadFromDisk();
 * if (state.hasVoted() && !state.isVotedCandidate(candidateKey)) {
 *     // 已经投给了别人，拒绝投票
 *     return false;
 * }
 *
 * // 投票并持久化
 * ElectionState newState = ElectionState.withVotedCandidate(epoch, candidateKey, voters);
 * saveToDisk(newState);
 * </pre>
 *
 * 场景2：节点当选Leader
 * <pre>
 * // 收到多数派投票，当选Leader
 * ElectionState state = ElectionState.withElectedLeader(epoch, myId, votedKey, voters);
 * saveToDisk(state);
 * </pre>
 *
 * 场景3：Follower收到Leader心跳
 * <pre>
 * // 发现了新Leader
 * ElectionState state = ElectionState.withElectedLeader(epoch, leaderId, Optional.empty(), voters);
 * saveToDisk(state);
 * </pre>
 *
 * 【版本兼容性】
 *
 * ElectionState支持两种磁盘格式版本：
 *
 * Version 0（旧版本）：
 * - 不支持directoryId
 * - votedKey中的directoryId为empty
 * - 包含voters列表（现已弃用）
 *
 * Version 1（新版本）：
 * - 支持directoryId
 * - votedKey包含完整的ReplicaKey（id + directoryId）
 * - 不再持久化voters列表
 *
 * 升级路径：
 * <pre>
 * Version 0文件 → 读取时votedKey.directoryId = empty
 *               → isVotedCandidate()会忽略directoryId比较
 *               → 兼容旧版本行为
 * </pre>
 *
 * 【isVotedCandidate逻辑】
 *
 * 判断是否投票给了指定候选者，需要考虑版本兼容性：
 *
 * 1. 如果votedKey为empty → 没投过票 → false
 * 2. 如果votedKey.id != 候选者id → 投给了别人 → false
 * 3. 如果votedKey.directoryId为empty → version 0格式 → true（假设投给了这个候选者）
 * 4. 如果votedKey.directoryId == 候选者directoryId → 完全匹配 → true
 * 5. 否则 → directoryId不匹配 → false
 *
 * 为什么步骤3返回true？
 * - Version 0不记录directoryId
 * - 如果id匹配，假设就是投给了这个候选者
 * - 保证向后兼容性
 *
 * 【不可变性】
 *
 * ElectionState是final类，所有字段都是final：
 * - 线程安全
 * - 状态变更通过创建新实例完成
 * - 避免意外修改
 *
 * @see QuorumStateData 用于序列化/反序列化的数据结构
 * @see ReplicaKey 副本标识符（id + directoryId）
 */
public final class ElectionState {
    /**
     * UNKNOWN_LEADER_ID - 表示"未知Leader"的哨兵值
     *
     * 使用-1表示还不知道谁是Leader。
     * 在序列化时，OptionalInt.empty()会被转换为-1。
     */
    private static final int UNKNOWN_LEADER_ID = -1;

    /**
     * NOT_VOTED - 表示"未投票"的哨兵值
     *
     * 使用-1表示还没有投票。
     * 在序列化时，Optional.empty()会被转换为-1。
     */
    private static final int NOT_VOTED = -1;

    /**
     * 选举周期（epoch）
     *
     * Raft中的"任期"概念，单调递增。
     * 每次开始新选举时，epoch会递增。
     *
     * 示例：
     * - epoch=0：初始状态
     * - epoch=1：第一次选举
     * - epoch=2：Leader崩溃，第二次选举
     */
    private final int epoch;

    /**
     * Leader ID（可选）
     *
     * - OptionalInt.empty()：还没有Leader（正在选举中）
     * - OptionalInt.of(id)：已经选出了Leader
     *
     * 为什么用OptionalInt而不是int？
     * - 避免使用-1等魔法值表示"无Leader"
     * - 类型安全，编译期检查
     * - 明确表达"可能没有值"的语义
     */
    private final OptionalInt leaderId;

    /**
     * 投票给的候选者（可选）
     *
     * - Optional.empty()：还没有投票
     * - Optional.of(key)：已经投票给某个候选者
     *
     * ReplicaKey包含：
     * - id：候选者的节点ID
     * - directoryId：候选者的目录UUID（version 1新增）
     *
     * 为什么记录votedKey？
     * - 保证同一epoch中最多投一票（Raft安全性）
     * - 重启后能记住已经投给了谁
     */
    private final Optional<ReplicaKey> votedKey;

    /**
     * 投票者集合（已弃用）
     *
     * 这是一个历史遗留字段，仅在以下场景使用：
     * 1. 写入version 0格式的状态文件时
     * 2. 从version 0格式读取时
     *
     * 为什么弃用？
     * - 新版本使用VoterSet统一管理投票者配置
     * - 不应该在每次选举状态变更时都持久化整个投票者列表
     * - 投票者配置应该独立管理（通过配置变更机制）
     */
    private final Set<Integer> voters;

    /**
     * 包级别构造函数
     *
     * 不对外公开，强制使用静态工厂方法创建实例。
     * 好处：
     * 1. 工厂方法名称更有语义（withVotedCandidate等）
     * 2. 可以在工厂方法中添加验证逻辑
     * 3. 控制对象创建方式
     *
     * @param epoch 选举周期
     * @param leaderId Leader ID（可选）
     * @param votedKey 投票的候选者（可选）
     * @param voters 投票者集合（已弃用）
     */
    ElectionState(
        int epoch,
        OptionalInt leaderId,
        Optional<ReplicaKey> votedKey,
        Set<Integer> voters
    ) {
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.votedKey = votedKey;
        this.voters = voters;
    }

    /**
     * 获取选举周期
     *
     * @return 当前epoch
     */
    public int epoch() {
        return epoch;
    }

    /**
     * 判断指定节点是否是当前Leader
     *
     * @param nodeId 节点ID
     * @return 如果是当前Leader返回true
     * @throws IllegalArgumentException 如果nodeId为负数
     */
    public boolean isLeader(int nodeId) {
        if (nodeId < 0)
            throw new IllegalArgumentException("Invalid negative nodeId: " + nodeId);
        return leaderIdOrSentinel() == nodeId;
    }

    /**
     * 判断是否投票给了指定候选者
     *
     * 一个副本被认为"投票给了指定候选者"需要满足以下条件：
     * 1. 节点ID匹配
     * 2. 如果持久化的votedKey包含directoryId，则directoryId也必须匹配
     *
     * 特殊情况（版本兼容性）：
     * - 如果持久化的votedKey不包含directoryId（version 0格式）
     * - 只要节点ID匹配，就认为投给了这个候选者
     * - 这是为了向后兼容
     *
     * 示例：
     * <pre>
     * // Version 1：完整匹配
     * ElectionState state = withVotedCandidate(5, ReplicaKey.of(1, uuid1), voters);
     * state.isVotedCandidate(ReplicaKey.of(1, uuid1)) → true
     * state.isVotedCandidate(ReplicaKey.of(1, uuid2)) → false（directoryId不匹配）
     *
     * // Version 0：只比较ID
     * ElectionState state = withVotedCandidate(5, ReplicaKey.of(1, NO_DIRECTORY_ID), voters);
     * state.isVotedCandidate(ReplicaKey.of(1, anyUuid)) → true（忽略directoryId）
     * </pre>
     *
     * @param nodeKey 候选者的ReplicaKey（id + directoryId）
     * @return 如果投票给了这个候选者返回true
     * @throws IllegalArgumentException 如果nodeKey的id为负数
     */
    public boolean isVotedCandidate(ReplicaKey nodeKey) {
        if (nodeKey.id() < 0) {
            throw new IllegalArgumentException("Invalid node key " + nodeKey);
        } else if (votedKey.isEmpty()) {
            // 没有投过票
            return false;
        } else if (votedKey.get().id() != nodeKey.id()) {
            // 节点ID不匹配，投给了别人
            return false;
        } else if (votedKey.get().directoryId().isEmpty()) {
            // Version 0格式：没有记录directoryId
            // 只要节点ID匹配，就认为投给了这个候选者
            // 这保证了向后兼容性
            return true;
        }

        // Version 1格式：比较完整的ReplicaKey（id + directoryId）
        return votedKey.get().directoryId().equals(nodeKey.directoryId());
    }

    /**
     * 获取Leader ID
     *
     * @return Leader的节点ID
     * @throws IllegalStateException 如果当前没有Leader
     */
    public int leaderId() {
        if (leaderId.isEmpty())
            throw new IllegalStateException("Attempt to access nil leaderId");
        return leaderId.getAsInt();
    }

    /**
     * 获取Leader ID或哨兵值
     *
     * 如果没有Leader，返回UNKNOWN_LEADER_ID（-1）。
     * 这个方法主要用于序列化。
     *
     * @return Leader ID或-1
     */
    public int leaderIdOrSentinel() {
        return leaderId.orElse(UNKNOWN_LEADER_ID);
    }

    /**
     * 获取Optional包装的Leader ID
     *
     * @return OptionalInt，如果没有Leader则为empty
     */
    public OptionalInt optionalLeaderId() {
        return leaderId;
    }

    /**
     * 获取投票的候选者
     *
     * @return 投票的ReplicaKey
     * @throws IllegalStateException 如果还没有投票
     */
    public ReplicaKey votedKey() {
        if (votedKey.isEmpty()) {
            throw new IllegalStateException("Attempt to access nil votedId");
        }

        return votedKey.get();
    }

    /**
     * 获取Optional包装的投票候选者
     *
     * @return Optional<ReplicaKey>，如果没有投票则为empty
     */
    public Optional<ReplicaKey> optionalVotedKey() {
        return votedKey;
    }

    /**
     * 判断是否已经有Leader
     *
     * @return 如果已经选出Leader返回true
     */
    public boolean hasLeader() {
        return leaderId.isPresent();
    }

    /**
     * 判断是否已经投过票
     *
     * @return 如果已经投票返回true
     */
    public boolean hasVoted() {
        return votedKey.isPresent();
    }

    /**
     * 转换为QuorumStateData用于持久化
     *
     * 支持两种版本格式：
     *
     * Version 0（旧版本）：
     * - 不支持votedDirectoryId
     * - 包含currentVoters列表
     *
     * Version 1（新版本）：
     * - 支持votedDirectoryId
     * - 不包含currentVoters列表
     *
     * @param version 格式版本（0或1）
     * @return QuorumStateData对象
     * @throws IllegalStateException 如果版本不支持
     */
    public QuorumStateData toQuorumStateData(short version) {
        QuorumStateData data = new QuorumStateData()
            .setLeaderEpoch(epoch)
            .setLeaderId(leaderIdOrSentinel())
            .setVotedId(votedKey.map(ReplicaKey::id).orElse(NOT_VOTED));

        if (version == 0) {
            // Version 0：包含voters列表，不包含votedDirectoryId
            List<QuorumStateData.Voter> dataVoters = voters
                .stream()
                .map(voterId -> new QuorumStateData.Voter().setVoterId(voterId))
                .collect(Collectors.toList());
            data.setCurrentVoters(dataVoters);
        } else if (version == 1) {
            // Version 1：包含votedDirectoryId，不包含voters列表
            data.setVotedDirectoryId(
                votedKey.flatMap(ReplicaKey::directoryId).orElse(ReplicaKey.NO_DIRECTORY_ID)
            );
        } else {
            throw new IllegalStateException(
                String.format(
                    "File quorum state store doesn't handle supported version %d", version
                )
            );
        }

        return data;
    }

    /**
     * 字符串表示
     *
     * 格式：Election(epoch=5, leaderId=OptionalInt[1], votedKey=Optional[...], voters=[0,1,2])
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return String.format(
            "Election(epoch=%d, leaderId=%s, votedKey=%s, voters=%s)",
            epoch,
            leaderId,
            votedKey,
            voters
        );
    }

    /**
     * 判断相等性
     *
     * 两个ElectionState相等当且仅当所有字段都相同。
     *
     * @param o 要比较的对象
     * @return 如果相等返回true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElectionState that = (ElectionState) o;

        if (epoch != that.epoch) return false;
        if (!leaderId.equals(that.leaderId)) return false;
        if (!votedKey.equals(that.votedKey)) return false;

        return voters.equals(that.voters);
    }

    /**
     * 计算哈希码
     *
     * 基于所有字段计算哈希码。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(epoch, leaderId, votedKey, voters);
    }

    /**
     * 静态工厂方法：创建"已投票"状态
     *
     * 用于Follower投票给候选者后创建新的选举状态。
     *
     * 状态特征：
     * - epoch：指定的选举周期
     * - leaderId：empty（还不知道谁会当选）
     * - votedKey：已经投票给的候选者
     *
     * 使用场景：
     * <pre>
     * // Follower收到VoteRequest并决定投票
     * ElectionState newState = ElectionState.withVotedCandidate(
     *     requestEpoch,
     *     candidateKey,
     *     voters
     * );
     * saveToDisk(newState);
     * </pre>
     *
     * @param epoch 选举周期
     * @param votedKey 投票的候选者
     * @param voters 投票者集合
     * @return ElectionState实例
     * @throws IllegalArgumentException 如果votedKey.id为负数
     */
    public static ElectionState withVotedCandidate(int epoch, ReplicaKey votedKey, Set<Integer> voters) {
        if (votedKey.id() < 0) {
            throw new IllegalArgumentException("Illegal voted Id " + votedKey.id() + ": must be non-negative");
        }

        return new ElectionState(epoch, OptionalInt.empty(), Optional.of(votedKey), voters);
    }

    /**
     * 静态工厂方法：创建"已选出Leader"状态
     *
     * 用于以下场景：
     * 1. 节点自己当选Leader
     * 2. 收到当选Leader的心跳
     *
     * 状态特征：
     * - epoch：指定的选举周期
     * - leaderId：已确定的Leader
     * - votedKey：可能有（如果自己投过票），也可能没有
     *
     * 使用场景：
     * <pre>
     * // 场景1：自己当选Leader
     * ElectionState state = ElectionState.withElectedLeader(
     *     epoch,
     *     myId,
     *     Optional.of(myKey),  // 投票给了自己
     *     voters
     * );
     *
     * // 场景2：收到Leader心跳
     * ElectionState state = ElectionState.withElectedLeader(
     *     epoch,
     *     leaderId,
     *     Optional.empty(),  // 可能没投票，或投给了别人
     *     voters
     * );
     * </pre>
     *
     * @param epoch 选举周期
     * @param leaderId Leader的节点ID
     * @param votedKey 投票的候选者（可选）
     * @param voters 投票者集合
     * @return ElectionState实例
     * @throws IllegalArgumentException 如果leaderId为负数
     */
    public static ElectionState withElectedLeader(
        int epoch,
        int leaderId,
        Optional<ReplicaKey> votedKey,
        Set<Integer> voters
    ) {
        if (leaderId < 0) {
            throw new IllegalArgumentException("Illegal leader Id " + leaderId + ": must be non-negative");
        }

        return new ElectionState(epoch, OptionalInt.of(leaderId), votedKey, voters);
    }

    /**
     * 静态工厂方法：创建"未知Leader"状态
     *
     * 用于选举刚开始，还不知道谁是Leader，也还没有投票。
     *
     * 状态特征：
     * - epoch：指定的选举周期
     * - leaderId：empty（未知）
     * - votedKey：empty（未投票）
     *
     * 使用场景：
     * <pre>
     * // 选举超时，开始新的选举周期
     * ElectionState newState = ElectionState.withUnknownLeader(
     *     currentEpoch + 1,
     *     voters
     * );
     * saveToDisk(newState);
     * </pre>
     *
     * @param epoch 选举周期
     * @param voters 投票者集合
     * @return ElectionState实例
     */
    public static ElectionState withUnknownLeader(int epoch, Set<Integer> voters) {
        return new ElectionState(epoch, OptionalInt.empty(), Optional.empty(), voters);
    }

    /**
     * 从QuorumStateData反序列化
     *
     * 从磁盘读取的数据转换为ElectionState对象。
     *
     * 转换规则：
     * 1. votedId == NOT_VOTED（-1）→ votedKey = Optional.empty()
     * 2. 否则，构造ReplicaKey(votedId, votedDirectoryId)
     * 3. leaderId == UNKNOWN_LEADER_ID（-1）→ leaderId = OptionalInt.empty()
     * 4. 否则，leaderId = OptionalInt.of(leaderId)
     * 5. 从currentVoters提取voter IDs
     *
     * @param data QuorumStateData对象
     * @return ElectionState实例
     */
    public static ElectionState fromQuorumStateData(QuorumStateData data) {
        Optional<ReplicaKey> votedKey = data.votedId() == NOT_VOTED ?
            Optional.empty() :
            Optional.of(ReplicaKey.of(data.votedId(), data.votedDirectoryId()));

        return new ElectionState(
            data.leaderEpoch(),
            data.leaderId() == UNKNOWN_LEADER_ID ? OptionalInt.empty() : OptionalInt.of(data.leaderId()),
            votedKey,
            data.currentVoters().stream().map(QuorumStateData.Voter::voterId).collect(Collectors.toSet())
        );
    }
}
