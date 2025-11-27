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

import org.apache.kafka.server.common.KRaftVersion;

import java.nio.file.Path;
import java.util.Optional;

/**
 * QuorumStateStore - Quorum状态存储接口
 *
 * 负责持久化和读取Quorum的选举状态信息。
 *
 * 【为什么需要QuorumStateStore？】
 *
 * 在Raft协议中，某些状态必须持久化到磁盘，以便节点重启后能够恢复：
 *
 * 1. **选举状态（ElectionState）**
 *    - epoch：当前term/epoch编号
 *    - votedId：本轮epoch投票给了谁
 *    - leaderId：当前Leader是谁
 *    - leaderEpoch：Leader的epoch
 *
 * 2. **为什么需要持久化这些信息？**
 *    - 防止重复投票：节点在一个epoch内只能投票给一个候选人
 *    - 保证一致性：重启后仍然记住自己的投票决定
 *    - 避免脑裂：防止同一epoch内产生多个Leader
 *
 * 【典型使用场景】
 *
 * 场景1：节点启动时恢复状态
 * <pre>
 * // KafkaRaftClient 初始化时
 * QuorumStateStore stateStore = new FileQuorumStateStore(new File("/data/quorum-state"));
 *
 * // 读取上次保存的选举状态
 * Optional<ElectionState> state = stateStore.readElectionState();
 * if (state.isPresent()) {
 *     int lastEpoch = state.get().epoch();
 *     int votedFor = state.get().votedId();
 *
 *     // 恢复到上次的状态
 *     // 如果在epoch=5时投票给了节点3，重启后仍然记住这个决定
 *     // 即使再次收到epoch=5的投票请求，也会拒绝重复投票
 * }
 * </pre>
 *
 * 场景2：收到投票请求后更新状态
 * <pre>
 * // 处理VoteRequest
 * void handleVoteRequest(int epoch, int candidateId) {
 *     // 检查当前epoch
 *     Optional<ElectionState> current = stateStore.readElectionState();
 *
 *     if (current.isEmpty() || epoch > current.get().epoch()) {
 *         // 新的epoch，可以投票
 *         ElectionState newState = new ElectionState(
 *             epoch,              // 新的epoch
 *             candidateId,        // 投票给这个候选人
 *             -1,                 // leaderId未知
 *             Collections.emptySet()
 *         );
 *
 *         // 持久化投票决定（原子操作）
 *         stateStore.writeElectionState(newState, KRaftVersion.KRAFT_VERSION_0);
 *
 *         // 发送VoteResponse
 *         sendVoteGranted(candidateId);
 *     } else if (epoch == current.get().epoch() && current.get().votedId() == candidateId) {
 *         // 同一epoch，已经投票给这个候选人了，可以重复发送投票响应
 *         sendVoteGranted(candidateId);
 *     } else {
 *         // 同一epoch，但已经投票给其他人了，拒绝投票
 *         sendVoteRejected();
 *     }
 * }
 * </pre>
 *
 * 场景3：选举成功后更新Leader信息
 * <pre>
 * // 成为Leader或收到BeginQuorumEpoch通知
 * void becomeLeader(int epoch) {
 *     ElectionState newState = new ElectionState(
 *         epoch,              // 当前epoch
 *         myNodeId,           // 投票给了自己
 *         myNodeId,           // 自己是Leader
 *         voterSet
 *     );
 *
 *     // 持久化Leader状态
 *     stateStore.writeElectionState(newState, KRaftVersion.KRAFT_VERSION_0);
 * }
 *
 * void becomeFollower(int epoch, int leaderId) {
 *     ElectionState newState = new ElectionState(
 *         epoch,              // 当前epoch
 *         -1,                 // 未投票（或已经投票但Leader已产生）
 *         leaderId,           // 新的Leader
 *         voterSet
 *     );
 *
 *     // 持久化Follower状态
 *     stateStore.writeElectionState(newState, KRaftVersion.KRAFT_VERSION_0);
 * }
 * </pre>
 *
 * 【状态持久化的重要性】
 *
 * 假设没有持久化会发生什么：
 *
 * <pre>
 * 时间线：
 * T1: 节点1在epoch=5投票给节点2
 * T2: 节点1崩溃重启（内存状态丢失）
 * T3: 节点3在epoch=5向节点1请求投票
 * T4: 节点1忘记了之前的投票，又投票给节点3
 *
 * 结果：
 * - epoch=5产生了两个Leader（节点2和节点3）
 * - 集群出现脑裂
 * - 数据一致性被破坏
 * </pre>
 *
 * 有了持久化：
 * <pre>
 * 时间线：
 * T1: 节点1在epoch=5投票给节点2，并持久化到磁盘
 * T2: 节点1崩溃重启
 * T3: 节点1从磁盘恢复状态：epoch=5, votedId=2
 * T4: 节点3在epoch=5向节点1请求投票
 * T5: 节点1检查磁盘状态，发现已经投票给节点2了，拒绝节点3
 *
 * 结果：
 * - 只有节点2能成为Leader
 * - 集群正常运行
 * - 数据一致性得到保证
 * </pre>
 *
 * 【实现类】
 *
 * QuorumStateStore有一个标准实现：
 *
 * - **FileQuorumStateStore**：基于文件的实现
 *   - 使用JSON格式存储
 *   - 原子写入（先写临时文件，再原子重命名）
 *   - 默认文件名：quorum-state
 *
 * 示例文件内容：
 * <pre>
 * {
 *   "leaderId": 1,
 *   "leaderEpoch": 5,
 *   "votedId": 1,
 *   "votedDirectoryId": "J8aAPcfLQt2bqs1JT_rMgQ",
 *   "data_version": 1
 * }
 * </pre>
 *
 * 【线程安全性】
 *
 * QuorumStateStore的实现必须是线程安全的：
 * - writeElectionState()必须是原子操作
 * - 多个线程可以同时调用readElectionState()
 * - 写入时使用临时文件+原子重命名保证原子性
 *
 * 【与其他组件的关系】
 *
 * <pre>
 * KafkaRaftClient
 *   ├─ QuorumState (管理状态转移)
 *   │   └─ QuorumStateStore (持久化选举状态)
 *   │       └─ FileQuorumStateStore (文件实现)
 *   │           └─ quorum-state 文件
 *   │
 *   └─ ReplicatedLog (管理日志)
 *       └─ Log Segments (持久化日志数据)
 * </pre>
 *
 * 职责分离：
 * - QuorumStateStore：只负责选举状态（epoch、votedId、leaderId）
 * - ReplicatedLog：负责日志数据（offset、records、snapshot）
 *
 * @see ElectionState 选举状态数据结构
 * @see FileQuorumStateStore 基于文件的实现
 * @see QuorumState 使用此接口的状态管理器
 */
public interface QuorumStateStore {
    /**
     * 读取最新的选举状态
     *
     * 从持久化存储中读取上次保存的选举状态。
     *
     * 【什么时候调用？】
     *
     * 1. **节点启动时**
     *    <pre>
     *    // KafkaRaftClient初始化
     *    Optional<ElectionState> state = stateStore.readElectionState();
     *    if (state.isPresent()) {
     *        // 恢复到之前的状态
     *        int lastEpoch = state.get().epoch();
     *        int votedFor = state.get().votedId();
     *    }
     *    </pre>
     *
     * 2. **处理投票请求前**
     *    <pre>
     *    // 检查是否已经在当前epoch投票
     *    Optional<ElectionState> current = stateStore.readElectionState();
     *    if (current.isPresent() && current.get().epoch() == requestEpoch) {
     *        // 已经在这个epoch投票了
     *        if (current.get().votedId() == candidateId) {
     *            // 投票给同一个候选人，可以重复发送响应
     *        } else {
     *            // 投票给了不同的候选人，拒绝投票
     *        }
     *    }
     *    </pre>
     *
     * 【返回值】
     *
     * - Optional.empty()：第一次启动，还没有选举状态
     * - Optional.of(state)：恢复之前保存的状态
     *
     * 【异常情况】
     *
     * - 文件不存在：返回Optional.empty()
     * - 文件损坏：抛出UncheckedIOException
     * - 版本不兼容：抛出IllegalStateException
     *
     * 【性能考虑】
     *
     * readElectionState()会执行磁盘I/O，但是：
     * - 状态文件很小（通常<1KB）
     * - 读取频率不高（主要在启动和投票时）
     * - 不需要缓存（每次读取保证是最新的）
     *
     * @return 最新写入的选举状态，如果没有则返回Optional.empty()
     */
    Optional<ElectionState> readElectionState();

    /**
     * 持久化更新后的选举状态
     *
     * 原子地写入新的选举状态，并替换旧状态。
     *
     * 【什么时候调用？】
     *
     * 1. **投票给候选人时**
     *    <pre>
     *    // 处理VoteRequest，决定投票
     *    ElectionState newState = new ElectionState(epoch, candidateId, -1, voterSet);
     *    stateStore.writeElectionState(newState, KRaftVersion.KRAFT_VERSION_1);
     *    // 持久化后才发送VoteResponse
     *    sendVoteResponse(true);
     *    </pre>
     *
     * 2. **成为Leader时**
     *    <pre>
     *    // 赢得选举，成为Leader
     *    ElectionState newState = new ElectionState(epoch, myId, myId, voterSet);
     *    stateStore.writeElectionState(newState, KRaftVersion.KRAFT_VERSION_1);
     *    // 持久化后才开始作为Leader工作
     *    startLeaderOperations();
     *    </pre>
     *
     * 3. **收到BeginQuorumEpoch时**
     *    <pre>
     *    // 其他节点成为Leader，收到通知
     *    ElectionState newState = new ElectionState(epoch, -1, leaderId, voterSet);
     *    stateStore.writeElectionState(newState, KRaftVersion.KRAFT_VERSION_1);
     *    // 持久化后才转换为Follower
     *    becomeFollower(leaderId);
     *    </pre>
     *
     * 【原子性要求】
     *
     * writeElectionState()必须是原子操作：
     *
     * <pre>
     * 错误的实现（非原子）：
     * 1. 打开文件
     * 2. 写入新状态
     * 3. 关闭文件
     * 问题：如果在步骤2崩溃，文件可能只写入了一半，损坏了
     *
     * 正确的实现（原子）：
     * 1. 写入临时文件（quorum-state.tmp）
     * 2. 调用fsync()确保数据落盘
     * 3. 原子重命名：quorum-state.tmp → quorum-state
     * 优点：要么看到新状态，要么看到旧状态，永远不会看到半成品
     * </pre>
     *
     * 【为什么需要kraftVersion参数？】
     *
     * KRaft协议有多个版本，不同版本的状态格式可能不同：
     *
     * - KRAFT_VERSION_0 (data_version=0)：包含clusterId、appliedOffset等
     * - KRAFT_VERSION_1 (data_version=1)：简化格式，只包含必要字段
     *
     * <pre>
     * Version 0格式：
     * {
     *   "clusterId": "",
     *   "leaderId": 1,
     *   "leaderEpoch": 5,
     *   "votedId": 1,
     *   "appliedOffset": 0,
     *   "currentVoters": [],
     *   "data_version": 0
     * }
     *
     * Version 1格式：
     * {
     *   "leaderId": 1,
     *   "leaderEpoch": 5,
     *   "votedId": 1,
     *   "votedDirectoryId": "J8aAPcfLQt2bqs1JT_rMgQ",
     *   "data_version": 1
     * }
     * </pre>
     *
     * 【崩溃恢复】
     *
     * writeElectionState()保证崩溃安全：
     *
     * <pre>
     * 场景：在写入时崩溃
     * 1. 开始写入quorum-state.tmp
     * 2. 写入一半时崩溃
     * 3. 重启后，tmp文件存在但损坏
     * 4. readElectionState()读取quorum-state（旧文件，完好）
     * 5. 忽略损坏的tmp文件
     * 6. 使用旧状态继续运行（安全）
     * </pre>
     *
     * @param latest 最新的选举状态
     * @param kraftVersion 最终确定的kraft.version，决定状态文件的格式版本
     */
    void writeElectionState(ElectionState latest, KRaftVersion kraftVersion);

    /**
     * 获取quorum state store的路径
     *
     * 返回存储选举状态的文件或目录路径。
     *
     * 【用途】
     *
     * 1. **日志记录**
     *    <pre>
     *    logger.info("Quorum state store path: {}", stateStore.path());
     *    </pre>
     *
     * 2. **调试**
     *    <pre>
     *    // 管理员可以手动检查状态文件
     *    cat /var/lib/kafka/quorum-state
     *    </pre>
     *
     * 3. **备份**
     *    <pre>
     *    // 备份quorum状态
     *    cp $(stateStore.path()) /backup/quorum-state.backup
     *    </pre>
     *
     * 【示例返回值】
     *
     * - /var/lib/kafka/data/quorum-state
     * - /tmp/kafka-logs/__cluster_metadata-0/quorum-state
     *
     * @return quorum state store的路径
     */
    Path path();

    /**
     * 清除所有与store相关的状态，以便全新开始
     *
     * 删除持久化的选举状态，重置为初始状态。
     *
     * 【什么时候调用？】
     *
     * 1. **格式化存储时（kafka-storage.sh format）**
     *    <pre>
     *    // 清除旧数据，准备全新的集群
     *    stateStore.clear();
     *    log.deleteAll();
     *    </pre>
     *
     * 2. **测试清理**
     *    <pre>
     *    @AfterEach
     *    void cleanup() {
     *        stateStore.clear();  // 每个测试后清理
     *    }
     *    </pre>
     *
     * 3. **离开集群时（未来可能的场景）**
     *    <pre>
     *    // 节点永久离开集群
     *    stateStore.clear();
     *    log.deleteAll();
     *    shutdown();
     *    </pre>
     *
     * 【执行的操作】
     *
     * <pre>
     * 删除所有状态文件：
     * - quorum-state        （主文件）
     * - quorum-state.tmp    （临时文件，如果存在）
     * </pre>
     *
     * 【注意事项】
     *
     * - 此操作不可逆，数据将永久丢失
     * - 清除后，下次readElectionState()返回Optional.empty()
     * - 通常在节点停止后才调用clear()
     *
     * 【示例】
     *
     * <pre>
     * // 格式化Kafka存储
     * void formatStorage() {
     *     logger.info("Formatting storage at {}", dataDir);
     *
     *     // 清除quorum状态
     *     stateStore.clear();
     *
     *     // 清除日志
     *     replicatedLog.deleteAll();
     *
     *     // 清除快照
     *     snapshotStore.deleteAll();
     *
     *     logger.info("Storage formatted successfully");
     * }
     * </pre>
     */
    void clear();
}
