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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.raft.errors.BufferAllocationException;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriter;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * RaftClient - Raft客户端核心接口
 *
 * 这是KRaft（Kafka Raft）的最核心接口，定义了与Raft复制状态机交互的所有操作。
 *
 * 【什么是RaftClient？】
 *
 * RaftClient是Raft协议的"门面"（Facade），封装了：
 * 1. 数据写入：prepareAppend() + schedulePreparedAppend()
 * 2. 状态查询：highWatermark(), leaderAndEpoch(), logEndOffset()
 * 3. 事件监听：register(Listener) - 监听提交、快照、Leader变更
 * 4. 生命周期：shutdown(), resign(), close()
 * 5. 快照管理：createSnapshot(), latestSnapshotId()
 *
 * 【核心设计理念】
 *
 * 1. **泛型T** - 支持任意类型的记录
 *    <pre>
 *    // 示例1：使用字节数组
 *    RaftClient<byte[]> raftClient = ...;
 *
 *    // 示例2：使用自定义记录类型
 *    RaftClient<MyRecord> raftClient = ...;
 *    </pre>
 *
 * 2. **异步事件驱动** - 通过Listener回调通知状态变更
 *    - handleCommit()：记录已提交
 *    - handleLoadSnapshot()：需要加载快照
 *    - handleLeaderChange()：Leader变更
 *
 * 3. **两阶段提交** - 写入分为prepare和schedule两步
 *    - prepareAppend()：准备数据，返回预期offset
 *    - schedulePreparedAppend()：真正写入日志
 *    - 好处：批处理优化、更好的错误处理
 *
 * 4. **AutoCloseable** - 支持try-with-resources
 *    <pre>
 *    try (RaftClient<T> client = createRaftClient()) {
 *        // 使用client
 *    } // 自动关闭
 *    </pre>
 *
 * 【典型使用场景】
 *
 * 场景1：状态机应用（如KRaft Controller）
 * <pre>
 * // 1. 创建RaftClient
 * RaftClient<ApiMessageAndVersion> raftClient = new KafkaRaftClient<>(...);
 *
 * // 2. 注册监听器
 * raftClient.register(new RaftClient.Listener<>() {
 *     @Override
 *     public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
 *         // 读取已提交的记录并应用到状态机
 *         try {
 *             while (reader.hasNext()) {
 *                 Batch<ApiMessageAndVersion> batch = reader.next();
 *                 for (ApiMessageAndVersion record : batch) {
 *                     stateMachine.apply(record);
 *                 }
 *             }
 *         } finally {
 *             reader.close(); // 必须关闭
 *         }
 *     }
 *
 *     @Override
 *     public void handleLeaderChange(LeaderAndEpoch leader) {
 *         if (leader.isLeader(myNodeId)) {
 *             System.out.println("I am the leader now!");
 *         } else {
 *             System.out.println("New leader: " + leader.leaderId());
 *         }
 *     }
 * });
 *
 * // 3. 写入数据（仅Leader可以写）
 * if (raftClient.leaderAndEpoch().isLeader(myNodeId)) {
 *     long expectedOffset = raftClient.prepareAppend(epoch, records);
 *     raftClient.schedulePreparedAppend();
 * }
 * </pre>
 *
 * 场景2：读取已提交的数据
 * <pre>
 * // 查询高水位（已提交的最高offset）
 * OptionalLong hwm = raftClient.highWatermark();
 * if (hwm.isPresent()) {
 *     System.out.println("High watermark: " + hwm.getAsLong());
 * }
 *
 * // 查询日志末尾
 * long leo = raftClient.logEndOffset();
 * System.out.println("Log end offset: " + leo);
 * </pre>
 *
 * 场景3：优雅关闭
 * <pre>
 * // Leader优雅辞职
 * if (raftClient.leaderAndEpoch().isLeader(myNodeId)) {
 *     raftClient.resign(currentEpoch);
 * }
 *
 * // 等待pending操作完成
 * CompletableFuture<Void> shutdownFuture = raftClient.shutdown(30000);
 * shutdownFuture.get(); // 阻塞等待
 *
 * // 清理资源
 * raftClient.close();
 * </pre>
 *
 * 【与Kafka其他组件的关系】
 *
 * 1. KRaft Controller使用RaftClient
 *    <pre>
 *    QuorumController
 *      └─→ RaftClient<ApiMessageAndVersion>
 *           └─→ KafkaRaftClient<ApiMessageAndVersion>
 *                └─→ ReplicatedLog、QuorumState等
 *    </pre>
 *
 * 2. MetadataShell（只读观察者）
 *    <pre>
 *    MetadataShell
 *      └─→ RaftClient<ApiMessageAndVersion>  // nodeId = OptionalInt.empty()
 *    </pre>
 *
 * 【关键概念】
 *
 * 1. **High Watermark（高水位）**
 *    - 已被多数派确认的最高offset
 *    - 只有 <= 高水位的记录才是已提交的
 *    - 客户端只应该读取 <= 高水位的数据
 *
 * 2. **Log End Offset（日志末尾）**
 *    - 日志中最后一条记录的offset + 1
 *    - LEO >= 高水位（可能有未提交的记录）
 *
 * 3. **Epoch（任期）**
 *    - 每次Leader选举，epoch递增
 *    - 用于检测过期的Leader
 *
 * 4. **Listener（监听器）**
 *    - 状态机通过监听器感知Raft事件
 *    - 必须正确处理回调并关闭资源
 *
 * 【线程安全性】
 *
 * - 大部分方法线程安全，但建议从单线程调用
 * - logEndOffset() 明确标注为线程安全
 * - 监听器回调在Raft内部线程执行，需要小心同步
 *
 * @param <T> 记录类型（如ApiMessageAndVersion、byte[]等）
 * @see KafkaRaftClient Raft客户端的具体实现
 * @see Listener 监听器接口
 * @see BatchReader 批次读取器
 */
public interface RaftClient<T> extends AutoCloseable {

    /**
     * Listener - Raft事件监听器
     *
     * 状态机通过实现此接口来接收Raft的事件通知。
     *
     * 【三种核心事件】
     *
     * 1. handleCommit() - 记录已提交
     *    - 当记录被多数派确认后调用
     *    - 状态机应该应用这些记录
     *
     * 2. handleLoadSnapshot() - 需要加载快照
     *    - 当Follower落后太多，需要从快照恢复
     *    - 状态机应该丢弃所有内存状态，从快照重建
     *
     * 3. handleLeaderChange() - Leader变更
     *    - 当选出新Leader或Leader失效时调用
     *    - 状态机可以据此暂停/恢复写入
     *
     * 【实现注意事项】
     *
     * 1. **必须关闭reader**
     *    <pre>
     *    public void handleCommit(BatchReader<T> reader) {
     *        try {
     *            // 处理记录
     *            while (reader.hasNext()) {
     *                Batch<T> batch = reader.next();
     *                for (T record : batch) {
     *                    apply(record);
     *                }
     *            }
     *        } finally {
     *            reader.close(); // 必须关闭，否则资源泄漏
     *        }
     *    }
     *    </pre>
     *
     * 2. **不要阻塞回调**
     *    - 回调在Raft内部线程执行
     *    - 如果阻塞太久，会影响Raft性能
     *    - 建议快速处理并返回
     *
     * 3. **处理异常**
     *    - 回调抛出异常会导致Raft停止
     *    - 必须捕获所有异常
     *
     * @param <T> 记录类型
     */
    interface Listener<T> {
        /**
         * 处理已提交的记录批次
         *
         * 当记录被多数派确认（达到高水位）后，Raft会调用此方法通知状态机。
         *
         * 【调用时机】
         *
         * - Follower：从Leader同步并提交记录后
         * - Leader：写入日志并被多数派确认后
         *
         * 【重要保证】
         *
         * 1. **顺序性**：记录按照offset顺序调用
         * 2. **至少一次**：同一记录可能被多次调用（重启后）
         * 3. **批次边界**：不会跨越用户指定的批次边界
         *
         * 【批次与append的关系】
         *
         * prepareAppend() 和 handleCommit() 不是一对一的：
         * <pre>
         * // 用户代码
         * raftClient.prepareAppend(epoch, List.of(r1));  // 批次1
         * raftClient.prepareAppend(epoch, List.of(r2));  // 批次2
         * raftClient.schedulePreparedAppend();
         *
         * // Raft可能合并为一个批次调用handleCommit
         * handleCommit(reader);  // reader包含批次1和批次2
         * </pre>
         *
         * 但保证：
         * - r1和r2不会被拆分到不同的批次
         * - 如果r1和r2在同一个prepareAppend()调用中，它们一定在同一个Batch中
         *
         * 【资源管理】
         *
         * 必须调用 reader.close()，否则会导致：
         * - 内存泄漏（批次缓冲区未释放）
         * - 文件句柄泄漏（日志文件未关闭）
         *
         * 推荐使用try-finally：
         * <pre>
         * public void handleCommit(BatchReader<T> reader) {
         *     try {
         *         while (reader.hasNext()) {
         *             Batch<T> batch = reader.next();
         *             processBatch(batch);
         *         }
         *     } finally {
         *         reader.close(); // 确保关闭
         *     }
         * }
         * </pre>
         *
         * @param reader 批次读取器，包含已提交的记录（必须由实现者关闭）
         */
        void handleCommit(BatchReader<T> reader);

        /**
         * 处理快照加载请求
         *
         * 当Follower落后太多（Leader已经删除了需要的日志），需要从快照恢复状态。
         *
         * 【什么时候触发？】
         *
         * 场景1：Follower重启后，发现Leader的日志起始位置 > 本地的日志末尾
         * <pre>
         * Leader: [100, 101, 102, ...] (日志起始offset=100)
         * Follower: [50, 51, 52]       (日志末尾offset=52)
         * → Follower无法从Leader同步，需要快照
         * </pre>
         *
         * 场景2：新加入的节点，本地没有任何日志
         * <pre>
         * 新节点加入集群 → 从Leader获取快照 → 应用快照 → 继续同步增量日志
         * </pre>
         *
         * 【处理要求】
         *
         * 1. **丢弃所有之前的状态**
         *    <pre>
         *    public void handleLoadSnapshot(SnapshotReader<T> reader) {
         *        // 1. 清空内存中的状态
         *        stateMachine.clear();
         *
         *        // 2. 从快照重建状态
         *        try {
         *            while (reader.hasNext()) {
         *                Batch<T> batch = reader.next();
         *                for (T record : batch) {
         *                    stateMachine.apply(record);
         *                }
         *            }
         *        } finally {
         *            reader.close();
         *        }
         *    }
         *    </pre>
         *
         * 2. **假设之前的handleCommit()无效**
         *    - 快照可能覆盖之前handleCommit()的记录
         *    - 必须完全重建状态，不能增量更新
         *
         * 3. **必须关闭reader**
         *    - 快照可能很大（GB级别）
         *    - 不关闭会导致严重的资源泄漏
         *
         * 【快照内容】
         *
         * 快照包含：
         * - 截止到某个offset的所有已提交记录的状态
         * - snapshotId = OffsetAndEpoch(lastOffset + 1, lastEpoch)
         *
         * 示例：
         * <pre>
         * 快照ID = OffsetAndEpoch(100, 5)
         * 表示：快照包含offset < 100的所有记录
         *      这些记录是在epoch <= 5时提交的
         * </pre>
         *
         * @param reader 快照读取器，包含快照数据（必须由实现者关闭）
         */
        void handleLoadSnapshot(SnapshotReader<T> reader);

        /**
         * 处理Leader变更事件
         *
         * 当集群的Leader发生变化时调用此方法。
         *
         * 【调用时机】
         *
         * 1. **选出新Leader**
         *    - epoch递增
         *    - leader.leaderId 包含新Leader的ID
         *
         * 2. **Leader失效**
         *    - epoch可能不变（如果还没选出新Leader）
         *    - leader.leaderId = OptionalInt.empty()
         *
         * 3. **本节点成为Leader**
         *    - leader.leaderId = 本节点ID
         *    - 但会延迟到本节点追上高水位后才通知
         *
         * 【重要保证】
         *
         * 1. **Epoch单调递增**
         *    <pre>
         *    handleLeaderChange(LeaderAndEpoch(empty, 5))   // epoch 5，Leader未知
         *    handleLeaderChange(LeaderAndEpoch(3, 5))       // epoch 5，Leader=3
         *    handleLeaderChange(LeaderAndEpoch(empty, 6))   // epoch 6，新选举开始
         *    handleLeaderChange(LeaderAndEpoch(1, 6))       // epoch 6，Leader=1
         *    </pre>
         *
         * 2. **同一epoch最多两次调用**
         *    - 第一次：epoch变更，Leader未知
         *    - 第二次：发现了Leader
         *
         * 3. **Leader不变性**
         *    - 一旦epoch的Leader确定，就不会再变
         *    - 直到epoch递增
         *
         * 【本节点成为Leader的特殊处理】
         *
         * 如果本节点当选Leader，通知会延迟：
         * <pre>
         * 时刻1：节点当选Leader（epoch=5）
         *       → 本地日志：offset=100
         *       → 高水位：offset=80
         *       → 还不能通知，因为有offset 81-100未提交
         *
         * 时刻2：通过handleCommit()追上高水位
         *       → 应用offset 81-100的记录
         *       → 本地状态追上高水位
         *
         * 时刻3：调用handleLeaderChange(LeaderAndEpoch(myId, 5))
         *       → 现在可以安全地处理写入请求了
         * </pre>
         *
         * 为什么延迟？
         * - 确保Leader的状态机是最新的
         * - 避免基于陈旧状态做决策
         *
         * 【典型用法】
         *
         * <pre>
         * public void handleLeaderChange(LeaderAndEpoch leader) {
         *     if (leader.isLeader(myNodeId)) {
         *         // 我是Leader，可以处理写入
         *         isLeader = true;
         *         System.out.println("I am now the leader for epoch " + leader.epoch());
         *     } else {
         *         // 我不是Leader，暂停写入
         *         isLeader = false;
         *         if (leader.leaderId().isPresent()) {
         *             System.out.println("New leader: " + leader.leaderId().getAsInt());
         *         } else {
         *             System.out.println("Leader unknown for epoch " + leader.epoch());
         *         }
         *     }
         * }
         * </pre>
         *
         * 【默认实现】
         *
         * 提供空的默认实现，因为：
         * - 不是所有状态机都关心Leader变更
         * - 只读观察者不需要处理Leader变更
         *
         * @param leader 当前Leader和epoch
         */
        default void handleLeaderChange(LeaderAndEpoch leader) {}

        /**
         * 开始关闭
         *
         * 当RaftClient开始关闭流程时调用。
         *
         * 用途：
         * - 通知状态机停止接收新请求
         * - 准备清理资源
         *
         * 默认空实现，可选实现。
         */
        default void beginShutdown() {}
    }

    /**
     * 注册一个监听器
     *
     * 监听器会接收提交、快照和Leader变更的通知。
     *
     * 【重要约束】
     *
     * 1. **每个Listener实例只能注册一次**
     *    <pre>
     *    Listener<T> listener1 = new MyListener();
     *    raftClient.register(listener1); // OK
     *    raftClient.register(listener1); // 无效，被忽略
     *
     *    Listener<T> listener2 = new MyListener();
     *    raftClient.register(listener2); // OK，新实例
     *    </pre>
     *
     * 2. **注册后立即生效**
     *    - 可能错过之前的事件
     *    - 只接收注册之后的事件
     *
     * 3. **线程安全**
     *    - 可以从任意线程注册
     *    - 但回调在Raft内部线程执行
     *
     * 【典型用法】
     *
     * <pre>
     * RaftClient.Listener<T> listener = new RaftClient.Listener<>() {
     *     @Override
     *     public void handleCommit(BatchReader<T> reader) {
     *         // 处理提交
     *     }
     *
     *     @Override
     *     public void handleLeaderChange(LeaderAndEpoch leader) {
     *         // 处理Leader变更
     *     }
     * };
     *
     * raftClient.register(listener);
     * </pre>
     *
     * @param listener 要注册的监听器
     */
    void register(Listener<T> listener);

    /**
     * 注销一个监听器
     *
     * 注销后，监听器不再接收任何事件。
     *
     * 【区分新旧事件】
     *
     * 如果需要区分注销前和注销后的事件，必须使用不同的Listener实例：
     * <pre>
     * Listener<T> listener1 = new MyListener();
     * raftClient.register(listener1);
     * // ... 接收事件 ...
     * raftClient.unregister(listener1);
     *
     * // 如果再次注册，使用新实例
     * Listener<T> listener2 = new MyListener();
     * raftClient.register(listener2);
     * </pre>
     *
     * 【幂等性】
     *
     * 如果监听器未曾注册，注销操作会被忽略（不报错）：
     * <pre>
     * raftClient.unregister(neverRegistered); // OK，被忽略
     * </pre>
     *
     * @param listener 要注销的监听器
     */
    void unregister(Listener<T> listener);

    /**
     * 返回当前的高水位（High Watermark）
     *
     * 【什么是高水位？】
     *
     * 高水位 = 被多数派确认的最高offset
     *
     * 示例：
     * <pre>
     * 3节点集群：
     * Leader:     [0, 1, 2, 3, 4, 5]  (LEO=6)
     * Follower1:  [0, 1, 2, 3]        (LEO=4)
     * Follower2:  [0, 1, 2]           (LEO=3)
     *
     * 高水位 = 3 (多数派都有offset 0-2，3还没有)
     * </pre>
     *
     * 【为什么可能是empty？】
     *
     * 1. **刚启动**
     *    - 还没有选出Leader
     *    - 高水位未知
     *
     * 2. **选举中**
     *    - Leader正在切换
     *    - 暂时无法确定高水位
     *
     * 【使用场景】
     *
     * <pre>
     * // 检查某个offset是否已提交
     * OptionalLong hwm = raftClient.highWatermark();
     * if (hwm.isPresent() && offset <= hwm.getAsLong()) {
     *     // offset已提交，可以安全读取
     * } else {
     *     // offset未提交或高水位未知
     * }
     * </pre>
     *
     * @return 高水位，如果未知则返回OptionalLong.empty()
     */
    OptionalLong highWatermark();

    /**
     * 返回当前的Leader和epoch
     *
     * 【返回值说明】
     *
     * 1. **有Leader**
     *    <pre>
     *    LeaderAndEpoch(leaderId=Optional[1], epoch=5)
     *    // 表示：epoch 5的Leader是节点1
     *    </pre>
     *
     * 2. **Leader未知**
     *    <pre>
     *    LeaderAndEpoch(leaderId=Optional.empty(), epoch=5)
     *    // 表示：epoch 5，但不知道谁是Leader（可能正在选举）
     *    </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * LeaderAndEpoch leader = raftClient.leaderAndEpoch();
     *
     * // 检查是否是Leader
     * if (leader.isLeader(myNodeId)) {
     *     // 我是Leader，可以写入
     *     raftClient.prepareAppend(leader.epoch(), records);
     * } else {
     *     // 我不是Leader，拒绝写入
     *     throw new NotLeaderException(...);
     * }
     * </pre>
     *
     * @return 当前Leader和epoch
     */
    LeaderAndEpoch leaderAndEpoch();

    /**
     * 获取本地节点ID
     *
     * 【什么时候为empty？】
     *
     * 作为**匿名观察者**时，没有节点ID：
     * <pre>
     * // 示例：metadata shell工具
     * // 只读取元数据，不参与投票
     * RaftClient<T> client = createObserverClient();
     * OptionalInt nodeId = client.nodeId();
     * assert nodeId.isEmpty(); // 匿名观察者
     * </pre>
     *
     * 【正常集群成员】
     *
     * <pre>
     * RaftClient<T> client = createVoterClient(nodeId=1);
     * OptionalInt nodeId = client.nodeId();
     * assert nodeId.getAsInt() == 1;
     * </pre>
     *
     * @return 本地节点ID，如果是匿名观察者则为OptionalInt.empty()
     */
    OptionalInt nodeId();

    /**
     * 返回指定投票者的节点信息
     *
     * 用于获取集群中其他节点的网络地址。
     *
     * 【使用场景】
     *
     * <pre>
     * // 查询节点1在INTERNAL监听器上的地址
     * Optional<Node> node = raftClient.voterNode(1, ListenerName.normalised("INTERNAL"));
     *
     * if (node.isPresent()) {
     *     System.out.println("Node 1: " + node.get().host() + ":" + node.get().port());
     * } else {
     *     System.out.println("Node 1 not found or not a voter");
     * }
     * </pre>
     *
     * 【返回empty的情况】
     *
     * 1. 节点ID不存在
     * 2. 节点不是投票者（是观察者）
     * 3. 节点在指定监听器上没有地址
     *
     * @param id 投票者的ID
     * @param listenerName 监听器名称
     * @return 节点信息，如果不存在则返回Optional.empty()
     */
    Optional<Node> voterNode(int id, ListenerName listenerName);

    /**
     * 准备追加记录到日志
     *
     * 【两阶段写入的第一阶段】
     *
     * 此方法只是"准备"数据，并不真正写入日志。
     * 必须调用schedulePreparedAppend()才会真正写入。
     *
     * 【为什么两阶段？】
     *
     * 1. **批处理优化**
     *    <pre>
     *    // 准备多个批次
     *    long offset1 = raftClient.prepareAppend(epoch, records1);
     *    long offset2 = raftClient.prepareAppend(epoch, records2);
     *    long offset3 = raftClient.prepareAppend(epoch, records3);
     *
     *    // 一次性写入，减少I/O
     *    raftClient.schedulePreparedAppend();
     *    </pre>
     *
     * 2. **更好的错误处理**
     *    <pre>
     *    try {
     *        long offset = raftClient.prepareAppend(epoch, records);
     *        // 在真正写入前，可以做一些检查
     *        validateOffset(offset);
     *        raftClient.schedulePreparedAppend();
     *    } catch (NotLeaderException e) {
     *        // epoch不匹配，我不是Leader了
     *    }
     *    </pre>
     *
     * 【Epoch检查】
     *
     * 如果传入的epoch与当前epoch不匹配，抛出NotLeaderException：
     * <pre>
     * 场景：
     * 1. 状态机认为自己是epoch 5的Leader
     * 2. 但Raft已经切换到epoch 6了
     * 3. prepareAppend(5, records) → NotLeaderException
     * 4. 状态机收到异常，知道需要更新epoch
     * </pre>
     *
     * 【返回值】
     *
     * 返回最后一条记录的**预期offset**：
     * <pre>
     * 当前LEO = 100
     * prepareAppend(epoch, [r1, r2, r3]) → 返回102
     * // r1=100, r2=101, r3=102
     * </pre>
     *
     * 注意：
     * - 这只是预期offset，还没真正写入
     * - schedulePreparedAppend()后才会真正分配offset
     *
     * 【异常情况】
     *
     * 1. **RecordBatchTooLargeException**
     *    - 记录批次超过最大大小限制
     *    - 所有记录都不会被提交
     *
     * 2. **NotLeaderException**
     *    - 不是当前Leader
     *    - 或epoch不匹配
     *
     * 3. **BufferAllocationException**
     *    - 内存不足，无法分配缓冲区
     *
     * 4. **IllegalStateException**
     *    - 累积的批次数达到上限
     *    - 需要先调用schedulePreparedAppend()
     *
     * 【重要保证】
     *
     * 同一个prepareAppend()中的所有记录：
     * - 要么全部提交
     * - 要么全部不提交
     * - 不会部分提交
     *
     * @param epoch 当前Leader的epoch
     * @param records 要追加的记录列表
     * @return 最后一条记录的预期offset
     * @throws org.apache.kafka.common.errors.RecordBatchTooLargeException 批次过大
     * @throws NotLeaderException 不是Leader或epoch不匹配
     * @throws BufferAllocationException 内存分配失败
     * @throws IllegalStateException 批次数达到上限
     */
    long prepareAppend(int epoch, List<T> records);

    /**
     * 调度所有准备好的批次，写入日志
     *
     * 【两阶段写入的第二阶段】
     *
     * 将之前通过prepareAppend()准备的所有批次真正写入日志。
     *
     * 【批量写入】
     *
     * <pre>
     * // 准备3个批次
     * raftClient.prepareAppend(epoch, records1); // 批次1
     * raftClient.prepareAppend(epoch, records2); // 批次2
     * raftClient.prepareAppend(epoch, records3); // 批次3
     *
     * // 一次性写入所有3个批次
     * raftClient.schedulePreparedAppend();
     * // → 一次fsync，性能更好
     * </pre>
     *
     * 【幂等性】
     *
     * 如果没有准备的批次，此方法什么也不做：
     * <pre>
     * raftClient.schedulePreparedAppend(); // OK，什么也不做
     * </pre>
     *
     * 【异常情况】
     *
     * @throws NotLeaderException 不是当前Leader（可能在prepare和schedule之间失去了Leader地位）
     */
    void schedulePreparedAppend();

    /**
     * 优雅关闭客户端
     *
     * 允许Leader主动辞职并帮助选出新Leader，而不是让其他节点等待超时。
     *
     * 【什么是优雅关闭？】
     *
     * 对比：
     *
     * 1. **非优雅关闭（直接kill）**
     *    <pre>
     *    Leader突然崩溃
     *    → Follower等待选举超时（默认几秒）
     *    → 开始新选举
     *    → 选出新Leader
     *    总时间：选举超时 + 选举时间 = 5-10秒
     *    </pre>
     *
     * 2. **优雅关闭**
     *    <pre>
     *    Leader调用shutdown()
     *    → Leader主动辞职（发送EndQuorumEpoch）
     *    → Follower立即开始选举
     *    → 快速选出新Leader
     *    总时间：1-2秒
     *    </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * // 正常关闭流程
     * try {
     *     // 1. 停止接收新请求
     *     stopAcceptingRequests();
     *
     *     // 2. 等待pending操作完成，最多等30秒
     *     CompletableFuture<Void> shutdownFuture = raftClient.shutdown(30000);
     *     shutdownFuture.get(30, TimeUnit.SECONDS);
     *
     *     // 3. 清理资源
     *     raftClient.close();
     * } catch (TimeoutException e) {
     *     // 超时，强制关闭
     *     raftClient.close();
     * }
     * </pre>
     *
     * 【什么时候不应该调用？】
     *
     * 如果Raft已经遇到异常并处于不确定状态，跳过shutdown()：
     * <pre>
     * if (raftEncounteredFatalError) {
     *     // 直接关闭，不要尝试优雅关闭
     *     raftClient.close();
     * } else {
     *     // 正常情况，优雅关闭
     *     raftClient.shutdown(30000).get();
     *     raftClient.close();
     * }
     * </pre>
     *
     * 【超时行为】
     *
     * - 超时后，CompletableFuture会完成
     * - 但不保证所有pending操作都完成了
     * - 需要继续调用close()清理资源
     *
     * @param timeoutMs 等待超时时间（毫秒）
     * @return CompletableFuture，在关闭完成或超时时完成
     */
    CompletableFuture<Void> shutdown(int timeoutMs);

    /**
     * 辞去Leader职位
     *
     * Leader主动放弃领导权，触发新一轮选举。
     *
     * 【使用场景】
     *
     * 1. **负载均衡**
     *    <pre>
     *    // Leader负载过高，主动让出
     *    if (cpuUsage > 90%) {
     *        raftClient.resign(currentEpoch);
     *    }
     *    </pre>
     *
     * 2. **优雅关闭**
     *    <pre>
     *    // 关闭前先辞职，加速Leader切换
     *    raftClient.resign(currentEpoch);
     *    raftClient.shutdown(30000);
     *    </pre>
     *
     * 3. **手动干预**
     *    <pre>
     *    // 管理员命令：让出领导权
     *    admin.resignLeadership();
     *    </pre>
     *
     * 【Epoch检查】
     *
     * 只有当传入的epoch >= 当前epoch时才会辞职：
     * <pre>
     * 当前epoch = 5
     * resign(4) → 被忽略（过期的epoch）
     * resign(5) → 辞职成功
     * resign(6) → IllegalArgumentException（未来的epoch）
     * </pre>
     *
     * 【辞职后会怎样？】
     *
     * 1. Leader发送EndQuorumEpoch给所有Follower
     * 2. Follower立即开始选举
     * 3. 可能选出新Leader（也可能还是自己）
     * 4. 通过handleLeaderChange()通知状态机
     *
     * 【注意事项】
     *
     * - 辞职不保证不会再次当选
     * - 如果没有其他合格的候选者，可能还是自己当选
     *
     * @param epoch 要辞职的epoch（必须 <= 当前epoch）
     * @throws IllegalArgumentException 如果epoch无效（负数或大于当前epoch）或不是Leader
     */
    void resign(int epoch);

    /**
     * 创建快照
     *
     * 为已提交的数据创建一个可写的快照文件。
     *
     * 【什么是快照？】
     *
     * 快照 = 某个时间点的完整状态副本
     *
     * 用途：
     * 1. 加速Follower恢复（不需要重放所有日志）
     * 2. 清理旧日志（快照之前的日志可以删除）
     *
     * 【快照ID的含义】
     *
     * snapshotId = OffsetAndEpoch(offset, epoch)
     * - offset：快照的**排他上界**（不包含此offset）
     * - epoch：此offset对应的epoch
     *
     * 示例：
     * <pre>
     * 快照ID = OffsetAndEpoch(100, 5)
     * 表示：
     * - 快照包含offset 0-99的所有记录
     * - 不包含offset 100
     * - 这些记录在epoch <= 5时提交
     * </pre>
     *
     * 【使用流程】
     *
     * <pre>
     * // 1. 确定快照位置（通常是当前高水位）
     * OptionalLong hwm = raftClient.highWatermark();
     * OffsetAndEpoch snapshotId = new OffsetAndEpoch(hwm.getAsLong(), currentEpoch);
     *
     * // 2. 创建快照写入器
     * Optional<SnapshotWriter<T>> writerOpt = raftClient.createSnapshot(
     *     snapshotId,
     *     lastAppendTime
     * );
     *
     * // 3. 写入快照
     * if (writerOpt.isPresent()) {
     *     try (SnapshotWriter<T> writer = writerOpt.get()) {
     *         // 写入状态机的所有状态
     *         for (T record : stateMachine.getAllRecords()) {
     *             writer.append(record);
     *         }
     *         writer.freeze(); // 完成写入
     *     }
     * } else {
     *     // 快照已存在，无需重新创建
     * }
     * </pre>
     *
     * 【为什么可能返回empty？】
     *
     * 如果快照已存在，返回Optional.empty()：
     * - 避免重复创建
     * - 节省磁盘空间
     *
     * 【特殊情况：空快照】
     *
     * 可以使用offset=0, epoch=0创建空快照：
     * <pre>
     * // 节点刚启动，还没有任何提交
     * Optional<SnapshotWriter<T>> writer = raftClient.createSnapshot(
     *     new OffsetAndEpoch(0, 0),
     *     0
     * );
     * // 创建一个空快照
     * </pre>
     *
     * 【异常情况】
     *
     * @throws IllegalArgumentException 如果：
     *         - offset > 高水位（不能为未提交的数据创建快照）
     *         - offset < 日志起始位置
     *
     * @param snapshotId 快照ID（包含排他上界offset和epoch）
     * @param lastContainedLogTime 快照包含的最高记录的追加时间
     * @return 快照写入器，如果快照已存在则返回Optional.empty()
     */
    Optional<SnapshotWriter<T>> createSnapshot(OffsetAndEpoch snapshotId, long lastContainedLogTime);

    /**
     * 获取最新快照的ID
     *
     * 返回当前存在的最新快照的标识符。
     *
     * 【返回值】
     *
     * - 如果有快照：Optional.of(OffsetAndEpoch)
     * - 如果没有快照：Optional.empty()
     *
     * 【使用场景】
     *
     * <pre>
     * // 检查是否需要创建新快照
     * Optional<OffsetAndEpoch> latestSnapshot = raftClient.latestSnapshotId();
     * OptionalLong hwm = raftClient.highWatermark();
     *
     * if (hwm.isPresent()) {
     *     long hwmValue = hwm.getAsLong();
     *     long snapshotOffset = latestSnapshot
     *         .map(OffsetAndEpoch::offset)
     *         .orElse(0L);
     *
     *     if (hwmValue - snapshotOffset > 10000) {
     *         // 快照落后超过10000条记录，创建新快照
     *         createSnapshot(new OffsetAndEpoch(hwmValue, currentEpoch), ...);
     *     }
     * }
     * </pre>
     *
     * @return 最新快照的ID，如果不存在则返回Optional.empty()
     */
    Optional<OffsetAndEpoch> latestSnapshotId();

    /**
     * 返回日志末尾位置（Log End Offset）
     *
     * 【什么是LEO？】
     *
     * LEO = 下一条记录将要写入的位置
     *     = 最后一条记录的offset + 1
     *
     * 示例：
     * <pre>
     * 日志：[offset=0, offset=1, offset=2]
     * LEO = 3 (下一条记录的offset)
     *
     * 空日志：[]
     * LEO = 0
     * </pre>
     *
     * 【LEO vs 高水位】
     *
     * <pre>
     * Leader日志：[0, 1, 2, 3, 4, 5]
     * LEO = 6
     * 高水位 = 3 (假设Follower只同步到2)
     *
     * → offset 0-2：已提交（可以读取）
     * → offset 3-5：未提交（不能读取）
     * </pre>
     *
     * 【线程安全性】
     *
     * 此方法**明确标注为线程安全**，可以从任意线程调用。
     *
     * 【使用场景】
     *
     * <pre>
     * // 检查有多少未提交记录
     * long leo = raftClient.logEndOffset();
     * OptionalLong hwm = raftClient.highWatermark();
     * if (hwm.isPresent()) {
     *     long uncommitted = leo - hwm.getAsLong();
     *     System.out.println("Uncommitted records: " + uncommitted);
     * }
     * </pre>
     *
     * @return 日志末尾位置，如果日志为空则返回0
     */
    long logEndOffset();

    /**
     * 返回当前的KRaft版本
     *
     * 返回最新的kraft.version，即使还没有持久化提交到Raft。
     *
     * 【KRaft版本】
     *
     * KRaft版本控制协议特性：
     * - 类似于Kafka的API版本
     * - 允许渐进式升级集群
     *
     * 示例：
     * <pre>
     * KRaftVersion version = raftClient.kraftVersion();
     * if (version.isAtLeast(KRaftVersion.KRAFT_VERSION_1)) {
     *     // 使用新特性
     * } else {
     *     // 使用旧行为
     * }
     * </pre>
     *
     * 【未提交的版本】
     *
     * 此方法返回最新版本，即使还未提交：
     * - Leader调用upgradeKRaftVersion()后立即生效
     * - 但可能还没有复制到多数派
     *
     * @return 当前kraft.version
     */
    KRaftVersion kraftVersion();

    /**
     * 请求Leader升级KRaft版本
     *
     * 【版本升级流程】
     *
     * 1. **验证阶段（validateOnly=true）**
     *    <pre>
     *    // 先验证能否升级
     *    try {
     *        raftClient.upgradeKRaftVersion(epoch, newVersion, true);
     *        // 验证通过，可以升级
     *    } catch (ApiException e) {
     *        // 验证失败
     *    }
     *    </pre>
     *
     * 2. **执行阶段（validateOnly=false）**
     *    <pre>
     *    // 真正升级
     *    raftClient.upgradeKRaftVersion(epoch, newVersion, false);
     *    // 版本升级记录会被写入Raft日志
     *    </pre>
     *
     * 【验证检查】
     *
     * 常见的验证失败原因：
     * - 新版本 < 当前版本（不支持降级）
     * - 集群中有节点不支持新版本
     * - 版本跳跃过大（必须逐级升级）
     *
     * 【使用场景】
     *
     * <pre>
     * // 集群升级流程
     * KRaftVersion currentVersion = raftClient.kraftVersion();
     * KRaftVersion targetVersion = KRaftVersion.KRAFT_VERSION_1;
     *
     * if (currentVersion.isAtLeast(targetVersion)) {
     *     System.out.println("Already at target version");
     *     return;
     * }
     *
     * // 先验证
     * try {
     *     raftClient.upgradeKRaftVersion(epoch, targetVersion, true);
     *     System.out.println("Validation passed");
     * } catch (ApiException e) {
     *     System.out.println("Validation failed: " + e.getMessage());
     *     return;
     * }
     *
     * // 再升级
     * raftClient.upgradeKRaftVersion(epoch, targetVersion, false);
     * System.out.println("Upgrade initiated");
     * </pre>
     *
     * @param epoch 当前epoch
     * @param version 目标KRaft版本
     * @param validateOnly 是否只验证不执行
     * @throws ApiException 验证失败时抛出
     */
    void upgradeKRaftVersion(
        int epoch,
        KRaftVersion version,
        boolean validateOnly
    );
}
