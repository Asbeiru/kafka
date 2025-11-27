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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;

import java.util.Optional;

/**
 * ReplicatedLog - 复制日志接口
 *
 * 这是KRaft中最核心的日志抽象，封装了所有与日志存储、复制、快照相关的操作。
 *
 * 【什么是ReplicatedLog？】
 *
 * ReplicatedLog代表一个被Raft协议复制的分区日志：
 * 1. **日志写入**：Leader写入、Follower追加
 * 2. **日志读取**：按offset范围读取
 * 3. **日志截断**：处理冲突、清理旧数据
 * 4. **快照管理**：创建、读取、删除快照
 * 5. **元数据管理**：高水位、epoch、offset等
 *
 * 【核心概念】
 *
 * 1. **Log End Offset (LEO)**
 *    - 日志中最后一条记录的offset + 1
 *    - 即下一条记录将要写入的位置
 *
 * 2. **High Watermark (HWM)**
 *    - 被多数派确认的最高offset
 *    - 只有 <= HWM的记录才是已提交的
 *
 * 3. **Log Start Offset**
 *    - 日志的起始offset
 *    - 通常 = 最新快照的endOffset
 *
 * 4. **Epoch**
 *    - Leader任期编号
 *    - 用于检测日志冲突
 *
 * 【日志结构示意】
 *
 * <pre>
 * 快照                    日志段
 * ├─────────────┤├──────────────────────────────────┤
 * offset:  0   100                              500  600
 *              ↑                                 ↑    ↑
 *         startOffset                          HWM  LEO
 *
 * - offset 0-99: 已被快照覆盖，日志已删除
 * - offset 100-500: 已提交（可读）
 * - offset 501-599: 未提交（Leader独有，Follower可能没有）
 * </pre>
 *
 * 【Leader vs Follower的使用差异】
 *
 * Leader使用：
 * <pre>
 * // 1. 接收客户端写入
 * LogAppendInfo info = log.appendAsLeader(records, currentEpoch);
 *
 * // 2. 复制给Follower（通过FetchRequest）
 *
 * // 3. 收到多数派确认后更新高水位
 * log.updateHighWatermark(new LogOffsetMetadata(newHwm, ...));
 * </pre>
 *
 * Follower使用：
 * <pre>
 * // 1. 从Leader拉取日志
 * Records records = fetchFromLeader();
 *
 * // 2. 追加到本地日志
 * LogAppendInfo info = log.appendAsFollower(records, currentEpoch);
 *
 * // 3. Leader在FetchResponse中告知高水位
 * log.updateHighWatermark(leaderHwm);
 * </pre>
 *
 * 【日志冲突处理】
 *
 * Raft通过epoch检测日志冲突：
 * <pre>
 * Leader: [e1:0, e1:1, e2:2, e2:3, e3:4]
 * Follower: [e1:0, e1:1, e2:2, e4:3, e4:4, e4:5]
 *                              ↑ 冲突点
 *
 * 处理流程：
 * 1. Leader发现Follower的offset=3的epoch=e4 != e2
 * 2. Leader告诉Follower：你的日志从offset=3开始有冲突
 * 3. Follower截断：truncateTo(3)
 * 4. Follower重新从Leader同步
 * </pre>
 *
 * 【快照机制】
 *
 * 快照用于：
 * 1. **压缩日志**：删除旧日志，节省磁盘
 * 2. **加速恢复**：新节点或落后节点可以从快照恢复
 *
 * 创建快照流程：
 * <pre>
 * // 1. 状态机决定创建快照（如每10000条记录）
 * OffsetAndEpoch snapshotId = new OffsetAndEpoch(hwm, epoch);
 *
 * // 2. 创建快照写入器
 * Optional<RawSnapshotWriter> writer = log.createNewSnapshot(snapshotId);
 *
 * // 3. 写入状态机数据
 * if (writer.isPresent()) {
 *     try (RawSnapshotWriter w = writer.get()) {
 *         w.append(...);
 *         w.freeze(); // 完成写入
 *     }
 * }
 *
 * // 4. 通知日志
 * log.onSnapshotFrozen(snapshotId);
 *
 * // 5. 删除旧日志
 * log.deleteBeforeSnapshot(snapshotId);
 * </pre>
 *
 * 【实现类】
 *
 * KRaft中的实现：
 * - **KafkaRaftLog**：基于Kafka Log的实现
 *   - 复用Kafka的日志存储
 *   - 支持segment、索引、压缩等
 *
 * 【线程安全性】
 *
 * - 实现类通常不是线程安全的
 * - 应该从单线程调用（Raft主循环线程）
 * - 读写操作必须串行化
 *
 * 【与其他组件的关系】
 *
 * <pre>
 * RaftClient
 *   └─→ KafkaRaftClient
 *        └─→ ReplicatedLog (日志存储)
 *             └─→ KafkaRaftLog
 *                  ├─→ UnifiedLog (底层Kafka日志)
 *                  └─→ SnapshotRegistry (快照管理)
 * </pre>
 *
 * @see KafkaRaftLog ReplicatedLog的Kafka实现
 * @see LogAppendInfo 追加日志的元数据
 * @see LogFetchInfo 读取日志的元数据
 */
public interface ReplicatedLog extends AutoCloseable {

    // ==================== 写入操作 ====================

    /**
     * 以Leader身份写入记录到本地日志
     *
     * 这是Leader处理客户端写入请求的核心方法。
     *
     * 【什么时候调用？】
     *
     * 只有Leader才能调用此方法：
     * <pre>
     * if (isLeader) {
     *     LogAppendInfo info = log.appendAsLeader(records, currentEpoch);
     *     // 返回的info包含：baseOffset, lastOffset等
     * }
     * </pre>
     *
     * 【与appendAsFollower的区别】
     *
     * appendAsLeader特有的操作：
     * 1. **分配offset**：为每条记录分配全局唯一的offset
     * 2. **分配epoch**：所有记录的epoch = 当前Leader的epoch
     * 3. **额外验证**：检查batch的一致性
     *
     * 对比：
     * <pre>
     * // Leader写入
     * Records records = createRecords([r1, r2, r3]); // 没有offset
     * LogAppendInfo info = appendAsLeader(records, 5); // epoch=5
     * // → r1=offset 100, epoch=5
     * // → r2=offset 101, epoch=5
     * // → r3=offset 102, epoch=5
     *
     * // Follower写入（从Leader同步）
     * Records records = fetchFromLeader(); // 已经有offset和epoch
     * LogAppendInfo info = appendAsFollower(records, 5);
     * // → 直接使用records中的offset和epoch
     * </pre>
     *
     * 【原子性保证】
     *
     * 所有记录要么全部写入成功，要么全部失败：
     * <pre>
     * try {
     *     LogAppendInfo info = log.appendAsLeader(records, epoch);
     *     // 所有记录都写入成功
     * } catch (Exception e) {
     *     // 没有任何记录被写入
     * }
     * </pre>
     *
     * 【返回值】
     *
     * LogAppendInfo包含：
     * - baseOffset：第一条记录的offset
     * - lastOffset：最后一条记录的offset
     * - maxTimestamp：最大时间戳
     * - logAppendTime：追加时间
     *
     * <pre>
     * LogAppendInfo info = log.appendAsLeader(records, epoch);
     * System.out.println("Wrote offsets " + info.firstOffset() + " to " + info.lastOffset());
     * </pre>
     *
     * 【异常情况】
     *
     * 1. **IllegalArgumentException** - 记录集为空
     *    <pre>
     *    Records empty = MemoryRecords.EMPTY;
     *    log.appendAsLeader(empty, epoch); // 抛出异常
     *    </pre>
     *
     * 2. **RuntimeException** - batch的baseOffset与LEO不匹配
     *    <pre>
     *    // 这不应该发生，表示代码bug
     *    // batch.baseOffset() != log.endOffset().offset()
     *    </pre>
     *
     * 【使用示例】
     *
     * <pre>
     * // Leader处理客户端写入
     * public CompletableFuture<Long> append(List<ApiMessage> messages) {
     *     // 1. 序列化为Records
     *     MemoryRecords records = serialize(messages);
     *
     *     // 2. 写入本地日志
     *     LogAppendInfo info = replicatedLog.appendAsLeader(records, currentEpoch);
     *
     *     // 3. 返回最后一条记录的offset
     *     long lastOffset = info.lastOffset();
     *
     *     // 4. 等待复制到多数派（在其他地方处理）
     *     return waitForCommit(lastOffset);
     * }
     * </pre>
     *
     * @param records 要追加的记录批次（每个batch的所有记录必须一起写入）
     * @param epoch 当前Leader的epoch
     * @return 追加操作的元数据信息
     * @throws IllegalArgumentException 如果记录集为空
     * @throws RuntimeException 如果batch的baseOffset与日志末尾不匹配
     */
    LogAppendInfo appendAsLeader(Records records, int epoch);

    /**
     * 追加从Leader复制的记录
     *
     * Follower使用此方法将从Leader同步的日志追加到本地。
     *
     * 【与appendAsLeader的主要区别】
     *
     * 1. **不分配offset**
     *    - Leader已经分配了offset
     *    - Follower直接使用
     *
     * 2. **不分配epoch**
     *    - 每个batch已经有自己的epoch
     *    - 可能跨越多个epoch
     *
     * 3. **验证更宽松**
     *    - 信任Leader发送的数据
     *    - 减少验证开销
     *
     * 【Epoch过滤】
     *
     * 重要：此方法会过滤掉epoch > 传入epoch的批次：
     * <pre>
     * Records records = [
     *     Batch(epoch=3, offsets=100-105),
     *     Batch(epoch=4, offsets=106-110),
     *     Batch(epoch=5, offsets=111-115)  // 会被忽略
     * ];
     *
     * LogAppendInfo info = log.appendAsFollower(records, 4); // 只追加epoch <= 4的批次
     * // 结果：只写入offset 100-110，忽略111-115
     * </pre>
     *
     * 为什么需要过滤？
     * - Leader可能发送了新epoch的数据
     * - 但Follower还没有进入新epoch
     * - 必须等到Follower收到BeginQuorumEpoch后才能接受新epoch的数据
     *
     * 【使用场景】
     *
     * <pre>
     * // Follower的日志同步流程
     *
     * // 1. 发送FetchRequest给Leader
     * FetchRequest request = new FetchRequest(fetchOffset, currentEpoch);
     *
     * // 2. 收到FetchResponse
     * FetchResponse response = sendAndReceive(request);
     * Records records = response.records();
     *
     * // 3. 追加到本地日志
     * LogAppendInfo info = replicatedLog.appendAsFollower(records, currentEpoch);
     *
     * // 4. 更新高水位（从Leader获取）
     * replicatedLog.updateHighWatermark(response.highWatermark());
     * </pre>
     *
     * 【返回值】
     *
     * LogAppendInfo包含实际写入的记录信息：
     * - 如果所有记录都被过滤掉，返回空的LogAppendInfo
     * - 否则返回实际写入的范围
     *
     * @param records 从Leader复制的记录（可能跨越多个epoch）
     * @param epoch Follower当前的epoch（用于过滤）
     * @return 追加操作的元数据信息
     * @throws IllegalArgumentException 如果记录集为空
     * @throws RuntimeException 如果batch的baseOffset与日志末尾不匹配
     */
    LogAppendInfo appendAsFollower(Records records, int epoch);

    // ==================== 读取操作 ====================

    /**
     * 读取指定范围的记录
     *
     * 从指定offset开始读取日志记录。
     *
     * 【读取范围】
     *
     * 读取从startOffsetInclusive开始的记录：
     * <pre>
     * // 日志：[100, 101, 102, 103, 104, 105]
     * LogFetchInfo info = log.read(102, Isolation.UNCOMMITTED);
     * // 返回：[102, 103, 104, 105]
     * </pre>
     *
     * 【隔离级别】
     *
     * 两种隔离级别：
     *
     * 1. **Isolation.COMMITTED** - 只读取已提交的记录
     *    <pre>
     *    LEO = 200, HWM = 150
     *    LogFetchInfo info = log.read(100, Isolation.COMMITTED);
     *    // 最多读到offset 150（高水位）
     *    </pre>
     *
     * 2. **Isolation.UNCOMMITTED** - 读取所有记录（包括未提交）
     *    <pre>
     *    LEO = 200, HWM = 150
     *    LogFetchInfo info = log.read(100, Isolation.UNCOMMITTED);
     *    // 最多读到offset 199（LEO - 1）
     *    </pre>
     *
     * 【返回值】
     *
     * LogFetchInfo包含：
     * - records：读取到的记录
     * - divergingEpoch：如果发现日志冲突，包含冲突信息
     * - highWatermark：当前高水位（仅Isolation.COMMITTED时使用）
     *
     * 【使用场景】
     *
     * Leader读取日志发送给Follower：
     * <pre>
     * // Leader处理Follower的FetchRequest
     * long fetchOffset = request.fetchOffset();
     * LogFetchInfo info = log.read(fetchOffset, Isolation.UNCOMMITTED);
     *
     * // 发送给Follower
     * FetchResponse response = new FetchResponse(info.records, log.highWatermark());
     * </pre>
     *
     * 状态机读取已提交的记录：
     * <pre>
     * // 只读取已提交的数据
     * LogFetchInfo info = log.read(lastAppliedOffset + 1, Isolation.COMMITTED);
     * for (Record r : info.records) {
     *     stateMachine.apply(r);
     * }
     * </pre>
     *
     * @param startOffsetInclusive 起始offset（包含）
     * @param isolation 隔离级别（COMMITTED或UNCOMMITTED）
     * @return 读取结果，包含记录和元数据
     */
    LogFetchInfo read(long startOffsetInclusive, Isolation isolation);

    // ==================== 元数据查询 ====================

    /**
     * 返回日志中最后一个epoch
     *
     * 【什么是lastFetchedEpoch？】
     *
     * 这是日志中最后一个批次的epoch：
     * <pre>
     * 日志：
     * [Batch(epoch=1, offset=0-10),
     *  Batch(epoch=2, offset=11-20),
     *  Batch(epoch=3, offset=21-30)]
     *
     * lastFetchedEpoch() = 3
     * </pre>
     *
     * 【空日志的特殊情况】
     *
     * 如果日志为空，返回0（"原始epoch"）：
     * <pre>
     * 空日志 → lastFetchedEpoch() = 0
     * </pre>
     *
     * 为什么是0而不是Optional？
     * - 简化代码，避免到处使用Optional
     * - 0作为"原始epoch"，永远不会有Leader
     * - epoch总是从1开始递增
     *
     * 【使用场景】
     *
     * 检查日志是否up-to-date：
     * <pre>
     * // 投票时检查候选者的日志是否足够新
     * boolean isLogUpToDate(int candidateEpoch, long candidateOffset) {
     *     int myLastEpoch = log.lastFetchedEpoch();
     *     long myLastOffset = log.endOffset().offset() - 1;
     *
     *     // 先比较epoch
     *     if (candidateEpoch > myLastEpoch) {
     *         return true; // 候选者的epoch更大
     *     } else if (candidateEpoch < myLastEpoch) {
     *         return false; // 候选者的epoch太小
     *     } else {
     *         // epoch相同，比较offset
     *         return candidateOffset >= myLastOffset;
     *     }
     * }
     * </pre>
     *
     * @return 最后一个批次的epoch，如果日志为空则返回0
     */
    int lastFetchedEpoch();

    /**
     * 查找指定epoch的结束位置
     *
     * 返回第一个 <= epoch的epoch及其end offset。
     *
     * 【什么是epoch的endOffset？】
     *
     * 每个epoch对应一段日志范围，endOffset是这段范围的上界（不包含）：
     * <pre>
     * Epoch 1: offset 0-10   → endOffset = 11
     * Epoch 2: offset 11-20  → endOffset = 21
     * Epoch 3: offset 21-30  → endOffset = 31
     *
     * endOffsetForEpoch(2) = OffsetAndEpoch(21, 2)
     * // 表示：epoch 2结束于offset 21（不包含）
     * </pre>
     *
     * 【查找规则】
     *
     * 查找第一个 <= 传入epoch的epoch：
     * <pre>
     * 日志的epoch序列：[1, 2, 4, 5]
     *
     * endOffsetForEpoch(3) = OffsetAndEpoch(endOf2, 2)
     * // epoch 3不存在，返回最接近的epoch 2
     *
     * endOffsetForEpoch(5) = OffsetAndEpoch(endOf5, 5)
     * // epoch 5存在，返回epoch 5
     *
     * endOffsetForEpoch(6) = OffsetAndEpoch(endOf5, 5)
     * // epoch 6太大，返回最大的epoch 5
     * </pre>
     *
     * 【核心用途：日志冲突检测】
     *
     * Leader使用此方法检测Follower的日志是否有冲突：
     * <pre>
     * // Follower告诉Leader：我的offset 15对应epoch 3
     * int followerEpoch = 3;
     * long followerOffset = 15;
     *
     * // Leader检查自己的日志
     * OffsetAndEpoch leaderEnd = log.endOffsetForEpoch(followerEpoch);
     *
     * if (leaderEnd.epoch() != followerEpoch) {
     *     // Leader没有epoch 3，说明Follower的日志有问题
     *     // 让Follower截断到leaderEnd.offset()
     * } else if (leaderEnd.offset() < followerOffset) {
     *     // Leader的epoch 3比Follower短，Follower的日志有问题
     *     // 让Follower截断到leaderEnd.offset()
     * } else {
     *     // 日志一致，可以继续同步
     * }
     * </pre>
     *
     * 【示例】
     *
     * <pre>
     * Leader日志：
     * Epoch 1: offset 0-10
     * Epoch 2: offset 11-20
     * Epoch 3: offset 21-30
     *
     * Follower日志：
     * Epoch 1: offset 0-10
     * Epoch 2: offset 11-20
     * Epoch 4: offset 21-25 ← 冲突！应该是epoch 3
     *
     * 检测流程：
     * Follower发送：prevOffset=20, prevEpoch=2
     * Leader检查：endOffsetForEpoch(2) = OffsetAndEpoch(21, 2) ✓ 匹配
     *
     * Follower发送：prevOffset=25, prevEpoch=4
     * Leader检查：endOffsetForEpoch(4) = OffsetAndEpoch(31, 3)
     * // epoch不匹配！Leader的epoch 4不存在，最大是3
     * → 让Follower截断到offset 31
     * </pre>
     *
     * @param epoch 要查找的epoch
     * @return 该epoch（或最接近的更小epoch）的结束位置
     */
    OffsetAndEpoch endOffsetForEpoch(int epoch);

    /**
     * 获取当前日志末尾位置的元数据
     *
     * 【什么是endOffset？】
     *
     * endOffset = 下一条记录将要写入的位置 = LEO（Log End Offset）
     *
     * 【返回值】
     *
     * LogOffsetMetadata包含：
     * - offset：LEO
     * - segmentBaseOffset：当前segment的起始offset（可选，用于优化）
     * - relativePositionInSegment：在segment中的相对位置（可选，用于优化）
     *
     * 【为什么返回metadata而不是long？】
     *
     * metadata包含额外信息，可以加速日志读取：
     * <pre>
     * LogOffsetMetadata metadata = log.endOffset();
     *
     * // 如果metadata包含segment信息
     * if (metadata.segmentBaseOffset.isPresent()) {
     *     // 可以直接定位到segment，无需查索引
     *     long segmentBase = metadata.segmentBaseOffset.get();
     *     int relativePos = metadata.relativePositionInSegment.get();
     *     // 快速读取
     * }
     * </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * // 检查日志是否为空
     * if (log.endOffset().offset() == log.startOffset()) {
     *     System.out.println("Log is empty");
     * }
     *
     * // 获取最后一条记录的offset
     * long lastRecordOffset = log.endOffset().offset() - 1;
     * </pre>
     *
     * @return 日志末尾位置的元数据，如果日志为空则offset = startOffset
     */
    LogOffsetMetadata endOffset();

    /**
     * 获取高水位（High Watermark）
     *
     * 【什么是高水位？】
     *
     * 高水位 = 被多数派确认的最高offset
     *
     * 只有 <= 高水位的记录才是已提交的、可以被状态机安全读取的。
     *
     * 【Leader vs Follower】
     *
     * - Leader：自己计算高水位（基于Follower的同步进度）
     * - Follower：从Leader的FetchResponse中获取高水位
     *
     * 【示例】
     *
     * <pre>
     * 3节点集群：
     * Leader:     [0-200]  LEO=201, HWM=150
     * Follower1:  [0-150]  LEO=151
     * Follower2:  [0-120]  LEO=121
     *
     * Leader计算HWM：
     * - 自己有0-200
     * - Follower1有0-150 ✓
     * - Follower2有0-120
     * → 多数派(2/3)都有0-150
     * → HWM = 150
     * </pre>
     *
     * @return 高水位元数据
     */
    LogOffsetMetadata highWatermark();

    /**
     * 获取日志起始offset
     *
     * 【什么是startOffset？】
     *
     * startOffset = 日志中第一条记录的offset
     *
     * 如果有快照：
     * <pre>
     * startOffset = 最新快照的endOffset
     *
     * 示例：
     * 快照覆盖0-99，快照ID = OffsetAndEpoch(100, 5)
     * → startOffset = 100
     * → 日志从offset 100开始
     * </pre>
     *
     * 如果没有快照：
     * <pre>
     * startOffset = 0
     * </pre>
     *
     * 【使用场景】
     *
     * 检查offset是否有效：
     * <pre>
     * long offset = request.fetchOffset();
     *
     * if (offset < log.startOffset()) {
     *     // offset太小，已被快照覆盖
     *     // 需要发送快照给Follower
     *     return sendSnapshot();
     * }
     * </pre>
     *
     * @return 日志起始offset
     */
    long startOffset();

    // ==================== 验证操作 ====================

    /**
     * 验证给定的offset和epoch是否与日志和快照一致
     *
     * 这是Raft日志一致性检查的核心方法，用于检测和修复日志冲突。
     *
     * 【验证规则】
     *
     * 给定(offset, epoch)，表示"offset-1位置的记录属于epoch"：
     * <pre>
     * 验证(offset=100, epoch=5)
     * 意思是：offset 99的记录的epoch是5
     * </pre>
     *
     * 【三种可能的返回值】
     *
     * 1. **ValidOffsetAndEpoch.valid()** - 验证通过
     *    <pre>
     *    // 日志中offset 99确实属于epoch 5
     *    ValidOffsetAndEpoch result = log.validateOffsetAndEpoch(100, 5);
     *    assert result.kind() == ValidOffsetAndEpoch.Kind.VALID;
     *    assert result.offsetAndEpoch().equals(new OffsetAndEpoch(100, 5));
     *    </pre>
     *
     * 2. **ValidOffsetAndEpoch.diverging()** - 发现冲突
     *    <pre>
     *    // 日志中offset 99不属于epoch 5（可能属于epoch 3）
     *    // 返回最大的有效位置
     *    ValidOffsetAndEpoch result = log.validateOffsetAndEpoch(100, 5);
     *    assert result.kind() == ValidOffsetAndEpoch.Kind.DIVERGING;
     *    // result.offsetAndEpoch() = 日志中epoch 5的结束位置（或更早）
     *    </pre>
     *
     * 3. **ValidOffsetAndEpoch.snapshot()** - 需要快照
     *    <pre>
     *    // offset太旧，已被快照覆盖
     *    ValidOffsetAndEpoch result = log.validateOffsetAndEpoch(50, 2);
     *    assert result.kind() == ValidOffsetAndEpoch.Kind.SNAPSHOT;
     *    // result.offsetAndEpoch() = 最新快照的ID
     *    </pre>
     *
     * 【默认实现逻辑】
     *
     * 此方法提供了完整的默认实现（实现类通常不需要覆盖）：
     *
     * 步骤1：检查特殊情况 - 空日志
     * <pre>
     * if (startOffset() == 0 && offset == 0) {
     *     return ValidOffsetAndEpoch.valid(new OffsetAndEpoch(0, 0));
     * }
     * </pre>
     *
     * 步骤2：检查是否需要快照
     * <pre>
     * if (offset < startOffset() ||
     *     epoch < earliestSnapshotEpoch) {
     *     // 太旧，返回快照
     *     return ValidOffsetAndEpoch.snapshot(latestSnapshotId());
     * }
     * </pre>
     *
     * 步骤3：验证日志一致性
     * <pre>
     * OffsetAndEpoch endOffset = endOffsetForEpoch(epoch);
     *
     * if (endOffset.epoch() != epoch || endOffset.offset() < offset) {
     *     // 冲突
     *     return ValidOffsetAndEpoch.diverging(endOffset);
     * } else {
     *     // 一致
     *     return ValidOffsetAndEpoch.valid(new OffsetAndEpoch(offset, epoch));
     * }
     * </pre>
     *
     * 【使用场景：Leader处理Follower的FetchRequest】
     *
     * <pre>
     * // Follower发送FetchRequest
     * FetchRequest request = {
     *     fetchOffset: 100,
     *     lastFetchedEpoch: 5
     * };
     *
     * // Leader验证Follower的日志位置
     * ValidOffsetAndEpoch validation = log.validateOffsetAndEpoch(
     *     request.fetchOffset,
     *     request.lastFetchedEpoch
     * );
     *
     * switch (validation.kind()) {
     *     case VALID:
     *         // Follower的日志一致，发送新记录
     *         LogFetchInfo fetch = log.read(request.fetchOffset, Isolation.UNCOMMITTED);
     *         return new FetchResponse(fetch.records);
     *
     *     case DIVERGING:
     *         // Follower的日志有冲突，让它截断
     *         OffsetAndEpoch divergingEpoch = validation.offsetAndEpoch();
     *         return new FetchResponse(divergingEpoch); // 告诉Follower截断位置
     *
     *     case SNAPSHOT:
     *         // Follower太落后，发送快照
     *         OffsetAndEpoch snapshotId = validation.offsetAndEpoch();
     *         return sendSnapshot(snapshotId);
     * }
     * </pre>
     *
     * 【详细示例】
     *
     * 示例1：验证通过
     * <pre>
     * Leader日志：
     * Epoch 1: offset 0-10
     * Epoch 2: offset 11-20
     * Epoch 3: offset 21-30
     *
     * validateOffsetAndEpoch(15, 2)
     * → VALID (offset 14确实属于epoch 2)
     * </pre>
     *
     * 示例2：发现冲突
     * <pre>
     * Leader日志：
     * Epoch 1: offset 0-10
     * Epoch 2: offset 11-20
     * Epoch 3: offset 21-30
     *
     * Follower声称：offset 15属于epoch 4
     * validateOffsetAndEpoch(15, 4)
     * → DIVERGING (Leader没有epoch 4)
     * → 返回OffsetAndEpoch(21, 2) // epoch 2的结束位置
     * → Follower应该截断到offset 21
     * </pre>
     *
     * 示例3：需要快照
     * <pre>
     * Leader：
     * 快照：offset 0-99, snapshotId=(100, 5)
     * 日志：offset 100-200
     *
     * Follower请求：offset 50
     * validateOffsetAndEpoch(50, 3)
     * → SNAPSHOT (offset 50已被快照覆盖)
     * → 返回snapshotId (100, 5)
     * → 发送快照给Follower
     * </pre>
     *
     * @param offset 要验证的offset（验证offset-1的记录）
     * @param epoch offset-1位置记录应该属于的epoch
     * @return 验证结果（valid、diverging或snapshot）
     */
    default ValidOffsetAndEpoch validateOffsetAndEpoch(long offset, int epoch) {
        // 特殊情况：空日志，offset=0是有效的
        if (startOffset() == 0 && offset == 0) {
            return ValidOffsetAndEpoch.valid(new OffsetAndEpoch(0, 0));
        }

        // 检查是否需要快照
        Optional<OffsetAndEpoch> earliestSnapshotId = earliestSnapshotId();
        if (earliestSnapshotId.isPresent() &&
            ((offset < startOffset()) ||
             (offset == startOffset() && epoch != earliestSnapshotId.get().epoch()) ||
             (epoch < earliestSnapshotId.get().epoch()))
        ) {
            /* 需要快照，如果：
             * 1. fetch offset < log start offset 或
             * 2. fetch offset == log start offset 但 epoch不匹配 或
             * 3. last fetch epoch < 最早快照的epoch
             */
            OffsetAndEpoch latestSnapshotId = latestSnapshotId().orElseThrow(() -> new IllegalStateException(
                String.format(
                    "Log start offset (%d) is greater than zero but latest snapshot was not found",
                    startOffset()
                )
            ));

            return ValidOffsetAndEpoch.snapshot(latestSnapshotId);
        } else {
            // 检查日志一致性
            OffsetAndEpoch endOffsetAndEpoch = endOffsetForEpoch(epoch);

            if (endOffsetAndEpoch.epoch() != epoch || endOffsetAndEpoch.offset() < offset) {
                // 日志冲突
                return ValidOffsetAndEpoch.diverging(endOffsetAndEpoch);
            } else {
                // 验证通过
                return ValidOffsetAndEpoch.valid(new OffsetAndEpoch(offset, epoch));
            }
        }
    }

    // ==================== 日志管理操作 ====================

    /**
     * 初始化新的Leader epoch
     *
     * 当节点成为Leader时调用此方法，标记新epoch的开始位置。
     *
     * 【什么时候调用？】
     *
     * 节点当选Leader后立即调用：
     * <pre>
     * // 赢得选举，成为epoch 6的Leader
     * void becomeLeader(int epoch) {
     *     log.initializeLeaderEpoch(epoch);
     *     // 现在可以开始写入epoch 6的记录
     * }
     * </pre>
     *
     * 【作用】
     *
     * 在日志中记录epoch边界，用于后续的epoch查询：
     * <pre>
     * 调用前：
     * Epoch 5: offset 0-100
     * LEO = 101
     *
     * initializeLeaderEpoch(6)
     *
     * 调用后：
     * Epoch 5: offset 0-100
     * Epoch 6: offset 101-...  ← 新epoch开始于LEO
     * LEO = 101
     * </pre>
     *
     * 【为什么需要这个方法？】
     *
     * Raft需要能够回答"epoch X结束于哪个offset？"这个问题：
     * <pre>
     * // 后续调用
     * OffsetAndEpoch end = log.endOffsetForEpoch(5);
     * // 返回 OffsetAndEpoch(101, 5)
     * // 表示epoch 5结束于offset 101（不包含）
     * </pre>
     *
     * 如果不调用initializeLeaderEpoch()：
     * - endOffsetForEpoch()无法准确返回epoch边界
     * - 日志冲突检测会失败
     *
     * 【使用示例】
     *
     * <pre>
     * // 节点状态转换：Candidate → Leader
     * class QuorumState {
     *     void transitionToLeader(int epoch) {
     *         // 1. 初始化Leader epoch
     *         log.initializeLeaderEpoch(epoch);
     *
     *         // 2. 切换到Leader状态
     *         this.state = new LeaderState(epoch, ...);
     *
     *         // 3. 现在可以接受写入请求
     *         acceptWrites = true;
     *     }
     * }
     * </pre>
     *
     * 【注意事项】
     *
     * - 每个epoch只能初始化一次
     * - epoch必须递增（不能初始化更小的epoch）
     * - 在写入任何epoch的记录之前调用
     *
     * @param epoch 新Leader的epoch
     */
    void initializeLeaderEpoch(int epoch);

    /**
     * 截断日志到指定offset
     *
     * 删除所有offset >= 给定offset的记录。
     *
     * 【截断语义】
     *
     * truncateTo(offset)：保留offset之前的记录，删除offset及之后的：
     * <pre>
     * 截断前：[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
     * truncateTo(5)
     * 截断后：[0, 1, 2, 3, 4]  // offset 5-9被删除
     * LEO = 5
     * </pre>
     *
     * 【什么时候需要截断？】
     *
     * 1. **Follower发现日志冲突**
     *    <pre>
     *    Leader告诉Follower：你的offset 5开始有冲突
     *    Follower：truncateTo(5)  // 删除5及之后的
     *    Follower：重新从Leader同步offset 5开始的正确日志
     *    </pre>
     *
     * 2. **新Leader发现未提交的记录**
     *    <pre>
     *    旧Leader在崩溃前写入了offset 100-105，但未提交
     *    新Leader当选后发现这些记录
     *    新Leader：truncateTo(100)  // 删除未提交的记录
     *    </pre>
     *
     * 【典型场景：Follower修复冲突】
     *
     * <pre>
     * Leader日志：[e1:0, e1:1, e2:2, e2:3, e3:4]
     * Follower日志：[e1:0, e1:1, e2:2, e4:3, e4:4]  // offset 3开始冲突
     *
     * 修复流程：
     * 1. Follower发送FetchRequest(offset=5, lastEpoch=4)
     * 2. Leader验证：validateOffsetAndEpoch(5, 4) → DIVERGING
     * 3. Leader返回：divergingEpoch = OffsetAndEpoch(3, 2)
     * 4. Follower截断：truncateTo(3)  // 删除错误的offset 3-4
     * 5. Follower日志：[e1:0, e1:1, e2:2]
     * 6. Follower重新同步：从offset 3开始拉取正确的日志
     * </pre>
     *
     * 【对高水位的影响】
     *
     * 截断可能导致高水位超出新的LEO：
     * <pre>
     * 截断前：LEO=100, HWM=80
     * truncateTo(50)
     * 截断后：LEO=50, HWM=80  // HWM > LEO，非法！
     *
     * 实现必须自动调整高水位：
     * 截断后：LEO=50, HWM=50  // HWM被自动调整
     * </pre>
     *
     * 【使用示例】
     *
     * <pre>
     * // Follower处理divergingEpoch响应
     * void handleDivergingEpoch(OffsetAndEpoch divergingEpoch) {
     *     long truncateOffset = divergingEpoch.offset();
     *
     *     logger.warn("Detected log divergence, truncating to offset {}", truncateOffset);
     *
     *     // 截断冲突的日志
     *     log.truncateTo(truncateOffset);
     *
     *     // 更新fetch offset
     *     nextFetchOffset = truncateOffset;
     *
     *     // 重新从Leader同步
     *     sendFetchRequest(nextFetchOffset);
     * }
     * </pre>
     *
     * @param offset 截断位置（保留offset之前的记录，删除offset及之后的）
     */
    void truncateTo(long offset);

    /**
     * 如果最新快照比日志末尾更新，完全截断日志
     *
     * 这是一个特殊的截断操作，用于Follower从快照恢复的场景。
     *
     * 【什么时候调用？】
     *
     * Follower收到并应用快照后：
     * <pre>
     * // Follower落后太多，Leader发送快照
     * Follower当前日志：offset 0-50
     * Leader发送快照：snapshotId = OffsetAndEpoch(200, 10)
     *
     * // Follower应用快照
     * 1. stateMachine.loadSnapshot(snapshot)
     * 2. log.truncateToLatestSnapshot()  ← 调用此方法
     * 3. 现在Follower的日志从offset 200开始
     * </pre>
     *
     * 【操作】
     *
     * 如果latestSnapshotId.offset > LEO：
     * <pre>
     * 1. 删除所有日志
     * 2. 设置startOffset = snapshotId.offset
     * 3. 设置LEO = snapshotId.offset
     * 4. 设置HWM = snapshotId.offset
     *
     * 结果：空日志，从快照位置重新开始
     * </pre>
     *
     * 【返回值】
     *
     * - true：执行了完全截断
     * - false：快照不比日志新，无需截断
     *
     * 【示例】
     *
     * <pre>
     * // Follower加载快照
     * void loadSnapshot(SnapshotReader snapshot) {
     *     // 1. 应用快照到状态机
     *     stateMachine.clear();
     *     while (snapshot.hasNext()) {
     *         stateMachine.apply(snapshot.next());
     *     }
     *     snapshot.close();
     *
     *     // 2. 截断旧日志
     *     boolean truncated = log.truncateToLatestSnapshot();
     *     if (truncated) {
     *         logger.info("Truncated log to snapshot {}", log.startOffset());
     *     }
     *
     *     // 3. 从快照位置继续同步
     *     nextFetchOffset = log.endOffset().offset();
     * }
     * </pre>
     *
     * @return true表示执行了截断，false表示无需截断
     */
    boolean truncateToLatestSnapshot();

    /**
     * 截断到指定的offset和epoch
     *
     * 这是truncateTo()的增强版本，结合了epoch信息。
     *
     * 【与truncateTo()的区别】
     *
     * truncateTo(offset)：直接截断到offset
     * <pre>
     * log.truncateTo(100);  // 保留0-99
     * </pre>
     *
     * truncateToEndOffset(OffsetAndEpoch)：考虑epoch，可能截断得更少
     * <pre>
     * log.truncateToEndOffset(new OffsetAndEpoch(100, 5));
     * // 如果日志中epoch 5结束于offset 80，只截断到80
     * </pre>
     *
     * 【截断逻辑】
     *
     * 此方法提供了默认实现：
     * <pre>
     * default long truncateToEndOffset(OffsetAndEpoch endOffset) {
     *     long truncationOffset;
     *     int leaderEpoch = endOffset.epoch();
     *
     *     if (leaderEpoch == 0) {
     *         // Epoch 0是特殊的，直接使用offset
     *         truncationOffset = Math.min(endOffset.offset(), endOffset().offset());
     *     } else {
     *         // 查找本地日志中这个epoch的结束位置
     *         OffsetAndEpoch localEndOffset = endOffsetForEpoch(leaderEpoch);
     *
     *         if (localEndOffset.epoch() == leaderEpoch) {
     *             // 本地有这个epoch，取较小值
     *             truncationOffset = Math.min(localEndOffset.offset(), endOffset.offset());
     *         } else {
     *             // 本地没有这个epoch，使用本地的结束位置
     *             truncationOffset = localEndOffset.offset();
     *         }
     *     }
     *
     *     truncateTo(truncationOffset);
     *     return truncationOffset;
     * }
     * </pre>
     *
     * 【使用场景】
     *
     * Leader告诉Follower截断到特定的epoch和offset：
     * <pre>
     * // Leader发现Follower的日志冲突
     * OffsetAndEpoch divergingEpoch = new OffsetAndEpoch(100, 5);
     *
     * // Follower收到后调用
     * long actualTruncation = log.truncateToEndOffset(divergingEpoch);
     * logger.info("Truncated to offset {}", actualTruncation);
     * </pre>
     *
     * @param endOffset 目标epoch和offset
     * @return 实际的截断offset
     */
    default long truncateToEndOffset(OffsetAndEpoch endOffset) {
        final long truncationOffset;
        int leaderEpoch = endOffset.epoch();
        if (leaderEpoch == 0) {
            truncationOffset = Math.min(endOffset.offset(), endOffset().offset());
        } else {
            OffsetAndEpoch localEndOffset = endOffsetForEpoch(leaderEpoch);
            if (localEndOffset.epoch() == leaderEpoch) {
                truncationOffset = Math.min(localEndOffset.offset(), endOffset.offset());
            } else {
                truncationOffset = localEndOffset.offset();
            }
        }

        truncateTo(truncationOffset);
        return truncationOffset;
    }

    /**
     * 更新高水位
     *
     * 设置新的高水位位置。
     *
     * 【谁调用？】
     *
     * - Leader：计算多数派同步进度后更新
     * - Follower：从Leader的FetchResponse中获取后更新
     *
     * 【Leader的高水位计算】
     *
     * <pre>
     * // Leader周期性计算高水位
     * void maybeUpdateHighWatermark() {
     *     // 1. 收集所有Follower的LEO
     *     List<Long> replicaOffsets = new ArrayList<>();
     *     replicaOffsets.add(log.endOffset().offset()); // Leader自己
     *     for (Follower f : followers) {
     *         replicaOffsets.add(f.fetchOffset);
     *     }
     *
     *     // 2. 排序，找中位数（多数派的最小值）
     *     Collections.sort(replicaOffsets);
     *     int quorum = (replicaOffsets.size() / 2) + 1;
     *     long newHwm = replicaOffsets.get(quorum - 1);
     *
     *     // 3. 更新高水位
     *     if (newHwm > log.highWatermark().offset()) {
     *         log.updateHighWatermark(new LogOffsetMetadata(newHwm));
     *     }
     * }
     * </pre>
     *
     * 【Follower的高水位更新】
     *
     * <pre>
     * // Follower处理FetchResponse
     * void handleFetchResponse(FetchResponse response) {
     *     // 1. 追加记录
     *     if (!response.records.isEmpty()) {
     *         log.appendAsFollower(response.records, currentEpoch);
     *     }
     *
     *     // 2. 更新高水位（从Leader获取）
     *     LogOffsetMetadata leaderHwm = response.highWatermark;
     *     log.updateHighWatermark(leaderHwm);
     *
     *     // 3. 应用已提交的记录到状态机
     *     applyCommittedRecords();
     * }
     * </pre>
     *
     * 【高水位约束】
     *
     * 新的高水位必须满足：
     * <pre>
     * 旧HWM <= 新HWM <= LEO
     *
     * 不满足约束的行为：
     * - 新HWM < 旧HWM：通常被拒绝或忽略（高水位不能倒退）
     * - 新HWM > LEO：可能被截断到LEO（不能超过日志末尾）
     * </pre>
     *
     * 【使用metadata的好处】
     *
     * LogOffsetMetadata包含segment信息，可以优化读取：
     * <pre>
     * // 读取已提交的记录时，如果HWM包含segment信息
     * LogOffsetMetadata hwm = log.highWatermark();
     * if (hwm.segmentBaseOffset.isPresent()) {
     *     // 可以直接定位segment，无需查索引
     *     readFromSegment(hwm.segmentBaseOffset.get(), hwm.relativePosition);
     * }
     * </pre>
     *
     * @param offsetMetadata 新的高水位及其元数据
     */
    void updateHighWatermark(LogOffsetMetadata offsetMetadata);

    /**
     * 删除快照之前的旧日志
     *
     * 在创建新快照后，可以删除被快照覆盖的旧日志以节省磁盘空间。
     *
     * 【什么时候调用？】
     *
     * 创建快照后：
     * <pre>
     * // 1. 创建快照
     * OffsetAndEpoch snapshotId = new OffsetAndEpoch(1000, 5);
     * Optional<RawSnapshotWriter> writer = log.createNewSnapshot(snapshotId);
     * writer.get().freeze();
     *
     * // 2. 通知日志
     * log.onSnapshotFrozen(snapshotId);
     *
     * // 3. 删除旧日志
     * boolean deleted = log.deleteBeforeSnapshot(snapshotId);
     * if (deleted) {
     *     logger.info("Deleted logs before offset {}", snapshotId.offset());
     * }
     * </pre>
     *
     * 【操作】
     *
     * 如果快照的offset > 当前startOffset：
     * 1. 删除offset < snapshotId.offset()的日志segment
     * 2. 更新startOffset = snapshotId.offset()
     * 3. 返回true
     *
     * 否则：
     * - 不做任何操作
     * - 返回false
     *
     * 【安全性】
     *
     * 删除前会检查：
     * - 快照确实存在
     * - 快照已经frozen（完成写入）
     * - startOffset不会超过HWM
     *
     * 【示例】
     *
     * <pre>
     * 删除前：
     * 日志：offset 0-1000
     * startOffset = 0
     * 快照：snapshotId = OffsetAndEpoch(500, 3)
     *
     * deleteBeforeSnapshot(snapshotId)
     *
     * 删除后：
     * 日志：offset 500-1000
     * startOffset = 500
     * 已删除：offset 0-499的segment文件
     * </pre>
     *
     * @param snapshotId 快照ID（offset和epoch）
     * @return true表示删除了日志，false表示无需删除
     */
    boolean deleteBeforeSnapshot(OffsetAndEpoch snapshotId);

    /**
     * 刷新日志到磁盘
     *
     * 将内存中的日志数据强制写入磁盘。
     *
     * 【什么时候调用？】
     *
     * 1. **关闭前**：确保所有数据持久化
     *    <pre>
     *    log.flush(true);  // forceFlushActiveSegment=true
     *    log.close();
     *    </pre>
     *
     * 2. **周期性刷新**：定期持久化数据
     *    <pre>
     *    // 每5秒刷新一次
     *    scheduler.scheduleAtFixedRate(() -> {
     *        log.flush(false);  // 不强制刷新active segment
     *    }, 5, 5, TimeUnit.SECONDS);
     *    </pre>
     *
     * 3. **重要操作后**：关键操作后确保持久化
     *    <pre>
     *    log.truncateTo(offset);
     *    log.flush(true);  // 确保截断操作持久化
     *    </pre>
     *
     * 【forceFlushActiveSegment参数】
     *
     * - **true**：强制刷新active segment
     *   - 适用于关闭前、紧急情况
     *   - 性能开销较大
     *   - 数据一致性最高
     *
     * - **false**：只刷新已关闭的segment
     *   - 适用于周期性刷新
     *   - 性能开销较小
     *   - active segment可能丢失
     *
     * 【Kafka的segment管理】
     *
     * Kafka日志由多个segment组成：
     * <pre>
     * Log
     *  ├─ Segment 0-100 (closed, 已刷新)
     *  ├─ Segment 101-200 (closed, 已刷新)
     *  └─ Segment 201-... (active, 内存中)
     * </pre>
     *
     * flush(false)：只刷新closed segment
     * flush(true)：刷新所有segment（包括active）
     *
     * 【性能考虑】
     *
     * 频繁刷新会降低性能：
     * <pre>
     * // 不好：每次写入都刷新
     * log.appendAsLeader(records, epoch);
     * log.flush(true);  // 性能很差
     *
     * // 好：批量写入后刷新
     * for (int i = 0; i < 100; i++) {
     *     log.appendAsLeader(records, epoch);
     * }
     * log.flush(false);  // 性能好
     * </pre>
     *
     * 【使用示例】
     *
     * <pre>
     * // 关闭流程
     * public void close() {
     *     // 1. 停止接收新写入
     *     acceptingWrites = false;
     *
     *     // 2. 刷新所有数据
     *     log.flush(true);  // 强制刷新active segment
     *
     *     // 3. 关闭日志
     *     log.close();
     * }
     * </pre>
     *
     * @param forceFlushActiveSegment 是否强制刷新active segment（关闭时应为true）
     */
    void flush(boolean forceFlushActiveSegment);

    /**
     * 执行日志和快照清理
     *
     * 清理不再需要的旧日志和快照。
     *
     * 【什么时候调用？】
     *
     * 周期性调用（如每分钟）：
     * <pre>
     * scheduler.scheduleAtFixedRate(() -> {
     *     boolean cleaned = log.maybeClean();
     *     if (cleaned) {
     *         logger.info("Performed log cleanup");
     *     }
     * }, 1, 1, TimeUnit.MINUTES);
     * </pre>
     *
     * 【清理内容】
     *
     * 可能清理：
     * 1. **旧日志segment**
     *    - 被快照覆盖的segment
     *    - 超过保留期的segment
     *
     * 2. **旧快照**
     *    - 只保留最新的N个快照
     *    - 删除更早的快照
     *
     * 3. **日志压缩**（如果配置了）
     *    - 合并小segment
     *    - 删除重复key的旧记录
     *
     * 【清理策略示例】
     *
     * <pre>
     * // 保留策略
     * 当前快照：snapshotId = OffsetAndEpoch(1000, 5)
     * 历史快照：
     *   - OffsetAndEpoch(800, 4)  ← 保留（最近的2个）
     *   - OffsetAndEpoch(600, 3)  ← 删除（太旧）
     *   - OffsetAndEpoch(400, 2)  ← 删除（太旧）
     *
     * 日志：
     *   - offset 0-599     ← 删除（被快照覆盖）
     *   - offset 600-1000  ← 保留（需要同步给Follower）
     *   - offset 1001-...  ← 保留（未提交）
     * </pre>
     *
     * 【返回值】
     *
     * - true：执行了清理操作
     * - false：无需清理或清理被跳过
     *
     * 【非阻塞】
     *
     * 此方法是"maybe"清理：
     * - 可能延迟清理（如果正在写入）
     * - 可能跳过清理（如果最近刚清理过）
     * - 不会阻塞写入操作
     *
     * @return true表示执行了清理，false表示无需清理
     */
    boolean maybeClean();

    // ==================== 快照管理操作 ====================

    /**
     * 创建新快照（带验证）
     *
     * 为已提交的数据创建快照，会验证快照ID的合法性。
     *
     * 【验证规则】
     *
     * 快照ID必须满足：
     * 1. 不能已存在
     * 2. offset > log start offset
     * 3. offset <= 高水位（只能为已提交数据创建快照）
     * 4. (offset, epoch)在日志中存在
     *
     * 【使用流程】
     *
     * <pre>
     * // 1. 确定快照位置（通常是当前高水位）
     * OptionalLong hwm = raftClient.highWatermark();
     * if (hwm.isEmpty()) {
     *     return; // 还没有提交的数据
     * }
     *
     * long snapshotOffset = hwm.getAsLong();
     * int epoch = log.lastFetchedEpoch();
     * OffsetAndEpoch snapshotId = new OffsetAndEpoch(snapshotOffset, epoch);
     *
     * // 2. 创建快照
     * Optional<RawSnapshotWriter> writerOpt = log.createNewSnapshot(snapshotId);
     * if (writerOpt.isEmpty()) {
     *     return; // 快照已存在
     * }
     *
     * // 3. 写入快照数据
     * try (RawSnapshotWriter writer = writerOpt.get()) {
     *     // 写入状态机的所有数据
     *     for (Record r : stateMachine.getAll()) {
     *         writer.append(ByteBuffer.wrap(serialize(r)));
     *     }
     *
     *     // 完成写入
     *     writer.freeze();
     * }
     *
     * // 4. 通知日志
     * log.onSnapshotFrozen(snapshotId);
     * </pre>
     *
     * 【返回empty的情况】
     *
     * <pre>
     * // 快照已存在
     * log.createNewSnapshot(snapshotId);  // 返回Optional.of(writer)
     * log.createNewSnapshot(snapshotId);  // 返回Optional.empty()
     * </pre>
     *
     * 【异常情况】
     *
     * @throws IllegalArgumentException 如果：
     *         - snapshotId.offset() > highWatermark()（不能为未提交数据创建快照）
     *         - snapshotId.offset() < startOffset()（offset太小）
     *         - snapshotId不在日志中（无效的offset/epoch组合）
     *
     * @param snapshotId 快照ID（offset和epoch）
     * @return 快照写入器，如果快照已存在则返回Optional.empty()
     */
    Optional<RawSnapshotWriter> createNewSnapshot(OffsetAndEpoch snapshotId);

    /**
     * 创建新快照（不验证）
     *
     * 创建快照但不验证快照ID的合法性，用于从可信来源（如Leader）接收快照。
     *
     * 【与createNewSnapshot的区别】
     *
     * createNewSnapshot()：
     * - 验证offset <= 高水位
     * - 验证(offset, epoch)存在于日志
     * - 验证offset > startOffset
     * - 用于本地创建快照
     *
     * createNewSnapshotUnchecked()：
     * - **不验证**任何条件
     * - 只检查快照是否已存在
     * - 用于接收远程快照
     *
     * 【什么时候使用？】
     *
     * Follower从Leader接收快照：
     * <pre>
     * // Leader发送快照
     * SnapshotChunk chunk = receiveFromLeader();
     * OffsetAndEpoch snapshotId = chunk.snapshotId();
     *
     * // Follower创建快照（不验证，因为信任Leader）
     * Optional<RawSnapshotWriter> writer = log.createNewSnapshotUnchecked(snapshotId);
     *
     * if (writer.isPresent()) {
     *     // 写入快照数据
     *     while (hasMoreChunks()) {
     *         ByteBuffer data = receiveChunk();
     *         writer.get().append(data);
     *     }
     *     writer.get().freeze();
     * }
     * </pre>
     *
     * 【为什么需要unchecked版本？】
     *
     * Follower可能：
     * - 高水位还没更新到快照位置
     * - 本地日志不包含快照的offset/epoch
     * - 但Leader说这个快照是有效的
     *
     * 示例：
     * <pre>
     * Follower: LEO=50, HWM=30
     * Leader发送: snapshotId = OffsetAndEpoch(200, 10)
     *
     * createNewSnapshot(snapshotId)
     * → IllegalArgumentException (offset 200 > HWM 30)
     *
     * createNewSnapshotUnchecked(snapshotId)
     * → Optional.of(writer) ✓ （信任Leader）
     * </pre>
     *
     * @param snapshotId 快照ID（offset和epoch）
     * @return 快照写入器，如果快照已存在则返回Optional.empty()
     */
    Optional<RawSnapshotWriter> createNewSnapshotUnchecked(OffsetAndEpoch snapshotId);

    /**
     * 打开快照读取器
     *
     * 读取指定的快照数据。
     *
     * 【使用场景】
     *
     * 1. **Leader发送快照给Follower**
     *    <pre>
     *    OffsetAndEpoch snapshotId = log.latestSnapshotId().get();
     *    Optional<RawSnapshotReader> reader = log.readSnapshot(snapshotId);
     *
     *    if (reader.isPresent()) {
     *        try (RawSnapshotReader r = reader.get()) {
     *            // 读取并发送给Follower
     *            while (r.hasNext()) {
     *                ByteBuffer chunk = r.next();
     *                sendToFollower(chunk);
     *            }
     *        }
     *    }
     *    </pre>
     *
     * 2. **状态机从快照恢复**
     *    <pre>
     *    OffsetAndEpoch snapshotId = log.latestSnapshotId().get();
     *    Optional<RawSnapshotReader> reader = log.readSnapshot(snapshotId);
     *
     *    if (reader.isPresent()) {
     *        stateMachine.clear();
     *        try (RawSnapshotReader r = reader.get()) {
     *            while (r.hasNext()) {
     *                ByteBuffer data = r.next();
     *                Record record = deserialize(data);
     *                stateMachine.apply(record);
     *            }
     *        }
     *    }
     *    </pre>
     *
     * 【返回empty的情况】
     *
     * - 快照不存在
     * - 快照已被删除
     * - 快照文件损坏
     *
     * @param snapshotId 快照ID
     * @return 快照读取器，如果快照不存在则返回Optional.empty()
     */
    Optional<RawSnapshotReader> readSnapshot(OffsetAndEpoch snapshotId);

    /**
     * 获取最新快照的读取器
     *
     * 返回最新快照的读取器（如果存在）。
     *
     * 【使用场景】
     *
     * <pre>
     * // 重启后从快照恢复
     * Optional<RawSnapshotReader> snapshot = log.latestSnapshot();
     * if (snapshot.isPresent()) {
     *     logger.info("Restoring from latest snapshot");
     *     stateMachine.loadSnapshot(snapshot.get());
     * } else {
     *     logger.info("No snapshot found, replaying full log");
     *     replayLog();
     * }
     * </pre>
     *
     * @return 最新快照的读取器，如果没有快照则返回Optional.empty()
     */
    Optional<RawSnapshotReader> latestSnapshot();

    /**
     * 获取最新快照的ID
     *
     * 返回最新快照的标识符。
     *
     * 【使用场景】
     *
     * <pre>
     * // 检查是否需要创建新快照
     * Optional<OffsetAndEpoch> latestSnapshot = log.latestSnapshotId();
     * long snapshotLag = log.highWatermark().offset() -
     *                    latestSnapshot.map(OffsetAndEpoch::offset).orElse(0L);
     *
     * if (snapshotLag > 10000) {
     *     // 快照落后超过10000条，创建新快照
     *     createNewSnapshot();
     * }
     * </pre>
     *
     * @return 最新快照的ID，如果没有快照则返回Optional.empty()
     */
    Optional<OffsetAndEpoch> latestSnapshotId();

    /**
     * 获取日志起始位置的快照ID
     *
     * 返回与log start offset对应的快照ID。
     *
     * 【什么时候有值？】
     *
     * 如果startOffset > 0，说明有快照：
     * <pre>
     * startOffset = 100
     * earliestSnapshotId() = Optional.of(OffsetAndEpoch(100, 5))
     * // 表示：offset 0-99被快照覆盖，快照ID是(100, 5)
     * </pre>
     *
     * 如果startOffset == 0，没有快照：
     * <pre>
     * startOffset = 0
     * earliestSnapshotId() = Optional.empty()
     * </pre>
     *
     * 【使用场景】
     *
     * 验证offset时检查是否需要快照：
     * <pre>
     * Optional<OffsetAndEpoch> earliest = log.earliestSnapshotId();
     *
     * if (fetchOffset < log.startOffset() && earliest.isPresent()) {
     *     // offset太旧，需要发送快照
     *     return sendSnapshot(log.latestSnapshotId().get());
     * }
     * </pre>
     *
     * @return 最早快照的ID（通常等于startOffset），如果没有快照则返回Optional.empty()
     */
    Optional<OffsetAndEpoch> earliestSnapshotId();

    /**
     * 通知日志新快照已完成
     *
     * 在快照写入完成（freeze）后调用。
     *
     * 【什么时候调用？】
     *
     * <pre>
     * // 1. 创建快照
     * Optional<RawSnapshotWriter> writer = log.createNewSnapshot(snapshotId);
     *
     * // 2. 写入数据
     * try (RawSnapshotWriter w = writer.get()) {
     *     w.append(...);
     *     w.freeze();  // 完成写入
     * }
     *
     * // 3. 通知日志 ← 这里调用
     * log.onSnapshotFrozen(snapshotId);
     * </pre>
     *
     * 【作用】
     *
     * 允许日志内部更新状态：
     * - 记录最新快照ID
     * - 标记快照为可用
     * - 触发后续清理操作
     *
     * 【完整的快照创建流程】
     *
     * <pre>
     * OffsetAndEpoch snapshotId = new OffsetAndEpoch(hwm, epoch);
     *
     * // 1. 创建快照
     * Optional<RawSnapshotWriter> writer = log.createNewSnapshot(snapshotId);
     * if (writer.isEmpty()) {
     *     return; // 快照已存在
     * }
     *
     * // 2. 写入快照
     * try (RawSnapshotWriter w = writer.get()) {
     *     stateMachine.writeSnapshot(w);
     *     w.freeze();
     * }
     *
     * // 3. 通知日志
     * log.onSnapshotFrozen(snapshotId);
     *
     * // 4. 删除旧日志（可选）
     * log.deleteBeforeSnapshot(snapshotId);
     * </pre>
     *
     * @param snapshotId 已完成的快照ID
     */
    void onSnapshotFrozen(OffsetAndEpoch snapshotId);

    // ==================== 其他方法 ====================

    /**
     * 返回Topic分区信息
     *
     * 每个ReplicatedLog关联到一个TopicPartition。
     *
     * 【使用场景】
     *
     * <pre>
     * TopicPartition tp = log.topicPartition();
     * logger.info("Log for partition {}-{}", tp.topic(), tp.partition());
     * </pre>
     *
     * @return Topic分区
     */
    TopicPartition topicPartition();

    /**
     * 返回Topic ID
     *
     * Topic的UUID标识符。
     *
     * @return Topic的UUID
     */
    Uuid topicId();

    @Override
    default void close() {}
}
