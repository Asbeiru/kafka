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

import org.apache.kafka.common.record.Records;

/**
 * LogFetchInfo - 日志读取信息
 *
 * 从日志读取记录后返回的结果。
 * 包含读取到的原始记录数据和起始位置的元数据。
 *
 * 【为什么需要LogFetchInfo？】
 *
 * 从日志读取记录时，不仅需要记录数据本身，还需要一些元数据：
 * 1. records：读取到的原始字节数据（还未反序列化）
 * 2. startOffsetMetadata：读取起始位置的元数据（包含物理位置信息）
 *
 * 这些信息用于：
 * - Follower从Leader拉取日志
 * - Leader读取日志准备复制给Follower
 * - 状态机读取已提交的日志
 *
 * 【核心字段】
 *
 * 1. records - 原始记录数据
 *    - 类型：Records（Kafka的底层记录格式）
 *    - 内容：一批或多批记录的原始字节数据
 *    - 状态：可变的（Records允许修改内部缓冲区）
 *    - 用途：高效传输，支持零拷贝
 *
 * 2. startOffsetMetadata - 起始位置元数据
 *    - 类型：LogOffsetMetadata
 *    - 内容：逻辑偏移量 + 可选的物理位置
 *    - 用途：快速定位，优化后续读取
 *
 * 【为什么不是Java record？】
 *
 * 类注释说明了原因：
 * "The class is not converted to a Java record since records are typically intended
 *  to be immutable, but this one contains a mutable field records"
 *
 * Java record的设计原则：
 * - 所有字段都应该是不可变的（immutable）
 * - 提供值语义（value semantics）
 * - 适合作为纯数据载体
 *
 * Records字段的问题：
 * - Records内部包含可变的ByteBuffer
 * - 可以修改内部缓冲区的位置和内容
 * - 不符合record的不可变性原则
 *
 * 因此使用传统class而不是record：
 * - 明确表达"这个类包含可变状态"
 * - 避免误导（record暗示不可变性）
 * - 保持语义清晰
 *
 * 【Records的可变性】
 *
 * Records为什么是可变的？
 * <pre>
 * Records records = ...;
 *
 * // Records内部包含ByteBuffer，可以：
 * 1. 修改读取位置：buffer.position()
 * 2. 修改内容：buffer.put()
 * 3. 翻转缓冲区：buffer.flip()
 * </pre>
 *
 * 为什么需要可变性？
 * - 性能：避免拷贝大量数据
 * - 零拷贝：直接在缓冲区上操作
 * - 内存效率：重用缓冲区
 *
 * 【使用场景】
 *
 * 场景1：Follower从Leader拉取日志
 * <pre>
 * // Follower发送Fetch请求
 * FetchRequest request = new FetchRequest()
 *     .setFetchOffset(100)       // 从offset=100开始读取
 *     .setMaxBytes(1024 * 1024); // 最多读取1MB
 *
 * // Leader读取日志
 * LogFetchInfo fetchInfo = log.read(
 *     request.fetchOffset(),
 *     Isolation.UNCOMMITTED      // Leader读取所有记录（包括未提交的）
 * );
 *
 * // fetchInfo包含：
 * // - records：从offset=100开始的记录数据（原始字节）
 * // - startOffsetMetadata：offset=100的元数据（逻辑位置+物理位置）
 *
 * // Leader发送给Follower
 * FetchResponse response = new FetchResponse()
 *     .setRecords(fetchInfo.records);
 *
 * // Follower接收并追加
 * log.append(response.records);
 * </pre>
 *
 * 场景2：客户端读取已提交的日志
 * <pre>
 * // 客户端从offset=100开始读取
 * LogFetchInfo fetchInfo = log.read(
 *     100,
 *     Isolation.COMMITTED  // 只读取已提交的记录（<= 高水位）
 * );
 *
 * // fetchInfo.records包含：
 * // - 如果高水位=150，可以读取offset=100-150的记录
 * // - 如果高水位=99，读取为空（offset=100还未提交）
 *
 * // startOffsetMetadata包含：
 * // - offset: 100（逻辑位置）
 * // - metadata: 物理位置信息（快速访问）
 * </pre>
 *
 * 场景3：状态机读取日志
 * <pre>
 * // 状态机从上次处理的位置继续读取
 * LogFetchInfo fetchInfo = log.read(
 *     lastProcessedOffset + 1,
 *     Isolation.COMMITTED
 * );
 *
 * // 反序列化并应用到状态机
 * for (Batch<Record> batch : deserialize(fetchInfo.records)) {
 *     for (Record record : batch) {
 *         stateMachine.apply(record);
 *     }
 * }
 * </pre>
 *
 * 【零拷贝优化】
 *
 * Records支持零拷贝（zero-copy）传输：
 * <pre>
 * // 传统方式（多次拷贝）：
 * 1. 磁盘 → 内核缓冲区
 * 2. 内核缓冲区 → 用户空间
 * 3. 用户空间 → 网络缓冲区
 * 4. 网络缓冲区 → 网卡
 * 总共：4次拷贝
 *
 * // 零拷贝方式（sendfile系统调用）：
 * 1. 磁盘 → 内核缓冲区
 * 2. 内核缓冲区 → 网卡（直接）
 * 总共：2次拷贝（甚至可以1次，使用DMA）
 * </pre>
 *
 * LogFetchInfo配合零拷贝：
 * <pre>
 * LogFetchInfo fetchInfo = log.read(...);
 *
 * // Records内部可能是FileChannel，支持零拷贝
 * sendToFollower(fetchInfo.records); // 直接从磁盘到网络，不经过用户空间
 * </pre>
 *
 * 【startOffsetMetadata的作用】
 *
 * 为什么需要startOffsetMetadata？
 *
 * 1. 性能优化：
 *    - 保存了物理位置信息
 *    - 下次读取可以快速定位（O(1)而不是O(log n)）
 *
 * 2. 连续读取：
 *    <pre>
 *    // 第一次读取
 *    LogFetchInfo fetch1 = log.read(100, ...);
 *    // fetch1.startOffsetMetadata包含offset=100的物理位置
 *
 *    // 处理完后，下次读取
 *    long nextOffset = calculateNextOffset(fetch1.records);
 *    LogFetchInfo fetch2 = log.read(nextOffset, ...);
 *    // 如果有物理元数据，可以快速定位
 *    </pre>
 *
 * 3. 追踪读取进度：
 *    <pre>
 *    // Leader追踪Follower的拉取位置
 *    followerState.updateFetchPosition(fetchInfo.startOffsetMetadata);
 *    </pre>
 *
 * 【字段可见性】
 *
 * 为什么字段是public final？
 *
 * 1. public：简化访问，这是一个简单的数据传输对象
 * 2. final：字段引用不可变（虽然Records内容可变）
 *
 * 设计权衡：
 * - 简洁性 vs 封装性
 * - 这里选择简洁性（类似于record的公开访问）
 * - 但保留class形式（明确表达可变性）
 *
 * 【与LogAppendInfo的对比】
 *
 * LogAppendInfo：追加操作的结果
 * - 不可变：使用Java record
 * - 简单：只包含offset范围
 * - 用途：追踪写入进度
 *
 * LogFetchInfo：读取操作的结果
 * - 可变：使用class（Records可变）
 * - 复杂：包含原始数据+元数据
 * - 用途：传输记录，支持零拷贝
 *
 * 【实际应用】
 *
 * 在KRaft的日志复制流程中：
 * <pre>
 * // Leader端：
 * 1. Follower发送FetchRequest(offset=100)
 * 2. Leader读取：fetchInfo = log.read(100, UNCOMMITTED)
 * 3. Leader响应：FetchResponse(records=fetchInfo.records)
 *
 * // Follower端：
 * 1. 接收FetchResponse
 * 2. 追加到日志：log.append(response.records)
 * 3. 更新复制进度
 * </pre>
 *
 * @see LogAppendInfo 日志追加信息
 * @see LogOffsetMetadata 偏移量元数据
 * @see Records Kafka原始记录格式
 */
// 这个类没有转换为Java record，因为record通常是不可变的，
// 但这个类包含可变字段records
public class LogFetchInfo {

    /**
     * 原始记录数据
     *
     * 从日志读取的原始字节数据，可能包含一个或多个批次。
     *
     * 特性：
     * - 可变的：Records内部包含可变的ByteBuffer
     * - 原始的：还未反序列化为具体的记录对象
     * - 高效的：支持零拷贝传输
     *
     * 用途：
     * - 网络传输：发送给Follower或客户端
     * - 零拷贝：直接从磁盘到网络，不经过用户空间
     * - 延迟反序列化：只在需要时才解析
     */
    public final Records records;

    /**
     * 起始位置元数据
     *
     * 读取起始位置的逻辑偏移量和物理位置信息。
     *
     * 包含：
     * - offset：逻辑偏移量（从哪个offset开始读取）
     * - metadata：可选的物理位置（快速定位）
     *
     * 用途：
     * - 快速定位：下次读取可以利用物理位置信息
     * - 进度追踪：记录Follower的拉取位置
     * - 性能优化：避免重复查找
     */
    public final LogOffsetMetadata startOffsetMetadata;

    /**
     * 构造函数
     *
     * 创建日志读取信息实例。
     *
     * @param records 原始记录数据（可变）
     * @param startOffsetMetadata 起始位置元数据
     */
    public LogFetchInfo(Records records, LogOffsetMetadata startOffsetMetadata) {
        this.records = records;
        this.startOffsetMetadata = startOffsetMetadata;
    }
}
