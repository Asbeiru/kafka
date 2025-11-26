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

import java.util.Objects;
import java.util.Optional;

/**
 * LogOffsetMetadata - 本地日志偏移量的元数据
 *
 * 这个类组合了两种信息：
 * 1. 逻辑偏移量（offset）：日志中记录的逻辑位置，从0开始递增
 * 2. 物理元数据（metadata）：可选的物理位置信息，用于优化查找
 *
 * 【核心设计思想】
 * 在分布式日志系统中，一个位置可以用两种方式表示：
 * - 逻辑偏移量：用户可见的、连续的序号（offset=100, 101, 102...）
 * - 物理位置：实际存储在磁盘上的位置（第3个段文件的第500字节处）
 *
 * LogOffsetMetadata将这两者结合起来，既提供逻辑视图，又可以快速访问物理位置。
 *
 * 【为什么需要物理元数据？】
 * 性能优化：
 * 1. 没有元数据时：要读取offset=100的记录
 *    - 需要从头扫描，找到第100条记录
 *    - 或者查询索引，然后定位到文件位置
 *    - 时间复杂度：O(log n) 或 O(n)
 *
 * 2. 有元数据时：直接知道
 *    - 在哪个段文件（segment base offset）
 *    - 在段文件的哪个字节位置（relative position）
 *    - 直接seek到该位置读取
 *    - 时间复杂度：O(1)
 *
 * 【使用场景】
 * 1. 高水位（High Watermark）：
 *    - Leader追踪已被多数派确认的最大偏移量
 *    - 使用LogOffsetMetadata可以快速定位这个位置
 *
 * 2. 日志截断（Log Truncation）：
 *    - Follower需要截断到某个偏移量
 *    - 有了物理元数据，可以快速定位到截断点
 *
 * 3. Follower追踪（Follower Tracking）：
 *    - Leader追踪每个Follower的复制进度
 *    - 物理元数据帮助Leader快速定位要发送的日志
 *
 * 【metadata为什么是Optional？】
 * 并非所有情况都需要物理元数据：
 * 1. 初始化时可能只知道逻辑偏移量
 * 2. 从网络接收的偏移量可能没有本地物理信息
 * 3. 快照的偏移量可能不对应当前日志的任何物理位置
 *
 * 使用Optional表示"可能有，也可能没有"，避免null检查。
 *
 * 【示例】
 * <pre>
 * // 只有逻辑偏移量
 * LogOffsetMetadata simple = new LogOffsetMetadata(100);
 *
 * // 带物理元数据
 * OffsetMetadata physical = ...;  // 由日志实现提供
 * LogOffsetMetadata detailed = new LogOffsetMetadata(100, Optional.of(physical));
 * </pre>
 *
 * @see OffsetMetadata 物理元数据接口
 * @see org.apache.kafka.raft.LeaderState 在Leader状态中追踪高水位
 */
public class LogOffsetMetadata {
    /**
     * 逻辑偏移量
     *
     * 这是日志记录的逻辑位置，是用户可见的、连续递增的序号。
     * 例如：第100条记录的offset就是100。
     */
    private final long offset;

    /**
     * 物理元数据（可选）
     *
     * 包含这条记录在磁盘上的物理位置信息，用于性能优化。
     * 如果为empty，表示只有逻辑偏移量，需要时再查找物理位置。
     * 如果为present，表示已知物理位置，可以直接访问。
     */
    private final Optional<OffsetMetadata> metadata;

    /**
     * 构造函数：只有逻辑偏移量
     *
     * 当只知道逻辑偏移量时使用这个构造函数。
     * 物理元数据设置为Optional.empty()。
     *
     * @param offset 逻辑偏移量
     */
    public LogOffsetMetadata(long offset) {
        this(offset, Optional.empty());
    }

    /**
     * 构造函数：逻辑偏移量 + 物理元数据
     *
     * 当既知道逻辑偏移量，又知道物理位置时使用这个构造函数。
     *
     * @param offset 逻辑偏移量
     * @param metadata 物理元数据（可选）
     */
    public LogOffsetMetadata(long offset, Optional<OffsetMetadata> metadata) {
        this.offset = offset;
        this.metadata = metadata;
    }

    /**
     * 获取逻辑偏移量
     *
     * @return 逻辑偏移量
     */
    public long offset() {
        return offset;
    }

    /**
     * 获取物理元数据
     *
     * @return 物理元数据（Optional），可能为empty
     */
    public Optional<OffsetMetadata> metadata() {
        return metadata;
    }

    /**
     * 字符串表示
     *
     * 格式：LogOffsetMetadata(offset=100, metadata=Optional[...])
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return "LogOffsetMetadata(offset=" + offset +
                ", metadata=" + metadata + ")";
    }

    /**
     * 判断相等性
     *
     * 两个LogOffsetMetadata相等当且仅当：
     * 1. offset相同
     * 2. metadata相同（都为empty，或都present且内容相同）
     *
     * @param obj 要比较的对象
     * @return 如果相等返回true
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LogOffsetMetadata other) {
            return this.offset == other.offset &&
                   this.metadata.equals(other.metadata);
        } else {
            return false;
        }
    }

    /**
     * 计算哈希码
     *
     * 基于offset和metadata计算哈希码，确保：
     * - 相同的对象有相同的哈希码
     * - 可以用作HashMap的key
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(offset, metadata);
    }
}
