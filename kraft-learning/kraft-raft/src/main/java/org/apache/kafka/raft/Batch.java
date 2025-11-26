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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Batch - 记录批次
 *
 * 封装了一批记录（records）及其关联的元数据。
 * 这是Kafka日志系统中的基本存储单元。
 *
 * 【为什么要批处理？】
 *
 * 如果每条记录单独处理，会有很多开销：
 *
 * 1. 存储开销：
 *    - 每条记录需要存储元数据（offset, epoch, timestamp等）
 *    - 1000条记录 × 每条20字节元数据 = 20KB开销
 *
 * 2. I/O开销：
 *    - 每条记录单独写磁盘，产生大量系统调用
 *    - 1000条记录 = 1000次fsync = 性能灾难
 *
 * 3. 网络开销：
 *    - 每条记录单独发送，TCP/IP协议头开销大
 *    - 1000条记录 × 每条40字节TCP头 = 40KB浪费
 *
 * 批处理的好处：
 * - 元数据共享：1000条记录共享一套元数据（baseOffset, epoch等）
 * - 批量I/O：1000条记录一次性写入磁盘
 * - 批量网络传输：一次发送，减少往返次数
 * - 更好的压缩：压缩整个批次比压缩单条记录效果更好
 *
 * 【两种批次类型】
 *
 * 1. Data Batch（数据批次）：
 *    - 包含用户数据记录
 *    - records字段非空，controlRecords字段为空
 *    - 例如：[record1, record2, record3]
 *
 * 2. Control Batch（控制批次）：
 *    - 包含Raft协议的控制记录
 *    - controlRecords字段非空，records字段为空
 *    - 例如：[LeaderChangeRecord]，记录Leader变更
 *
 * 为什么分开？
 * - 用户数据（T类型）和控制记录（ControlRecord类型）是不同类型
 * - 控制记录对用户不可见，只用于Raft内部
 * - 分开处理逻辑更清晰
 *
 * 【核心字段】
 *
 * 1. baseOffset - 起始偏移量
 *    - 批次中第一条记录的偏移量
 *    - 例如：如果batch从offset=100开始，baseOffset=100
 *
 * 2. lastOffset - 结束偏移量
 *    - 批次中最后一条记录的偏移量
 *    - 计算公式：baseOffset + 记录数 - 1
 *    - 例如：3条记录，baseOffset=100，lastOffset=102
 *
 * 3. epoch - Leader的epoch
 *    - 创建这个批次的Leader的任期
 *    - 用于检测Leader变更
 *
 * 4. appendTimestamp - 追加时间戳
 *    - 批次写入日志的时间（毫秒）
 *    - 用于日志保留策略、监控等
 *
 * 5. sizeInBytes - 批次大小
 *    - 整个批次在磁盘上占用的字节数
 *    - 用于日志段管理、配额控制
 *
 * 6. records - 数据记录列表
 *    - 用户数据记录（类型T）
 *    - Data Batch中非空，Control Batch中为空
 *
 * 7. controlRecords - 控制记录列表
 *    - Raft控制记录
 *    - Control Batch中非空，Data Batch中为空
 *
 * 【使用场景】
 *
 * 场景1：Leader写入数据
 * <pre>
 * // 客户端提交了3条记录
 * List<MyRecord> userRecords = List.of(record1, record2, record3);
 *
 * // Leader创建一个批次
 * Batch<MyRecord> batch = Batch.data(
 *     100,                      // baseOffset
 *     5,                        // epoch
 *     System.currentTimeMillis(), // appendTimestamp
 *     1024,                     // sizeInBytes
 *     userRecords
 * );
 *
 * // 批次信息：
 * // - offset范围：100-102（3条记录）
 * // - epoch=5（Leader的任期）
 * // - 共占用1024字节
 * </pre>
 *
 * 场景2：Follower读取批次
 * <pre>
 * // 从Leader拉取到一个批次
 * Batch<MyRecord> batch = fetchFromLeader();
 *
 * // 遍历批次中的记录
 * for (MyRecord record : batch) {
 *     // 处理每条记录
 *     processRecord(record);
 * }
 * </pre>
 *
 * 场景3：Leader变更时写入控制记录
 * <pre>
 * // 新Leader当选后，写入LeaderChange控制记录
 * List<ControlRecord> controlRecords = List.of(
 *     new LeaderChangeRecord(newLeaderId, voters)
 * );
 *
 * Batch<MyRecord> controlBatch = Batch.control(
 *     103,                      // baseOffset
 *     6,                        // epoch（新Leader的epoch）
 *     System.currentTimeMillis(),
 *     256,                      // sizeInBytes
 *     controlRecords
 * );
 *
 * // 这个控制批次：
 * // - records为空（不包含用户数据）
 * // - controlRecords包含LeaderChange记录
 * // - 告诉Follower："epoch=6开始，新Leader是xxx"
 * </pre>
 *
 * 【Iterable实现】
 *
 * Batch实现了Iterable<T>接口：
 * <pre>
 * // 可以直接使用for-each循环
 * Batch<String> batch = ...;
 * for (String record : batch) {
 *     System.out.println(record);
 * }
 * </pre>
 *
 * 为什么实现Iterable？
 * - 语法简洁：for-each循环比手动迭代更清晰
 * - 符合Java习惯：集合类都实现Iterable
 * - 支持流式API：可以用batch.stream()
 *
 * 【不可变性】
 *
 * Batch是final类，所有字段都是final：
 * - 线程安全
 * - 创建后不可更改
 * - 可以安全地跨线程共享
 *
 * 【性能优化】
 *
 * 1. 批次大小权衡：
 *    - 太小：批处理优势不明显，元数据占比高
 *    - 太大：延迟增加（等待批次填满），内存占用大
 *    - 推荐：16KB - 1MB（根据场景调整）
 *
 * 2. 零拷贝：
 *    - 批次可以直接从网络读取到磁盘
 *    - 避免经过用户空间，减少拷贝次数
 *
 * 3. 压缩：
 *    - 整个批次一起压缩，压缩比更好
 *    - 解压时也是批量操作，减少开销
 *
 * @param <T> 记录类型（用户数据的类型）
 * @see ControlRecord Raft控制记录
 */
public final class Batch<T> implements Iterable<T> {
    /**
     * 起始偏移量
     *
     * 批次中第一条记录的偏移量。
     * 例如：如果批次包含offset=100,101,102三条记录，baseOffset=100。
     */
    private final long baseOffset;

    /**
     * Leader的epoch
     *
     * 创建这个批次的Leader的任期编号。
     * 用于：
     * - 检测Leader变更：如果epoch变了，说明换Leader了
     * - 日志一致性检查：相同offset的记录，epoch必须相同
     */
    private final int epoch;

    /**
     * 追加时间戳（毫秒）
     *
     * 批次写入日志的时间。
     * 用于：
     * - 日志保留策略：删除超过N天的日志
     * - 性能监控：追踪写入延迟
     * - 故障诊断：分析时间线
     */
    private final long appendTimestamp;

    /**
     * 批次大小（字节）
     *
     * 整个批次在磁盘上占用的字节数，包括：
     * - 批次头部元数据
     * - 所有记录的数据
     * - CRC校验和
     * - 可能的压缩数据
     *
     * 用于：
     * - 日志段切分：段文件达到大小限制时切换到新文件
     * - 配额控制：限制客户端的写入速率
     * - 磁盘空间管理
     */
    private final int sizeInBytes;

    /**
     * 结束偏移量
     *
     * 批次中最后一条记录的偏移量。
     * 计算公式：baseOffset + 记录数 - 1
     *
     * 例如：
     * - baseOffset=100，3条记录 → lastOffset=102
     * - baseOffset=100，1条记录 → lastOffset=100
     *
     * 为什么存储？
     * - 避免重复计算（需要频繁查询）
     * - 快速判断批次范围
     */
    private final long lastOffset;

    /**
     * 数据记录列表
     *
     * 用户数据记录（类型T）。
     *
     * 规则：
     * - Data Batch：records非空，controlRecords为空
     * - Control Batch：records为空，controlRecords非空
     *
     * 为什么是List而不是数组？
     * - List更灵活（可以传入ArrayList, ImmutableList等）
     * - List接口更清晰（只读操作）
     * - 避免数组协变问题
     */
    private final List<T> records;

    /**
     * 控制记录列表
     *
     * Raft协议的内部控制记录。
     *
     * 常见控制记录：
     * - LeaderChangeRecord：记录Leader变更
     * - SnapshotHeaderRecord：快照头部信息
     * - VotersRecord：投票者配置变更
     *
     * 规则：
     * - Control Batch：controlRecords非空，records为空
     * - Data Batch：controlRecords为空，records非空
     */
    private final List<ControlRecord> controlRecords;

    /**
     * 私有构造函数
     *
     * 强制使用静态工厂方法（data()或control()）创建实例。
     * 好处：
     * 1. 工厂方法名称更有语义
     * 2. 可以添加验证逻辑
     * 3. 确保data和control批次的字段约束
     *
     * @param baseOffset 起始偏移量
     * @param epoch Leader的epoch
     * @param appendTimestamp 追加时间戳
     * @param sizeInBytes 批次大小
     * @param lastOffset 结束偏移量
     * @param records 数据记录列表
     * @param controlRecords 控制记录列表
     */
    private Batch(
        long baseOffset,
        int epoch,
        long appendTimestamp,
        int sizeInBytes,
        long lastOffset,
        List<T> records,
        List<ControlRecord> controlRecords
    ) {
        this.baseOffset = baseOffset;
        this.epoch = epoch;
        this.appendTimestamp = appendTimestamp;
        this.sizeInBytes = sizeInBytes;
        this.lastOffset = lastOffset;
        this.records = records;
        this.controlRecords = controlRecords;
    }

    /**
     * 获取结束偏移量
     *
     * @return 批次中最后一条记录的偏移量
     */
    public long lastOffset() {
        return lastOffset;
    }

    /**
     * 获取起始偏移量
     *
     * @return 批次中第一条记录的偏移量
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * 获取追加时间戳
     *
     * @return 批次写入时间（毫秒）
     */
    public long appendTimestamp() {
        return appendTimestamp;
    }

    /**
     * 获取数据记录列表
     *
     * @return 用户数据记录（如果是Control Batch则为空列表）
     */
    public List<T> records() {
        return records;
    }

    /**
     * 获取控制记录列表
     *
     * @return 控制记录（如果是Data Batch则为空列表）
     */
    public List<ControlRecord> controlRecords() {
        return controlRecords;
    }

    /**
     * 获取Leader的epoch
     *
     * @return 创建这个批次的Leader的任期编号
     */
    public int epoch() {
        return epoch;
    }

    /**
     * 获取批次大小
     *
     * @return 批次占用的字节数
     */
    public int sizeInBytes() {
        return sizeInBytes;
    }

    /**
     * 实现Iterable接口
     *
     * 允许使用for-each循环遍历批次中的记录。
     *
     * 示例：
     * <pre>
     * Batch<String> batch = ...;
     * for (String record : batch) {
     *     System.out.println(record);
     * }
     * </pre>
     *
     * 注意：只遍历数据记录（records），不包括控制记录。
     *
     * @return 数据记录的迭代器
     */
    @Override
    public Iterator<T> iterator() {
        return records.iterator();
    }

    /**
     * 字符串表示
     *
     * 格式：Batch(baseOffset=100, epoch=5, appendTimestamp=..., sizeInBytes=1024, lastOffset=102, records=[...], controlRecords=[...])
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return "Batch(" +
            "baseOffset=" + baseOffset +
            ", epoch=" + epoch +
            ", appendTimestamp=" + appendTimestamp +
            ", sizeInBytes=" + sizeInBytes +
            ", lastOffset=" + lastOffset +
            ", records=" + records +
            ", controlRecords=" + controlRecords +
            ')';
    }

    /**
     * 判断相等性
     *
     * 两个Batch相等当且仅当所有字段都相同。
     *
     * @param o 要比较的对象
     * @return 如果相等返回true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Batch<?> batch = (Batch<?>) o;
        return baseOffset == batch.baseOffset &&
            epoch == batch.epoch &&
            appendTimestamp == batch.appendTimestamp &&
            sizeInBytes == batch.sizeInBytes &&
            lastOffset == batch.lastOffset &&
            Objects.equals(records, batch.records) &&
            Objects.equals(controlRecords, batch.controlRecords);
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
        return Objects.hash(
            baseOffset,
            epoch,
            appendTimestamp,
            sizeInBytes,
            lastOffset,
            records,
            controlRecords
        );
    }

    /**
     * 静态工厂方法：创建控制批次
     *
     * 控制批次包含Raft协议的内部控制记录，不包含用户数据。
     *
     * 约束：
     * - records字段为空列表
     * - controlRecords字段非空
     * - 至少包含一条控制记录
     *
     * lastOffset计算：
     * - lastOffset = baseOffset + controlRecords.size() - 1
     * - 每条控制记录占用一个offset
     *
     * 使用场景：
     * <pre>
     * // Leader变更时写入LeaderChangeRecord
     * List<ControlRecord> controlRecords = List.of(
     *     new LeaderChangeRecord(newLeaderId, voters)
     * );
     *
     * Batch<MyRecord> batch = Batch.control(
     *     baseOffset,
     *     newEpoch,
     *     System.currentTimeMillis(),
     *     sizeInBytes,
     *     controlRecords
     * );
     * </pre>
     *
     * @param <T> 记录类型（实际不包含T类型的记录）
     * @param baseOffset 起始偏移量
     * @param epoch Leader的epoch
     * @param appendTimestamp 追加时间戳
     * @param sizeInBytes 批次大小
     * @param records 控制记录列表
     * @return 控制批次
     * @throws IllegalArgumentException 如果控制记录列表为空
     */
    public static <T> Batch<T> control(
        long baseOffset,
        int epoch,
        long appendTimestamp,
        int sizeInBytes,
        List<ControlRecord> records
    ) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(
                    "Control batch must contain at least one record; baseOffset = %d; epoch = %d",
                    baseOffset,
                    epoch
                )
            );
        }

        return new Batch<>(
            baseOffset,
            epoch,
            appendTimestamp,
            sizeInBytes,
            baseOffset + records.size() - 1,  // 计算lastOffset
            List.of(),                         // 数据记录为空
            records                            // 控制记录
        );
    }

    /**
     * 静态工厂方法：创建数据批次
     *
     * 数据批次包含用户数据记录，不包含控制记录。
     *
     * 约束：
     * - records字段非空
     * - controlRecords字段为空列表
     * - 至少包含一条数据记录
     *
     * lastOffset计算：
     * - lastOffset = baseOffset + records.size() - 1
     * - 每条记录占用一个offset
     *
     * 使用场景：
     * <pre>
     * // Leader接收客户端写入的数据
     * List<MyRecord> userRecords = List.of(record1, record2, record3);
     *
     * Batch<MyRecord> batch = Batch.data(
     *     baseOffset,
     *     currentEpoch,
     *     System.currentTimeMillis(),
     *     sizeInBytes,
     *     userRecords
     * );
     *
     * // 批次包含3条记录：
     * // - baseOffset到lastOffset = 3个offset
     * // - 只包含用户数据，不包含控制记录
     * </pre>
     *
     * @param <T> 记录类型
     * @param baseOffset 起始偏移量
     * @param epoch Leader的epoch
     * @param appendTimestamp 追加时间戳
     * @param sizeInBytes 批次大小
     * @param records 数据记录列表
     * @return 数据批次
     * @throws IllegalArgumentException 如果记录列表为空
     */
    public static <T> Batch<T> data(
        long baseOffset,
        int epoch,
        long appendTimestamp,
        int sizeInBytes,
        List<T> records
    ) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(
                    "Batch must contain at least one record; baseOffset = %d; epoch = %d",
                    baseOffset,
                    epoch
                )
            );
        }

        return new Batch<>(
            baseOffset,
            epoch,
            appendTimestamp,
            sizeInBytes,
            baseOffset + records.size() - 1,  // 计算lastOffset
            records,                           // 数据记录
            List.of()                          // 控制记录为空
        );
    }
}
