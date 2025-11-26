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
import java.util.OptionalLong;

/**
 * BatchReader - 批次读取器接口
 *
 * 用于从RaftClient向注册的Listener传递已提交的数据批次。
 *
 * 【为什么需要BatchReader？】
 *
 * 将批次消费隐藏在接口后面的好处是：
 * 1. **异步I/O**：将阻塞的磁盘读取操作推到Raft I/O线程之外
 * 2. **解耦**：慢速状态机不会影响Raft复制
 * 3. **延迟读取**：只在需要时才从磁盘读取数据
 *
 * 【为什么不直接传List<Batch<T>>？】
 *
 * 对比两种设计：
 *
 * **方案A：直接传List（不好）**
 * <pre>
 * void handleCommit(List<Batch<T>> batches) {
 *     // 问题：RaftClient必须先把所有批次从磁盘读入内存
 *     // → 阻塞Raft I/O线程
 *     // → 大量内存占用
 * }
 * </pre>
 *
 * **方案B：使用BatchReader（更好）**
 * <pre>
 * void handleCommit(BatchReader<T> reader) {
 *     // RaftClient只传递reader接口
 *     // → 状态机在自己的线程中读取
 *     // → Raft I/O线程不阻塞
 *     // → 按需读取，节省内存
 *
 *     while (reader.hasNext()) {
 *         Batch<T> batch = reader.next(); // 这里可能触发磁盘I/O
 *         apply(batch); // 应用到状态机
 *     }
 *     reader.close(); // 释放资源
 * }
 * </pre>
 *
 * 【核心设计】
 *
 * BatchReader结合了三个接口：
 * 1. **Iterator<Batch<T>>** - 提供hasNext()和next()
 * 2. **AutoCloseable** - 提供close()资源管理
 * 3. **自定义方法** - baseOffset()和lastOffset()
 *
 * 【Iterator模式】
 *
 * 使用标准的Iterator接口：
 * <pre>
 * while (reader.hasNext()) {
 *     Batch<T> batch = reader.next();
 *     // 处理batch
 * }
 * </pre>
 *
 * 也支持for-each循环：
 * <pre>
 * for (Batch<T> batch : reader) {
 *     // 处理batch
 * }
 * </pre>
 *
 * 【资源管理】
 *
 * 实现AutoCloseable，支持try-with-resources：
 * <pre>
 * public void handleCommit(BatchReader<T> reader) {
 *     try (reader) {  // 自动关闭
 *         while (reader.hasNext()) {
 *             Batch<T> batch = reader.next();
 *             stateMachine.apply(batch);
 *         }
 *     }
 * }
 * </pre>
 *
 * 不使用try-with-resources也必须手动关闭：
 * <pre>
 * public void handleCommit(BatchReader<T> reader) {
 *     try {
 *         while (reader.hasNext()) {
 *             Batch<T> batch = reader.next();
 *             stateMachine.apply(batch);
 *         }
 *     } finally {
 *         reader.close(); // 必须关闭！
 *     }
 * }
 * </pre>
 *
 * 【实现类】
 *
 * KRaft中的BatchReader实现：
 *
 * 1. **MemoryBatchReader<T>** - 从内存读取
 *    <pre>
 *    // 数据已经在内存中（如Leader刚写入的数据）
 *    List<Batch<T>> batches = ...;
 *    BatchReader<T> reader = new MemoryBatchReader<>(batches);
 *    </pre>
 *
 * 2. **RecordsBatchReader<T>** - 从磁盘读取
 *    <pre>
 *    // 数据在磁盘上（如Follower从Leader同步的数据）
 *    Records records = log.read(offset, maxBytes);
 *    BatchReader<T> reader = new RecordsBatchReader<>(records, serde);
 *    </pre>
 *
 * 【baseOffset vs lastOffset】
 *
 * 两者的区别：
 *
 * **baseOffset**：
 * - 第一个批次的第一条记录的offset
 * - 创建BatchReader时确定，永不改变
 * - 总是已知的
 *
 * **lastOffset**：
 * - 最后一个批次的最后一条记录的offset
 * - 可能未知（从磁盘读取时）
 * - 只有读完所有批次后才知道
 *
 * 示例：
 * <pre>
 * BatchReader reader = createReader();
 *
 * // baseOffset立即可用
 * long baseOffset = reader.baseOffset(); // 100
 *
 * // lastOffset可能未知
 * OptionalLong lastOffset = reader.lastOffset();
 * if (lastOffset.isPresent()) {
 *     System.out.println("Last offset: " + lastOffset.getAsLong()); // 150
 * } else {
 *     System.out.println("Last offset unknown, must read all batches");
 * }
 * </pre>
 *
 * 【为什么lastOffset可能未知？】
 *
 * 从磁盘读取时的问题：
 * <pre>
 * // RecordsBatchReader从Records读取
 * Records records = log.read(100, 1MB);
 *
 * // Records是一段连续的字节流
 * // 包含多个批次：[Batch1, Batch2, Batch3, ...]
 * // 不解析就不知道最后一个批次的lastOffset是多少
 *
 * BatchReader reader = new RecordsBatchReader(records);
 * reader.lastOffset(); // OptionalLong.empty()
 *
 * // 必须读完所有批次
 * while (reader.hasNext()) {
 *     reader.next();
 * }
 *
 * // 现在知道了
 * reader.lastOffset(); // OptionalLong.of(150)
 * </pre>
 *
 * 【状态机的责任】
 *
 * 如果lastOffset未知，状态机必须：
 * 1. 读完BatchReader的所有批次
 * 2. 才能处理下一个BatchReader
 *
 * 示例：
 * <pre>
 * class MyStateMachine implements RaftClient.Listener<T> {
 *     private BatchReader<T> pendingReader;
 *
 *     @Override
 *     public void handleCommit(BatchReader<T> reader) {
 *         if (pendingReader != null) {
 *             throw new IllegalStateException("Previous reader not fully consumed");
 *         }
 *
 *         // 如果lastOffset未知，必须先处理完
 *         if (reader.lastOffset().isEmpty()) {
 *             while (reader.hasNext()) {
 *                 apply(reader.next());
 *             }
 *             reader.close();
 *         } else {
 *             // lastOffset已知，可以异步处理
 *             pendingReader = reader;
 *             scheduleAsyncProcessing(reader);
 *         }
 *     }
 * }
 * </pre>
 *
 * 【典型使用模式】
 *
 * 模式1：同步处理（最简单）
 * <pre>
 * @Override
 * public void handleCommit(BatchReader<T> reader) {
 *     try {
 *         while (reader.hasNext()) {
 *             Batch<T> batch = reader.next();
 *             for (T record : batch) {
 *                 stateMachine.apply(record);
 *             }
 *         }
 *     } finally {
 *         reader.close();
 *     }
 * }
 * </pre>
 *
 * 模式2：异步处理（更复杂但性能更好）
 * <pre>
 * @Override
 * public void handleCommit(BatchReader<T> reader) {
 *     // 在后台线程处理
 *     executor.submit(() -> {
 *         try {
 *             while (reader.hasNext()) {
 *                 Batch<T> batch = reader.next();
 *                 processBatch(batch);
 *             }
 *         } finally {
 *             reader.close();
 *         }
 *     });
 * }
 * </pre>
 *
 * 模式3：批量处理
 * <pre>
 * @Override
 * public void handleCommit(BatchReader<T> reader) {
 *     List<Batch<T>> batches = new ArrayList<>();
 *     try {
 *         // 收集所有批次
 *         while (reader.hasNext()) {
 *             batches.add(reader.next());
 *         }
 *
 *         // 批量应用
 *         stateMachine.applyBatches(batches);
 *     } finally {
 *         reader.close();
 *     }
 * }
 * </pre>
 *
 * 【性能优化】
 *
 * BatchReader的设计支持以下优化：
 *
 * 1. **延迟反序列化**
 *    - next()时才反序列化当前批次
 *    - 节省内存和CPU
 *
 * 2. **预读（Prefetching）**
 *    - 后台线程提前读取下一个批次
 *    - 隐藏I/O延迟
 *
 * 3. **零拷贝**
 *    - 如果状态机不需要修改数据
 *    - 可以直接使用原始ByteBuffer
 *    - 避免数据拷贝
 *
 * 【错误处理】
 *
 * next()可能抛出异常：
 * <pre>
 * try {
 *     while (reader.hasNext()) {
 *         Batch<T> batch = reader.next(); // 可能抛出IOException等
 *         apply(batch);
 *     }
 * } catch (Exception e) {
 *     logger.error("Failed to read batch", e);
 *     // 处理错误：可能需要重启或跳过
 * } finally {
 *     reader.close(); // 即使出错也要关闭
 * }
 * </pre>
 *
 * @param <T> 记录类型（由RecordSerde定义如何序列化/反序列化）
 * @see Batch 记录批次
 * @see RaftClient.Listener#handleCommit(BatchReader) 状态机处理已提交批次
 */
public interface BatchReader<T> extends Iterator<Batch<T>>, AutoCloseable {

    /**
     * 获取起始偏移量
     *
     * 返回可读批次的第一条记录的offset。
     *
     * 【不变性】
     *
     * 这个值在BatchReader创建时确定，永不改变。
     * 即使调用next()消费批次，baseOffset()返回值也不变。
     *
     * 示例：
     * <pre>
     * BatchReader reader = createReader(); // baseOffset=100
     *
     * System.out.println(reader.baseOffset()); // 100
     * reader.next(); // 读取第一个批次
     * System.out.println(reader.baseOffset()); // 还是100（不变）
     * reader.next(); // 读取第二个批次
     * System.out.println(reader.baseOffset()); // 还是100（不变）
     * </pre>
     *
     * 【用途】
     *
     * 1. **日志记录**
     *    <pre>
     *    logger.info("Processing batches starting from offset {}", reader.baseOffset());
     *    </pre>
     *
     * 2. **进度追踪**
     *    <pre>
     *    // 记录状态机处理的起始位置
     *    stateMachine.recordProgress(reader.baseOffset());
     *    </pre>
     *
     * 3. **验证连续性**
     *    <pre>
     *    long expectedOffset = lastProcessedOffset + 1;
     *    long actualOffset = reader.baseOffset();
     *    if (expectedOffset != actualOffset) {
     *        throw new IllegalStateException("Gap in log");
     *    }
     *    </pre>
     *
     * @return 起始偏移量（第一个批次的第一条记录的offset）
     */
    long baseOffset();

    /**
     * 获取结束偏移量（如果已知）
     *
     * 返回最后一个批次的最后一条记录的offset。
     *
     * 【为什么可能未知？】
     *
     * 从磁盘读取时，在解析所有批次之前，无法知道lastOffset：
     *
     * <pre>
     * // RecordsBatchReader从原始字节流读取
     * Records records = log.read(100, 1MB); // 读取1MB数据
     *
     * // records是未解析的字节流：
     * // [Batch1: offset=100-104, Batch2: offset=105-109, Batch3: offset=110-115]
     * // 不解析，不知道Batch3的lastOffset是115
     *
     * BatchReader reader = new RecordsBatchReader(records);
     * reader.lastOffset(); // OptionalLong.empty()（未知）
     *
     * // 读取所有批次后
     * while (reader.hasNext()) {
     *     reader.next();
     * }
     * reader.lastOffset(); // OptionalLong.of(115)（已知）
     * </pre>
     *
     * 【MemoryBatchReader vs RecordsBatchReader】
     *
     * MemoryBatchReader（内存）：
     * <pre>
     * List<Batch<T>> batches = List.of(batch1, batch2, batch3);
     * BatchReader reader = new MemoryBatchReader(batches);
     *
     * // lastOffset立即已知（从List中获取）
     * reader.lastOffset(); // OptionalLong.of(115)
     * </pre>
     *
     * RecordsBatchReader（磁盘）：
     * <pre>
     * Records records = log.read(...);
     * BatchReader reader = new RecordsBatchReader(records);
     *
     * // lastOffset未知（需要解析）
     * reader.lastOffset(); // OptionalLong.empty()
     * </pre>
     *
     * 【状态机的处理策略】
     *
     * 策略1：如果未知，必须同步处理完
     * <pre>
     * if (reader.lastOffset().isEmpty()) {
     *     // 必须立即处理完所有批次
     *     while (reader.hasNext()) {
     *         apply(reader.next());
     *     }
     *     reader.close();
     * } else {
     *     // lastOffset已知，可以异步处理
     *     asyncProcess(reader);
     * }
     * </pre>
     *
     * 策略2：总是同步处理（最简单）
     * <pre>
     * // 不管lastOffset是否已知，都同步处理
     * try {
     *     while (reader.hasNext()) {
     *         apply(reader.next());
     *     }
     * } finally {
     *     reader.close();
     * }
     * </pre>
     *
     * 【为什么需要lastOffset？】
     *
     * 1. **提前知道范围**
     *    <pre>
     *    OptionalLong lastOpt = reader.lastOffset();
     *    if (lastOpt.isPresent()) {
     *        long last = lastOpt.getAsLong();
     *        long count = last - reader.baseOffset() + 1;
     *        System.out.println("Will process " + count + " records");
     *    }
     *    </pre>
     *
     * 2. **支持异步处理**
     *    <pre>
     *    // 如果lastOffset已知，可以立即更新进度
     *    if (reader.lastOffset().isPresent()) {
     *        long last = reader.lastOffset().getAsLong();
     *        // 提前更新进度指针，允许接收下一个reader
     *        updateProgress(last + 1);
     *         // 在后台异步处理
     *        asyncProcess(reader);
     *    }
     *    </pre>
     *
     * 3. **优化内存分配**
     *    <pre>
     *    OptionalLong lastOpt = reader.lastOffset();
     *    if (lastOpt.isPresent()) {
     *        long count = lastOpt.getAsLong() - reader.baseOffset() + 1;
     *        List<T> results = new ArrayList<>((int) count); // 预分配
     *    }
     *    </pre>
     *
     * @return 结束偏移量（如果已知），如果还不知道则返回OptionalLong.empty()
     */
    OptionalLong lastOffset();

    /**
     * 关闭此读取器并释放资源
     *
     * 【Listener的责任】
     *
     * RaftClient.Listener必须关闭传递给handleCommit()的每个reader。
     * 这是Listener的**强制责任**，不关闭会导致资源泄漏。
     *
     * 【资源泄漏的后果】
     *
     * 如果不关闭BatchReader：
     * 1. 内存泄漏：批次缓冲区无法释放
     * 2. 文件句柄泄漏：日志文件保持打开
     * 3. 磁盘空间泄漏：旧日志无法删除
     *
     * 【正确的使用方式】
     *
     * 方式1：try-with-resources（推荐）
     * <pre>
     * @Override
     * public void handleCommit(BatchReader<T> reader) {
     *     try (reader) {  // 自动调用close()
     *         while (reader.hasNext()) {
     *             apply(reader.next());
     *         }
     *     }
     * }
     * </pre>
     *
     * 方式2：try-finally
     * <pre>
     * @Override
     * public void handleCommit(BatchReader<T> reader) {
     *     try {
     *         while (reader.hasNext()) {
     *             apply(reader.next());
     *         }
     *     } finally {
     *         reader.close(); // 确保关闭
     *     }
     * }
     * </pre>
     *
     * 方式3：异步处理（注意传递close责任）
     * <pre>
     * @Override
     * public void handleCommit(BatchReader<T> reader) {
     *     executor.submit(() -> {
     *         try {
     *             processAsync(reader);
     *         } finally {
     *             reader.close(); // 后台线程负责关闭
     *         }
     *     });
     * }
     * </pre>
     *
     * 【错误的做法】
     *
     * ❌ 忘记关闭：
     * <pre>
     * public void handleCommit(BatchReader<T> reader) {
     *     while (reader.hasNext()) {
     *         apply(reader.next());
     *     }
     *     // 忘记close() → 资源泄漏！
     * }
     * </pre>
     *
     * ❌ 异常时未关闭：
     * <pre>
     * public void handleCommit(BatchReader<T> reader) {
     *     while (reader.hasNext()) {
     *         apply(reader.next()); // 可能抛异常
     *     }
     *     reader.close(); // 如果抛异常，这行不执行 → 泄漏！
     * }
     * </pre>
     *
     * 【幂等性】
     *
     * close()可以安全地多次调用：
     * <pre>
     * reader.close();
     * reader.close(); // OK，第二次调用无操作
     * </pre>
     *
     * 【close()后的行为】
     *
     * 关闭后不能再使用reader：
     * <pre>
     * reader.close();
     * reader.hasNext(); // 可能抛异常或返回false
     * reader.next();    // 抛异常
     * </pre>
     */
    @Override
    void close();
}
