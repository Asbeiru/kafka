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

/**
 * LogAppendInfo - 日志追加信息
 *
 * 追加记录批次到日志后返回的元数据。
 * 包含新追加批次的偏移量范围信息。
 *
 * 【为什么需要LogAppendInfo？】
 *
 * 当Leader或Follower追加一个批次到本地日志时，需要知道：
 * 1. 批次从哪个offset开始（firstOffset）
 * 2. 批次到哪个offset结束（lastOffset）
 *
 * 这些信息用于：
 * - 更新日志的末尾偏移量（Log End Offset）
 * - 通知客户端写入成功的offset范围
 * - Leader追踪Follower的复制进度
 * - 计算高水位（High Watermark）
 *
 * 【使用场景】
 *
 * 场景1：Leader追加客户端写入的数据
 * <pre>
 * // 客户端提交了3条记录
 * Batch<Record> batch = ...;  // 包含3条记录
 *
 * // Leader追加到本地日志
 * LogAppendInfo appendInfo = log.append(batch);
 *
 * // appendInfo告诉我们：
 * // - firstOffset = 100（批次起始位置）
 * // - lastOffset = 102（批次结束位置，包含3条记录：100,101,102）
 *
 * // Leader可以：
 * // 1. 更新Log End Offset = 103（lastOffset + 1）
 * // 2. 响应客户端："你的数据写入成功，offset范围是100-102"
 * // 3. 开始复制这个批次给Follower
 * </pre>
 *
 * 场景2：Follower追加从Leader拉取的数据
 * <pre>
 * // Follower从Leader拉取到一个批次
 * Batch<Record> batch = fetchFromLeader();
 *
 * // Follower追加到本地日志
 * LogAppendInfo appendInfo = log.append(batch);
 *
 * // appendInfo告诉Follower：
 * // - firstOffset = 100
 * // - lastOffset = 102
 *
 * // Follower可以：
 * // 1. 更新自己的Log End Offset = 103
 * // 2. 在下次Fetch请求中告诉Leader："我已经同步到offset=102了"
 * </pre>
 *
 * 场景3：Leader计算高水位
 * <pre>
 * // Leader追加批次后
 * LogAppendInfo appendInfo = log.append(batch);
 *
 * // 收集所有Follower的复制进度
 * if (majorityReplicatedTo(appendInfo.lastOffset())) {
 *     // 如果多数派都复制到了lastOffset
 *     // 更新高水位
 *     updateHighWatermark(appendInfo.lastOffset());
 * }
 * </pre>
 *
 * 【字段说明】
 *
 * 1. firstOffset - 批次起始偏移量
 *    - 批次中第一条记录的offset
 *    - 等价于批次的baseOffset
 *
 * 2. lastOffset - 批次结束偏移量
 *    - 批次中最后一条记录的offset
 *    - 如果批次包含N条记录：lastOffset = firstOffset + N - 1
 *
 * 示例：
 * <pre>
 * // 批次包含3条记录
 * LogAppendInfo info = new LogAppendInfo(100, 102);
 *
 * // 这表示：
 * // - 第一条记录的offset = 100
 * // - 最后一条记录的offset = 102
 * // - 批次包含3条记录（100, 101, 102）
 * // - Log End Offset会更新为103（lastOffset + 1）
 * </pre>
 *
 * 【与Batch的关系】
 *
 * LogAppendInfo是追加Batch后返回的结果：
 * <pre>
 * Batch<T> batch = Batch.data(
 *     100,          // baseOffset
 *     5,            // epoch
 *     timestamp,
 *     sizeInBytes,
 *     records       // 3条记录
 * );
 *
 * LogAppendInfo info = log.append(batch);
 *
 * // 关系：
 * // info.firstOffset() == batch.baseOffset()  → 100
 * // info.lastOffset() == batch.lastOffset()   → 102
 * </pre>
 *
 * 【为什么是record？】
 *
 * 使用Java record（Java 14+）的好处：
 * 1. 简洁：自动生成构造函数、getter、equals、hashCode、toString
 * 2. 不可变：所有字段都是final，线程安全
 * 3. 语义清晰：明确表达"这是一个简单的数据载体"
 * 4. 性能：编译器可以做更多优化
 *
 * 等价的传统类定义需要更多代码：
 * <pre>
 * public final class LogAppendInfo {
 *     private final long firstOffset;
 *     private final long lastOffset;
 *
 *     public LogAppendInfo(long firstOffset, long lastOffset) {
 *         this.firstOffset = firstOffset;
 *         this.lastOffset = lastOffset;
 *     }
 *
 *     public long firstOffset() { return firstOffset; }
 *     public long lastOffset() { return lastOffset; }
 *
 *     @Override public boolean equals(Object o) { ... }
 *     @Override public int hashCode() { ... }
 *     @Override public String toString() { ... }
 * }
 * </pre>
 *
 * 而使用record只需要一行代码！
 *
 * 【实际应用】
 *
 * 在KRaft的日志追加流程中：
 * <pre>
 * // Leader端：
 * 1. 接收客户端写入请求
 * 2. 创建Batch
 * 3. 追加到日志：appendInfo = log.append(batch)
 * 4. 使用appendInfo.lastOffset()追踪复制进度
 * 5. 多数派复制后，更新高水位到appendInfo.lastOffset()
 *
 * // Follower端：
 * 1. 从Leader拉取Batch
 * 2. 追加到日志：appendInfo = log.append(batch)
 * 3. 使用appendInfo.lastOffset()更新自己的复制进度
 * 4. 在下次Fetch请求中报告进度
 * </pre>
 *
 * @param firstOffset 批次起始偏移量（第一条记录的offset）
 * @param lastOffset 批次结束偏移量（最后一条记录的offset）
 * @see Batch 记录批次
 * @see LogFetchInfo 日志读取信息
 */
public record LogAppendInfo(long firstOffset, long lastOffset) {
}
