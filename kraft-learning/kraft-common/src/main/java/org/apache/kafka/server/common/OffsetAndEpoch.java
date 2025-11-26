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
package org.apache.kafka.server.common;

/**
 * OffsetAndEpoch - 偏移量和Epoch的不可变组合
 *
 * 这是Raft协议中最基础的数据结构之一，用于唯一标识日志中的一个位置。
 *
 * 【核心概念】
 * - Offset（偏移量）：日志中记录的位置，从0开始递增的整数
 * - Epoch（纪元）：Leader的任期编号，每次选举都会递增
 *
 * 【为什么需要Epoch？】
 * 在Raft中，仅用offset无法唯一标识日志位置，因为：
 * 1. 不同的Leader可能在同一个offset写入不同的数据
 * 2. 需要Epoch来区分这些不同的历史
 *
 * 例如：
 * - Leader 1（epoch=5）在offset=100处写入了数据A
 * - Leader 2（epoch=6）在offset=100处写入了数据B（截断后重写）
 * - 通过(offset=100, epoch=6)可以明确指向数据B
 *
 * 【比较规则】
 * 在比较两个OffsetAndEpoch时，优先比较epoch，epoch相同再比较offset：
 * 1. 如果epoch不同，epoch大的更新
 * 2. 如果epoch相同，offset大的更新
 *
 * 这符合Raft的日志比较规则：高epoch的日志总是比低epoch的更权威。
 *
 * 【使用场景】
 * 1. 标识日志的结束位置（end offset）
 * 2. 标识快照的最后位置
 * 3. 在选举中比较谁的日志更新
 * 4. 追踪Follower的复制进度
 *
 * 【Java Record】
 * 使用Java 14引入的record特性，自动提供：
 * - 不可变性（final字段）
 * - 构造函数
 * - getter方法（offset(), epoch()）
 * - equals()和hashCode()
 * - toString()
 *
 * @param offset 日志偏移量，从0开始的递增整数
 * @param epoch Leader的任期编号（纪元），每次选举递增
 */
public record OffsetAndEpoch(long offset, int epoch) implements Comparable<OffsetAndEpoch> {

    /**
     * 比较两个OffsetAndEpoch的大小
     *
     * 比较规则（符合Raft协议）：
     * 1. 优先比较epoch：高epoch的更大（更新）
     * 2. epoch相同时比较offset：高offset的更大（更新）
     *
     * 例如：
     * - (offset=100, epoch=6) > (offset=200, epoch=5)  // epoch优先
     * - (offset=100, epoch=5) < (offset=200, epoch=5)  // epoch相同比offset
     *
     * 为什么epoch优先？
     * 因为高epoch意味着是更新的Leader任期，即使offset小，
     * 也代表更权威的日志状态。
     *
     * @param o 要比较的另一个OffsetAndEpoch
     * @return 负数表示this < o，0表示相等，正数表示this > o
     */
    @Override
    public int compareTo(OffsetAndEpoch o) {
        // 先比较epoch
        if (epoch == o.epoch) {
            // epoch相同，比较offset
            return Long.compare(offset, o.offset);
        }
        // epoch不同，epoch大的更新
        return Integer.compare(epoch, o.epoch);
    }
}
