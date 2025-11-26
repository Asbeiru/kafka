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
import java.util.OptionalInt;

/**
 * LeaderAndEpoch - Leader标识和Epoch的不可变组合
 *
 * 这是Raft协议中用于标识当前Leader状态的核心数据结构。
 *
 * 【核心概念】
 * - Leader ID：当前Leader节点的唯一标识符（节点ID）
 * - Epoch：当前Leader的任期编号（纪元）
 *
 * 【为什么需要这个类？】
 * 在Raft中，我们经常需要同时知道"谁是Leader"和"Leader的任期"：
 * 1. 判断Leader是否发生了变化
 * 2. 检测过时的Leader（epoch较小）
 * 3. 在状态转移时传递Leader信息
 *
 * 将这两个信息组合在一起，可以：
 * - 原子性地表示Leader状态
 * - 避免不一致（Leader和epoch不匹配）
 * - 简化代码，避免传递两个独立参数
 *
 * 【使用场景】
 * 1. 状态查询：
 *    <pre>
 *    LeaderAndEpoch current = quorum.currentLeader();
 *    if (current.isLeader(localNodeId)) {
 *        // 我是Leader
 *    }
 *    </pre>
 *
 * 2. Leader变更通知：
 *    <pre>
 *    listener.handleLeaderChange(oldLeader, newLeader);
 *    </pre>
 *
 * 3. 状态转移：
 *    <pre>
 *    quorum.transitionToFollower(
 *        newLeaderAndEpoch.epoch(),
 *        newLeaderAndEpoch.leaderId().getAsInt()
 *    );
 *    </pre>
 *
 * 【Leader ID为什么是OptionalInt？】
 * 在某些情况下，可能没有Leader：
 * 1. 集群刚启动时
 * 2. 正在进行选举时
 * 3. 网络分区导致无法选出Leader时
 *
 * 使用OptionalInt表示"可能有Leader，也可能没有"，避免：
 * - 使用-1等magic number表示"无Leader"
 * - null引用导致的NullPointerException
 * - 代码中需要大量的null检查
 *
 * OptionalInt的优势：
 * - 类型安全：编译器强制检查
 * - 语义清晰：isEmpty()明确表示无Leader
 * - 避免装箱：相比Optional<Integer>，OptionalInt不需要装箱
 *
 * 【UNKNOWN常量】
 * 表示"未知Leader状态"，用于初始化或错误情况：
 * - leaderId = OptionalInt.empty()（无Leader）
 * - epoch = 0（初始epoch）
 *
 * 【Compact Constructor】
 * Java record的紧凑构造函数：
 * <pre>
 * public LeaderAndEpoch {
 *     Objects.requireNonNull(leaderId);  // 验证参数非null
 * }
 * </pre>
 * 这个语法会在标准构造函数之前执行，用于参数验证。
 * 虽然leaderId是OptionalInt（值类型），但仍然检查确保传入的不是null。
 *
 * 【示例】
 * <pre>
 * // 有Leader的情况
 * LeaderAndEpoch withLeader = new LeaderAndEpoch(OptionalInt.of(1), 5);
 * assert withLeader.isLeader(1);  // true
 *
 * // 无Leader的情况
 * LeaderAndEpoch noLeader = LeaderAndEpoch.UNKNOWN;
 * assert !noLeader.leaderId().isPresent();  // true
 *
 * // Leader变更检测
 * if (!oldLeader.equals(newLeader)) {
 *     log.info("Leader changed from {} to {}", oldLeader, newLeader);
 * }
 * </pre>
 *
 * @param leaderId Leader节点ID（可能为空，表示无Leader）
 * @param epoch Leader的任期编号（纪元）
 */
public record LeaderAndEpoch(OptionalInt leaderId, int epoch) {

    /**
     * UNKNOWN常量 - 表示未知的Leader状态
     *
     * 用途：
     * 1. 初始化：集群刚启动时的默认值
     * 2. 错误处理：当无法确定Leader时的返回值
     * 3. 占位符：某些算法中表示"尚未确定"的状态
     *
     * 值：
     * - leaderId = OptionalInt.empty()：表示无Leader
     * - epoch = 0：初始epoch（通常第一次选举会使用epoch=1）
     */
    public static final LeaderAndEpoch UNKNOWN = new LeaderAndEpoch(OptionalInt.empty(), 0);

    /**
     * 紧凑构造函数 - 参数验证
     *
     * 在实际构造对象之前执行，用于验证参数的有效性。
     * 这里确保leaderId不为null（虽然它是OptionalInt值类型，但仍需检查）。
     *
     * 如果leaderId为null，会抛出NullPointerException。
     */
    public LeaderAndEpoch {
        Objects.requireNonNull(leaderId, "leaderId cannot be null");
    }

    /**
     * 判断指定节点是否是当前Leader
     *
     * 用于快速检查某个节点是否是Leader，常见用法：
     * <pre>
     * if (currentLeader.isLeader(localNodeId)) {
     *     // 我是Leader，处理写请求
     * } else {
     *     // 我不是Leader，拒绝写请求或转发给Leader
     * }
     * </pre>
     *
     * 返回true的条件：
     * 1. leaderId不为空（有Leader）
     * 2. leaderId等于指定的nodeId
     *
     * @param nodeId 要检查的节点ID
     * @return 如果指定节点是当前Leader返回true，否则返回false
     */
    public boolean isLeader(int nodeId) {
        return leaderId.isPresent() && leaderId.getAsInt() == nodeId;
    }

    /**
     * record自动生成的方法：
     * - leaderId()：获取Leader ID
     * - epoch()：获取Epoch
     * - equals(Object)：判断相等（Leader ID和Epoch都相同）
     * - hashCode()：计算哈希码
     * - toString()：字符串表示，格式：LeaderAndEpoch[leaderId=OptionalInt[1], epoch=5]
     */
}
