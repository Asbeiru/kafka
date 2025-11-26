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
 * Isolation - 读隔离级别枚举
 *
 * 定义了从Raft日志读取数据时的隔离级别，类似于数据库中的隔离级别概念。
 *
 * 【核心概念】
 * 在Raft中，日志有两个重要的位置：
 * 1. Log End Offset（日志末尾偏移量）：Leader已写入的最新位置
 * 2. High Watermark（高水位）：已被多数派确认的最新位置
 *
 * 只有到达高水位的日志才被认为是"已提交"的，因为它们已被多数派副本持久化，
 * 即使Leader崩溃也不会丢失。
 *
 * 【为什么需要隔离级别？】
 * 考虑以下场景：
 *
 * 场景1：Leader刚写入，还未复制
 * - Leader写入了offset=100的记录
 * - 但还没来得及复制给Follower
 * - 此时高水位还在offset=99
 *
 * 场景2：正在复制中
 * - Leader写入了offset=100-105的记录
 * - 复制给了1个Follower（共3个节点）
 * - 还没达到多数派，高水位还在offset=99
 *
 * 问题：读取offset=100的记录安全吗？
 * - 如果Leader崩溃，这条记录可能丢失（还未被多数派确认）
 * - 新Leader可能没有这条记录
 *
 * 【两种隔离级别】
 *
 * 1. COMMITTED（已提交）- 安全但可能稍旧
 *    - 只读取 <= 高水位的记录
 *    - 保证：这些记录已被多数派确认，永远不会丢失
 *    - 代价：可能读不到最新的记录
 *    - 适用：需要强一致性保证的场景
 *
 *    示例：
 *    - Log End = 105，High Watermark = 99
 *    - COMMITTED只能读到offset <= 99的记录
 *    - offset 100-105的记录"不可见"
 *
 * 2. UNCOMMITTED（未提交）- 最新但可能不安全
 *    - 可以读取到Log End的所有记录
 *    - 保证：可以读到最新的数据
 *    - 风险：这些记录可能还未被多数派确认，Leader崩溃后可能丢失
 *    - 适用：需要最新数据且可以容忍可能回滚的场景
 *
 *    示例：
 *    - Log End = 105，High Watermark = 99
 *    - UNCOMMITTED可以读到offset <= 105的所有记录
 *    - 包括还未提交的100-105
 *
 * 【使用场景】
 *
 * COMMITTED（推荐用于客户端读取）：
 * <pre>
 * // 客户端读取已提交的数据
 * LogFetchInfo info = log.read(startOffset, Isolation.COMMITTED);
 * // 只会读到高水位之前的数据，保证不会读到未提交的
 * </pre>
 *
 * UNCOMMITTED（用于内部复制）：
 * <pre>
 * // Follower从Leader复制时，需要读取所有数据（包括未提交的）
 * LogFetchInfo info = log.read(startOffset, Isolation.UNCOMMITTED);
 * // 读取到Log End，包括还未被多数派确认的记录
 * </pre>
 *
 * 【Raft协议保证】
 *
 * 根据Raft协议：
 * 1. 已提交的日志（<= 高水位）永远不会丢失
 * 2. 未提交的日志（> 高水位）在Leader切换时可能被覆盖
 * 3. 高水位只会前进，不会后退
 *
 * 【类比数据库】
 *
 * 这类似于数据库的隔离级别：
 * - COMMITTED ≈ Read Committed（读已提交）
 * - UNCOMMITTED ≈ Read Uncommitted（读未提交）
 *
 * 区别：
 * - 数据库：事务隔离
 * - Raft：复制隔离
 *
 * 【性能考虑】
 *
 * COMMITTED：
 * - 优点：安全，数据不会丢失
 * - 缺点：可能有轻微延迟（等待多数派确认）
 *
 * UNCOMMITTED：
 * - 优点：延迟最低，立即可读
 * - 缺点：可能读到会被回滚的数据
 *
 * 【实际应用】
 *
 * Kafka KRaft中：
 * 1. 客户端读取元数据：使用COMMITTED（保证一致性）
 * 2. Follower复制日志：使用UNCOMMITTED（需要复制所有记录）
 * 3. Leader本地读取：可以使用UNCOMMITTED（Leader自己的数据）
 *
 * @see org.apache.kafka.raft.ReplicatedLog#read(long, Isolation) 读取日志时指定隔离级别
 * @see org.apache.kafka.raft.LeaderState 高水位的计算和管理
 */
public enum Isolation {
    /**
     * COMMITTED - 已提交的隔离级别
     *
     * 只读取已被多数派确认的记录（<= 高水位）。
     *
     * 保证：
     * - 读到的数据永远不会丢失
     * - 读到的数据在集群中是一致的
     * - 即使Leader崩溃，这些数据仍然存在
     *
     * 限制：
     * - 不能读到最新写入但还未被多数派确认的数据
     * - 可能有轻微的读延迟
     *
     * 适用场景：
     * - 客户端读取操作
     * - 需要强一致性保证的场景
     * - 不能容忍数据回滚的场景
     */
    COMMITTED,

    /**
     * UNCOMMITTED - 未提交的隔离级别
     *
     * 可以读取所有记录，包括还未被多数派确认的（到Log End）。
     *
     * 保证：
     * - 可以读到最新的数据
     * - 延迟最低
     *
     * 风险：
     * - 读到的数据可能还未被多数派确认
     * - Leader崩溃时，这些数据可能丢失
     * - 数据可能被后续覆盖
     *
     * 适用场景：
     * - Follower从Leader复制日志
     * - Leader读取自己的日志
     * - 内部操作，不暴露给外部客户端
     */
    UNCOMMITTED
}
