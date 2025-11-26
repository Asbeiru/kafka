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

import org.apache.kafka.server.common.OffsetAndEpoch;

import java.util.Objects;

/**
 * ValidOffsetAndEpoch - 带验证状态的偏移量和Epoch
 *
 * 这个类在OffsetAndEpoch的基础上增加了"有效性类型"的标记，用于表示
 * 日志验证的结果。
 *
 * 【核心概念】
 * 在Raft中，当Leader需要验证Follower的日志时，会检查Follower报告的
 * 某个偏移量和epoch是否与Leader的日志一致。验证结果有三种可能：
 *
 * 1. VALID（有效）：Follower的日志与Leader一致
 * 2. DIVERGING（分歧）：Follower的日志与Leader不一致，需要截断
 * 3. SNAPSHOT（快照）：该偏移量已被压缩成快照，不在日志中
 *
 * 【为什么需要这个类？】
 * 在Raft协议中，Follower可能因为网络分区、崩溃恢复等原因，导致日志与
 * Leader不一致。Leader需要找到Follower日志的"分歧点"，然后指导
 * Follower从该点开始重新复制。
 *
 * 【使用场景】
 *
 * 场景1：Follower追赶Leader
 * <pre>
 * // Follower报告：我的日志到(offset=100, epoch=5)
 * OffsetAndEpoch followerEnd = new OffsetAndEpoch(100, 5);
 *
 * // Leader验证：检查我的日志在(100, 5)是否一致
 * ValidOffsetAndEpoch result = leader.validateOffsetAndEpoch(followerEnd);
 *
 * if (result.kind() == Kind.VALID) {
 *     // 一致！从offset=101开始发送新日志
 * } else if (result.kind() == Kind.DIVERGING) {
 *     // 不一致！Follower需要截断，然后从分歧点重新复制
 *     OffsetAndEpoch divergePoint = result.offsetAndEpoch();
 *     follower.truncateTo(divergePoint.offset());
 * }
 * </pre>
 *
 * 场景2：处理快照
 * <pre>
 * // Follower报告的offset已经被压缩成快照了
 * ValidOffsetAndEpoch result = leader.validateOffsetAndEpoch(oldOffset);
 *
 * if (result.kind() == Kind.SNAPSHOT) {
 *     // 该offset在快照中，直接发送快照给Follower
 *     leader.sendSnapshot(follower);
 * }
 * </pre>
 *
 * 【三种状态详解】
 *
 * 1. VALID（有效）
 *    - 含义：Follower报告的(offset, epoch)与Leader的日志完全匹配
 *    - 行动：Leader可以从该offset之后继续发送日志
 *    - 示例：Follower说"我到offset=100"，Leader检查发现确实一致
 *
 * 2. DIVERGING（分歧）
 *    - 含义：Follower报告的(offset, epoch)与Leader的日志不匹配
 *    - 行动：Follower需要截断到分歧点，然后重新复制
 *    - 示例：Follower说"我的offset=100是epoch=5"，但Leader的offset=100是epoch=6
 *    - 原因：可能Follower曾经跟随过旧Leader，现在需要修正
 *
 * 3. SNAPSHOT（快照）
 *    - 含义：请求的offset已经被压缩成快照，不在日志中了
 *    - 行动：发送快照给Follower
 *    - 示例：Follower请求offset=10，但Leader的日志从offset=100开始（0-99已成快照）
 *
 * 【设计模式】
 * 这是一个典型的"Tagged Union"模式：
 * - Kind（标签）：标识是哪种类型
 * - OffsetAndEpoch（值）：携带的数据
 * - 静态工厂方法：提供类型安全的构造方式
 *
 * 【不可变性】
 * - private构造函数：只能通过静态方法创建
 * - final字段：创建后不可修改
 * - 好处：线程安全，可以安全共享
 *
 * 【静态工厂方法】
 * 使用静态方法而不是公共构造函数的好处：
 * 1. 语义清晰：diverging()比new ValidOffsetAndEpoch(DIVERGING, ...)更易读
 * 2. 类型安全：确保kind和offsetAndEpoch的组合总是有效
 * 3. 灵活性：可以返回缓存的实例、子类等
 *
 * @see OffsetAndEpoch 底层的偏移量和epoch组合
 * @see Kind 验证结果的类型
 */
public final class ValidOffsetAndEpoch {
    /**
     * 验证结果的类型
     */
    private final Kind kind;

    /**
     * 具体的偏移量和epoch
     *
     * 不同kind下的含义：
     * - VALID: 表示这是一个有效的、匹配的offset
     * - DIVERGING: 表示这是分歧开始的offset（需要截断到此）
     * - SNAPSHOT: 表示这是快照覆盖的offset范围
     */
    private final OffsetAndEpoch offsetAndEpoch;

    /**
     * 私有构造函数 - 只能通过静态工厂方法创建
     *
     * 设计为private是为了：
     * 1. 强制使用语义清晰的静态方法（diverging(), snapshot(), valid()）
     * 2. 确保kind和offsetAndEpoch的组合总是有效的
     * 3. 未来可以改变内部实现而不影响API
     *
     * @param kind 验证结果类型
     * @param offsetAndEpoch 偏移量和epoch
     */
    private ValidOffsetAndEpoch(Kind kind, OffsetAndEpoch offsetAndEpoch) {
        this.kind = kind;
        this.offsetAndEpoch = offsetAndEpoch;
    }

    /**
     * 获取验证结果类型
     *
     * @return VALID, DIVERGING, 或 SNAPSHOT
     */
    public Kind kind() {
        return kind;
    }

    /**
     * 获取偏移量和epoch
     *
     * @return 偏移量和epoch的组合
     */
    public OffsetAndEpoch offsetAndEpoch() {
        return offsetAndEpoch;
    }

    /**
     * Kind - 验证结果的类型枚举
     *
     * 定义了日志验证的三种可能结果。
     */
    public enum Kind {
        /**
         * DIVERGING - 日志分歧
         *
         * 表示Follower的日志与Leader不一致，需要截断并重新复制。
         *
         * 发生原因：
         * 1. Follower曾经跟随过旧Leader，写入了不同的日志
         * 2. Follower在网络分区期间写入了日志，但后来被覆盖
         * 3. Follower崩溃恢复后，部分日志丢失
         *
         * 处理方式：
         * - Follower截断到分歧点
         * - Leader从分歧点开始发送正确的日志
         */
        DIVERGING,

        /**
         * SNAPSHOT - 已成快照
         *
         * 表示请求的offset已经被压缩成快照，不在日志中了。
         *
         * 发生原因：
         * 1. Leader定期清理旧日志，压缩成快照
         * 2. Follower落后太多，请求的offset已被清理
         *
         * 处理方式：
         * - Leader发送快照给Follower
         * - Follower加载快照，然后从快照之后继续复制
         */
        SNAPSHOT,

        /**
         * VALID - 有效匹配
         *
         * 表示Follower的日志与Leader一致。
         *
         * 含义：
         * - Follower报告的(offset, epoch)与Leader的日志匹配
         * - 可以安全地从该offset之后继续复制
         *
         * 处理方式：
         * - Leader从下一个offset开始发送新日志
         */
        VALID
    }

    /**
     * 创建DIVERGING类型的结果
     *
     * 用于表示日志在指定的offset和epoch处开始分歧。
     *
     * @param offsetAndEpoch 分歧开始的位置
     * @return DIVERGING类型的ValidOffsetAndEpoch
     */
    public static ValidOffsetAndEpoch diverging(OffsetAndEpoch offsetAndEpoch) {
        return new ValidOffsetAndEpoch(Kind.DIVERGING, offsetAndEpoch);
    }

    /**
     * 创建SNAPSHOT类型的结果
     *
     * 用于表示请求的offset已被压缩成快照。
     *
     * @param offsetAndEpoch 快照覆盖的offset范围
     * @return SNAPSHOT类型的ValidOffsetAndEpoch
     */
    public static ValidOffsetAndEpoch snapshot(OffsetAndEpoch offsetAndEpoch) {
        return new ValidOffsetAndEpoch(Kind.SNAPSHOT, offsetAndEpoch);
    }

    /**
     * 创建VALID类型的结果
     *
     * 用于表示日志在指定offset和epoch处是有效的。
     *
     * @param offsetAndEpoch 有效的位置
     * @return VALID类型的ValidOffsetAndEpoch
     */
    public static ValidOffsetAndEpoch valid(OffsetAndEpoch offsetAndEpoch) {
        return new ValidOffsetAndEpoch(Kind.VALID, offsetAndEpoch);
    }

    /**
     * 创建默认的VALID结果（无具体offset）
     *
     * 使用(-1, -1)表示"有效但没有具体位置"，用于：
     * 1. 初始化
     * 2. 表示"整体有效"而不是"某个位置有效"
     *
     * @return VALID类型的ValidOffsetAndEpoch，offset和epoch都是-1
     */
    public static ValidOffsetAndEpoch valid() {
        return valid(new OffsetAndEpoch(-1, -1));
    }

    /**
     * 判断相等性
     *
     * 两个ValidOffsetAndEpoch相等当且仅当：
     * 1. kind相同
     * 2. offsetAndEpoch相同
     *
     * @param obj 要比较的对象
     * @return 如果相等返回true
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ValidOffsetAndEpoch that = (ValidOffsetAndEpoch) obj;
        return kind == that.kind &&
                offsetAndEpoch.equals(that.offsetAndEpoch);
    }

    /**
     * 计算哈希码
     *
     * 基于kind和offsetAndEpoch计算。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(kind, offsetAndEpoch);
    }

    /**
     * 字符串表示
     *
     * 格式：ValidOffsetAndEpoch(kind=VALID, offsetAndEpoch=OffsetAndEpoch[offset=100, epoch=5])
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return String.format(
            "ValidOffsetAndEpoch(kind=%s, offsetAndEpoch=%s)",
            kind,
            offsetAndEpoch
        );
    }
}
