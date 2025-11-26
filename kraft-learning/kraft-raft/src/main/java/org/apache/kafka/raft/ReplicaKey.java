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

import org.apache.kafka.common.Uuid;

import java.util.Objects;
import java.util.Optional;

/**
 * ReplicaKey - 副本唯一标识符
 *
 * 在分布式系统中唯一标识一个副本（Replica）。组合了两个信息：
 * 1. 副本ID（replica id）：节点的逻辑标识符，例如0、1、2
 * 2. 目录ID（directory id）：存储目录的UUID，用于多磁盘部署（JBOD）
 *
 * 【为什么需要ReplicaKey？】
 *
 * 在简单场景下，只用副本ID就够了：
 * - 节点0、节点1、节点2
 * - 每个节点只有一个数据目录
 *
 * 但在生产环境中，一个节点可能有多块磁盘（JBOD - Just a Bunch Of Disks）：
 * - 节点0有磁盘A、磁盘B
 * - 节点1有磁盘C、磁盘D
 *
 * 问题来了：
 * - 如果节点0的磁盘A损坏，需要替换
 * - 新磁盘应该如何识别？只用节点ID=0不够准确
 * - 需要知道是"节点0的磁盘A"而不是"节点0的磁盘B"
 *
 * ReplicaKey = 节点ID + 目录UUID，提供精确的副本识别。
 *
 * 【核心字段】
 *
 * 1. id - 副本ID（必需）
 *    - 节点的逻辑标识符，通常是0, 1, 2...
 *    - 在Raft集群中唯一标识一个节点
 *
 * 2. directoryId - 目录UUID（可选）
 *    - 存储目录的唯一标识符
 *    - Optional类型，因为：
 *      a) 旧版本可能不支持多目录，此时为empty
 *      b) 向后兼容性 - 不破坏现有代码
 *      c) 单磁盘部署可以不指定目录ID
 *
 * 【为什么directoryId是Optional？】
 *
 * 1. 兼容性考虑：
 *    - Kafka早期版本不支持多目录
 *    - 升级时不能强制所有副本都有directoryId
 *    - Optional.empty()表示"未指定目录"
 *
 * 2. 渐进式升级：
 *    <pre>
 *    // 旧集群（不支持多目录）
 *    ReplicaKey key1 = ReplicaKey.of(0, Uuid.ZERO_UUID);  // directoryId = empty
 *
 *    // 新集群（支持多目录）
 *    ReplicaKey key2 = ReplicaKey.of(0, Uuid.randomUuid()); // 有具体的目录UUID
 *    </pre>
 *
 * 3. 简化单磁盘场景：
 *    - 如果节点只有一块磁盘，无需创建UUID
 *    - 使用NO_DIRECTORY_ID（Uuid.ZERO_UUID）即可
 *
 * 【使用场景】
 *
 * 场景1：单磁盘部署（最简单）
 * <pre>
 * // 节点0，单磁盘
 * ReplicaKey replica0 = ReplicaKey.of(0, ReplicaKey.NO_DIRECTORY_ID);
 * // directoryId = Optional.empty()
 * </pre>
 *
 * 场景2：多磁盘部署（JBOD）
 * <pre>
 * // 节点0有两块磁盘，每块磁盘作为独立副本
 * Uuid diskA = Uuid.randomUuid();  // 磁盘A的UUID
 * Uuid diskB = Uuid.randomUuid();  // 磁盘B的UUID
 *
 * ReplicaKey replica0A = ReplicaKey.of(0, diskA);  // 节点0的磁盘A
 * ReplicaKey replica0B = ReplicaKey.of(0, diskB);  // 节点0的磁盘B
 *
 * // 现在可以精确区分同一节点的不同磁盘
 * </pre>
 *
 * 场景3：磁盘故障恢复
 * <pre>
 * // 节点0的磁盘A损坏
 * ReplicaKey failedDisk = ReplicaKey.of(0, oldDiskUuid);
 *
 * // 替换为新磁盘，分配新UUID
 * ReplicaKey newDisk = ReplicaKey.of(0, newDiskUuid);
 *
 * // Leader可以识别出这是一个新磁盘，需要完整复制
 * </pre>
 *
 * 【比较逻辑】
 *
 * compareTo实现的排序规则：
 * 1. 首先按副本ID排序（主要排序键）
 * 2. 如果ID相同，再按目录UUID排序（次要排序键）
 *
 * 示例：
 * <pre>
 * ReplicaKey r1 = ReplicaKey.of(0, uuid1);
 * ReplicaKey r2 = ReplicaKey.of(0, uuid2);
 * ReplicaKey r3 = ReplicaKey.of(1, uuid3);
 *
 * // 排序结果：r1, r2, r3（先按ID=0, 1，再按uuid1 < uuid2）
 * </pre>
 *
 * 【NO_DIRECTORY_ID常量】
 *
 * Uuid.ZERO_UUID是一个特殊值（00000000-0000-0000-0000-000000000000），表示：
 * - "未指定目录"
 * - 在单磁盘场景下使用
 * - 等价于Optional.empty()
 *
 * 设计选择：
 * - 外部API使用Uuid（方便调用者，不需要理解Optional）
 * - 内部存储使用Optional（类型安全，避免null）
 * - 静态工厂方法负责转换：NO_DIRECTORY_ID → Optional.empty()
 *
 * 【不可变性设计】
 *
 * ReplicaKey是final类且所有字段都是final：
 * - 一旦创建，id和directoryId不可更改
 * - 线程安全，可以安全地在多线程环境中共享
 * - 可以作为HashMap的key使用
 *
 * 【实际应用】
 *
 * 在KRaft中的使用：
 * 1. LeaderState追踪每个副本的复制进度：
 *    Map<ReplicaKey, ReplicaState> voterStates
 *
 * 2. 处理Fetch请求时识别请求来自哪个副本：
 *    ReplicaKey fetcherId = ReplicaKey.of(replicaId, directoryId);
 *
 * 3. 管理观察者副本（Observers）：
 *    Map<ReplicaKey, ObserverState> observerStates
 *
 * 【类比】
 *
 * 可以类比为：
 * - id = 员工工号
 * - directoryId = 工位号
 * - ReplicaKey = 完整的员工标识（工号 + 工位）
 *
 * 为什么需要工位号？
 * - 同一个工号可能换工位（磁盘更换）
 * - 需要精确知道员工当前在哪个工位
 *
 * @see org.apache.kafka.raft.LeaderState Leader状态中使用ReplicaKey追踪副本
 */
public final class ReplicaKey implements Comparable<ReplicaKey> {
    /**
     * NO_DIRECTORY_ID - 表示"未指定目录"的特殊UUID
     *
     * 使用Uuid.ZERO_UUID（全0的UUID）作为哨兵值。
     *
     * 使用场景：
     * 1. 单磁盘部署 - 不需要区分目录
     * 2. 向后兼容 - 旧版本不支持多目录
     * 3. 简化API - 调用者传Uuid而不是Optional
     */
    public static final Uuid NO_DIRECTORY_ID = Uuid.ZERO_UUID;

    /**
     * 副本ID
     *
     * 节点的逻辑标识符，在Raft集群中唯一标识一个节点。
     * 例如：0, 1, 2分别代表三个节点。
     */
    private final int id;

    /**
     * 目录UUID（可选）
     *
     * 存储目录的唯一标识符，用于多磁盘部署（JBOD）。
     * - Optional.empty()：未指定目录（单磁盘或旧版本）
     * - Optional.of(uuid)：指定了具体的目录UUID
     */
    private final Optional<Uuid> directoryId;

    /**
     * 私有构造函数
     *
     * 防止直接构造，强制使用静态工厂方法of()。
     * 好处：
     * 1. 集中处理NO_DIRECTORY_ID → Optional.empty()的转换
     * 2. 可以在工厂方法中添加验证逻辑
     * 3. 保持API的一致性
     *
     * @param id 副本ID
     * @param directoryId 目录UUID（Optional）
     */
    private ReplicaKey(int id, Optional<Uuid> directoryId) {
        this.id = id;
        this.directoryId = directoryId;
    }

    /**
     * 获取副本ID
     *
     * @return 副本ID
     */
    public int id() {
        return id;
    }

    /**
     * 获取目录UUID
     *
     * @return 目录UUID（Optional，可能为empty）
     */
    public Optional<Uuid> directoryId() {
        return directoryId;
    }

    /**
     * 比较两个ReplicaKey的大小
     *
     * 排序规则：
     * 1. 首先比较id（主要排序键）
     * 2. 如果id相同，再比较directoryId（次要排序键）
     *
     * 对于directoryId的比较：
     * - 如果是Optional.empty()，使用NO_DIRECTORY_ID代替
     * - 保证即使是empty也能进行比较
     *
     * 示例：
     * <pre>
     * ReplicaKey r1 = ReplicaKey.of(0, uuid1);
     * ReplicaKey r2 = ReplicaKey.of(0, uuid2);
     * ReplicaKey r3 = ReplicaKey.of(1, uuid3);
     *
     * // r1.compareTo(r2) < 0 (假设uuid1 < uuid2)
     * // r2.compareTo(r3) < 0 (因为0 < 1)
     * </pre>
     *
     * @param that 要比较的另一个ReplicaKey
     * @return 负数（小于），0（等于），正数（大于）
     */
    @Override
    public int compareTo(ReplicaKey that) {
        int idComparison = Integer.compare(this.id, that.id);
        if (idComparison == 0) {
            // id相同，比较directoryId
            // 如果directoryId为empty，使用NO_DIRECTORY_ID代替
            return directoryId
                .orElse(NO_DIRECTORY_ID)
                .compareTo(that.directoryId.orElse(NO_DIRECTORY_ID));
        } else {
            // id不同，直接返回id的比较结果
            return idComparison;
        }
    }

    /**
     * 判断相等性
     *
     * 两个ReplicaKey相等当且仅当：
     * 1. id相同
     * 2. directoryId相同（都为empty，或都present且UUID相同）
     *
     * @param o 要比较的对象
     * @return 如果相等返回true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicaKey that = (ReplicaKey) o;

        if (id != that.id) return false;
        return Objects.equals(directoryId, that.directoryId);
    }

    /**
     * 计算哈希码
     *
     * 基于id和directoryId计算哈希码。
     * 保证：equals()返回true的对象有相同的hashCode()。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, directoryId);
    }

    /**
     * 字符串表示
     *
     * 格式：ReplicaKey(id=0, directoryId=xxxx-xxxx-xxxx-xxxx)
     * 如果directoryId为empty，显示为"<undefined>"
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return String.format("ReplicaKey(id=%d, directoryId=%s)", id, directoryId.map(Uuid::toString).orElse("<undefined>"));
    }

    /**
     * 静态工厂方法 - 创建ReplicaKey实例
     *
     * 这是创建ReplicaKey的唯一公开方法。
     *
     * 特殊处理：
     * - 如果directoryId == NO_DIRECTORY_ID，转换为Optional.empty()
     * - 否则，转换为Optional.of(directoryId)
     *
     * 这种设计的好处：
     * 1. 外部调用者使用Uuid（简单直观）
     * 2. 内部存储使用Optional（类型安全）
     * 3. 工厂方法负责两者的转换
     *
     * 使用示例：
     * <pre>
     * // 单磁盘副本
     * ReplicaKey simple = ReplicaKey.of(0, ReplicaKey.NO_DIRECTORY_ID);
     * // simple.directoryId() == Optional.empty()
     *
     * // 多磁盘副本
     * Uuid diskId = Uuid.randomUuid();
     * ReplicaKey multiDisk = ReplicaKey.of(0, diskId);
     * // multiDisk.directoryId() == Optional.of(diskId)
     * </pre>
     *
     * @param id 副本ID
     * @param directoryId 目录UUID（使用NO_DIRECTORY_ID表示无目录）
     * @return ReplicaKey实例
     */
    public static ReplicaKey of(int id, Uuid directoryId) {
        return new ReplicaKey(
            id,
            directoryId.equals(NO_DIRECTORY_ID) ? Optional.empty() : Optional.of(directoryId)
        );
    }
}
