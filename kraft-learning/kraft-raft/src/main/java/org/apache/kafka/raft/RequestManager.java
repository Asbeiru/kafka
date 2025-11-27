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

import org.apache.kafka.common.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;

/**
 * RequestManager - 请求管理器
 *
 * 【核心职责】
 * 管理与远程副本的连接状态，追踪请求的生命周期，实施错误退避策略。
 *
 * 【设计动机】
 *
 * 在分布式系统中，网络请求可能失败或超时。简单的重试可能导致：
 * 1. 雪崩效应：大量重试进一步压垮故障节点
 * 2. 资源浪费：向已知不可用的节点持续发送请求
 * 3. 状态混乱：并发请求导致状态不一致
 *
 * RequestManager通过状态跟踪和退避策略解决这些问题。
 *
 * 【状态机】
 *
 * 每个远程节点的连接有三种状态：
 *
 * <pre>
 * ┌─────────┐
 * │  READY  │  可以发送新请求
 * └────┬────┘
 *      │ onRequestSent()
 *      ▼
 * ┌──────────────────┐
 * │ AWAITING_RESPONSE│  等待响应
 * └────┬───────┬─────┘
 *      │       │
 *      │       │ hasRequestTimedOut()
 *      │       ├──────────────────┐
 *      │       │                  │
 *      │       │ onResponseResult(success=true)
 *      │       ├──────────────────┐
 *      │       │                  │
 *      │       ▼                  ▼
 *      │  ┌──────────┐      ┌─────────┐
 *      │  │ BACKING  │      │  READY  │
 *      └─▶│   OFF    │      └─────────┘
 *         └────┬─────┘
 *              │ isBackoffComplete()
 *              ▼
 *         ┌─────────┐
 *         │  READY  │
 *         └─────────┘
 * </pre>
 *
 * 状态转换规则：
 * 1. READY -> AWAITING_RESPONSE：调用onRequestSent()发送请求
 * 2. AWAITING_RESPONSE -> READY：收到成功响应
 * 3. AWAITING_RESPONSE -> BACKING_OFF：收到错误响应
 * 4. AWAITING_RESPONSE -> READY：请求超时
 * 5. BACKING_OFF -> READY：退避时间结束
 *
 * 【为什么需要状态管理？】
 *
 * 1. 防止重复请求
 * <pre>
 * // 没有状态管理：
 * if (needsData) {
 *     sendFetchRequest(node);  // 可能重复发送
 * }
 *
 * // 有状态管理：
 * if (needsData && requestManager.isReady(node, now)) {
 *     sendFetchRequest(node);
 *     requestManager.onRequestSent(node, correlationId, now);
 * }
 * </pre>
 *
 * 2. 实施退避策略
 * <pre>
 * // 节点持续失败：
 * onResponseResult(node, id, false, now);  // 失败
 * // 状态变为BACKING_OFF，retryBackoffMs期间不会重试
 *
 * // 等待一段时间后再重试
 * if (requestManager.isReady(node, now + retryBackoffMs)) {
 *     sendRetry(node);  // 现在可以重试了
 * }
 * </pre>
 *
 * 3. 检测超时
 * <pre>
 * // 发送请求
 * onRequestSent(node, correlationId, 1000);
 *
 * // 超时检测
 * if (requestManager.hasRequestTimedOut(node, 1000 + requestTimeoutMs + 1)) {
 *     // 请求超时，状态自动变为READY
 *     // 可以重试或标记节点失败
 * }
 * </pre>
 *
 * 【Fetch请求的特殊约束】
 *
 * Raft的Fetch请求有严格的单一性要求：
 *
 * 问题场景：
 * <pre>
 * // Follower的LEO（Log End Offset）是100
 * // 如果同时发送两个Fetch请求：
 * Fetch1: fetchOffset=100 -> 收到记录101-105
 * Fetch2: fetchOffset=100 -> 收到记录101-105
 *
 * // 可能导致：
 * - 日志重复写入101-105
 * - 状态机重复应用相同的记录
 * - 数据不一致
 * </pre>
 *
 * 解决方案：
 * - hasAnyInflightRequest()确保同一时间只有一个Fetch请求
 * - findReadyBootstrapServer()只在没有任何inflight请求时返回节点
 *
 * <pre>
 * // 安全的Fetch发送：
 * Optional<Node> readyNode = requestManager.findReadyBootstrapServer(now);
 * if (readyNode.isPresent()) {
 *     // 确保没有其他inflight请求
 *     sendFetchRequest(readyNode.get());
 * }
 * </pre>
 *
 * 【Bootstrap Servers】
 *
 * Bootstrap servers是初始的连接节点列表：
 * - Follower不知道Leader是谁时，向bootstrap servers发送Fetch请求
 * - Bootstrap servers会返回当前Leader信息
 * - 随机选择避免所有follower都连接同一个节点
 *
 * <pre>
 * // 场景：新加入集群的follower
 * // 不知道Leader是谁，向bootstrap server发现
 * Optional<Node> bootstrap = requestManager.findReadyBootstrapServer(now);
 * if (bootstrap.isPresent()) {
 *     FetchRequest fetch = buildDiscoveryFetch();
 *     send(bootstrap.get(), fetch);
 *     // 响应会包含Leader信息
 * }
 * </pre>
 *
 * 【使用示例】
 *
 * 1. 初始化：
 * <pre>
 * // 配置bootstrap servers（集群中的几个已知节点）
 * List<Node> bootstrapServers = Arrays.asList(
 *     new Node(1, "kafka1", 9092),
 *     new Node(2, "kafka2", 9092),
 *     new Node(3, "kafka3", 9092)
 * );
 *
 * RequestManager manager = new RequestManager(
 *     bootstrapServers,
 *     100,    // retryBackoffMs: 失败后退避100ms
 *     5000,   // requestTimeoutMs: 请求超时5秒
 *     new Random()
 * );
 * </pre>
 *
 * 2. 发送请求：
 * <pre>
 * Node targetNode = new Node(5, "kafka5", 9092);
 * long now = System.currentTimeMillis();
 *
 * // 检查是否可以发送
 * if (manager.isReady(targetNode, now)) {
 *     int correlationId = generateCorrelationId();
 *     RaftRequest.Outbound request = createRequest(targetNode, correlationId);
 *
 *     // 发送请求
 *     networkChannel.send(request);
 *
 *     // 更新状态
 *     manager.onRequestSent(targetNode, correlationId, now);
 * }
 * </pre>
 *
 * 3. 处理响应：
 * <pre>
 * // 收到响应
 * request.completion.whenComplete((response, exception) -> {
 *     long now = System.currentTimeMillis();
 *     boolean success = (exception == null);
 *
 *     // 更新RequestManager
 *     manager.onResponseResult(
 *         targetNode,
 *         request.correlationId(),
 *         success,
 *         now
 *     );
 *
 *     if (success) {
 *         // 节点恢复READY状态，可以发送新请求
 *         processResponse(response);
 *     } else {
 *         // 节点进入BACKING_OFF状态
 *         // retryBackoffMs期间不会重试
 *     }
 * });
 * </pre>
 *
 * 4. 超时检测：
 * <pre>
 * // 定期检查超时
 * long now = System.currentTimeMillis();
 *
 * if (manager.hasRequestTimedOut(targetNode, now)) {
 *     // 请求超时，自动恢复到READY状态
 *     // 可以重试或标记节点失败
 *     handleTimeout(targetNode);
 * }
 * </pre>
 *
 * 5. 使用Bootstrap Server发现Leader：
 * <pre>
 * // Follower不知道Leader，向bootstrap server查询
 * long now = System.currentTimeMillis();
 *
 * // 计算需要等待的时间
 * long backoffMs = manager.backoffBeforeAvailableBootstrapServer(now);
 * if (backoffMs == 0) {
 *     // 有可用的bootstrap server
 *     Optional<Node> node = manager.findReadyBootstrapServer(now);
 *     if (node.isPresent()) {
 *         sendDiscoveryFetch(node.get());
 *     }
 * } else {
 *     // 需要等待backoffMs后重试
 *     scheduleRetry(backoffMs);
 * }
 * </pre>
 *
 * @see NetworkChannel 网络通道接口
 * @see RaftRequest 请求类
 */
public class RequestManager {
    /**
     * 连接状态映射
     *
     * Key: Node.idString()（节点ID字符串）
     * Value: ConnectionState（连接状态）
     *
     * 只有非READY状态的连接才会在map中：
     * - AWAITING_RESPONSE：等待响应
     * - BACKING_OFF：退避中
     *
     * READY状态的连接会从map中移除，节省内存。
     */
    private final Map<String, ConnectionState> connections = new HashMap<>();

    /**
     * Bootstrap servers列表
     *
     * 用于：
     * 1. Follower发现Leader
     * 2. 新节点加入集群
     * 3. Leader失败后重新发现
     *
     * 通常配置为集群中的几个已知节点。
     */
    private final ArrayList<Node> bootstrapServers;

    /**
     * 重试退避时间（毫秒）
     *
     * 请求失败后，等待retryBackoffMs才能重试。
     * 防止对故障节点的频繁重试。
     *
     * 典型值：100-1000ms
     */
    private final int retryBackoffMs;

    /**
     * 请求超时时间（毫秒）
     *
     * 请求发送后，超过requestTimeoutMs未收到响应，视为超时。
     * 超时后连接恢复到READY状态。
     *
     * 典型值：5000-30000ms
     */
    private final int requestTimeoutMs;

    /**
     * 随机数生成器
     *
     * 用于随机选择bootstrap server，实现负载均衡。
     */
    private final Random random;

    /**
     * 构造RequestManager
     *
     * @param bootstrapServers Bootstrap服务器列表
     * @param retryBackoffMs 重试退避时间（毫秒）
     * @param requestTimeoutMs 请求超时时间（毫秒）
     * @param random 随机数生成器
     */
    public RequestManager(
        Collection<Node> bootstrapServers,
        int retryBackoffMs,
        int requestTimeoutMs,
        Random random
    ) {
        this.bootstrapServers = new ArrayList<>(bootstrapServers);
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.random = random;
    }

    /**
     * 检查是否有任何inflight请求
     *
     * 【重要性】
     * 这是确保Fetch请求单一性的关键。
     * 如果有任何inflight请求，不应该发送新的Fetch请求。
     *
     * 【副作用】
     * 遍历过程中会清理过期的连接：
     * - 超时的请求：转为READY
     * - 完成退避的连接：转为READY
     *
     * @param currentTimeMs 当前时间
     * @return true表示至少有一个inflight请求
     */
    public boolean hasAnyInflightRequest(long currentTimeMs) {
        boolean result = false;

        Iterator<ConnectionState> iterator = connections.values().iterator();
        while (iterator.hasNext()) {
            ConnectionState connection = iterator.next();
            if (connection.hasRequestTimedOut(currentTimeMs)) {
                // 请求超时，标记为READY
                iterator.remove();
            } else if (connection.isBackoffComplete(currentTimeMs)) {
                // 退避完成，标记为READY
                iterator.remove();
            } else if (connection.hasInflightRequest(currentTimeMs)) {
                // 发现inflight请求，立即返回
                // 不需要继续检查其他连接
                result = true;
                break;
            }
        }

        return result;
    }

    /**
     * 找到一个可用的bootstrap server
     *
     * 【使用场景】
     * 主要用于发送Fetch请求，发现Leader。
     *
     * 【Fetch请求的约束】
     * 只有在没有任何inflight请求时才返回节点，
     * 确保同一时间只有一个Fetch请求。
     *
     * 【随机选择策略】
     * 从随机位置开始遍历bootstrap servers，
     * 避免所有follower都连接同一个节点。
     *
     * <pre>
     * // 示例：3个bootstrap servers，startIndex=1
     * bootstrapServers = [node0, node1, node2]
     * 遍历顺序：node1 -> node2 -> node0
     *
     * // 不同follower可能选择不同的节点：
     * Follower1 (startIndex=0): node0 -> node1 -> node2
     * Follower2 (startIndex=1): node1 -> node2 -> node0
     * Follower3 (startIndex=2): node2 -> node0 -> node1
     * </pre>
     *
     * @param currentTimeMs 当前时间
     * @return 可用的bootstrap server（如果没有则返回空）
     */
    public Optional<Node> findReadyBootstrapServer(long currentTimeMs) {
        // 检查是否有任何inflight请求
        // 这确保同一时间只有一个Fetch请求
        if (hasAnyInflightRequest(currentTimeMs)) {
            return Optional.empty();
        }

        // 从随机位置开始遍历，实现负载均衡
        int startIndex = random.nextInt(bootstrapServers.size());
        Optional<Node> result = Optional.empty();
        for (int i = 0; i < bootstrapServers.size(); i++) {
            int index = (startIndex + i) % bootstrapServers.size();
            Node node = bootstrapServers.get(index);

            if (isReady(node, currentTimeMs)) {
                result = Optional.of(node);
                break;
            }
        }

        return result;
    }

    /**
     * 计算直到bootstrap server可用的等待时间
     *
     * 【返回值含义】
     * - 0：至少有一个bootstrap server是READY的
     * - > 0：需要等待的毫秒数
     *
     * 【计算逻辑】
     *
     * 1. 如果有inflight请求：
     *    - 返回请求的剩余超时时间
     *    - 等待请求完成（成功或超时）
     *
     * 2. 如果所有连接都在BACKING_OFF：
     *    - 返回最小的剩余退避时间
     *    - 等待最快恢复的节点
     *
     * 3. 如果有READY的bootstrap server：
     *    - 返回0（立即可用）
     *
     * 【使用场景】
     * 决定下一次Fetch请求的延迟。
     *
     * <pre>
     * long backoffMs = manager.backoffBeforeAvailableBootstrapServer(now);
     * if (backoffMs == 0) {
     *     // 立即发送Fetch
     *     sendFetch();
     * } else {
     *     // 延迟backoffMs后发送
     *     scheduleDelayedFetch(backoffMs);
     * }
     * </pre>
     *
     * @param currentTimeMs 当前时间
     * @return 等待时间（毫秒），0表示立即可用
     */
    public long backoffBeforeAvailableBootstrapServer(long currentTimeMs) {
        long minBackoffMs = retryBackoffMs;

        Iterator<ConnectionState> iterator = connections.values().iterator();
        while (iterator.hasNext()) {
            ConnectionState connection = iterator.next();
            if (connection.hasRequestTimedOut(currentTimeMs)) {
                // 请求超时，标记为READY
                iterator.remove();
            } else if (connection.isBackoffComplete(currentTimeMs)) {
                // 退避完成，标记为READY
                iterator.remove();
            } else if (connection.hasInflightRequest(currentTimeMs)) {
                // 有inflight Fetch请求，等待它完成
                // 最多等待剩余的超时时间
                return connection.remainingRequestTimeMs(currentTimeMs);
            } else if (connection.isBackingOff(currentTimeMs)) {
                // 连接在退避中，计算最小退避时间
                minBackoffMs = Math.min(minBackoffMs, connection.remainingBackoffMs(currentTimeMs));
            }
        }

        // 没有inflight请求，检查是否有READY的bootstrap server
        for (Node node : bootstrapServers) {
            if (isReady(node, currentTimeMs)) {
                return 0L;  // 立即可用
            }
        }

        // 所有bootstrap server都在退避，返回最小退避时间
        return minBackoffMs;
    }

    /**
     * 检查请求是否超时
     *
     * @param node 目标节点
     * @param timeMs 当前时间
     * @return true表示请求已超时
     */
    public boolean hasRequestTimedOut(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return false;
        }

        return state.hasRequestTimedOut(timeMs);
    }

    /**
     * 检查节点是否可以接收新请求
     *
     * 【READY条件】
     * 1. 连接不存在（从未使用过）
     * 2. 请求超时（AWAITING_RESPONSE -> READY）
     * 3. 退避完成（BACKING_OFF -> READY）
     *
     * 【副作用】
     * 如果连接是READY的，会从map中移除（节省内存）。
     *
     * @param node 目标节点
     * @param timeMs 当前时间
     * @return true表示可以发送新请求
     */
    public boolean isReady(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            // 连接不存在，视为READY
            return true;
        }

        boolean ready = state.isReady(timeMs);
        if (ready) {
            // READY状态的连接从map中移除
            reset(node);
        }

        return ready;
    }

    /**
     * 检查节点是否在退避中
     *
     * @param node 目标节点
     * @param timeMs 当前时间
     * @return true表示在退避中
     */
    public boolean isBackingOff(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return false;
        }

        return state.isBackingOff(timeMs);
    }

    /**
     * 获取请求的剩余超时时间
     *
     * @param node 目标节点
     * @param timeMs 当前时间
     * @return 剩余超时时间（毫秒），0表示无inflight请求或已超时
     */
    public long remainingRequestTimeMs(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return 0;
        }

        return state.remainingRequestTimeMs(timeMs);
    }

    /**
     * 获取剩余退避时间
     *
     * @param node 目标节点
     * @param timeMs 当前时间
     * @return 剩余退避时间（毫秒），0表示不在退避中或退避已完成
     */
    public long remainingBackoffMs(Node node, long timeMs) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return 0;
        }

        return state.remainingBackoffMs(timeMs);
    }

    /**
     * 检查是否期待指定correlation ID的响应
     *
     * 用于验证收到的响应是否匹配当前inflight请求。
     *
     * @param node 来源节点
     * @param correlationId 响应的correlation ID
     * @return true表示这是期待的响应
     */
    public boolean isResponseExpected(Node node, long correlationId) {
        ConnectionState state = connections.get(node.idString());
        if (state == null) {
            return false;
        }

        return state.isResponseExpected(correlationId);
    }

    /**
     * 处理响应结果
     *
     * 【状态转换】
     * - success=true：AWAITING_RESPONSE -> READY
     * - success=false：AWAITING_RESPONSE -> BACKING_OFF
     *
     * 【使用场景】
     * <pre>
     * request.completion.whenComplete((response, exception) -> {
     *     boolean success = (exception == null && response.errorCode() == NONE);
     *     manager.onResponseResult(node, correlationId, success, now);
     * });
     * </pre>
     *
     * @param node 响应来源节点
     * @param correlationId 响应的correlation ID
     * @param success 是否成功
     * @param timeMs 当前时间
     */
    public void onResponseResult(Node node, long correlationId, boolean success, long timeMs) {
        if (isResponseExpected(node, correlationId)) {
            if (success) {
                // 成功响应，恢复到READY状态
                reset(node);
            } else {
                // 失败响应，进入BACKING_OFF状态
                connections.get(node.idString()).onResponseError(correlationId, timeMs);
            }
        }
    }

    /**
     * 记录请求已发送
     *
     * 【状态转换】
     * READY -> AWAITING_RESPONSE
     *
     * 【使用场景】
     * <pre>
     * if (manager.isReady(node, now)) {
     *     networkChannel.send(request);
     *     manager.onRequestSent(node, correlationId, now);
     * }
     * </pre>
     *
     * @param node 目标节点
     * @param correlationId 请求的correlation ID
     * @param timeMs 当前时间
     */
    public void onRequestSent(Node node, long correlationId, long timeMs) {
        ConnectionState state = connections.computeIfAbsent(
            node.idString(),
            key -> new ConnectionState(node, retryBackoffMs, requestTimeoutMs)
        );

        state.onRequestSent(correlationId, timeMs);
    }

    /**
     * 重置节点连接状态
     *
     * 将节点恢复到READY状态（从map中移除）。
     *
     * @param node 目标节点
     */
    public void reset(Node node) {
        connections.remove(node.idString());
    }

    /**
     * 重置所有连接状态
     *
     * 清空所有连接，所有节点恢复到READY状态。
     *
     * 使用场景：
     * - Leader选举后，重新建立连接
     * - 配置变更后，重置所有状态
     */
    public void resetAll() {
        connections.clear();
    }

    /**
     * 连接状态枚举
     */
    private enum State {
        /**
         * 等待响应
         *
         * 请求已发送，等待响应。
         * 如果超时，自动转为READY。
         */
        AWAITING_RESPONSE,

        /**
         * 退避中
         *
         * 请求失败，等待退避时间结束。
         * 退避完成后转为READY。
         */
        BACKING_OFF,

        /**
         * 就绪
         *
         * 可以发送新请求。
         * READY状态的连接不在map中。
         */
        READY
    }

    /**
     * ConnectionState - 单个连接的状态
     *
     * 【字段说明】
     * - state：当前状态
     * - lastSendTimeMs：最后发送请求的时间
     * - lastFailTimeMs：最后失败的时间
     * - inFlightCorrelationId：inflight请求的correlation ID
     *
     * 【状态转换】
     * - onRequestSent()：转为AWAITING_RESPONSE
     * - onResponseError()：转为BACKING_OFF
     * - 超时或退避完成：转为READY
     */
    private static final class ConnectionState {
        private final Node node;
        private final int retryBackoffMs;
        private final int requestTimeoutMs;

        private State state = State.READY;
        private long lastSendTimeMs = 0L;
        private long lastFailTimeMs = 0L;
        private OptionalLong inFlightCorrelationId = OptionalLong.empty();

        private ConnectionState(
            Node node,
            int retryBackoffMs,
            int requestTimeoutMs
        ) {
            this.node = node;
            this.retryBackoffMs = retryBackoffMs;
            this.requestTimeoutMs = requestTimeoutMs;
        }

        /**
         * 检查退避是否完成
         */
        private boolean isBackoffComplete(long timeMs) {
            return state == State.BACKING_OFF && timeMs >= lastFailTimeMs + retryBackoffMs;
        }

        /**
         * 检查请求是否超时
         */
        boolean hasRequestTimedOut(long timeMs) {
            return state == State.AWAITING_RESPONSE && timeMs >= lastSendTimeMs + requestTimeoutMs;
        }

        /**
         * 检查是否READY
         *
         * 如果超时或退避完成，自动转为READY。
         */
        boolean isReady(long timeMs) {
            if (isBackoffComplete(timeMs) || hasRequestTimedOut(timeMs)) {
                state = State.READY;
            }
            return state == State.READY;
        }

        /**
         * 检查是否在退避中
         */
        boolean isBackingOff(long timeMs) {
            if (state != State.BACKING_OFF) {
                return false;
            } else {
                return !isBackoffComplete(timeMs);
            }
        }

        /**
         * 检查是否有inflight请求（未超时）
         */
        private boolean hasInflightRequest(long timeMs) {
            if (state != State.AWAITING_RESPONSE) {
                return false;
            } else {
                return !hasRequestTimedOut(timeMs);
            }
        }

        /**
         * 获取请求的剩余超时时间
         */
        long remainingRequestTimeMs(long timeMs) {
            if (hasInflightRequest(timeMs)) {
                return lastSendTimeMs + requestTimeoutMs - timeMs;
            } else {
                return 0;
            }
        }

        /**
         * 获取剩余退避时间
         */
        long remainingBackoffMs(long timeMs) {
            if (isBackingOff(timeMs)) {
                return lastFailTimeMs + retryBackoffMs - timeMs;
            } else {
                return 0;
            }
        }

        /**
         * 检查是否期待指定的响应
         */
        boolean isResponseExpected(long correlationId) {
            return inFlightCorrelationId.isPresent() && inFlightCorrelationId.getAsLong() == correlationId;
        }

        /**
         * 处理响应错误
         *
         * 转为BACKING_OFF状态。
         */
        void onResponseError(long correlationId, long timeMs) {
            inFlightCorrelationId.ifPresent(inflightRequestId -> {
                if (inflightRequestId == correlationId) {
                    lastFailTimeMs = timeMs;
                    state = State.BACKING_OFF;
                    inFlightCorrelationId = OptionalLong.empty();
                }
            });
        }

        /**
         * 记录请求已发送
         *
         * 转为AWAITING_RESPONSE状态。
         */
        void onRequestSent(long correlationId, long timeMs) {
            lastSendTimeMs = timeMs;
            inFlightCorrelationId = OptionalLong.of(correlationId);
            state = State.AWAITING_RESPONSE;
        }

        @Override
        public String toString() {
            return String.format(
                "ConnectionState(node=%s, state=%s, lastSendTimeMs=%d, lastFailTimeMs=%d, inFlightCorrelationId=%s)",
                node,
                state,
                lastSendTimeMs,
                lastFailTimeMs,
                inFlightCorrelationId.isPresent() ? inFlightCorrelationId.getAsLong() : "undefined"
            );
        }
    }
}
