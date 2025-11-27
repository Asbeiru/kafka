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
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;

import java.util.concurrent.CompletableFuture;

/**
 * RaftRequest - Raft协议请求的抽象基类
 *
 * 【核心概念】
 * 在Raft协议中，节点之间需要交换多种类型的消息（投票请求、日志同步、心跳等）。
 * RaftRequest抽象了所有Raft请求的公共特征。
 *
 * 【设计要点】
 * 1. 统一的请求模型
 *    - 所有Raft请求都有correlation ID（关联ID）用于匹配请求和响应
 *    - 所有请求都携带ApiMessage数据（具体的协议消息）
 *    - 所有请求都记录创建时间（用于超时检测和延迟监控）
 *
 * 2. 双向消息流
 *    - Inbound：从网络接收到的入站请求（其他节点发给我的）
 *    - Outbound：准备发送的出站请求（我要发给其他节点的）
 *
 * 3. 异步响应模型
 *    - 使用CompletableFuture实现异步请求-响应模式
 *    - 避免阻塞线程等待网络响应
 *    - 支持超时、重试、并发控制
 *
 * 【实现RaftMessage接口】
 * RaftMessage是所有Raft消息（请求和响应）的标记接口，提供：
 * - correlationId()：获取关联ID
 * - data()：获取消息数据
 *
 * 【使用场景】
 *
 * 1. 发送投票请求：
 * <pre>
 * VoteRequestData voteData = new VoteRequestData()
 *     .setClusterId(clusterId)
 *     .setCandidateId(localId)
 *     .setCandidateEpoch(epoch);
 *
 * RaftRequest.Outbound request = new RaftRequest.Outbound(
 *     nextCorrelationId++,      // 关联ID
 *     voteData,                 // 投票请求数据
 *     followerNode,             // 目标节点
 *     time.milliseconds()       // 创建时间
 * );
 *
 * // 发送请求并异步处理响应
 * networkChannel.send(request);
 * request.completion.whenComplete((response, exception) -> {
 *     if (exception != null) {
 *         handleNetworkError(exception);
 *     } else {
 *         processVoteResponse(response);
 *     }
 * });
 * </pre>
 *
 * 2. 处理接收到的请求：
 * <pre>
 * // 从网络层接收到请求
 * RaftRequest.Inbound inboundRequest = networkChannel.receive();
 *
 * // 根据消息类型处理
 * ApiMessage data = inboundRequest.data();
 * if (data instanceof VoteRequestData) {
 *     VoteResponseData response = handleVoteRequest((VoteRequestData) data);
 *
 *     // 创建并发送响应
 *     RaftResponse.Outbound outboundResponse = new RaftResponse.Outbound(
 *         inboundRequest.correlationId(),  // 使用相同的关联ID
 *         response
 *     );
 *     inboundRequest.completion.complete(outboundResponse);
 * }
 * </pre>
 *
 * 【关联ID的作用】
 * Correlation ID是请求-响应匹配的关键：
 * 1. 客户端发送请求时分配唯一的correlation ID
 * 2. 服务端在响应中返回相同的correlation ID
 * 3. 客户端根据correlation ID匹配异步响应
 * 4. 支持多个并发请求而不会混淆响应
 *
 * 【时间戳的作用】
 * createdTimeMs记录请求创建时间，用于：
 * 1. 检测超时：if (now - createdTimeMs > timeout) { ... }
 * 2. 监控延迟：latency = responseTime - createdTimeMs
 * 3. 调试和追踪：在日志中显示请求的生命周期
 *
 * @see RaftResponse 对应的响应类
 * @see RaftMessage 消息接口
 * @see NetworkChannel 网络通道接口
 */
public abstract class RaftRequest implements RaftMessage {
    /**
     * 关联ID - 用于匹配请求和响应
     *
     * 在异步网络通信中，可能同时有多个请求在处理中。
     * correlation ID确保每个响应能正确匹配到对应的请求。
     *
     * 通常由RequestManager自动分配和管理。
     */
    private final int correlationId;

    /**
     * 消息数据 - 具体的Raft协议消息
     *
     * ApiMessage是Kafka协议消息的基类，具体类型包括：
     * - VoteRequestData/VoteResponseData：投票请求/响应
     * - FetchRequestData/FetchResponseData：日志获取请求/响应
     * - BeginQuorumEpochRequestData：开始新epoch的通知
     * - EndQuorumEpochRequestData：结束当前epoch的通知
     *
     * 这些类都是从JSON schema自动生成的。
     */
    private final ApiMessage data;

    /**
     * 创建时间戳（毫秒）
     *
     * 用于超时检测和性能监控。
     * 通常使用Time.milliseconds()获取当前时间。
     */
    private final long createdTimeMs;

    /**
     * 构造RaftRequest
     *
     * @param correlationId 关联ID，用于匹配请求和响应
     * @param data 消息数据（VoteRequest、FetchRequest等）
     * @param createdTimeMs 创建时间戳（毫秒）
     */
    public RaftRequest(int correlationId, ApiMessage data, long createdTimeMs) {
        this.correlationId = correlationId;
        this.data = data;
        this.createdTimeMs = createdTimeMs;
    }

    @Override
    public int correlationId() {
        return correlationId;
    }

    @Override
    public ApiMessage data() {
        return data;
    }

    /**
     * 获取请求创建时间
     *
     * @return 创建时间戳（毫秒）
     */
    public long createdTimeMs() {
        return createdTimeMs;
    }

    /**
     * Inbound - 入站请求（从网络接收到的请求）
     *
     * 【使用场景】
     * 当本节点从网络接收到其他节点发来的Raft请求时，创建Inbound实例。
     *
     * 【关键特征】
     * 1. 包含ListenerName
     *    - Kafka支持多个网络监听器（如PLAINTEXT、SSL）
     *    - 知道请求从哪个监听器进来，可以：
     *      * 使用正确的安全配置发送响应
     *      * 实施监听器级别的访问控制
     *      * 监控不同监听器的流量
     *
     * 2. 包含API版本
     *    - 不同版本的Kafka可能使用不同版本的Raft协议
     *    - 记录请求的API版本，确保响应使用兼容的版本
     *    - 支持协议的向后兼容
     *
     * 3. 提供CompletableFuture用于发送响应
     *    - 处理完请求后，通过completion.complete()发送响应
     *    - 网络层会自动将响应发送回请求来源
     *    - 支持异步处理，不阻塞网络线程
     *
     * 【处理流程】
     * <pre>
     * // 1. 网络层接收到请求
     * RaftRequest.Inbound request = networkChannel.receive();
     *
     * // 2. 异步处理请求
     * executor.submit(() -> {
     *     try {
     *         // 处理业务逻辑
     *         ApiMessage responseData = handleRequest(request.data());
     *
     *         // 创建响应
     *         RaftResponse.Outbound response = new RaftResponse.Outbound(
     *             request.correlationId(),
     *             responseData
     *         );
     *
     *         // 完成future，触发响应发送
     *         request.completion.complete(response);
     *     } catch (Exception e) {
     *         // 异常情况也要完成future
     *         request.completion.completeExceptionally(e);
     *     }
     * });
     * </pre>
     *
     * 【为什么需要ListenerName】
     * 考虑以下场景：
     * - 节点配置了两个监听器：INTERNAL（内部通信）和EXTERNAL（客户端访问）
     * - 从INTERNAL监听器收到Raft请求
     * - 响应也应该从INTERNAL监听器发送
     * - ListenerName确保请求和响应使用同一个监听器
     */
    public static final class Inbound extends RaftRequest {
        /**
         * API版本 - 请求使用的协议版本
         *
         * Raft协议可能随Kafka版本演进，不同版本的消息格式可能不同。
         * 记录请求版本，确保响应使用兼容的版本。
         */
        private final short apiVersion;

        /**
         * 监听器名称 - 请求来源的网络监听器
         *
         * Kafka支持多个监听器，如PLAINTEXT、SSL、SASL_SSL等。
         * 知道请求来源的监听器，可以确保响应从同一监听器发送。
         */
        private final ListenerName listenerName;

        /**
         * 响应的Future
         *
         * 当处理完请求后，调用completion.complete(response)发送响应。
         * 网络层监听这个future，当完成时自动发送响应。
         *
         * 使用public final的原因：
         * 1. 需要在请求处理器中直接访问
         * 2. final确保不会被替换
         * 3. CompletableFuture本身是线程安全的
         */
        public final CompletableFuture<RaftResponse.Outbound> completion = new CompletableFuture<>();

        /**
         * 构造入站请求
         *
         * @param listenerName 监听器名称
         * @param correlationId 关联ID
         * @param apiVersion API版本
         * @param data 消息数据
         * @param createdTimeMs 创建时间戳
         */
        public Inbound(
            ListenerName listenerName,
            int correlationId,
            short apiVersion,
            ApiMessage data,
            long createdTimeMs
        ) {
            super(correlationId, data, createdTimeMs);
            this.listenerName = listenerName;
            this.apiVersion = apiVersion;
        }

        /**
         * 获取API版本
         */
        public short apiVersion() {
            return apiVersion;
        }

        /**
         * 获取监听器名称
         */
        public ListenerName listenerName() {
            return listenerName;
        }

        @Override
        public String toString() {
            return String.format(
                "InboundRequest(listenerName=%s, correlationId=%d, apiVersion=%d, data=%s, " +
                "createdTimeMs=%d)",
                listenerName,
                correlationId(),
                apiVersion,
                data(),
                createdTimeMs()
            );
        }
    }

    /**
     * Outbound - 出站请求（准备发送给其他节点的请求）
     *
     * 【使用场景】
     * 当本节点需要向其他节点发送Raft请求时，创建Outbound实例。
     *
     * 【关键特征】
     * 1. 包含目标节点信息
     *    - Node包含节点ID、主机名、端口
     *    - 网络层根据目标节点建立连接并发送请求
     *
     * 2. 提供CompletableFuture用于接收响应
     *    - 发送请求后，通过completion.get()或completion.whenComplete()处理响应
     *    - 支持超时控制：completion.get(timeout, TimeUnit.SECONDS)
     *    - 支持取消：completion.cancel()
     *
     * 【发送流程】
     * <pre>
     * // 1. 创建请求
     * VoteRequestData voteData = buildVoteRequest();
     * RaftRequest.Outbound request = new RaftRequest.Outbound(
     *     correlationId,
     *     voteData,
     *     targetNode,
     *     currentTimeMs
     * );
     *
     * // 2. 发送请求
     * networkChannel.send(request);
     *
     * // 3. 异步处理响应
     * request.completion
     *     .orTimeout(5, TimeUnit.SECONDS)  // 5秒超时
     *     .whenComplete((response, exception) -> {
     *         if (exception != null) {
     *             if (exception instanceof TimeoutException) {
     *                 handleTimeout(targetNode);
     *             } else {
     *                 handleNetworkError(targetNode, exception);
     *             }
     *         } else {
     *             processVoteResponse(response);
     *         }
     *     });
     * </pre>
     *
     * 【并发请求处理】
     * CompletableFuture使得可以轻松处理多个并发请求：
     * <pre>
     * // 向所有follower发送心跳
     * List<CompletableFuture<RaftResponse.Inbound>> futures = new ArrayList<>();
     * for (Node follower : followers) {
     *     RaftRequest.Outbound heartbeat = buildHeartbeat(follower);
     *     networkChannel.send(heartbeat);
     *     futures.add(heartbeat.completion);
     * }
     *
     * // 等待所有响应（或超时）
     * CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
     *     .orTimeout(1, TimeUnit.SECONDS)
     *     .whenComplete((v, e) -> {
     *         int successCount = (int) futures.stream()
     *             .filter(f -> !f.isCompletedExceptionally())
     *             .count();
     *         updateQuorumStatus(successCount);
     *     });
     * </pre>
     *
     * 【重试机制】
     * CompletableFuture配合重试：
     * <pre>
     * private CompletableFuture<RaftResponse.Inbound> sendWithRetry(
     *     RaftRequest.Outbound request,
     *     int maxRetries
     * ) {
     *     return request.completion
     *         .orTimeout(5, TimeUnit.SECONDS)
     *         .exceptionallyCompose(e -> {
     *             if (maxRetries > 0 && isRetriable(e)) {
     *                 // 创建新请求重试
     *                 RaftRequest.Outbound retry = new RaftRequest.Outbound(
     *                     nextCorrelationId(),
     *                     request.data(),
     *                     request.destination(),
     *                     currentTimeMs()
     *                 );
     *                 networkChannel.send(retry);
     *                 return sendWithRetry(retry, maxRetries - 1);
     *             }
     *             return CompletableFuture.failedFuture(e);
     *         });
     * }
     * </pre>
     */
    public static final class Outbound extends RaftRequest {
        /**
         * 目标节点 - 请求将发送到的节点
         *
         * Node包含：
         * - id: 节点ID
         * - host: 主机名或IP地址
         * - port: 端口号
         * - rack: 机架信息（可选）
         */
        private final Node destination;

        /**
         * 响应的Future
         *
         * 当网络层接收到响应时，调用completion.complete(response)。
         * 请求发送者通过这个future获取响应。
         *
         * 使用public final的原因：
         * 1. 需要在请求发送者中直接访问
         * 2. final确保不会被替换
         * 3. CompletableFuture本身是线程安全的
         *
         * 注意：响应类型是RaftResponse.Inbound（从网络接收的响应）
         */
        public final CompletableFuture<RaftResponse.Inbound> completion = new CompletableFuture<>();

        /**
         * 构造出站请求
         *
         * @param correlationId 关联ID
         * @param data 消息数据
         * @param destination 目标节点
         * @param createdTimeMs 创建时间戳
         */
        public Outbound(int correlationId, ApiMessage data, Node destination, long createdTimeMs) {
            super(correlationId, data, createdTimeMs);
            this.destination = destination;
        }

        /**
         * 获取目标节点
         */
        public Node destination() {
            return destination;
        }

        @Override
        public String toString() {
            return String.format(
                "OutboundRequest(correlationId=%d, data=%s, createdTimeMs=%d, destination=%s)",
                correlationId(),
                data(),
                createdTimeMs(),
                destination
            );
        }
    }
}
