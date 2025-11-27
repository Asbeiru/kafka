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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AddRaftVoterRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.RemoveRaftVoterRequest;
import org.apache.kafka.common.requests.UpdateRaftVoterRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KafkaNetworkChannel - 基于Kafka网络层的NetworkChannel实现
 *
 * 【核心职责】
 * 1. 实现NetworkChannel接口，为Raft提供网络通信能力
 * 2. 桥接Raft协议层和Kafka网络层（KafkaClient）
 * 3. 管理出站请求的发送和响应处理
 * 4. 生成和管理correlation ID
 *
 * 【架构设计】
 *
 * 1. 线程模型
 * <pre>
 * ┌─────────────────┐
 * │  Raft线程       │
 * │  (业务逻辑)     │
 * └────────┬────────┘
 *          │ send(request)
 *          ▼
 * ┌─────────────────┐
 * │  请求队列       │  线程安全的队列
 * │  (queue)        │  ConcurrentLinkedQueue
 * └────────┬────────┘
 *          │ poll()
 *          ▼
 * ┌─────────────────┐
 * │  SendThread     │  专用的发送线程
 * │  (网络I/O)      │  InterBrokerSendThread
 * └────────┬────────┘
 *          │ KafkaClient
 *          ▼
 * ┌─────────────────┐
 * │  网络层         │  TCP连接、NIO
 * │  (Socket)       │  选择器、缓冲区
 * └─────────────────┘
 * </pre>
 *
 * 2. 为什么需要专用线程？
 *    - Raft业务逻辑线程不应该被网络I/O阻塞
 *    - 网络I/O可能涉及连接建立、超时等待等耗时操作
 *    - SendThread专门负责网络操作，Raft线程只需要入队即可
 *    - 支持批量发送，提高网络效率
 *
 * 【关键组件】
 *
 * 1. SendThread
 *    - 继承自InterBrokerSendThread（Kafka的broker间通信线程）
 *    - 维护请求队列，批量取出请求
 *    - 调用KafkaClient执行实际的网络I/O
 *    - 处理响应并完成CompletableFuture
 *
 * 2. KafkaClient
 *    - Kafka的网络客户端抽象
 *    - 管理到其他节点的TCP连接
 *    - 实现请求-响应的匹配
 *    - 处理网络层的超时、重连等
 *
 * 3. ListenerName
 *    - 标识使用哪个监听器发送请求
 *    - Kafka支持多个监听器（PLAINTEXT、SSL、SASL_SSL等）
 *    - 确保使用正确的安全配置
 *
 * 【工作流程】
 *
 * 发送请求的完整流程：
 * <pre>
 * // 1. Raft层创建请求
 * VoteRequestData voteData = new VoteRequestData()...;
 * RaftRequest.Outbound request = new RaftRequest.Outbound(
 *     correlationId,
 *     voteData,
 *     targetNode,
 *     currentTimeMs
 * );
 *
 * // 2. 调用send()将请求入队
 * networkChannel.send(request);
 * // send()内部：
 * // - 将ApiMessage转换为AbstractRequest.Builder
 * // - 创建RequestAndCompletionHandler（包含请求和回调）
 * // - 放入队列
 * // - 唤醒SendThread
 *
 * // 3. SendThread处理
 * // - 从队列中批量取出请求
 * // - 调用KafkaClient.send()发送到网络
 * // - KafkaClient管理TCP连接和实际发送
 *
 * // 4. 接收响应
 * // - KafkaClient接收网络响应
 * // - 调用CompletionHandler回调
 * // - sendOnComplete()处理响应
 * // - 完成request.completion future
 *
 * // 5. Raft层异步获取响应
 * request.completion.whenComplete((response, exception) -> {
 *     processVoteResponse(response);
 * });
 * </pre>
 *
 * 【错误处理】
 *
 * KafkaNetworkChannel处理多种网络错误：
 *
 * 1. 版本不匹配（Version Mismatch）
 *    - 目标节点不支持请求的协议版本
 *    - 返回UNSUPPORTED_VERSION错误响应
 *    - Raft层可以降级到旧版本重试
 *
 * 2. 认证失败（Authentication Error）
 *    - SSL/SASL认证失败
 *    - 返回NETWORK_EXCEPTION（可重试）
 *    - 记录错误日志供管理员排查
 *
 * 3. 连接断开（Disconnected）
 *    - TCP连接断开
 *    - 返回BROKER_NOT_AVAILABLE错误
 *    - Raft层会重试或标记节点为不可用
 *
 * 4. 目标节点为空（Null Destination）
 *    - 请求没有目标节点
 *    - 直接返回BROKER_NOT_AVAILABLE错误
 *    - 不进入发送队列
 *
 * 【使用示例】
 *
 * 1. 创建和启动：
 * <pre>
 * // 创建网络客户端
 * KafkaClient kafkaClient = new NetworkClient(...);
 *
 * // 创建网络通道
 * KafkaNetworkChannel networkChannel = new KafkaNetworkChannel(
 *     Time.SYSTEM,
 *     new ListenerName("PLAINTEXT"),
 *     kafkaClient,
 *     5000,  // 请求超时5秒
 *     "raft-node-1"
 * );
 *
 * // 启动发送线程
 * networkChannel.start();
 * </pre>
 *
 * 2. 发送投票请求：
 * <pre>
 * // 生成correlation ID
 * int correlationId = networkChannel.newCorrelationId();
 *
 * // 创建投票请求
 * VoteRequestData voteData = RaftUtil.singletonVoteRequest(
 *     topicPartition, clusterId, epoch, candidateId, lastEpoch, lastOffset
 * );
 *
 * // 创建出站请求
 * RaftRequest.Outbound request = new RaftRequest.Outbound(
 *     correlationId,
 *     voteData,
 *     followerNode,
 *     time.milliseconds()
 * );
 *
 * // 发送请求
 * networkChannel.send(request);
 *
 * // 异步处理响应
 * request.completion
 *     .orTimeout(5, TimeUnit.SECONDS)
 *     .whenComplete((response, exception) -> {
 *         if (exception != null) {
 *             handleNetworkError(followerNode, exception);
 *         } else {
 *             VoteResponseData voteResp = (VoteResponseData) response.data();
 *             if (voteResp.voteGranted()) {
 *                 recordVote(response.source());
 *             }
 *         }
 *     });
 * </pre>
 *
 * 3. 批量发送心跳：
 * <pre>
 * // 向所有follower发送心跳
 * for (Node follower : followers) {
 *     FetchRequestData fetchData = buildHeartbeat(follower);
 *     RaftRequest.Outbound heartbeat = new RaftRequest.Outbound(
 *         networkChannel.newCorrelationId(),
 *         fetchData,
 *         follower,
 *         time.milliseconds()
 *     );
 *     networkChannel.send(heartbeat);
 *
 *     heartbeat.completion.whenComplete((response, exception) -> {
 *         updateFollowerState(follower, response);
 *     });
 * }
 * // 所有请求会在SendThread中批量处理，提高效率
 * </pre>
 *
 * 4. 优雅关闭：
 * <pre>
 * // 关闭网络通道
 * networkChannel.close();  // 会等待SendThread停止
 * </pre>
 *
 * 【性能优化】
 *
 * 1. 批量处理
 *    - SendThread.generateRequests()一次性取出所有待发送请求
 *    - KafkaClient可以批量发送多个请求
 *    - 减少系统调用和上下文切换
 *
 * 2. 非阻塞设计
 *    - send()方法只是入队，立即返回
 *    - Raft线程不会被网络I/O阻塞
 *    - 使用CompletableFuture异步处理响应
 *
 * 3. 连接复用
 *    - KafkaClient维护长连接池
 *    - 多个请求共享同一TCP连接
 *    - 减少连接建立开销
 *
 * @see NetworkChannel 网络通道接口
 * @see InterBrokerSendThread Kafka的broker间发送线程基类
 * @see KafkaClient Kafka网络客户端接口
 */
public class KafkaNetworkChannel implements NetworkChannel {

    /**
     * SendThread - 专用的网络发送线程
     *
     * 【职责】
     * 1. 维护待发送请求的队列
     * 2. 批量取出请求并通过KafkaClient发送
     * 3. 处理网络响应
     *
     * 【线程安全】
     * - queue是ConcurrentLinkedQueue，支持并发读写
     * - Raft线程调用sendRequest()入队（生产者）
     * - SendThread调用generateRequests()出队（消费者）
     * - wakeup()唤醒SendThread立即处理新请求
     *
     * 【继承InterBrokerSendThread的好处】
     * - 复用Kafka的broker间通信逻辑
     * - 自动处理超时、重连等网络问题
     * - 统一的监控指标和日志
     */
    static class SendThread extends InterBrokerSendThread {

        /**
         * 待发送请求的队列
         *
         * 使用ConcurrentLinkedQueue的原因：
         * 1. 无锁实现，高性能
         * 2. 支持多生产者（虽然通常只有一个Raft线程）
         * 3. 支持单消费者（SendThread）
         * 4. 无界队列，不会因满而阻塞
         *
         * 注意：虽然是无界队列，但Raft的请求速率是有限的，
         * 不会无限堆积（Leader选举、心跳都有固定频率）。
         */
        private final Queue<RequestAndCompletionHandler> queue = new ConcurrentLinkedQueue<>();

        /**
         * 构造SendThread
         *
         * @param name 线程名称（用于日志和监控）
         * @param networkClient Kafka网络客户端
         * @param requestTimeoutMs 请求超时时间（毫秒）
         * @param time 时间抽象（用于超时检测）
         * @param isInterruptible 是否可中断（false表示关闭前必须发送完所有请求）
         */
        public SendThread(String name, KafkaClient networkClient, int requestTimeoutMs, Time time, boolean isInterruptible) {
            super(name, networkClient, requestTimeoutMs, time, isInterruptible);
        }

        /**
         * 生成待发送的请求列表
         *
         * InterBrokerSendThread会定期调用此方法获取待发送的请求。
         *
         * 【批量处理】
         * 一次性取出队列中所有请求，而不是逐个处理。
         * 好处：
         * 1. 减少poll()的系统调用开销
         * 2. KafkaClient可以批量发送，提高网络利用率
         * 3. 减少线程唤醒次数
         *
         * 【实现细节】
         * 使用while循环持续poll()直到队列为空。
         * poll()是O(1)操作，性能很高。
         *
         * @return 待发送的请求列表（可能为空）
         */
        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            List<RequestAndCompletionHandler> list = new ArrayList<>();
            while (true) {
                RequestAndCompletionHandler request = queue.poll();
                if (request == null) {
                    // 队列为空，返回已收集的请求
                    return list;
                } else {
                    list.add(request);
                }
            }
        }

        /**
         * 发送请求（将请求加入队列）
         *
         * 由Raft线程调用。
         *
         * 【流程】
         * 1. 将请求加入队列（非阻塞）
         * 2. 唤醒SendThread立即处理
         *
         * 【为什么需要wakeup()？】
         * - SendThread可能在等待超时或poll网络
         * - 如果不唤醒，新请求可能要等到下一个poll周期才发送
         * - wakeup()确保请求尽快发送，降低延迟
         *
         * @param request 待发送的请求（包含目标节点、请求数据、完成回调）
         */
        public void sendRequest(RequestAndCompletionHandler request) {
            queue.add(request);
            wakeup();  // 唤醒SendThread立即处理
        }
    }

    private static final Logger log = LoggerFactory.getLogger(KafkaNetworkChannel.class);

    /**
     * 网络发送线程
     *
     * 负责从队列取出请求并通过KafkaClient发送。
     */
    private final SendThread requestThread;

    /**
     * Correlation ID计数器
     *
     * 使用AtomicInteger确保线程安全地生成唯一ID。
     * 每次调用newCorrelationId()递增并返回新值。
     *
     * 为什么需要唯一ID？
     * - 异步网络通信中，多个请求可能同时处理中
     * - 响应通过correlation ID匹配到对应的请求
     * - ID必须唯一，否则会混淆响应
     */
    private final AtomicInteger correlationIdCounter = new AtomicInteger(0);

    /**
     * 监听器名称
     *
     * 标识使用哪个网络监听器发送请求。
     * 常见的监听器：
     * - PLAINTEXT：无加密的明文通信
     * - SSL：TLS加密
     * - SASL_PLAINTEXT：SASL认证 + 明文
     * - SASL_SSL：SASL认证 + TLS加密
     */
    private final ListenerName listenerName;

    /**
     * 构造KafkaNetworkChannel
     *
     * @param time 时间抽象（用于超时检测、监控等）
     * @param listenerName 监听器名称（PLAINTEXT、SSL等）
     * @param client Kafka网络客户端
     * @param requestTimeoutMs 请求超时时间（毫秒）
     * @param threadNamePrefix 线程名称前缀（用于识别日志）
     */
    public KafkaNetworkChannel(
        Time time,
        ListenerName listenerName,
        KafkaClient client,
        int requestTimeoutMs,
        String threadNamePrefix
    ) {
        this.listenerName = listenerName;
        this.requestThread = new SendThread(
            threadNamePrefix + "-outbound-request-thread",  // 线程名
            client,
            requestTimeoutMs,
            time,
            false  // 不可中断，确保优雅关闭
        );
    }

    /**
     * 生成新的correlation ID
     *
     * 每次调用返回一个递增的唯一ID。
     * 使用AtomicInteger保证线程安全。
     *
     * @return 新的correlation ID
     */
    @Override
    public int newCorrelationId() {
        return correlationIdCounter.getAndIncrement();
    }

    /**
     * 发送出站请求
     *
     * 【流程】
     * 1. 检查目标节点是否为null
     * 2. 将ApiMessage转换为AbstractRequest.Builder
     * 3. 创建RequestAndCompletionHandler（包含回调）
     * 4. 提交给SendThread发送
     *
     * 【异步处理】
     * 此方法立即返回，不等待网络响应。
     * 响应通过request.completion future异步传递。
     *
     * 【错误处理】
     * 如果目标节点为null，直接返回BROKER_NOT_AVAILABLE错误，
     * 不进入发送队列。
     *
     * @param request 出站请求
     */
    @Override
    public void send(RaftRequest.Outbound request) {
        Node node = request.destination();
        if (node != null) {
            // 创建请求和完成处理器
            requestThread.sendRequest(new RequestAndCompletionHandler(
                request.createdTimeMs(),                    // 请求创建时间
                node,                                        // 目标节点
                buildRequest(request.data()),                // 转换为Kafka请求
                response -> sendOnComplete(request, response)  // 响应回调
            ));
        } else {
            // 目标节点为null，直接返回错误
            sendCompleteFuture(request, errorResponse(request.data(), Errors.BROKER_NOT_AVAILABLE));
        }
    }

    /**
     * 完成请求的CompletableFuture
     *
     * 创建RaftResponse.Inbound并完成request.completion。
     *
     * @param request 原始请求
     * @param message 响应消息（可能是正常响应或错误响应）
     */
    private void sendCompleteFuture(RaftRequest.Outbound request, ApiMessage message) {
        RaftResponse.Inbound response = new RaftResponse.Inbound(
            request.correlationId(),
            message,
            request.destination()  // source就是原来的destination
        );
        request.completion.complete(response);
    }

    /**
     * 处理网络响应的回调
     *
     * 当KafkaClient接收到响应（或发生错误）时调用。
     *
     * 【处理的错误类型】
     *
     * 1. 版本不匹配（Version Mismatch）
     *    - 目标节点不支持请求的API版本
     *    - 可能是节点版本太老或太新
     *    - 返回UNSUPPORTED_VERSION
     *    - Raft层可以尝试降级到旧版本
     *
     * 2. 认证错误（Authentication Exception）
     *    - SSL证书验证失败
     *    - SASL认证失败
     *    - 返回NETWORK_EXCEPTION（标记为可重试）
     *    - 记录错误日志供管理员排查配置问题
     *
     * 3. 连接断开（Disconnected）
     *    - TCP连接关闭
     *    - 可能是网络故障或目标节点宕机
     *    - 返回BROKER_NOT_AVAILABLE
     *    - Raft层会重试或标记节点失败
     *
     * 4. 正常响应
     *    - 从ClientResponse提取响应数据
     *    - 传递给Raft层处理
     *
     * @param request 原始Raft请求
     * @param clientResponse Kafka客户端响应
     */
    private void sendOnComplete(RaftRequest.Outbound request, ClientResponse clientResponse) {
        ApiMessage response;
        if (clientResponse.versionMismatch() != null) {
            // 版本不匹配
            log.error("Request {} failed due to unsupported version error", request, clientResponse.versionMismatch());
            response = errorResponse(request.data(), Errors.UNSUPPORTED_VERSION);
        } else if (clientResponse.authenticationException() != null) {
            // 认证失败
            // 暂时视为可重试错误，使用NETWORK_EXCEPTION
            // 日志会记录认证错误，管理员可以修复配置
            log.error("Request {} failed due to authentication error", request, clientResponse.authenticationException());
            response = errorResponse(request.data(), Errors.NETWORK_EXCEPTION);
        } else if (clientResponse.wasDisconnected()) {
            // 连接断开
            response = errorResponse(request.data(), Errors.BROKER_NOT_AVAILABLE);
        } else {
            // 正常响应
            response = clientResponse.responseBody().data();
        }
        sendCompleteFuture(request, response);
    }

    /**
     * 创建错误响应
     *
     * 根据请求类型和错误码，创建对应的错误响应。
     * 使用RaftUtil.errorResponse()生成标准的错误响应。
     *
     * @param request 请求消息（用于确定响应类型）
     * @param error 错误码
     * @return 错误响应消息
     */
    private ApiMessage errorResponse(ApiMessage request, Errors error) {
        ApiKeys apiKey = ApiKeys.forId(request.apiKey());
        return RaftUtil.errorResponse(apiKey, error);
    }

    /**
     * 获取监听器名称
     *
     * @return 监听器名称
     */
    @Override
    public ListenerName listenerName() {
        return listenerName;
    }

    /**
     * 启动网络通道
     *
     * 启动SendThread，开始处理网络请求。
     * 必须在使用send()之前调用。
     */
    public void start() {
        requestThread.start();
    }

    /**
     * 关闭网络通道
     *
     * 优雅地关闭SendThread。
     * 会等待队列中的请求发送完成。
     *
     * @throws InterruptedException 如果等待被中断
     */
    @Override
    public void close() throws InterruptedException {
        requestThread.shutdown();
    }

    /**
     * 执行一次poll（用于测试）
     *
     * 直接调用SendThread的doWork()方法，
     * 处理一批请求和响应。
     *
     * 在生产环境中，SendThread自动循环调用doWork()。
     * 此方法主要用于单元测试，精确控制网络操作的时机。
     */
    // Visible for testing
    public void pollOnce() {
        requestThread.doWork();
    }

    /**
     * 构建Kafka请求Builder
     *
     * 将ApiMessage（协议消息）转换为AbstractRequest.Builder。
     *
     * 【为什么需要转换？】
     * - ApiMessage是协议数据（VoteRequestData等）
     * - AbstractRequest是Kafka网络层的请求封装
     * - Builder模式支持设置版本、超时等参数
     *
     * 【支持的请求类型】
     * - VoteRequest：投票请求（Leader选举）
     * - BeginQuorumEpochRequest：通知新epoch开始
     * - EndQuorumEpochRequest：通知epoch结束
     * - FetchRequest：日志获取请求（心跳和日志复制）
     * - FetchSnapshotRequest：快照获取请求
     * - UpdateRaftVoterRequest：更新投票者配置
     * - AddRaftVoterRequest：添加投票者
     * - RemoveRaftVoterRequest：移除投票者
     * - ApiVersionsRequest：查询支持的API版本
     *
     * 【版本处理】
     * 大多数请求使用默认版本（由Builder决定）。
     * ApiVersionsRequest特殊处理：使用oldestVersion到latestVersion范围，
     * 确保与不同版本的节点兼容。
     *
     * @param requestData 请求数据（ApiMessage）
     * @return Kafka请求Builder
     * @throws IllegalArgumentException 如果请求类型未知
     */
    static AbstractRequest.Builder<? extends AbstractRequest> buildRequest(ApiMessage requestData) {
        if (requestData instanceof VoteRequestData)
            return new VoteRequest.Builder((VoteRequestData) requestData);
        else if (requestData instanceof BeginQuorumEpochRequestData)
            return new BeginQuorumEpochRequest.Builder((BeginQuorumEpochRequestData) requestData);
        else if (requestData instanceof EndQuorumEpochRequestData)
            return new EndQuorumEpochRequest.Builder((EndQuorumEpochRequestData) requestData);
        else if (requestData instanceof FetchRequestData)
            return new FetchRequest.SimpleBuilder((FetchRequestData) requestData);
        else if (requestData instanceof FetchSnapshotRequestData)
            return new FetchSnapshotRequest.Builder((FetchSnapshotRequestData) requestData);
        else if (requestData instanceof UpdateRaftVoterRequestData)
            return new UpdateRaftVoterRequest.Builder((UpdateRaftVoterRequestData) requestData);
        else if (requestData instanceof AddRaftVoterRequestData)
            return new AddRaftVoterRequest.Builder((AddRaftVoterRequestData) requestData);
        else if (requestData instanceof RemoveRaftVoterRequestData)
            return new RemoveRaftVoterRequest.Builder((RemoveRaftVoterRequestData) requestData);
        else if (requestData instanceof ApiVersionsRequestData)
            // ApiVersionsRequest使用版本范围，确保兼容性
            return new ApiVersionsRequest.Builder((ApiVersionsRequestData) requestData,
                ApiKeys.API_VERSIONS.oldestVersion(),
                ApiKeys.API_VERSIONS.latestVersion());
        else
            throw new IllegalArgumentException("Unexpected type for requestData: " + requestData);
    }
}
