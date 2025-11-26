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

import org.apache.kafka.common.network.ListenerName;

/**
 * NetworkChannel - 网络通道接口
 *
 * 一个简单的网络接口，几乎不做任何假设。
 *
 * 【核心设计理念】
 *
 * 1. **不保证顺序**
 *    - 请求可能乱序到达
 *    - 响应可能乱序返回
 *    - Raft必须自己处理乱序
 *
 * 2. **不保证响应**
 *    - 不是每个请求都会收到响应
 *    - 网络可能丢包
 *    - Raft必须处理超时和重试
 *
 * 3. **简单抽象**
 *    - 只有3个方法：newCorrelationId(), send(), listenerName()
 *    - 不包含接收逻辑（由Raft主循环poll()处理）
 *    - 专注于发送端
 *
 * 【为什么设计得这么简单？】
 *
 * Raft协议本身就设计为能容忍：
 * - 消息丢失
 * - 消息重复
 * - 消息乱序
 *
 * 因此NetworkChannel无需提供可靠性保证，Raft会自己处理。
 *
 * 【与TCP的区别】
 *
 * TCP提供：
 * - 有序delivery（in-order）
 * - 可靠delivery（reliable）
 * - 流控制（flow control）
 *
 * NetworkChannel只提供：
 * - 尽力而为delivery（best-effort）
 * - 无序可能（unordered）
 *
 * 为什么？因为在上层Raft已经实现了：
 * - 超时重传（reliability）
 * - 序列号检查（ordering）
 * - 背压处理（flow control）
 *
 * 【接收消息的处理】
 *
 * NetworkChannel只负责发送，接收由RaftClient的poll()处理：
 *
 * <pre>
 * // RaftClient的主循环
 * while (running) {
 *     // poll()会从网络读取消息
 *     List<RaftMessage> messages = poll();
 *
 *     for (RaftMessage message : messages) {
 *         if (message instanceof RaftRequest.Inbound) {
 *             handleRequest((RaftRequest.Inbound) message);
 *         } else if (message instanceof RaftResponse.Inbound) {
 *             handleResponse((RaftResponse.Inbound) message);
 *         }
 *     }
 * }
 * </pre>
 *
 * 【典型的消息流】
 *
 * <pre>
 * // 节点A发送VoteRequest给节点B
 *
 * // 1. 生成correlationId
 * int correlationId = channel.newCorrelationId(); // 123
 *
 * // 2. 创建请求
 * VoteRequestData data = new VoteRequestData()
 *     .setCandidateId(myId)
 *     .setLastLogOffset(lastOffset);
 *
 * RaftRequest.Outbound request = new RaftRequest.Outbound(
 *     correlationId,
 *     data,
 *     destinationNodeId
 * );
 *
 * // 3. 发送
 * channel.send(request);
 *
 * // 4. 记录pending request
 * pendingRequests.put(correlationId, new PendingRequest(request, System.currentTimeMillis()));
 *
 * // 5. 等待响应（在poll()中处理）
 * // ...稍后在poll()中收到响应...
 *
 * // 6. 匹配响应
 * RaftResponse.Inbound response = poll();
 * if (response.correlationId() == correlationId) {
 *     // 这是我的请求的响应
 *     handleVoteResponse(response);
 * }
 * </pre>
 *
 * 【实现类】
 *
 * KRaft中的NetworkChannel实现：
 *
 * **KafkaNetworkChannel**：
 * - 使用Kafka的NetworkClient
 * - 复用Kafka的网络层
 * - 支持SSL、SASL等安全特性
 *
 * <pre>
 * class KafkaNetworkChannel implements NetworkChannel {
 *     private final NetworkClient client;
 *     private final AtomicInteger correlationIdGenerator;
 *
 *     @Override
 *     public int newCorrelationId() {
 *         return correlationIdGenerator.incrementAndGet();
 *     }
 *
 *     @Override
 *     public void send(RaftRequest.Outbound request) {
 *         client.send(request);
 *     }
 * }
 * </pre>
 *
 * 【监听器（Listener）】
 *
 * 一个Kafka节点可能有多个监听器：
 * - INTERNAL：集群内部通信
 * - EXTERNAL：客户端连接
 * - CONTROLLER：Controller通信
 *
 * NetworkChannel关联到特定的监听器：
 * <pre>
 * NetworkChannel internalChannel = createChannel(
 *     ListenerName.normalised("INTERNAL")
 * );
 *
 * // 所有通过这个channel发送的请求都使用INTERNAL监听器
 * internalChannel.send(request);
 * </pre>
 *
 * 【线程安全性】
 *
 * NetworkChannel通常不是线程安全的：
 * - 应该从单个线程调用（Raft主循环线程）
 * - 如果需要多线程访问，需要外部同步
 *
 * 【超时和重试】
 *
 * NetworkChannel不处理超时，由Raft处理：
 *
 * <pre>
 * class RaftClient {
 *     private final Map<Integer, PendingRequest> pending = new HashMap<>();
 *
 *     void sendRequest(RaftRequest.Outbound request) {
 *         int correlationId = request.correlationId();
 *         pending.put(correlationId, new PendingRequest(
 *             request,
 *             System.currentTimeMillis()
 *         ));
 *         channel.send(request);
 *     }
 *
 *     void checkTimeouts() {
 *         long now = System.currentTimeMillis();
 *         Iterator<Map.Entry<Integer, PendingRequest>> it = pending.entrySet().iterator();
 *         while (it.hasNext()) {
 *             Map.Entry<Integer, PendingRequest> entry = it.next();
 *             PendingRequest req = entry.getValue();
 *
 *             if (now - req.sentTime > TIMEOUT_MS) {
 *                 // 超时，重试
 *                 channel.send(req.request);
 *                 req.sentTime = now;
 *             }
 *         }
 *     }
 * }
 * </pre>
 *
 * 【对比其他网络抽象】
 *
 * NetworkChannel vs NIO Selector：
 * - NetworkChannel：更高级，面向消息
 * - Selector：更低级，面向字节
 *
 * NetworkChannel vs RPC框架（如gRPC）：
 * - NetworkChannel：更简单，单向发送
 * - gRPC：更复杂，支持流式、双向通信
 *
 * @see RaftRequest Raft请求消息
 * @see KafkaNetworkChannel NetworkChannel的Kafka实现
 */
public interface NetworkChannel extends AutoCloseable {

    /**
     * 生成新的唯一correlationId
     *
     * 为即将发送的新请求生成一个唯一的关联ID。
     *
     * 【唯一性保证】
     *
     * correlationId在**当前连接**中唯一：
     * - 每次调用返回不同的ID
     * - 通常是递增的整数
     * - 允许溢出（int范围：-2^31 到 2^31-1）
     *
     * 【实现方式】
     *
     * 典型实现：
     * <pre>
     * class KafkaNetworkChannel implements NetworkChannel {
     *     private final AtomicInteger nextCorrelationId = new AtomicInteger(0);
     *
     *     @Override
     *     public int newCorrelationId() {
     *         return nextCorrelationId.incrementAndGet();
     *     }
     * }
     * </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * // 发送投票请求
     * int correlationId = channel.newCorrelationId();
     *
     * VoteRequestData voteData = new VoteRequestData()
     *     .setCandidateId(myId)
     *     .setLastLogOffset(lastOffset);
     *
     * RaftRequest.Outbound request = new RaftRequest.Outbound(
     *     correlationId,
     *     voteData,
     *     destinationId
     * );
     *
     * // 记录pending request
     * pendingVotes.put(correlationId, new PendingVote(request, System.currentTimeMillis()));
     *
     * // 发送
     * channel.send(request);
     * </pre>
     *
     * 【线程安全性】
     *
     * 实现必须是线程安全的（如使用AtomicInteger），
     * 即使NetworkChannel本身可能不是线程安全的。
     *
     * 为什么？
     * - newCorrelationId()可能在不同线程调用
     * - 但send()通常只在主线程调用
     *
     * @return 新的唯一correlationId
     */
    int newCorrelationId();

    /**
     * 发送出站请求消息
     *
     * 将Raft请求发送到目标节点。
     *
     * 【不保证delivery】
     *
     * 此方法只是"尽力发送"：
     * - 可能成功
     * - 可能丢包（网络问题）
     * - 可能被目标节点拒绝
     * - 可能超时无响应
     *
     * 【不阻塞】
     *
     * send()通常是非阻塞的：
     * - 将消息放入发送缓冲区
     * - 立即返回
     * - 实际发送由后台线程完成
     *
     * 【无序性】
     *
     * 消息可能乱序到达：
     * <pre>
     * channel.send(request1); // correlationId=1
     * channel.send(request2); // correlationId=2
     * channel.send(request3); // correlationId=3
     *
     * // 目标节点可能收到的顺序：2, 1, 3（乱序）
     * </pre>
     *
     * Raft使用epoch和offset来处理乱序。
     *
     * 【重复发送】
     *
     * 可以安全地重复发送同一个请求（幂等性）：
     * <pre>
     * // 第一次发送
     * channel.send(request);
     *
     * // 超时后重试
     * if (timeout) {
     *     channel.send(request); // 重复发送，correlationId相同
     * }
     * </pre>
     *
     * 接收方会根据correlationId去重。
     *
     * 【典型的请求类型】
     *
     * 1. **VoteRequest** - 投票请求
     *    <pre>
     *    // Candidate请求Follower投票
     *    VoteRequestData data = new VoteRequestData()
     *        .setCandidateId(myId)
     *        .setCandidateEpoch(currentEpoch);
     *    channel.send(new RaftRequest.Outbound(correlationId, data, voterId));
     *    </pre>
     *
     * 2. **FetchRequest** - 日志复制请求
     *    <pre>
     *    // Leader发送日志给Follower
     *    FetchRequestData data = new FetchRequestData()
     *        .setFetchOffset(followerOffset);
     *    channel.send(new RaftRequest.Outbound(correlationId, data, followerId));
     *    </pre>
     *
     * 3. **BeginQuorumEpochRequest** - Leader选举成功通知
     *    <pre>
     *    // 新Leader通知Follower
     *    BeginQuorumEpochRequestData data = new BeginQuorumEpochRequestData()
     *        .setLeaderId(myId)
     *        .setLeaderEpoch(newEpoch);
     *    channel.send(new RaftRequest.Outbound(correlationId, data, followerId));
     *    </pre>
     *
     * 【错误处理】
     *
     * send()通常不抛异常：
     * - 网络错误：记录日志但不抛异常
     * - 缓冲区满：可能丢弃消息或阻塞
     *
     * 错误通过其他机制处理：
     * - 超时检测：pending request超时
     * - 重试：重新发送
     * - 故障检测：标记节点为down
     *
     * 【批量发送优化】
     *
     * 实现可能批量发送多个请求：
     * <pre>
     * // 用户代码
     * channel.send(request1);
     * channel.send(request2);
     * channel.send(request3);
     *
     * // 实现可能合并为一个TCP包发送
     * // → 减少网络往返
     * // → 提高吞吐量
     * </pre>
     *
     * @param request 要发送的出站请求
     */
    void send(RaftRequest.Outbound request);

    /**
     * 获取发送请求时使用的监听器名称
     *
     * 返回此NetworkChannel关联的监听器。
     *
     * 【什么是监听器？】
     *
     * 监听器（Listener）是Kafka节点的网络端点：
     * - INTERNAL：集群内部通信（如Raft复制）
     * - EXTERNAL：客户端连接（如Producer/Consumer）
     * - CONTROLLER：Controller通信
     *
     * 每个监听器有：
     * - 独立的端口
     * - 独立的安全配置（SSL, SASL等）
     * - 独立的网络接口
     *
     * 【为什么需要多个监听器？】
     *
     * 1. **安全隔离**
     *    <pre>
     *    INTERNAL: 内网IP，无认证（信任内网）
     *    EXTERNAL: 公网IP，SSL + SASL（严格认证）
     *    </pre>
     *
     * 2. **网络隔离**
     *    <pre>
     *    INTERNAL: 192.168.1.10:9092（内网，快）
     *    EXTERNAL: 10.0.0.10:9093（公网，慢）
     *    </pre>
     *
     * 3. **流量隔离**
     *    <pre>
     *    INTERNAL: Raft复制流量（高优先级）
     *    EXTERNAL: 客户端流量（低优先级）
     *    </pre>
     *
     * 【KRaft使用场景】
     *
     * KRaft通常使用CONTROLLER监听器：
     * <pre>
     * // 创建NetworkChannel for Raft
     * ListenerName raftListener = ListenerName.normalised("CONTROLLER");
     * NetworkChannel channel = new KafkaNetworkChannel(raftListener, ...);
     *
     * // 所有Raft消息通过CONTROLLER监听器发送
     * channel.send(voteRequest);  // 使用CONTROLLER端口
     * channel.send(fetchRequest); // 使用CONTROLLER端口
     * </pre>
     *
     * 【使用场景】
     *
     * 1. **日志记录**
     *    <pre>
     *    logger.info("Sending request via listener: {}", channel.listenerName());
     *    </pre>
     *
     * 2. **查找目标地址**
     *    <pre>
     *    // 根据监听器名称获取目标节点的地址
     *    Endpoints endpoints = followerEndpoints;
     *    Optional<InetSocketAddress> address = endpoints.address(channel.listenerName());
     *    if (address.isPresent()) {
     *        // 发送到这个地址
     *    }
     *    </pre>
     *
     * 3. **配置验证**
     *    <pre>
     *    // 检查是否使用了正确的监听器
     *    if (!channel.listenerName().equals(expectedListener)) {
     *        throw new IllegalStateException("Wrong listener");
     *    }
     *    </pre>
     *
     * 【不可变性】
     *
     * listenerName()返回值不变：
     * - NetworkChannel创建时确定
     * - 生命周期内不变
     *
     * @return 监听器名称
     */
    ListenerName listenerName();

    /**
     * 关闭网络通道
     *
     * 清理网络资源。
     *
     * 【默认实现】
     *
     * 提供空的默认实现：
     * <pre>
     * default void close() throws InterruptedException {}
     * </pre>
     *
     * 为什么？
     * - 不是所有实现都需要清理资源
     * - 简化简单实现的代码
     *
     * 【实际实现示例】
     *
     * <pre>
     * class KafkaNetworkChannel implements NetworkChannel {
     *     private final NetworkClient client;
     *
     *     @Override
     *     public void close() throws InterruptedException {
     *         // 关闭底层网络客户端
     *         client.close();
     *     }
     * }
     * </pre>
     *
     * 【为什么抛出InterruptedException？】
     *
     * 关闭可能需要等待：
     * - 发送缓冲区flush完成
     * - 等待连接优雅关闭
     * - 等待后台线程停止
     *
     * 如果被中断，抛出InterruptedException。
     *
     * 【使用方式】
     *
     * <pre>
     * try {
     *     channel.close();
     * } catch (InterruptedException e) {
     *     Thread.currentThread().interrupt();
     *     logger.warn("Interrupted while closing channel", e);
     * }
     * </pre>
     *
     * @throws InterruptedException 如果关闭过程被中断
     */
    default void close() throws InterruptedException {}
}
