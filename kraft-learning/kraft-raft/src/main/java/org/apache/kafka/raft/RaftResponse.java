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
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * RaftResponse - Raft协议响应的抽象基类
 *
 * 【核心概念】
 * RaftResponse是对RaftRequest的响应，包含处理结果数据。
 * 与请求类似，响应也分为入站（Inbound）和出站（Outbound）两种。
 *
 * 【设计要点】
 * 1. 响应必须包含关联ID
 *    - 与请求的correlationId一致
 *    - 用于异步匹配请求和响应
 *    - 支持多个并发请求同时处理
 *
 * 2. 响应包含ApiMessage数据
 *    - VoteResponseData：投票响应
 *    - FetchResponseData：日志获取响应
 *    - 其他各种Raft协议响应
 *
 * 3. 响应不需要createdTimeMs
 *    - 响应通常立即创建和发送
 *    - 延迟计算使用请求的createdTimeMs：latency = now - request.createdTimeMs()
 *    - 如果需要追踪响应时间，由网络层记录
 *
 * 【请求-响应的生命周期】
 *
 * 出站请求场景（A节点向B节点请求）：
 * <pre>
 * // A节点：创建并发送出站请求
 * RaftRequest.Outbound request = new RaftRequest.Outbound(
 *     correlationId: 123,
 *     data: voteRequestData,
 *     destination: nodeB,
 *     createdTimeMs: 1000
 * );
 * networkChannel.send(request);
 *
 * // B节点：接收到入站请求
 * RaftRequest.Inbound receivedRequest = networkChannel.receive();
 * // receivedRequest.correlationId() == 123
 *
 * // B节点：处理请求并创建出站响应
 * VoteResponseData responseData = handleVoteRequest(receivedRequest.data());
 * RaftResponse.Outbound response = new RaftResponse.Outbound(
 *     receivedRequest.correlationId(),  // 使用相同的123
 *     responseData
 * );
 * receivedRequest.completion.complete(response);  // 触发发送
 *
 * // A节点：接收到入站响应
 * request.completion.whenComplete((inboundResponse, exception) -> {
 *     // inboundResponse.correlationId() == 123
 *     // inboundResponse.source() == nodeB
 *     processVoteResponse(inboundResponse.data());
 * });
 * </pre>
 *
 * 【为什么Outbound不需要目标节点？】
 *
 * 关键理解：响应总是返回到请求来源
 *
 * 1. 请求-响应在同一TCP连接上
 *    - 收到请求的连接已经建立
 *    - 响应直接在同一连接上发送回去
 *    - 不需要指定目标，网络层知道往哪发
 *
 * 2. 简化响应创建
 *    - 处理请求的代码不需要知道请求来自哪里
 *    - 只需要创建响应数据，网络层负责路由
 *
 * 3. 对比请求
 *    - RaftRequest.Outbound需要destination（主动发起连接）
 *    - RaftResponse.Outbound不需要destination（使用已有连接）
 *
 * 【Inbound为什么需要来源节点？】
 *
 * 当接收到响应时，需要知道是哪个节点发来的：
 *
 * 1. 更新节点状态
 *    - 记录最后通信时间：nodeStates.get(source).updateLastContact(now)
 *    - 检测节点活跃性
 *    - 实施退避策略（某节点频繁失败则减少请求频率）
 *
 * 2. 处理多节点响应
 * <pre>
 * // Leader向所有follower发送心跳
 * for (Node follower : followers) {
 *     RaftRequest.Outbound heartbeat = buildHeartbeat(follower);
 *     networkChannel.send(heartbeat);
 *
 *     heartbeat.completion.whenComplete((response, exception) -> {
 *         if (response != null) {
 *             // 知道是哪个follower的响应
 *             updateFollowerState(response.source(), response.data());
 *         }
 *     });
 * }
 * </pre>
 *
 * 3. 调试和监控
 *    - 日志中显示响应来源
 *    - 监控各节点的响应延迟
 *    - 追踪网络问题
 *
 * 【使用场景】
 *
 * 1. 处理投票请求并响应：
 * <pre>
 * // 收到投票请求
 * RaftRequest.Inbound voteRequest = ...;
 * VoteRequestData requestData = (VoteRequestData) voteRequest.data();
 *
 * // 根据本地状态决定是否投票
 * boolean granted = canGrantVote(requestData);
 *
 * // 创建响应
 * VoteResponseData responseData = new VoteResponseData()
 *     .setErrorCode(Errors.NONE.code())
 *     .setVoteGranted(granted)
 *     .setLeaderId(currentLeaderId)
 *     .setLeaderEpoch(currentEpoch);
 *
 * RaftResponse.Outbound response = new RaftResponse.Outbound(
 *     voteRequest.correlationId(),  // 必须使用请求的correlationId
 *     responseData
 * );
 *
 * // 完成future，触发响应发送
 * voteRequest.completion.complete(response);
 * </pre>
 *
 * 2. 处理接收到的投票响应：
 * <pre>
 * // 发送投票请求
 * RaftRequest.Outbound voteRequest = new RaftRequest.Outbound(...);
 * networkChannel.send(voteRequest);
 *
 * // 异步处理响应
 * voteRequest.completion.whenComplete((response, exception) -> {
 *     if (exception != null) {
 *         // 网络错误或超时
 *         handleVoteRequestFailure(voteRequest.destination(), exception);
 *     } else {
 *         // 收到响应
 *         VoteResponseData responseData = (VoteResponseData) response.data();
 *         Node voter = response.source();
 *
 *         if (responseData.voteGranted()) {
 *             recordVote(voter);
 *             if (hasQuorum()) {
 *                 becomeLeader();
 *             }
 *         } else {
 *             handleVoteRejection(voter, responseData);
 *         }
 *     }
 * });
 * </pre>
 *
 * 3. 批量处理响应：
 * <pre>
 * // Leader发送心跳给所有follower
 * Map<Node, RaftRequest.Outbound> heartbeats = new HashMap<>();
 * for (Node follower : followers) {
 *     RaftRequest.Outbound heartbeat = buildHeartbeat(follower);
 *     networkChannel.send(heartbeat);
 *     heartbeats.put(follower, heartbeat);
 * }
 *
 * // 收集所有响应
 * List<CompletableFuture<RaftResponse.Inbound>> futures =
 *     heartbeats.values().stream()
 *         .map(req -> req.completion)
 *         .collect(Collectors.toList());
 *
 * // 等待所有响应（或超时）
 * CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
 *     .orTimeout(1, TimeUnit.SECONDS)
 *     .whenComplete((v, e) -> {
 *         // 统计成功响应
 *         int successCount = 0;
 *         for (Map.Entry<Node, RaftRequest.Outbound> entry : heartbeats.entrySet()) {
 *             RaftRequest.Outbound req = entry.getValue();
 *             if (req.completion.isDone() && !req.completion.isCompletedExceptionally()) {
 *                 RaftResponse.Inbound resp = req.completion.join();
 *                 updateFollowerProgress(resp.source(), resp.data());
 *                 successCount++;
 *             }
 *         }
 *
 *         // 检查是否保持quorum
 *         if (successCount + 1 < quorumSize) {  // +1是leader自己
 *             stepDownAsLeader();
 *         }
 *     });
 * </pre>
 *
 * 【错误处理】
 *
 * 响应可能包含错误信息：
 * <pre>
 * RaftResponse.Inbound response = ...;
 * ApiMessage data = response.data();
 *
 * if (data instanceof VoteResponseData) {
 *     VoteResponseData voteResp = (VoteResponseData) data;
 *     if (voteResp.errorCode() != Errors.NONE.code()) {
 *         // 处理协议级别的错误
 *         handleVoteError(response.source(), voteResp.errorCode());
 *     } else if (!voteResp.voteGranted()) {
 *         // 投票被拒绝（不是错误，只是拒绝）
 *         handleVoteRejection(response.source(), voteResp);
 *     } else {
 *         // 投票成功
 *         recordVote(response.source());
 *     }
 * }
 * </pre>
 *
 * @see RaftRequest 对应的请求类
 * @see RaftMessage 消息接口
 */
public abstract class RaftResponse implements RaftMessage {
    /**
     * 关联ID - 必须与对应请求的correlationId一致
     *
     * 这是请求-响应匹配的关键。
     * 异步网络通信中可能有多个并发请求，
     * 通过correlationId确保每个响应匹配到正确的请求。
     */
    private final int correlationId;

    /**
     * 响应数据 - 具体的Raft协议响应消息
     *
     * 常见的响应类型：
     * - VoteResponseData：投票响应（是否同意投票）
     * - FetchResponseData：日志获取响应（返回日志记录）
     * - BeginQuorumEpochResponseData：确认新epoch开始
     * - EndQuorumEpochResponseData：确认epoch结束
     *
     * 所有响应都包含errorCode字段，用于报告错误。
     */
    private final ApiMessage data;

    /**
     * 构造RaftResponse
     *
     * @param correlationId 关联ID，必须与请求的correlationId一致
     * @param data 响应数据（VoteResponse、FetchResponse等）
     */
    protected RaftResponse(int correlationId, ApiMessage data) {
        this.correlationId = correlationId;
        this.data = data;
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
     * Inbound - 入站响应（从网络接收到的响应）
     *
     * 【使用场景】
     * 当本节点接收到其他节点发来的响应时，网络层创建Inbound实例。
     *
     * 【关键特征】
     * 1. 包含来源节点信息
     *    - 知道响应来自哪个节点
     *    - 用于更新节点状态、检测活跃性
     *    - 用于响应的业务处理（如投票统计）
     *
     * 2. 通过CompletableFuture传递
     *    - 网络层接收响应后，调用request.completion.complete(inboundResponse)
     *    - 请求发送者通过future.whenComplete()处理响应
     *
     * 【处理流程】
     * <pre>
     * // 1. 发送请求
     * RaftRequest.Outbound request = new RaftRequest.Outbound(...);
     * networkChannel.send(request);
     *
     * // 2. 网络层接收响应并创建Inbound
     * // （这部分由网络层自动完成）
     * RaftResponse.Inbound inbound = new RaftResponse.Inbound(
     *     correlationId,  // 从网络消息中解析
     *     responseData,   // 从网络消息中解析
     *     sourceNode      // 从TCP连接中获取
     * );
     * request.completion.complete(inbound);  // 完成对应请求的future
     *
     * // 3. 请求发送者处理响应
     * request.completion.whenComplete((inbound, exception) -> {
     *     if (inbound != null) {
     *         // 知道响应来自哪个节点
     *         processResponse(inbound.source(), inbound.data());
     *     }
     * });
     * </pre>
     *
     * 【为什么需要source？】
     * 考虑Leader向多个follower发送心跳的场景：
     * - Leader并发发送多个请求，每个请求的destination不同
     * - 响应可能以任意顺序返回
     * - 必须知道每个响应来自哪个follower，才能更新对应的状态
     * - source字段提供了这个关键信息
     */
    public static final class Inbound extends RaftResponse {
        /**
         * 来源节点 - 发送此响应的节点
         *
         * Node包含：
         * - id: 节点ID
         * - host: 主机名或IP地址
         * - port: 端口号
         * - rack: 机架信息（可选）
         *
         * 用途：
         * 1. 更新节点状态（最后通信时间、健康状态）
         * 2. 业务处理（如统计投票结果）
         * 3. 监控和调试（追踪响应来源）
         */
        private final Node source;

        /**
         * 构造入站响应
         *
         * @param correlationId 关联ID，与请求的correlationId一致
         * @param data 响应数据
         * @param source 来源节点
         */
        public Inbound(int correlationId, ApiMessage data, Node source) {
            super(correlationId, data);
            this.source = source;
        }

        /**
         * 获取来源节点
         */
        public Node source() {
            return source;
        }

        @Override
        public String toString() {
            return String.format(
                "InboundResponse(correlationId=%d, data=%s, source=%s)",
                correlationId(),
                data(),
                source
            );
        }
    }

    /**
     * Outbound - 出站响应（准备发送的响应）
     *
     * 【使用场景】
     * 当本节点处理完请求，需要发送响应时，创建Outbound实例。
     *
     * 【关键特征】
     * 1. 不包含目标节点信息
     *    - 响应总是发送回请求来源
     *    - 网络层已经知道目标（请求来源的连接）
     *    - 简化响应创建，不需要追踪请求来源
     *
     * 2. 通过CompletableFuture发送
     *    - 完成请求的completion future触发响应发送
     *    - 网络层监听future，自动发送响应
     *
     * 【创建和发送流程】
     * <pre>
     * // 1. 接收请求
     * RaftRequest.Inbound request = networkChannel.receive();
     *
     * // 2. 处理请求
     * ApiMessage requestData = request.data();
     * ApiMessage responseData;
     *
     * if (requestData instanceof VoteRequestData) {
     *     VoteRequestData voteReq = (VoteRequestData) requestData;
     *     boolean granted = canGrantVote(voteReq);
     *     responseData = new VoteResponseData()
     *         .setVoteGranted(granted)
     *         .setLeaderEpoch(currentEpoch);
     * } else if (requestData instanceof FetchRequestData) {
     *     FetchRequestData fetchReq = (FetchRequestData) requestData;
     *     responseData = fetchLogRecords(fetchReq);
     * }
     *
     * // 3. 创建出站响应
     * RaftResponse.Outbound response = new RaftResponse.Outbound(
     *     request.correlationId(),  // 使用请求的correlationId
     *     responseData
     * );
     *
     * // 4. 完成future，触发发送
     * // 注意：不需要指定目标，网络层知道往哪发
     * request.completion.complete(response);
     * </pre>
     *
     * 【为什么不需要目标节点？】
     *
     * 技术原因：
     * 1. TCP连接是双向的
     *    - 请求从某个TCP连接进来
     *    - 响应就从同一TCP连接发回去
     *    - 网络层维护了连接到节点的映射
     *
     * 2. 简化请求处理
     *    - 请求处理器不需要知道请求从哪来
     *    - 只需要根据请求数据生成响应数据
     *    - 网络层负责路由和发送
     *
     * 3. 代码解耦
     *    - 业务逻辑（处理请求）与网络层（发送响应）分离
     *    - 请求处理器专注于状态机逻辑
     *    - 网络细节由网络层封装
     *
     * 对比：
     * - RaftRequest.Outbound需要destination（主动发起连接）
     * - RaftResponse.Outbound不需要destination（响应已有连接）
     *
     * 【异步处理示例】
     * <pre>
     * // 异步处理请求，不阻塞网络线程
     * public void handleRequest(RaftRequest.Inbound request) {
     *     // 提交到线程池异步处理
     *     executor.submit(() -> {
     *         try {
     *             // 处理可能耗时的业务逻辑
     *             ApiMessage responseData = processRequest(request.data());
     *
     *             // 创建响应
     *             RaftResponse.Outbound response = new RaftResponse.Outbound(
     *                 request.correlationId(),
     *                 responseData
     *             );
     *
     *             // 完成future，触发响应发送
     *             request.completion.complete(response);
     *         } catch (Exception e) {
     *             // 异常时也要完成future
     *             request.completion.completeExceptionally(e);
     *         }
     *     });
     * }
     * </pre>
     */
    public static final class Outbound extends RaftResponse {
        /**
         * 构造出站响应
         *
         * @param requestId 关联ID（即对应请求的correlationId）
         * @param data 响应数据
         */
        public Outbound(int requestId, ApiMessage data) {
            super(requestId, data);
        }

        @Override
        public String toString() {
            return String.format(
                "OutboundResponse(correlationId=%d, data=%s)",
                correlationId(),
                data()
            );
        }
    }
}
