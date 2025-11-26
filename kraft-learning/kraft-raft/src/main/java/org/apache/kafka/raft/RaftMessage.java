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

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * RaftMessage - Raft消息接口
 *
 * 封装Raft协议中的请求和响应消息。
 *
 * 【为什么需要RaftMessage？】
 *
 * Raft协议包含多种消息类型：
 * - VoteRequest/VoteResponse：投票请求和响应
 * - FetchRequest/FetchResponse：日志复制请求和响应
 * - BeginQuorumEpochRequest/Response：Leader选举成功通知
 * - EndQuorumEpochRequest/Response：Leader辞职通知
 *
 * RaftMessage提供了统一的抽象接口，封装：
 * 1. correlationId - 请求/响应关联ID
 * 2. data - 实际的消息数据（ApiMessage）
 *
 * 【消息关联机制】
 *
 * 客户端发送请求时分配correlationId，服务端响应时携带相同的ID：
 *
 * <pre>
 * // 客户端
 * int correlationId = nextCorrelationId++;
 * VoteRequest request = new VoteRequest(correlationId, ...);
 * send(request);
 *
 * // 服务端
 * VoteResponse response = new VoteResponse(request.correlationId(), ...);
 * send(response);
 *
 * // 客户端收到响应
 * if (response.correlationId() == sentRequest.correlationId()) {
 *     // 匹配成功，这是我的请求的响应
 * }
 * </pre>
 *
 * 【为什么需要correlationId？】
 *
 * 1. **异步通信**
 *    - 可以同时发送多个请求
 *    - 通过correlationId匹配请求和响应
 *
 * 2. **管道化（Pipelining）**
 *    <pre>
 *    发送请求1 (correlationId=1)
 *    发送请求2 (correlationId=2)
 *    发送请求3 (correlationId=3)
 *    收到响应  (correlationId=2) → 匹配请求2
 *    收到响应  (correlationId=1) → 匹配请求1
 *    收到响应  (correlationId=3) → 匹配请求3
 *    </pre>
 *
 * 3. **超时检测**
 *    <pre>
 *    // 检查哪些请求超时了
 *    for (PendingRequest req : pendingRequests) {
 *        if (now - req.sentTime > timeout) {
 *            // 请求correlationId=X超时
 *            handleTimeout(req.correlationId());
 *        }
 *    }
 *    </pre>
 *
 * 【ApiMessage是什么？】
 *
 * ApiMessage是Kafka协议消息的基类，所有Raft消息都实现了这个接口：
 * - VoteRequestData extends ApiMessage
 * - VoteResponseData extends ApiMessage
 * - FetchRequestData extends ApiMessage
 * - ...
 *
 * ApiMessage提供：
 * - 序列化/反序列化
 * - 版本管理
 * - 字段访问
 *
 * 【实现类】
 *
 * RaftMessage有两种实现：
 *
 * 1. **RaftRequest.Inbound** - 入站请求
 *    <pre>
 *    class Inbound implements RaftMessage {
 *        private final int correlationId;
 *        private final ApiMessage data; // VoteRequest, FetchRequest等
 *
 *        public int correlationId() { return correlationId; }
 *        public ApiMessage data() { return data; }
 *    }
 *    </pre>
 *
 * 2. **RaftResponse.Outbound** - 出站响应
 *    <pre>
 *    class Outbound implements RaftMessage {
 *        private final int correlationId;
 *        private final ApiMessage data; // VoteResponse, FetchResponse等
 *
 *        public int correlationId() { return correlationId; }
 *        public ApiMessage data() { return data; }
 *    }
 *    </pre>
 *
 * 【典型使用场景】
 *
 * 场景1：发送Vote请求
 * <pre>
 * // 创建VoteRequest
 * int correlationId = generateCorrelationId();
 * VoteRequestData voteData = new VoteRequestData()
 *     .setCandidateId(myId)
 *     .setCandidateEpoch(currentEpoch)
 *     .setLastLogOffset(lastOffset);
 *
 * RaftMessage voteRequest = new RaftRequest.Outbound(correlationId, voteData);
 *
 * // 发送给其他节点
 * channel.send(voteRequest);
 * </pre>
 *
 * 场景2：处理收到的响应
 * <pre>
 * // 收到消息
 * RaftMessage message = channel.receive();
 *
 * // 检查correlationId
 * int correlationId = message.correlationId();
 * PendingRequest pending = pendingRequests.get(correlationId);
 * if (pending != null) {
 *     // 找到了对应的请求
 *     ApiMessage responseData = message.data();
 *     if (responseData instanceof VoteResponseData) {
 *         VoteResponseData response = (VoteResponseData) responseData;
 *         handleVoteResponse(response);
 *     }
 * }
 * </pre>
 *
 * 场景3：超时处理
 * <pre>
 * // 周期性检查超时
 * void checkTimeouts() {
 *     long now = System.currentTimeMillis();
 *     for (Map.Entry<Integer, PendingRequest> entry : pendingRequests.entrySet()) {
 *         int correlationId = entry.getKey();
 *         PendingRequest pending = entry.getValue();
 *
 *         if (now - pending.sentTime > REQUEST_TIMEOUT) {
 *             // 请求超时
 *             logger.warn("Request {} timed out", correlationId);
 *             pendingRequests.remove(correlationId);
 *             handleTimeout(pending);
 *         }
 *     }
 * }
 * </pre>
 *
 * 【与Kafka其他协议的关系】
 *
 * RaftMessage复用了Kafka的协议框架：
 * <pre>
 * Kafka Protocol Framework
 *   ├─ Produce/Fetch (数据平面)
 *   ├─ Metadata/ListOffsets (控制平面)
 *   └─ Vote/BeginQuorumEpoch (Raft协议)
 *         └─ 都实现ApiMessage接口
 * </pre>
 *
 * 好处：
 * - 重用序列化/反序列化代码
 * - 重用版本管理机制
 * - 统一的协议生成工具
 *
 * 【线程安全性】
 *
 * RaftMessage本身是不可变的：
 * - correlationId不变
 * - data引用不变（虽然ApiMessage内容可能可变）
 * - 可以安全地跨线程传递
 *
 * @see RaftRequest Raft请求消息
 * @see RaftResponse Raft响应消息
 * @see ApiMessage Kafka协议消息基类
 */
public interface RaftMessage {
    /**
     * 获取关联ID
     *
     * CorrelationId用于匹配请求和响应。
     *
     * 【生成规则】
     *
     * - 客户端生成：单调递增的整数
     * - 服务端复制：响应的correlationId = 请求的correlationId
     *
     * 【数值范围】
     *
     * int类型，范围 -2^31 到 2^31-1
     * - 通常从0或1开始递增
     * - 溢出后从负数继续（允许溢出）
     *
     * 【唯一性】
     *
     * correlationId在**单个连接**内唯一：
     * - 同一连接的不同请求有不同的correlationId
     * - 不同连接可以有相同的correlationId
     *
     * 示例：
     * <pre>
     * // 连接A
     * request1: correlationId=1
     * request2: correlationId=2
     *
     * // 连接B（独立计数）
     * request1: correlationId=1  // OK，不同连接
     * request2: correlationId=2
     * </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * // 发送请求时记录correlationId
     * int correlationId = message.correlationId();
     * pendingRequests.put(correlationId, new PendingRequest(message, System.currentTimeMillis()));
     *
     * // 收到响应时查找对应的请求
     * int correlationId = response.correlationId();
     * PendingRequest pending = pendingRequests.remove(correlationId);
     * if (pending != null) {
     *     handleResponse(pending, response);
     * }
     * </pre>
     *
     * @return 关联ID，用于匹配请求和响应
     */
    int correlationId();

    /**
     * 获取消息数据
     *
     * 返回实际的Raft协议消息。
     *
     * 【消息类型】
     *
     * 请求类型：
     * - VoteRequestData：投票请求
     * - BeginQuorumEpochRequestData：Leader选举成功通知
     * - EndQuorumEpochRequestData：Leader辞职通知
     * - FetchRequestData：日志复制请求
     * - FetchSnapshotRequestData：快照拉取请求
     *
     * 响应类型：
     * - VoteResponseData：投票响应
     * - BeginQuorumEpochResponseData：Leader选举响应
     * - EndQuorumEpochResponseData：Leader辞职响应
     * - FetchResponseData：日志复制响应
     * - FetchSnapshotResponseData：快照拉取响应
     *
     * 【类型判断】
     *
     * 使用instanceof检查消息类型：
     * <pre>
     * ApiMessage data = message.data();
     *
     * if (data instanceof VoteRequestData) {
     *     handleVoteRequest((VoteRequestData) data);
     * } else if (data instanceof FetchRequestData) {
     *     handleFetchRequest((FetchRequestData) data);
     * } else if (data instanceof BeginQuorumEpochRequestData) {
     *     handleBeginQuorumEpoch((BeginQuorumEpochRequestData) data);
     * }
     * // ... 其他消息类型
     * </pre>
     *
     * 【ApiMessage特性】
     *
     * ApiMessage提供的功能：
     * 1. **序列化**：toStruct() - 转换为可序列化的结构
     * 2. **反序列化**：fromStruct() - 从字节流解析
     * 3. **版本管理**：支持多个协议版本
     * 4. **字段访问**：get/set方法访问字段
     *
     * 【消息生命周期】
     *
     * <pre>
     * 1. 创建消息对象
     *    VoteRequestData request = new VoteRequestData()
     *        .setCandidateId(1)
     *        .setLastLogOffset(100);
     *
     * 2. 封装为RaftMessage
     *    RaftMessage message = new RaftRequest.Outbound(correlationId, request);
     *
     * 3. 序列化并发送
     *    ByteBuffer buffer = serialize(message.data());
     *    channel.send(buffer);
     *
     * 4. 接收并反序列化
     *    ByteBuffer buffer = channel.receive();
     *    ApiMessage data = deserialize(buffer);
     *
     * 5. 封装为RaftMessage
     *    RaftMessage message = new RaftRequest.Inbound(correlationId, data);
     *
     * 6. 处理消息
     *    handleMessage(message);
     * </pre>
     *
     * 【不可变性】
     *
     * data()返回的引用不变，但ApiMessage内容可能可变：
     * - 可以修改ApiMessage的字段
     * - 但通常在创建后就不再修改
     *
     * @return Raft协议消息数据（ApiMessage的具体实现）
     */
    ApiMessage data();
}
