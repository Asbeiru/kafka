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

import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.network.ListenerName;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Endpoints - 网络端点集合
 *
 * 封装了一个Kafka节点的多个网络监听地址（endpoints）。
 * 每个监听器（listener）对应一个网络地址（host:port）。
 *
 * 【为什么需要多个监听器？】
 *
 * 在生产环境中，Kafka节点可能需要在不同的网络接口上监听：
 *
 * 1. 安全隔离：
 *    - INTERNAL监听器：用于集群内部通信（如Raft复制）
 *    - EXTERNAL监听器：用于客户端连接
 *    - 不同的监听器可以配置不同的安全策略
 *
 * 2. 网络分隔：
 *    - 公网IP：对外提供服务
 *    - 内网IP：集群内部通信（更快、更安全）
 *
 * 3. 多协议支持：
 *    - PLAINTEXT：明文通信（测试环境）
 *    - SSL：加密通信（生产环境）
 *    - SASL：认证通信（安全要求高的场景）
 *
 * 示例配置：
 * <pre>
 * listeners=INTERNAL://192.168.1.10:9092,EXTERNAL://10.0.0.10:9093
 *
 * INTERNAL：集群内部通信使用内网IP
 * EXTERNAL：客户端连接使用公网IP
 * </pre>
 *
 * 【核心设计】
 *
 * Endpoints = Map<监听器名称, 网络地址>
 *
 * 例如：
 * <pre>
 * {
 *   "INTERNAL" -> 192.168.1.10:9092,
 *   "EXTERNAL" -> 10.0.0.10:9093
 * }
 * </pre>
 *
 * 【使用场景】
 *
 * 场景1：Leader选举
 * <pre>
 * // 新Leader通知Follower："我是Leader，这是我的地址"
 * Endpoints leaderEndpoints = ...;
 * BeginQuorumEpochRequest request = new BeginQuorumEpochRequest()
 *     .setLeaderEndpoints(leaderEndpoints.toBeginQuorumEpochRequest());
 *
 * // Follower收到后，知道如何连接到Leader
 * Endpoints endpoints = Endpoints.fromBeginQuorumEpochRequest(request.leaderEndpoints());
 * InetSocketAddress address = endpoints.address(myListenerName).get();
 * // 使用这个地址连接到Leader
 * </pre>
 *
 * 场景2：添加新的投票者
 * <pre>
 * // 添加新节点时，需要告知它的监听地址
 * Endpoints voterEndpoints = ...;
 * AddRaftVoterRequest request = new AddRaftVoterRequest()
 *     .setListeners(voterEndpoints.toAddVoterRequest());
 * </pre>
 *
 * 场景3：查询集群状态
 * <pre>
 * // 返回集群中每个节点的监听地址
 * Endpoints endpoints = ...;
 * response.setListeners(endpoints.toDescribeQuorumResponseListeners());
 * </pre>
 *
 * 【为什么使用InetSocketAddress.createUnresolved？】
 *
 * createUnresolved vs 正常构造函数：
 *
 * 正常构造：
 * <pre>
 * InetSocketAddress addr = new InetSocketAddress("kafka.example.com", 9092);
 * // 会立即进行DNS解析，可能阻塞，可能失败
 * </pre>
 *
 * createUnresolved：
 * <pre>
 * InetSocketAddress addr = InetSocketAddress.createUnresolved("kafka.example.com", 9092);
 * // 不进行DNS解析，只存储hostname和port
 * </pre>
 *
 * 使用createUnresolved的原因：
 * 1. 避免阻塞：DNS解析可能很慢，不应该在消息解析时阻塞
 * 2. 延迟解析：只在真正需要连接时才解析
 * 3. 容错性：即使DNS暂时不可用，也不影响接收和存储地址
 * 4. 性能：避免重复解析同一个hostname
 *
 * 【不可变性】
 *
 * Endpoints是final类：
 * - 一旦创建，endpoints Map不可更改
 * - 线程安全
 * - 可以安全地跨线程传递
 *
 * 【空实例模式】
 *
 * 使用静态常量EMPTY而不是每次都创建新对象：
 * - 节省内存（只有一个空实例）
 * - 提高性能（避免重复创建）
 * - 语义清晰（表达"没有端点"的概念）
 *
 * @see ListenerName 监听器名称
 * @see InetSocketAddress 网络地址（host + port）
 */
public final class Endpoints {
    /**
     * 端点映射：监听器名称 → 网络地址
     *
     * Key: 监听器名称（如"INTERNAL"、"EXTERNAL"）
     * Value: 网络地址（如192.168.1.10:9092）
     *
     * 使用Map的原因：
     * - 快速查找：O(1)时间复杂度根据监听器名称查找地址
     * - 唯一性：每个监听器只有一个地址
     * - 灵活性：可以有任意数量的监听器
     */
    private final Map<ListenerName, InetSocketAddress> endpoints;

    /**
     * 私有构造函数
     *
     * 强制使用静态工厂方法创建实例。
     * 好处：
     * 1. 工厂方法名称更有语义（fromXxx）
     * 2. 可以返回单例（如empty()）
     * 3. 可以添加验证逻辑
     *
     * @param endpoints 端点映射
     */
    private Endpoints(Map<ListenerName, InetSocketAddress> endpoints) {
        this.endpoints = endpoints;
    }

    /**
     * 查询指定监听器的网络地址
     *
     * @param listener 监听器名称
     * @return 网络地址（Optional，可能不存在该监听器）
     */
    public Optional<InetSocketAddress> address(ListenerName listener) {
        return Optional.ofNullable(endpoints.get(listener));
    }

    /**
     * 转换为VotersRecord.Endpoint迭代器
     *
     * 用于序列化到VotersRecord（投票者记录）。
     * VotersRecord是Raft元数据主题中记录投票者配置的消息格式。
     *
     * @return VotersRecord.Endpoint迭代器
     */
    public Iterator<VotersRecord.Endpoint> votersRecordEndpoints() {
        return endpoints.entrySet()
            .stream()
            .map(entry ->
                new VotersRecord.Endpoint()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            )
            .iterator();
    }

    /**
     * 获取端点数量
     *
     * @return 监听器数量
     */
    public int size() {
        return endpoints.size();
    }

    /**
     * 判断是否为空
     *
     * @return 如果没有任何端点返回true
     */
    public boolean isEmpty() {
        return endpoints.isEmpty();
    }

    /**
     * 判断相等性
     *
     * 两个Endpoints相等当且仅当它们包含完全相同的端点映射。
     *
     * @param o 要比较的对象
     * @return 如果相等返回true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Endpoints that = (Endpoints) o;

        return endpoints.equals(that.endpoints);
    }

    /**
     * 计算哈希码
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(endpoints);
    }

    /**
     * 字符串表示
     *
     * 格式：Endpoints(endpoints={INTERNAL=192.168.1.10:9092, EXTERNAL=10.0.0.10:9093})
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return String.format("Endpoints(endpoints=%s)", endpoints);
    }

    /**
     * 转换为BeginQuorumEpochRequest中的端点集合
     *
     * BeginQuorumEpoch是Leader选举成功后，新Leader发送给Follower的第一个消息。
     * 消息中包含Leader的监听地址，让Follower知道如何连接到Leader。
     *
     * @return BeginQuorumEpochRequest的端点集合
     */
    public BeginQuorumEpochRequestData.LeaderEndpointCollection toBeginQuorumEpochRequest() {
        BeginQuorumEpochRequestData.LeaderEndpointCollection leaderEndpoints =
            new BeginQuorumEpochRequestData.LeaderEndpointCollection(endpoints.size());
        for (Map.Entry<ListenerName, InetSocketAddress> entry : endpoints.entrySet()) {
            leaderEndpoints.add(
                new BeginQuorumEpochRequestData.LeaderEndpoint()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            );
        }

        return leaderEndpoints;
    }

    /**
     * 转换为AddRaftVoterRequest中的监听器集合
     *
     * AddRaftVoter用于向Raft集群添加新的投票者。
     * 请求中包含新投票者的监听地址。
     *
     * @return AddRaftVoterRequest的监听器集合
     */
    public AddRaftVoterRequestData.ListenerCollection toAddVoterRequest() {
        AddRaftVoterRequestData.ListenerCollection listeners =
            new AddRaftVoterRequestData.ListenerCollection(endpoints.size());
        for (Map.Entry<ListenerName, InetSocketAddress> entry : endpoints.entrySet()) {
            listeners.add(
                new AddRaftVoterRequestData.Listener()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            );
        }
        return listeners;
    }

    /**
     * 转换为DescribeQuorumResponse中的监听器集合
     *
     * DescribeQuorum用于查询Raft集群状态。
     * 响应中包含每个节点的监听地址。
     *
     * @return DescribeQuorumResponse的监听器集合
     */
    public DescribeQuorumResponseData.ListenerCollection toDescribeQuorumResponseListeners() {
        DescribeQuorumResponseData.ListenerCollection listeners =
            new DescribeQuorumResponseData.ListenerCollection(endpoints.size());
        for (Map.Entry<ListenerName, InetSocketAddress> entry : endpoints.entrySet()) {
            listeners.add(
                new DescribeQuorumResponseData.Listener()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            );
        }
        return listeners;
    }

    /**
     * 转换为UpdateRaftVoterRequest中的监听器集合
     *
     * UpdateRaftVoter用于更新投票者的信息（如监听地址变更）。
     *
     * @return UpdateRaftVoterRequest的监听器集合
     */
    public UpdateRaftVoterRequestData.ListenerCollection toUpdateVoterRequest() {
        UpdateRaftVoterRequestData.ListenerCollection listeners =
            new UpdateRaftVoterRequestData.ListenerCollection(endpoints.size());
        for (Map.Entry<ListenerName, InetSocketAddress> entry : endpoints.entrySet()) {
            listeners.add(
                new UpdateRaftVoterRequestData.Listener()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            );
        }

        return listeners;
    }

    /**
     * 空端点单例
     *
     * 使用单例模式避免重复创建空对象：
     * - 节省内存
     * - 提高性能
     * - 可以用 == 比较（虽然应该用equals）
     */
    private static final Endpoints EMPTY = new Endpoints(Map.of());

    /**
     * 获取空端点实例
     *
     * 用于表示"没有任何监听地址"的情况。
     *
     * @return 空Endpoints实例
     */
    public static Endpoints empty() {
        return EMPTY;
    }

    /**
     * 从InetSocketAddress映射创建Endpoints
     *
     * 这是最直接的创建方式，直接传入Map。
     *
     * @param endpoints 监听器名称到网络地址的映射
     * @return Endpoints实例
     */
    public static Endpoints fromInetSocketAddresses(Map<ListenerName, InetSocketAddress> endpoints) {
        return new Endpoints(endpoints);
    }

    /**
     * 从VotersRecord.Endpoint集合创建Endpoints
     *
     * 用于从VotersRecord（投票者记录）反序列化。
     * VotersRecord存储在Raft元数据主题中，记录投票者配置。
     *
     * @param endpoints VotersRecord.Endpoint集合
     * @return Endpoints实例
     */
    public static Endpoints fromVotersRecordEndpoints(Collection<VotersRecord.Endpoint> endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (VotersRecord.Endpoint endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }

    /**
     * 从BeginQuorumEpochRequest创建Endpoints
     *
     * BeginQuorumEpoch是新Leader发送的第一个消息。
     * Follower从中提取Leader的监听地址。
     *
     * @param endpoints BeginQuorumEpochRequest中的端点集合
     * @return Endpoints实例
     */
    public static Endpoints fromBeginQuorumEpochRequest(BeginQuorumEpochRequestData.LeaderEndpointCollection endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (BeginQuorumEpochRequestData.LeaderEndpoint endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }

    /**
     * 从BeginQuorumEpochResponse创建Endpoints
     *
     * Follower响应BeginQuorumEpoch时，可能包含自己的监听地址。
     * Leader从中提取特定节点的地址。
     *
     * @param listenerName 要查找的监听器名称
     * @param leaderId Leader的节点ID
     * @param endpoints BeginQuorumEpochResponse中的端点集合
     * @return Endpoints实例，如果找不到则返回empty()
     */
    public static Endpoints fromBeginQuorumEpochResponse(
        ListenerName listenerName,
        int leaderId,
        BeginQuorumEpochResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    /**
     * 从EndQuorumEpochRequest创建Endpoints
     *
     * EndQuorumEpoch是Leader辞职时发送的消息。
     * 消息中可能包含Leader的监听地址。
     *
     * @param endpoints EndQuorumEpochRequest中的端点集合
     * @return Endpoints实例
     */
    public static Endpoints fromEndQuorumEpochRequest(EndQuorumEpochRequestData.LeaderEndpointCollection endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (EndQuorumEpochRequestData.LeaderEndpoint endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }

    /**
     * 从EndQuorumEpochResponse创建Endpoints
     *
     * Follower响应EndQuorumEpoch时，可能包含自己的监听地址。
     *
     * @param listenerName 要查找的监听器名称
     * @param leaderId Leader的节点ID
     * @param endpoints EndQuorumEpochResponse中的端点集合
     * @return Endpoints实例，如果找不到则返回empty()
     */
    public static Endpoints fromEndQuorumEpochResponse(
        ListenerName listenerName,
        int leaderId,
        EndQuorumEpochResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    /**
     * 从VoteResponse创建Endpoints
     *
     * 投票响应中可能包含候选者的监听地址。
     *
     * @param listenerName 要查找的监听器名称
     * @param leaderId 候选者的节点ID
     * @param endpoints VoteResponse中的端点集合
     * @return Endpoints实例，如果找不到则返回empty()
     */
    public static Endpoints fromVoteResponse(
        ListenerName listenerName,
        int leaderId,
        VoteResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    /**
     * 从FetchResponse创建Endpoints
     *
     * Fetch响应中可能包含Leader的监听地址。
     * Follower可以用来更新Leader地址信息。
     *
     * @param listenerName 要查找的监听器名称
     * @param leaderId Leader的节点ID
     * @param endpoints FetchResponse中的端点集合
     * @return Endpoints实例，如果找不到则返回empty()
     */
    public static Endpoints fromFetchResponse(
        ListenerName listenerName,
        int leaderId,
        FetchResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    /**
     * 从FetchSnapshotResponse创建Endpoints
     *
     * Follower从Leader拉取快照时，响应中可能包含Leader的监听地址。
     *
     * @param listenerName 要查找的监听器名称
     * @param leaderId Leader的节点ID
     * @param endpoints FetchSnapshotResponse中的端点集合
     * @return Endpoints实例，如果找不到则返回empty()
     */
    public static Endpoints fromFetchSnapshotResponse(
        ListenerName listenerName,
        int leaderId,
        FetchSnapshotResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    /**
     * 从AddRaftVoterRequest创建Endpoints
     *
     * 添加投票者请求中包含新投票者的监听地址。
     *
     * @param endpoints AddRaftVoterRequest中的监听器集合
     * @return Endpoints实例
     */
    public static Endpoints fromAddVoterRequest(AddRaftVoterRequestData.ListenerCollection endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (AddRaftVoterRequestData.Listener endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }

    /**
     * 从UpdateRaftVoterRequest创建Endpoints
     *
     * 更新投票者请求中包含投票者的新监听地址。
     *
     * @param endpoints UpdateRaftVoterRequest中的监听器集合
     * @return Endpoints实例
     */
    public static Endpoints fromUpdateVoterRequest(UpdateRaftVoterRequestData.ListenerCollection endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (UpdateRaftVoterRequestData.Listener endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }
}
