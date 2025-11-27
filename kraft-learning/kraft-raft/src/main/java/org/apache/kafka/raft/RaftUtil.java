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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.common.OffsetAndEpoch;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * RaftUtil - Raft协议工具类
 *
 * 提供创建各种Raft协议消息的便捷方法。
 *
 * 【为什么需要RaftUtil？】
 *
 * Raft协议包含许多复杂的请求/响应消息：
 * - Vote：投票请求和响应
 * - Fetch：日志复制请求和响应
 * - BeginQuorumEpoch：Leader选举成功通知
 * - EndQuorumEpoch：Leader辞职通知
 * - FetchSnapshot：快照拉取请求和响应
 * - Add/Remove/UpdateRaftVoter：动态成员变更
 * - DescribeQuorum：查询Quorum状态
 *
 * 这些消息的结构都很复杂（嵌套的topic/partition结构）。
 * RaftUtil提供工厂方法简化消息创建，避免重复代码。
 *
 * 【Kafka协议的嵌套结构】
 *
 * Kafka协议支持多个topic和partition，所以消息结构是嵌套的：
 * <pre>
 * VoteRequest {
 *   topics: [
 *     {
 *       topicName: "__cluster_metadata",
 *       partitions: [
 *         {
 *           partitionIndex: 0,
 *           candidateId: 1,
 *           lastOffset: 100
 *         }
 *       ]
 *     }
 *   ]
 * }
 * </pre>
 *
 * 但是对于KRaft来说：
 * - 只有一个topic：__cluster_metadata
 * - 只有一个partition：0
 *
 * 所以RaftUtil提供"singleton"方法：
 * - 输入：单个topic和partition
 * - 输出：包含嵌套结构的完整消息
 *
 * 【工具方法分类】
 *
 * 1. **错误响应创建**
 *    - errorResponse()：根据ApiKey创建错误响应
 *
 * 2. **Fetch消息**
 *    - singletonFetchRequest()：创建Fetch请求
 *    - singletonFetchResponse()：创建Fetch响应
 *
 * 3. **Vote消息**
 *    - singletonVoteRequest()：创建Vote请求
 *    - singletonVoteResponse()：创建Vote响应
 *
 * 4. **FetchSnapshot消息**
 *    - singletonFetchSnapshotRequest()：创建快照拉取请求
 *    - singletonFetchSnapshotResponse()：创建快照拉取响应
 *
 * 5. **BeginQuorumEpoch消息**
 *    - singletonBeginQuorumEpochRequest()：创建Leader选举通知请求
 *    - singletonBeginQuorumEpochResponse()：创建Leader选举通知响应
 *
 * 6. **EndQuorumEpoch消息**
 *    - singletonEndQuorumEpochRequest()：创建Leader辞职通知请求
 *    - singletonEndQuorumEpochResponse()：创建Leader辞职通知响应
 *
 * 7. **DescribeQuorum消息**
 *    - singletonDescribeQuorumRequest()：创建查询Quorum状态请求
 *    - singletonDescribeQuorumResponse()：创建查询Quorum状态响应
 *
 * 8. **动态成员变更消息**
 *    - addVoterRequest()/addVoterResponse()：添加投票者
 *    - removeVoterRequest()/removeVoterResponse()：移除投票者
 *    - updateVoterRequest()/updateVoterResponse()：更新投票者信息
 *
 * 9. **ReplicaKey提取方法**
 *    - voteRequestVoterKey()：从VoteRequest提取投票者Key
 *    - beginQuorumEpochRequestVoterKey()：从BeginQuorumEpochRequest提取Key
 *    - addVoterRequestVoterKey()：从AddRaftVoterRequest提取Key
 *    - removeVoterRequestVoterKey()：从RemoveRaftVoterRequest提取Key
 *    - updateVoterRequestVoterKey()：从UpdateRaftVoterRequest提取Key
 *
 * 10. **消息验证方法**
 *     - hasValidTopicPartition()：验证消息是否包含正确的topic/partition
 *
 * 【典型使用流程】
 *
 * 创建Vote请求：
 * <pre>
 * // 构建Vote请求
 * VoteRequestData request = RaftUtil.singletonVoteRequest(
 *     topicPartition,        // __cluster_metadata:0
 *     clusterId,             // 集群ID
 *     currentEpoch,          // 当前epoch
 *     replicaKey,            // 候选人Key
 *     voterKey,              // 投票者Key
 *     lastEpoch,             // 最后日志的epoch
 *     lastOffset,            // 最后日志的offset
 *     false                  // 不是pre-vote
 * );
 *
 * // 发送请求
 * channel.send(new RaftRequest.Outbound(correlationId, request));
 * </pre>
 *
 * 创建Fetch响应：
 * <pre>
 * // 构建Fetch响应
 * FetchResponseData response = RaftUtil.singletonFetchResponse(
 *     listenerName,          // 监听器名称
 *     apiVersion,            // API版本
 *     topicPartition,        // __cluster_metadata:0
 *     topicId,               // Topic UUID
 *     Errors.NONE,           // 顶层错误码
 *     leaderId,              // Leader ID
 *     leaderEndpoints,       // Leader地址
 *     partition -> {
 *         // 填充分区响应数据
 *         partition
 *             .setHighWatermark(highWatermark)
 *             .setLogStartOffset(logStartOffset)
 *             .setRecords(records);
 *     }
 * );
 *
 * // 发送响应
 * channel.send(new RaftResponse.Outbound(correlationId, response));
 * </pre>
 *
 * 【为什么使用Consumer/UnaryOperator？】
 *
 * 部分方法接受Consumer或UnaryOperator参数：
 * <pre>
 * singletonFetchResponse(
 *     ...,
 *     partition -> {
 *         // 使用Consumer填充特定字段
 *         partition.setHighWatermark(hwm);
 *         partition.setRecords(records);
 *     }
 * )
 * </pre>
 *
 * 好处：
 * 1. 灵活性：调用者可以设置任何需要的字段
 * 2. 简洁性：避免传递大量参数
 * 3. 可读性：清晰表达设置哪些字段
 *
 * 【版本兼容性】
 *
 * 许多方法接受apiVersion参数，根据版本填充不同的字段：
 * <pre>
 * if (apiVersion >= 17) {
 *     // Fetch API版本17+支持NodeEndpoints
 *     response.setNodeEndpoints(endpoints);
 * }
 *
 * if (apiVersion >= 1) {
 *     // Vote API版本1+支持NodeEndpoints
 *     response.setNodeEndpoints(endpoints);
 * }
 * </pre>
 *
 * 这确保了向后兼容性，旧客户端不会收到无法识别的字段。
 *
 * @see RaftRequest Raft请求包装类
 * @see RaftResponse Raft响应包装类
 * @see RaftMessage 统一的消息接口
 */
@SuppressWarnings({ "ClassDataAbstractionCoupling", "ClassFanOutComplexity" })
public class RaftUtil {

    /**
     * 根据ApiKey创建错误响应
     *
     * 当处理请求失败时，需要返回错误响应。
     * 不同的ApiKey对应不同的响应类型。
     *
     * 【支持的ApiKey】
     *
     * - VOTE → VoteResponseData
     * - BEGIN_QUORUM_EPOCH → BeginQuorumEpochResponseData
     * - END_QUORUM_EPOCH → EndQuorumEpochResponseData
     * - FETCH → FetchResponseData
     * - FETCH_SNAPSHOT → FetchSnapshotResponseData
     * - API_VERSIONS → ApiVersionsResponseData
     * - UPDATE_RAFT_VOTER → UpdateRaftVoterResponseData
     * - ADD_RAFT_VOTER → AddRaftVoterResponseData
     * - REMOVE_RAFT_VOTER → RemoveRaftVoterResponseData
     *
     * 【使用场景】
     *
     * <pre>
     * // 处理Vote请求失败
     * try {
     *     handleVoteRequest(request);
     * } catch (Exception e) {
     *     ApiMessage errorResponse = RaftUtil.errorResponse(
     *         ApiKeys.VOTE,
     *         Errors.UNKNOWN_SERVER_ERROR
     *     );
     *     channel.send(new RaftResponse.Outbound(correlationId, errorResponse));
     * }
     * </pre>
     *
     * @param apiKey 请求的ApiKey
     * @param error 错误码
     * @return 对应的错误响应对象
     * @throws IllegalArgumentException 如果ApiKey不被支持
     */
    public static ApiMessage errorResponse(ApiKeys apiKey, Errors error) {
        return switch (apiKey) {
            case VOTE -> new VoteResponseData().setErrorCode(error.code());
            case BEGIN_QUORUM_EPOCH -> new BeginQuorumEpochResponseData().setErrorCode(error.code());
            case END_QUORUM_EPOCH -> new EndQuorumEpochResponseData().setErrorCode(error.code());
            case FETCH -> new FetchResponseData().setErrorCode(error.code());
            case FETCH_SNAPSHOT -> new FetchSnapshotResponseData().setErrorCode(error.code());
            case API_VERSIONS -> new ApiVersionsResponseData().setErrorCode(error.code());
            case UPDATE_RAFT_VOTER -> new UpdateRaftVoterResponseData().setErrorCode(error.code());
            case ADD_RAFT_VOTER -> new AddRaftVoterResponseData().setErrorCode(error.code());
            case REMOVE_RAFT_VOTER -> new RemoveRaftVoterResponseData().setErrorCode(error.code());
            default -> throw new IllegalArgumentException("Received response for unexpected request type: " + apiKey);
        };
    }

    /**
     * 创建单topic单partition的Fetch请求
     *
     * Follower使用Fetch请求从Leader拉取日志。
     *
     * 【使用场景】
     *
     * <pre>
     * // Follower拉取日志
     * FetchRequestData request = RaftUtil.singletonFetchRequest(
     *     topicPartition,
     *     topicId,
     *     partition -> {
     *         partition
     *             .setCurrentLeaderEpoch(currentEpoch)
     *             .setFetchOffset(fetchOffset)
     *             .setLastFetchedEpoch(lastFetchedEpoch)
     *             .setLogStartOffset(logStartOffset);
     *     }
     * );
     * </pre>
     *
     * @param topicPartition topic和partition
     * @param topicId topic的UUID
     * @param partitionConsumer 填充partition字段的Consumer
     * @return 完整的FetchRequestData对象
     */
    public static FetchRequestData singletonFetchRequest(
        TopicPartition topicPartition,
        Uuid topicId,
        Consumer<FetchRequestData.FetchPartition> partitionConsumer
    ) {
        FetchRequestData.FetchPartition fetchPartition =
            new FetchRequestData.FetchPartition()
                .setPartition(topicPartition.partition());
        partitionConsumer.accept(fetchPartition);

        FetchRequestData.FetchTopic fetchTopic =
            new FetchRequestData.FetchTopic()
                .setTopic(topicPartition.topic())
                .setTopicId(topicId)
                .setPartitions(List.of(fetchPartition));

        return new FetchRequestData()
            .setTopics(List.of(fetchTopic));
    }

    /**
     * 创建单topic单partition的Fetch响应
     *
     * Leader使用此方法响应Follower的Fetch请求。
     *
     * 【版本兼容性】
     *
     * - apiVersion >= 17：包含NodeEndpoints（Leader地址信息）
     * - apiVersion < 17：不包含NodeEndpoints
     *
     * 【使用场景】
     *
     * <pre>
     * // Leader响应Fetch请求
     * FetchResponseData response = RaftUtil.singletonFetchResponse(
     *     listenerName,
     *     apiVersion,
     *     topicPartition,
     *     topicId,
     *     Errors.NONE,
     *     myNodeId,
     *     myEndpoints,
     *     partition -> {
     *         partition
     *             .setHighWatermark(highWatermark)
     *             .setLogStartOffset(logStartOffset)
     *             .setRecords(fetchedRecords);
     *     }
     * );
     * </pre>
     *
     * @param listenerName 监听器名称
     * @param apiVersion API版本
     * @param topicPartition topic和partition
     * @param topicId topic的UUID
     * @param topLevelError 顶层错误码
     * @param leaderId Leader节点ID
     * @param endpoints Leader的endpoint信息
     * @param partitionConsumer 填充partition数据的Consumer
     * @return 完整的FetchResponseData对象
     */
    public static FetchResponseData singletonFetchResponse(
        ListenerName listenerName,
        short apiVersion,
        TopicPartition topicPartition,
        Uuid topicId,
        Errors topLevelError,
        int leaderId,
        Endpoints endpoints,
        Consumer<FetchResponseData.PartitionData> partitionConsumer
    ) {
        FetchResponseData.PartitionData fetchablePartition =
            new FetchResponseData.PartitionData();

        fetchablePartition.setPartitionIndex(topicPartition.partition());

        partitionConsumer.accept(fetchablePartition);

        FetchResponseData.FetchableTopicResponse fetchableTopic =
            new FetchResponseData.FetchableTopicResponse()
                .setTopic(topicPartition.topic())
                .setTopicId(topicId)
                .setPartitions(List.of(fetchablePartition));

        FetchResponseData response = new FetchResponseData();

        if (apiVersion >= 17) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // 填充NodeEndpoints（API版本17+）
                FetchResponseData.NodeEndpointCollection nodeEndpoints = new FetchResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new FetchResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response
            .setErrorCode(topLevelError.code())
            .setResponses(List.of(fetchableTopic));
    }

    /**
     * 创建单topic单partition的Vote请求
     *
     * Candidate使用此方法向其他节点请求投票。
     *
     * 【Vote请求包含的关键信息】
     *
     * - replicaEpoch：候选人的epoch
     * - replicaKey：候选人的节点Key
     * - voterKey：投票者的节点Key
     * - lastEpoch：候选人最后一条日志的epoch
     * - lastEpochEndOffset：候选人最后一条日志的offset
     * - preVote：是否为pre-vote（预投票）
     *
     * 【Pre-Vote机制】
     *
     * preVote=true时：
     * - 候选人不会递增epoch
     * - 只是询问"如果我发起选举，你会投票给我吗？"
     * - 避免不必要的epoch递增
     *
     * preVote=false时：
     * - 正式的投票请求
     * - 候选人已经递增了epoch
     * - 投票者需要持久化投票决定
     *
     * 【使用场景】
     *
     * <pre>
     * // Candidate向其他节点请求投票
     * VoteRequestData request = RaftUtil.singletonVoteRequest(
     *     topicPartition,
     *     clusterId,
     *     currentEpoch,
     *     myReplicaKey,
     *     voterKey,
     *     lastEpoch,
     *     lastOffset,
     *     false  // 正式投票
     * );
     * channel.send(new RaftRequest.Outbound(correlationId, request, voterId));
     * </pre>
     *
     * @param topicPartition topic和partition
     * @param clusterId 集群ID
     * @param replicaEpoch 候选人的epoch
     * @param replicaKey 候选人的Key
     * @param voterKey 投票者的Key
     * @param lastEpoch 候选人最后日志的epoch
     * @param lastEpochEndOffset 候选人最后日志的offset
     * @param preVote 是否为pre-vote
     * @return 完整的VoteRequestData对象
     */
    public static VoteRequestData singletonVoteRequest(
        TopicPartition topicPartition,
        String clusterId,
        int replicaEpoch,
        ReplicaKey replicaKey,
        ReplicaKey voterKey,
        int lastEpoch,
        long lastEpochEndOffset,
        boolean preVote
    ) {
        return new VoteRequestData()
            .setClusterId(clusterId)
            .setVoterId(voterKey.id())
            .setTopics(
                List.of(
                    new VoteRequestData.TopicData()
                        .setTopicName(topicPartition.topic())
                        .setPartitions(
                            List.of(
                                new VoteRequestData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition())
                                    .setReplicaEpoch(replicaEpoch)
                                    .setReplicaId(replicaKey.id())
                                    .setReplicaDirectoryId(
                                        replicaKey
                                            .directoryId()
                                            .orElse(ReplicaKey.NO_DIRECTORY_ID)
                                    )
                                    .setVoterDirectoryId(
                                        voterKey
                                            .directoryId()
                                            .orElse(ReplicaKey.NO_DIRECTORY_ID)
                                    )
                                    .setLastOffsetEpoch(lastEpoch)
                                    .setLastOffset(lastEpochEndOffset)
                                    .setPreVote(preVote)
                            )
                        )
                )
            );
    }

    /**
     * 创建单topic单partition的Vote响应
     *
     * 投票者使用此方法响应Vote请求。
     *
     * 【Vote响应包含的关键信息】
     *
     * - voteGranted：是否同意投票
     * - leaderEpoch：投票者知道的Leader epoch
     * - leaderId：投票者知道的Leader ID
     *
     * 【版本兼容性】
     *
     * - apiVersion >= 1：包含NodeEndpoints（Leader地址信息）
     * - apiVersion < 1：不包含NodeEndpoints
     *
     * 【使用场景】
     *
     * <pre>
     * // 同意投票
     * VoteResponseData response = RaftUtil.singletonVoteResponse(
     *     listenerName,
     *     apiVersion,
     *     Errors.NONE,
     *     topicPartition,
     *     Errors.NONE,
     *     currentEpoch,
     *     myNodeId,
     *     true,  // voteGranted=true
     *     myEndpoints
     * );
     *
     * // 拒绝投票（已有Leader）
     * VoteResponseData response = RaftUtil.singletonVoteResponse(
     *     listenerName,
     *     apiVersion,
     *     Errors.NONE,
     *     topicPartition,
     *     Errors.NONE,
     *     leaderEpoch,
     *     leaderId,
     *     false,  // voteGranted=false
     *     leaderEndpoints
     * );
     * </pre>
     *
     * @param listenerName 监听器名称
     * @param apiVersion API版本
     * @param topLevelError 顶层错误码
     * @param topicPartition topic和partition
     * @param partitionLevelError 分区级错误码
     * @param leaderEpoch Leader epoch
     * @param leaderId Leader ID
     * @param voteGranted 是否同意投票
     * @param endpoints endpoint信息
     * @return 完整的VoteResponseData对象
     */
    public static VoteResponseData singletonVoteResponse(
        ListenerName listenerName,
        short apiVersion,
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId,
        boolean voteGranted,
        Endpoints endpoints
    ) {
        VoteResponseData.PartitionData partitionData = new VoteResponseData.PartitionData()
            .setErrorCode(partitionLevelError.code())
            .setLeaderId(leaderId)
            .setLeaderEpoch(leaderEpoch)
            .setVoteGranted(voteGranted);

        VoteResponseData response = new VoteResponseData()
            .setErrorCode(topLevelError.code())
            .setTopics(List.of(
                new VoteResponseData.TopicData()
                    .setTopicName(topicPartition.topic())
                    .setPartitions(List.of(partitionData))));

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // 填充NodeEndpoints（API版本1+）
                VoteResponseData.NodeEndpointCollection nodeEndpoints = new VoteResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new VoteResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

    /**
     * 创建单topic单partition的FetchSnapshot请求
     *
     * Follower使用此方法从Leader拉取快照数据。
     *
     * 【快照拉取流程】
     *
     * <pre>
     * 1. Follower发现需要拉取快照（日志太旧，被删除了）
     * 2. Follower发送FetchSnapshotRequest（position=0）
     * 3. Leader返回快照数据的一部分
     * 4. Follower继续请求（position=已接收字节数）
     * 5. 重复步骤3-4直到接收完整个快照
     * 6. Follower加载快照，恢复状态
     * </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * // 开始拉取快照
     * FetchSnapshotRequestData request = RaftUtil.singletonFetchSnapshotRequest(
     *     clusterId,
     *     myReplicaKey,
     *     topicPartition,
     *     currentEpoch,
     *     snapshotId,  // 快照的offset和epoch
     *     maxBytes,    // 单次拉取的最大字节数
     *     0            // position=0，从头开始
     * );
     *
     * // 继续拉取后续数据
     * FetchSnapshotRequestData request = RaftUtil.singletonFetchSnapshotRequest(
     *     clusterId,
     *     myReplicaKey,
     *     topicPartition,
     *     currentEpoch,
     *     snapshotId,
     *     maxBytes,
     *     receivedBytes  // position=已接收字节数
     * );
     * </pre>
     *
     * @param clusterId 集群ID
     * @param replicaKey 请求者的Key
     * @param topicPartition topic和partition
     * @param epoch 当前epoch
     * @param offsetAndEpoch 快照ID（offset和epoch）
     * @param maxBytes 单次拉取的最大字节数
     * @param position 当前拉取位置（字节偏移量）
     * @return 完整的FetchSnapshotRequestData对象
     */
    public static FetchSnapshotRequestData singletonFetchSnapshotRequest(
        String clusterId,
        ReplicaKey replicaKey,
        TopicPartition topicPartition,
        int epoch,
        OffsetAndEpoch offsetAndEpoch,
        int maxBytes,
        long position
    ) {
        FetchSnapshotRequestData.SnapshotId snapshotId = new FetchSnapshotRequestData.SnapshotId()
            .setEndOffset(offsetAndEpoch.offset())
            .setEpoch(offsetAndEpoch.epoch());

        FetchSnapshotRequestData.PartitionSnapshot partitionSnapshot = new FetchSnapshotRequestData.PartitionSnapshot()
            .setPartition(topicPartition.partition())
            .setCurrentLeaderEpoch(epoch)
            .setSnapshotId(snapshotId)
            .setPosition(position)
            .setReplicaDirectoryId(replicaKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));

        return new FetchSnapshotRequestData()
            .setClusterId(clusterId)
            .setReplicaId(replicaKey.id())
            .setMaxBytes(maxBytes)
            .setTopics(
                List.of(
                    new FetchSnapshotRequestData.TopicSnapshot()
                        .setName(topicPartition.topic())
                        .setPartitions(List.of(partitionSnapshot))
                )
            );
    }

    /**
     * 创建单topic单partition的FetchSnapshot响应
     *
     * Leader使用此方法响应FetchSnapshot请求，返回快照数据。
     *
     * 调用operator填充PartitionSnapshot字段时，partitionIndex已经设置好了。
     *
     * 【版本兼容性】
     *
     * - apiVersion >= 1：包含NodeEndpoints
     * - apiVersion < 1：不包含NodeEndpoints
     *
     * 【使用场景】
     *
     * <pre>
     * FetchSnapshotResponseData response = RaftUtil.singletonFetchSnapshotResponse(
     *     listenerName,
     *     apiVersion,
     *     topicPartition,
     *     myNodeId,
     *     myEndpoints,
     *     partition -> partition
     *         .setErrorCode(Errors.NONE.code())
     *         .setSnapshotId(new SnapshotId().setEndOffset(offset).setEpoch(epoch))
     *         .setSize(snapshotSize)
     *         .setPosition(requestPosition)
     *         .setBytes(snapshotChunk)
     * );
     * </pre>
     *
     * @param listenerName 监听器名称
     * @param apiVersion API版本
     * @param topicPartition topic和partition
     * @param leaderId Leader ID
     * @param endpoints endpoint信息
     * @param operator 填充PartitionSnapshot的一元操作符
     * @return 完整的FetchSnapshotResponseData对象
     */
    public static FetchSnapshotResponseData singletonFetchSnapshotResponse(
        ListenerName listenerName,
        short apiVersion,
        TopicPartition topicPartition,
        int leaderId,
        Endpoints endpoints,
        UnaryOperator<FetchSnapshotResponseData.PartitionSnapshot> operator
    ) {
        FetchSnapshotResponseData.PartitionSnapshot partitionSnapshot = operator.apply(
            new FetchSnapshotResponseData.PartitionSnapshot().setIndex(topicPartition.partition())
        );

        FetchSnapshotResponseData response = new FetchSnapshotResponseData()
            .setTopics(
                List.of(
                    new FetchSnapshotResponseData.TopicSnapshot()
                        .setName(topicPartition.topic())
                        .setPartitions(List.of(partitionSnapshot))
                )
            );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // 填充NodeEndpoints（API版本1+）
                FetchSnapshotResponseData.NodeEndpointCollection nodeEndpoints =
                    new FetchSnapshotResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new FetchSnapshotResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

    /**
     * 创建单topic单partition的BeginQuorumEpoch请求
     *
     * 新当选的Leader使用此消息通知所有节点：
     * "我已经成为epoch X的Leader了"
     *
     * 【BeginQuorumEpoch的作用】
     *
     * 1. 通知其他节点新Leader的身份
     * 2. 通知新Leader的地址信息
     * 3. 让其他节点转换为Follower状态
     * 4. 开始接受Leader的日志复制
     *
     * 【使用场景】
     *
     * <pre>
     * // Candidate赢得选举，成为Leader
     * void becomeLeader(int newEpoch) {
     *     // 转换为Leader状态
     *     transitionToLeader(newEpoch);
     *
     *     // 通知所有节点
     *     for (int voterId : voterIds) {
     *         if (voterId != myNodeId) {
     *             BeginQuorumEpochRequestData request = RaftUtil.singletonBeginQuorumEpochRequest(
     *                 topicPartition,
     *                 clusterId,
     *                 newEpoch,
     *                 myNodeId,
     *                 myEndpoints,
     *                 voterKey(voterId)
     *             );
     *             channel.send(new RaftRequest.Outbound(correlationId, request, voterId));
     *         }
     *     }
     * }
     * </pre>
     *
     * @param topicPartition topic和partition
     * @param clusterId 集群ID
     * @param leaderEpoch 新Leader的epoch
     * @param leaderId 新Leader的节点ID
     * @param leaderEndpoints 新Leader的地址信息
     * @param voterKey 接收者的节点Key
     * @return 完整的BeginQuorumEpochRequestData对象
     */
    public static BeginQuorumEpochRequestData singletonBeginQuorumEpochRequest(
        TopicPartition topicPartition,
        String clusterId,
        int leaderEpoch,
        int leaderId,
        Endpoints leaderEndpoints,
        ReplicaKey voterKey
    ) {
        return new BeginQuorumEpochRequestData()
            .setClusterId(clusterId)
            .setVoterId(voterKey.id())
            .setTopics(
                List.of(
                    new BeginQuorumEpochRequestData.TopicData()
                        .setTopicName(topicPartition.topic())
                        .setPartitions(
                            List.of(
                                new BeginQuorumEpochRequestData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition())
                                    .setLeaderEpoch(leaderEpoch)
                                    .setLeaderId(leaderId)
                                    .setVoterDirectoryId(voterKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
                            )
                        )
                )
            )
            .setLeaderEndpoints(leaderEndpoints.toBeginQuorumEpochRequest());
    }

    /**
     * 创建单topic单partition的BeginQuorumEpoch响应
     *
     * 收到BeginQuorumEpoch请求的节点使用此方法响应。
     *
     * 【版本兼容性】
     *
     * - apiVersion >= 1：包含NodeEndpoints
     * - apiVersion < 1：不包含NodeEndpoints
     *
     * 【使用场景】
     *
     * <pre>
     * // 接受新Leader
     * BeginQuorumEpochResponseData response = RaftUtil.singletonBeginQuorumEpochResponse(
     *     listenerName,
     *     apiVersion,
     *     Errors.NONE,
     *     topicPartition,
     *     Errors.NONE,
     *     newLeaderEpoch,
     *     newLeaderId,
     *     leaderEndpoints
     * );
     *
     * // 拒绝（epoch太旧）
     * BeginQuorumEpochResponseData response = RaftUtil.singletonBeginQuorumEpochResponse(
     *     listenerName,
     *     apiVersion,
     *     Errors.NONE,
     *     topicPartition,
     *     Errors.FENCED_LEADER_EPOCH,
     *     currentEpoch,
     *     currentLeaderId,
     *     currentLeaderEndpoints
     * );
     * </pre>
     *
     * @param listenerName 监听器名称
     * @param apiVersion API版本
     * @param topLevelError 顶层错误码
     * @param topicPartition topic和partition
     * @param partitionLevelError 分区级错误码
     * @param leaderEpoch Leader epoch
     * @param leaderId Leader ID
     * @param endpoints endpoint信息
     * @return 完整的BeginQuorumEpochResponseData对象
     */
    public static BeginQuorumEpochResponseData singletonBeginQuorumEpochResponse(
        ListenerName listenerName,
        short apiVersion,
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId,
        Endpoints endpoints
    ) {
        BeginQuorumEpochResponseData response = new BeginQuorumEpochResponseData()
            .setErrorCode(topLevelError.code())
            .setTopics(
                List.of(
                    new BeginQuorumEpochResponseData.TopicData()
                        .setTopicName(topicPartition.topic())
                        .setPartitions(
                            List.of(
                                new BeginQuorumEpochResponseData.PartitionData()
                                    .setErrorCode(partitionLevelError.code())
                                    .setLeaderId(leaderId)
                                    .setLeaderEpoch(leaderEpoch)
                            )
                        )
                )
            );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // 填充NodeEndpoints（API版本1+）
                BeginQuorumEpochResponseData.NodeEndpointCollection nodeEndpoints =
                    new BeginQuorumEpochResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new BeginQuorumEpochResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

    /**
     * 创建单topic单partition的EndQuorumEpoch请求
     *
     * Leader辞职时使用此消息通知其他节点：
     * "我要辞职了，你们重新选举吧，建议优先考虑这些节点"
     *
     * 【EndQuorumEpoch的作用】
     *
     * 1. 通知其他节点Leader要辞职
     * 2. 提供优先候选人列表（preferredCandidates）
     * 3. 加速新Leader选举
     * 4. 减少不必要的epoch递增
     *
     * 【使用场景】
     *
     * <pre>
     * // Leader因为超时未收到多数派响应，主动辞职
     * void resignLeadership() {
     *     // 选择最新的Follower作为优先候选人
     *     List<ReplicaKey> preferredCandidates = selectPreferredCandidates();
     *
     *     // 通知所有节点
     *     for (int voterId : voterIds) {
     *         if (voterId != myNodeId) {
     *             EndQuorumEpochRequestData request = RaftUtil.singletonEndQuorumEpochRequest(
     *                 topicPartition,
     *                 clusterId,
     *                 currentEpoch,
     *                 myNodeId,
     *                 preferredCandidates
     *             );
     *             channel.send(new RaftRequest.Outbound(correlationId, request, voterId));
     *         }
     *     }
     *
     *     // 转换为Resigned状态
     *     transitionToResigned();
     * }
     * </pre>
     *
     * @param topicPartition topic和partition
     * @param clusterId 集群ID
     * @param leaderEpoch 当前Leader的epoch
     * @param leaderId 当前Leader的节点ID
     * @param preferredReplicaKeys 优先候选人列表
     * @return 完整的EndQuorumEpochRequestData对象
     */
    public static EndQuorumEpochRequestData singletonEndQuorumEpochRequest(
        TopicPartition topicPartition,
        String clusterId,
        int leaderEpoch,
        int leaderId,
        List<ReplicaKey> preferredReplicaKeys
    ) {
        List<Integer> preferredSuccessors = preferredReplicaKeys
                .stream()
                .map(ReplicaKey::id)
                .collect(Collectors.toList());

        List<EndQuorumEpochRequestData.ReplicaInfo> preferredCandidates = preferredReplicaKeys
                .stream()
                .map(replicaKey -> new EndQuorumEpochRequestData.ReplicaInfo()
                    .setCandidateId(replicaKey.id())
                    .setCandidateDirectoryId(replicaKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
                )
                .collect(Collectors.toList());

        return new EndQuorumEpochRequestData()
            .setClusterId(clusterId)
            .setTopics(
                List.of(
                    new EndQuorumEpochRequestData.TopicData()
                        .setTopicName(topicPartition.topic())
                        .setPartitions(
                            List.of(
                                new EndQuorumEpochRequestData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition())
                                    .setLeaderEpoch(leaderEpoch)
                                    .setLeaderId(leaderId)
                                    .setPreferredSuccessors(preferredSuccessors)
                                    .setPreferredCandidates(preferredCandidates)
                            )
                        )
                )
            );

    }

    /**
     * 创建单topic单partition的EndQuorumEpoch响应
     *
     * 收到EndQuorumEpoch请求的节点使用此方法响应。
     *
     * 【版本兼容性】
     *
     * - apiVersion >= 1：包含NodeEndpoints
     * - apiVersion < 1：不包含NodeEndpoints
     *
     * 【使用场景】
     *
     * <pre>
     * // 确认收到辞职通知
     * EndQuorumEpochResponseData response = RaftUtil.singletonEndQuorumEpochResponse(
     *     listenerName,
     *     apiVersion,
     *     Errors.NONE,
     *     topicPartition,
     *     Errors.NONE,
     *     resignedEpoch,
     *     resignedLeaderId,
     *     endpoints
     * );
     * </pre>
     *
     * @param listenerName 监听器名称
     * @param apiVersion API版本
     * @param topLevelError 顶层错误码
     * @param topicPartition topic和partition
     * @param partitionLevelError 分区级错误码
     * @param leaderEpoch Leader epoch
     * @param leaderId Leader ID
     * @param endpoints endpoint信息
     * @return 完整的EndQuorumEpochResponseData对象
     */
    public static EndQuorumEpochResponseData singletonEndQuorumEpochResponse(
        ListenerName listenerName,
        short apiVersion,
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId,
        Endpoints endpoints
    ) {
        EndQuorumEpochResponseData response = new EndQuorumEpochResponseData()
                   .setErrorCode(topLevelError.code())
                   .setTopics(List.of(
                       new EndQuorumEpochResponseData.TopicData()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(List.of(
                               new EndQuorumEpochResponseData.PartitionData()
                                   .setErrorCode(partitionLevelError.code())
                                   .setLeaderId(leaderId)
                                   .setLeaderEpoch(leaderEpoch)
                           )))
                   );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // 填充NodeEndpoints（API版本1+）
                EndQuorumEpochResponseData.NodeEndpointCollection nodeEndpoints =
                    new EndQuorumEpochResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new EndQuorumEpochResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }


    /**
     * 创建单topic单partition的DescribeQuorum请求
     *
     * 管理员使用此方法查询Quorum的状态。
     *
     * 【使用场景】
     *
     * <pre>
     * // kafka-metadata-quorum.sh --describe
     * DescribeQuorumRequestData request = RaftUtil.singletonDescribeQuorumRequest(
     *     topicPartition
     * );
     * channel.send(new RaftRequest.Outbound(correlationId, request));
     * </pre>
     *
     * @param topicPartition topic和partition
     * @return 完整的DescribeQuorumRequestData对象
     */
    public static DescribeQuorumRequestData singletonDescribeQuorumRequest(
        TopicPartition topicPartition
    ) {

        return new DescribeQuorumRequestData()
            .setTopics(
                List.of(
                    new DescribeQuorumRequestData.TopicData()
                        .setTopicName(topicPartition.topic())
                        .setPartitions(
                            List.of(
                                new DescribeQuorumRequestData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition())
                            )
                        )
                )
            );
    }

    /**
     * 创建单topic单partition的DescribeQuorum响应
     *
     * Leader使用此方法响应DescribeQuorum请求，返回Quorum的状态信息。
     *
     * 【返回的信息】
     *
     * - leaderId：当前Leader
     * - leaderEpoch：当前epoch
     * - highWatermark：高水位
     * - currentVoters：当前投票者列表及其状态
     * - observers：观察者列表及其状态
     *
     * 【版本兼容性】
     *
     * - apiVersion >= 2：包含节点的详细地址信息
     * - apiVersion < 2：不包含详细地址信息
     *
     * 【使用场景】
     *
     * <pre>
     * DescribeQuorumResponseData response = RaftUtil.singletonDescribeQuorumResponse(
     *     apiVersion,
     *     topicPartition,
     *     myNodeId,
     *     currentEpoch,
     *     highWatermark,
     *     voterStates,
     *     observerStates,
     *     System.currentTimeMillis()
     * );
     * </pre>
     *
     * @param apiVersion API版本
     * @param topicPartition topic和partition
     * @param leaderId Leader ID
     * @param leaderEpoch Leader epoch
     * @param highWatermark 高水位
     * @param voters 投票者状态列表
     * @param observers 观察者状态列表
     * @param currentTimeMs 当前时间戳
     * @return 完整的DescribeQuorumResponseData对象
     */
    public static DescribeQuorumResponseData singletonDescribeQuorumResponse(
        short apiVersion,
        TopicPartition topicPartition,
        int leaderId,
        int leaderEpoch,
        long highWatermark,
        Collection<LeaderState.ReplicaState> voters,
        Collection<LeaderState.ReplicaState> observers,
        long currentTimeMs
    ) {
        DescribeQuorumResponseData response = new DescribeQuorumResponseData()
            .setTopics(
                List.of(
                    new DescribeQuorumResponseData.TopicData()
                        .setTopicName(topicPartition.topic())
                        .setPartitions(
                            List.of(
                                new DescribeQuorumResponseData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition())
                                    .setErrorCode(Errors.NONE.code())
                                    .setLeaderId(leaderId)
                                    .setLeaderEpoch(leaderEpoch)
                                    .setHighWatermark(highWatermark)
                                    .setCurrentVoters(toReplicaStates(apiVersion, leaderId, voters, currentTimeMs))
                                    .setObservers(toReplicaStates(apiVersion, leaderId, observers, currentTimeMs))))));
        if (apiVersion >= 2) {
            DescribeQuorumResponseData.NodeCollection nodes = new DescribeQuorumResponseData.NodeCollection(voters.size());
            for (LeaderState.ReplicaState voter : voters) {
                nodes.add(
                    new DescribeQuorumResponseData.Node()
                        .setNodeId(voter.replicaKey().id())
                        .setListeners(voter.listeners().toDescribeQuorumResponseListeners())
                );
            }
            response.setNodes(nodes);
        }
        return response;
    }

    /**
     * 创建AddRaftVoter请求
     *
     * 管理员使用此方法向Quorum添加新的投票者。
     *
     * 【使用场景】
     *
     * <pre>
     * // kafka-metadata-quorum.sh --add-voter 4
     * AddRaftVoterRequestData request = RaftUtil.addVoterRequest(
     *     clusterId,
     *     10000,  // 超时10秒
     *     ReplicaKey.of(4, directoryId),
     *     newVoterEndpoints,
     *     true    // 等待committed后才响应
     * );
     * </pre>
     *
     * @param clusterId 集群ID
     * @param timeoutMs 超时时间
     * @param voter 要添加的投票者Key
     * @param listeners 投票者的地址信息
     * @param ackWhenCommitted 是否等待committed后才响应
     * @return 完整的AddRaftVoterRequestData对象
     */
    public static AddRaftVoterRequestData addVoterRequest(
        String clusterId,
        int timeoutMs,
        ReplicaKey voter,
        Endpoints listeners,
        boolean ackWhenCommitted
    ) {
        return new AddRaftVoterRequestData()
            .setClusterId(clusterId)
            .setTimeoutMs(timeoutMs)
            .setVoterId(voter.id())
            .setVoterDirectoryId(voter.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
            .setListeners(listeners.toAddVoterRequest())
            .setAckWhenCommitted(ackWhenCommitted);
    }

    /**
     * 创建AddRaftVoter响应
     *
     * Leader使用此方法响应AddRaftVoter请求。
     *
     * 【使用场景】
     *
     * <pre>
     * // 成功添加
     * AddRaftVoterResponseData response = RaftUtil.addVoterResponse(
     *     Errors.NONE,
     *     null
     * );
     *
     * // 失败（权限不足）
     * AddRaftVoterResponseData response = RaftUtil.addVoterResponse(
     *     Errors.NOT_CONTROLLER,
     *     "Only leader can add voters"
     * );
     * </pre>
     *
     * @param error 错误码
     * @param errorMessage 错误消息
     * @return 完整的AddRaftVoterResponseData对象
     */
    public static AddRaftVoterResponseData addVoterResponse(
        Errors error,
        String errorMessage
    ) {
        // 如果errorMessage不存在且error不是NONE，使用error的默认消息
        if (errorMessage == null && error != Errors.NONE) {
            errorMessage = error.message();
        }
        return new AddRaftVoterResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }

    /**
     * 创建RemoveRaftVoter请求
     *
     * 管理员使用此方法从Quorum移除投票者。
     *
     * 【使用场景】
     *
     * <pre>
     * // kafka-metadata-quorum.sh --remove-voter 3
     * RemoveRaftVoterRequestData request = RaftUtil.removeVoterRequest(
     *     clusterId,
     *     ReplicaKey.of(3, directoryId)
     * );
     * </pre>
     *
     * @param clusterId 集群ID
     * @param voter 要移除的投票者Key
     * @return 完整的RemoveRaftVoterRequestData对象
     */
    public static RemoveRaftVoterRequestData removeVoterRequest(
        String clusterId,
        ReplicaKey voter
    ) {
        return new RemoveRaftVoterRequestData()
            .setClusterId(clusterId)
            .setVoterId(voter.id())
            .setVoterDirectoryId(voter.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));
    }

    /**
     * 创建RemoveRaftVoter响应
     *
     * Leader使用此方法响应RemoveRaftVoter请求。
     *
     * @param error 错误码
     * @param errorMessage 错误消息
     * @return 完整的RemoveRaftVoterResponseData对象
     */
    public static RemoveRaftVoterResponseData removeVoterResponse(
        Errors error,
        String errorMessage
    ) {
        // 如果errorMessage不存在且error不是NONE，使用error的默认消息
        if (errorMessage == null && error != Errors.NONE) {
            errorMessage = error.message();
        }
        return new RemoveRaftVoterResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }

    /**
     * 创建UpdateRaftVoter请求
     *
     * 投票者使用此方法向Leader更新自己的地址信息或版本信息。
     *
     * 【使用场景】
     *
     * <pre>
     * // 节点地址变更，通知Leader
     * UpdateRaftVoterRequestData request = RaftUtil.updateVoterRequest(
     *     clusterId,
     *     myReplicaKey,
     *     currentEpoch,
     *     supportedVersions,
     *     newEndpoints
     * );
     * </pre>
     *
     * @param clusterId 集群ID
     * @param voter 投票者Key
     * @param epoch 当前epoch
     * @param supportedVersions 支持的KRaft版本范围
     * @param endpoints 新的地址信息
     * @return 完整的UpdateRaftVoterRequestData对象
     */
    public static UpdateRaftVoterRequestData updateVoterRequest(
        String clusterId,
        ReplicaKey voter,
        int epoch,
        SupportedVersionRange supportedVersions,
        Endpoints endpoints
    ) {
        UpdateRaftVoterRequestData request = new UpdateRaftVoterRequestData()
            .setClusterId(clusterId)
            .setCurrentLeaderEpoch(epoch)
            .setVoterId(voter.id())
            .setVoterDirectoryId(voter.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID))
            .setListeners(endpoints.toUpdateVoterRequest());

        request.kRaftVersionFeature()
            .setMinSupportedVersion(supportedVersions.min())
            .setMaxSupportedVersion(supportedVersions.max());

        return request;
    }

    /**
     * 创建UpdateRaftVoter响应
     *
     * Leader使用此方法响应UpdateRaftVoter请求。
     *
     * 【使用场景】
     *
     * <pre>
     * // 成功更新
     * UpdateRaftVoterResponseData response = RaftUtil.updateVoterResponse(
     *     Errors.NONE,
     *     listenerName,
     *     new LeaderAndEpoch(myNodeId, currentEpoch),
     *     myEndpoints
     * );
     * </pre>
     *
     * @param error 错误码
     * @param listenerName 监听器名称
     * @param leaderAndEpoch Leader信息
     * @param endpoints endpoint信息
     * @return 完整的UpdateRaftVoterResponseData对象
     */
    public static UpdateRaftVoterResponseData updateVoterResponse(
        Errors error,
        ListenerName listenerName,
        LeaderAndEpoch leaderAndEpoch,
        Endpoints endpoints
    ) {
        UpdateRaftVoterResponseData response = new UpdateRaftVoterResponseData()
            .setErrorCode(error.code());

        response.currentLeader()
            .setLeaderId(leaderAndEpoch.leaderId().orElse(-1))
            .setLeaderEpoch(leaderAndEpoch.epoch());

        Optional<InetSocketAddress> address = endpoints.address(listenerName);
        address.ifPresent(inetSocketAddress -> response.currentLeader()
            .setHost(inetSocketAddress.getHostString())
            .setPort(inetSocketAddress.getPort()));

        return response;
    }

    /**
     * 将ReplicaState集合转换为DescribeQuorum响应格式
     *
     * 内部辅助方法，用于DescribeQuorum响应。
     *
     * @param apiVersion API版本
     * @param leaderId Leader ID
     * @param states ReplicaState集合
     * @param currentTimeMs 当前时间戳
     * @return DescribeQuorum响应的ReplicaState列表
     */
    private static List<DescribeQuorumResponseData.ReplicaState> toReplicaStates(
        short apiVersion,
        int leaderId,
        Collection<LeaderState.ReplicaState> states,
        long currentTimeMs
    ) {
        return states
            .stream()
            .map(replicaState -> toReplicaState(apiVersion, leaderId, replicaState, currentTimeMs))
            .collect(Collectors.toList());
    }

    /**
     * 将单个ReplicaState转换为DescribeQuorum响应格式
     *
     * 内部辅助方法，用于DescribeQuorum响应。
     *
     * 【特殊处理】
     *
     * 对于Leader自己：
     * - lastCaughtUpTimestamp = currentTimeMs（Leader总是最新的）
     * - lastFetchTimestamp = currentTimeMs（Leader不需要fetch）
     *
     * 对于Follower：
     * - 使用实际的lastCaughtUpTimestamp
     * - 使用实际的lastFetchTimestamp
     *
     * @param apiVersion API版本
     * @param leaderId Leader ID
     * @param replicaState ReplicaState对象
     * @param currentTimeMs 当前时间戳
     * @return DescribeQuorum响应的ReplicaState对象
     */
    private static DescribeQuorumResponseData.ReplicaState toReplicaState(
        short apiVersion,
        int leaderId,
        LeaderState.ReplicaState replicaState,
        long currentTimeMs
    ) {
        final long lastCaughtUpTimestamp;
        final long lastFetchTimestamp;
        if (replicaState.replicaKey().id() == leaderId) {
            lastCaughtUpTimestamp = currentTimeMs;
            lastFetchTimestamp = currentTimeMs;
        } else {
            lastCaughtUpTimestamp = replicaState.lastCaughtUpTimestamp();
            lastFetchTimestamp = replicaState.lastFetchTimestamp();
        }
        DescribeQuorumResponseData.ReplicaState replicaStateData = new DescribeQuorumResponseData.ReplicaState()
            .setReplicaId(replicaState.replicaKey().id())
            .setLogEndOffset(replicaState.endOffset().map(LogOffsetMetadata::offset).orElse(-1L))
            .setLastCaughtUpTimestamp(lastCaughtUpTimestamp)
            .setLastFetchTimestamp(lastFetchTimestamp);

        if (apiVersion >= 2) {
            replicaStateData.setReplicaDirectoryId(replicaState.replicaKey().directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));
        }
        return replicaStateData;
    }

    /**
     * 从VoteRequest提取投票者Key
     *
     * 【使用场景】
     *
     * <pre>
     * // 处理VoteRequest
     * Optional<ReplicaKey> voterKey = RaftUtil.voteRequestVoterKey(request, partition);
     * if (voterKey.isPresent()) {
     *     // 有效的投票者Key
     *     handleVoteFromVoter(voterKey.get());
     * }
     * </pre>
     *
     * @param request VoteRequest
     * @param partition PartitionData
     * @return 投票者Key（如果voterId < 0则返回empty）
     */
    public static Optional<ReplicaKey> voteRequestVoterKey(
        VoteRequestData request,
        VoteRequestData.PartitionData partition
    ) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), partition.voterDirectoryId()));
        }
    }

    /**
     * 从BeginQuorumEpochRequest提取投票者Key
     *
     * @param request BeginQuorumEpochRequest
     * @param partition PartitionData
     * @return 投票者Key（如果voterId < 0则返回empty）
     */
    public static Optional<ReplicaKey> beginQuorumEpochRequestVoterKey(
        BeginQuorumEpochRequestData request,
        BeginQuorumEpochRequestData.PartitionData partition
    ) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), partition.voterDirectoryId()));
        }
    }

    /**
     * 从AddRaftVoterRequest提取投票者Key
     *
     * @param request AddRaftVoterRequest
     * @return 投票者Key（如果voterId < 0则返回empty）
     */
    public static Optional<ReplicaKey> addVoterRequestVoterKey(AddRaftVoterRequestData request) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), request.voterDirectoryId()));
        }
    }

    /**
     * 从RemoveRaftVoterRequest提取投票者Key
     *
     * @param request RemoveRaftVoterRequest
     * @return 投票者Key（如果voterId < 0则返回empty）
     */
    public static Optional<ReplicaKey> removeVoterRequestVoterKey(RemoveRaftVoterRequestData request) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), request.voterDirectoryId()));
        }
    }

    /**
     * 从UpdateRaftVoterRequest提取投票者Key
     *
     * @param request UpdateRaftVoterRequest
     * @return 投票者Key（如果voterId < 0则返回empty）
     */
    public static Optional<ReplicaKey> updateVoterRequestVoterKey(UpdateRaftVoterRequestData request) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), request.voterDirectoryId()));
        }
    }

    // ==================== 消息验证方法 ====================
    // 以下方法用于验证消息是否包含正确的topic/partition信息

    static boolean hasValidTopicPartition(FetchRequestData data, TopicPartition topicPartition, Uuid topicId) {
        return data.topics().size() == 1 &&
            data.topics().get(0).topicId().equals(topicId) &&
            data.topics().get(0).partitions().size() == 1 &&
            data.topics().get(0).partitions().get(0).partition() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(FetchResponseData data, TopicPartition topicPartition, Uuid topicId) {
        return data.responses().size() == 1 &&
            data.responses().get(0).topicId().equals(topicId) &&
            data.responses().get(0).partitions().size() == 1 &&
            data.responses().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(VoteResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(VoteRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(DescribeQuorumRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }
}
