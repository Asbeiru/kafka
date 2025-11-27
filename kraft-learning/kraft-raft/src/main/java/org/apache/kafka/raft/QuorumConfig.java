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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;

/**
 * QuorumConfig - 封装KRaft集群元数据副本的专用配置
 *
 * 【为什么需要QuorumConfig？】
 *
 * KRaft使用Raft协议管理集群元数据，需要专门的配置：
 *
 * 1. **集群成员配置**
 *    - controller.quorum.voters：投票者列表（静态配置）
 *    - controller.quorum.bootstrap.servers：引导服务器（动态成员）
 *
 * 2. **超时配置**
 *    - controller.quorum.election.timeout.ms：选举超时
 *    - controller.quorum.fetch.timeout.ms：拉取超时
 *    - controller.quorum.request.timeout.ms：请求超时
 *
 * 3. **性能配置**
 *    - controller.quorum.append.linger.ms：批量追加延迟
 *    - controller.quorum.retry.backoff.ms：重试退避时间
 *
 * 4. **高级配置**
 *    - controller.quorum.election.backoff.max.ms：选举退避最大值
 *    - controller.quorum.auto.join.enable：自动加入集群
 *
 * 【超时设计哲学】
 *
 * KRaft的超时时间相对较短（相比传统Kafka配置），设计理念：
 *
 * "Leader切换应该是快速操作"
 *
 * 为什么？
 * - KIP-631 controller可以从standby快速切换到active
 * - Standby是"热备"而非"冷备"
 * - 无需重新加载所有元数据
 * - 切换延迟低（几秒内完成）
 *
 * 对比：
 * <pre>
 * 传统ZooKeeper模式：
 * - request.timeout.ms = 30000 (30秒)
 * - session.timeout.ms = 18000 (18秒)
 * - 切换需要重新加载元数据（可能几十秒）
 *
 * KRaft模式：
 * - controller.quorum.election.timeout.ms = 1000 (1秒)
 * - controller.quorum.fetch.timeout.ms = 2000 (2秒)
 * - 切换很快（standby已经有最新元数据）
 * </pre>
 *
 * 【配置示例】
 *
 * 静态成员配置（旧方式，不推荐）：
 * <pre>
 * # server.properties
 * controller.quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
 * </pre>
 *
 * 动态成员配置（新方式，推荐）：
 * <pre>
 * # server.properties
 * controller.quorum.bootstrap.servers=localhost:9092,localhost:9093,localhost:9094
 *
 * # 格式化时指定初始控制器
 * kafka-storage.sh format --initial-controllers 1,2,3
 * # 或者使用standalone模式
 * kafka-storage.sh format --standalone
 * </pre>
 *
 * 【静态 vs 动态成员】
 *
 * 静态成员（controller.quorum.voters）：
 * - 在配置文件中硬编码成员列表
 * - 添加/删除成员需要修改配置并重启
 * - 适合固定规模的集群
 * - 旧方式，不建议新部署使用
 *
 * 动态成员（controller.quorum.bootstrap.servers）：
 * - 通过Raft协议动态添加/删除成员
 * - 无需修改配置或重启
 * - 适合云环境和自动伸缩
 * - 新方式，推荐使用
 *
 * 【重要说明】
 *
 * 不能同时设置两种配置：
 * <pre>
 * # 错误：不能同时使用
 * controller.quorum.voters=1@localhost:9092,2@localhost:9093
 * controller.quorum.bootstrap.servers=localhost:9092,localhost:9093
 *
 * # 正确：只使用一种
 * controller.quorum.bootstrap.servers=localhost:9092,localhost:9093
 * </pre>
 *
 * @see QuorumState 使用这些配置的状态管理器
 * @see KafkaRaftClient 使用这些配置的Raft客户端
 */
public class QuorumConfig {

    /**
     * 配置前缀
     * 所有Quorum相关配置都以此开头
     */
    private static final String QUORUM_PREFIX = "controller.quorum.";

    /**
     * 非可路由地址
     * 0.0.0.0表示无法解析到任何特定节点的端点
     *
     * 【用途】
     *
     * 在某些配置场景下，节点可能配置为0.0.0.0：
     * - 表示监听所有网络接口
     * - 但不能用作其他节点连接的目标地址
     *
     * 【验证】
     *
     * parseVoterConnections会检查并拒绝0.0.0.0：
     * <pre>
     * if (address.getHostString().equals(NON_ROUTABLE_HOST)) {
     *     throw new ConfigException("Host string (0.0.0.0) is not routeable");
     * }
     * </pre>
     */
    public static final String NON_ROUTABLE_HOST = "0.0.0.0";

    // ==================== Voters 配置 ====================

    /**
     * 投票者配置键
     *
     * 【格式】
     *
     * {id}@{host}:{port}的逗号分隔列表
     *
     * 【示例】
     *
     * <pre>
     * controller.quorum.voters=1@localhost:9092,2@localhost:9093,3@localhost:9094
     * </pre>
     *
     * 解析后：
     * - 节点1：localhost:9092
     * - 节点2：localhost:9093
     * - 节点3：localhost:9094
     *
     * 【旧方式说明】
     *
     * 这是定义quorum成员的旧方式，不应该与动态quorum一起使用。
     * 如果使用动态quorum，应该设置controller.quorum.bootstrap.servers，
     * 并在格式化时使用--standalone或--initial-controllers标志确定投票者集合。
     */
    public static final String QUORUM_VOTERS_CONFIG = QUORUM_PREFIX + "voters";
    public static final String QUORUM_VOTERS_DOC = "Map of id/endpoint information for " +
        "the set of voters in a comma-separated list of <code>{id}@{host}:{port}</code> entries. " +
        "This is the old way of defining membership for controller quorums and should NOT be " +
        "set if using dynamic quorums. Instead, controller.quorum.bootstrap.servers should be set," +
        "and the voter set is determined by the --standalone or --initial-controllers flags when formatting." +
        "For example: <code>1@localhost:9092,2@localhost:9093,3@localhost:9094</code>";
    public static final List<String> DEFAULT_QUORUM_VOTERS = List.of();

    // ==================== Bootstrap Servers 配置 ====================

    /**
     * 引导服务器配置键
     *
     * 【格式】
     *
     * {host}:{port}的逗号分隔列表（注意：没有节点ID）
     *
     * 【示例】
     *
     * <pre>
     * controller.quorum.bootstrap.servers=localhost:9092,localhost:9093,localhost:9094
     * </pre>
     *
     * 【用途】
     *
     * 用于引导集群元数据：
     * 1. 节点启动时连接这些地址
     * 2. 发现当前的投票者集合
     * 3. 加入集群（如果配置允许）
     *
     * 【与voters的区别】
     *
     * <pre>
     * controller.quorum.voters：
     * - 格式：1@localhost:9092,2@localhost:9093
     * - 包含节点ID
     * - 静态成员列表
     *
     * controller.quorum.bootstrap.servers：
     * - 格式：localhost:9092,localhost:9093
     * - 不包含节点ID
     * - 只用于引导连接
     * </pre>
     */
    public static final String QUORUM_BOOTSTRAP_SERVERS_CONFIG = QUORUM_PREFIX + "bootstrap.servers";
    public static final String QUORUM_BOOTSTRAP_SERVERS_DOC = "List of endpoints to use for " +
        "bootstrapping the cluster metadata. The endpoints are specified in comma-separated list " +
        "of <code>{host}:{port}</code> entries. For example: " +
        "<code>localhost:9092,localhost:9093,localhost:9094</code>.";
    public static final List<String> DEFAULT_QUORUM_BOOTSTRAP_SERVERS = List.of();

    // ==================== 超时配置 ====================

    /**
     * 选举超时配置键
     *
     * 【含义】
     *
     * Follower在此时间内无法从Leader fetch数据，就会触发新的选举。
     *
     * 【为什么需要选举超时？】
     *
     * <pre>
     * 场景：Leader崩溃或网络分区
     * 1. T0: Follower从Leader成功fetch
     * 2. T0+500ms: Leader崩溃
     * 3. T0+1000ms: 选举超时触发
     * 4. T0+1000ms: Follower转换为Candidate，发起选举
     * 5. T0+1100ms: 新Leader选出
     * </pre>
     *
     * 【超时时间的权衡】
     *
     * 太短（如100ms）：
     * - 优点：故障检测快
     * - 缺点：容易误触发（网络抖动就触发选举）
     *
     * 太长（如10秒）：
     * - 优点：不会误触发
     * - 缺点：故障恢复慢（10秒后才检测到Leader失败）
     *
     * 默认1秒：
     * - 在检测速度和稳定性之间平衡
     * - 适合大多数部署环境
     *
     * 【与fetch超时的关系】
     *
     * <pre>
     * controller.quorum.election.timeout.ms = 1000
     * controller.quorum.fetch.timeout.ms = 2000
     *
     * fetch超时 > 选举超时：
     * - Follower在1秒无fetch后触发选举
     * - Leader在2秒无fetch后辞职
     * - 确保Follower先发起选举，再由Leader辞职
     * </pre>
     */
    public static final String QUORUM_ELECTION_TIMEOUT_MS_CONFIG = QUORUM_PREFIX + "election.timeout.ms";
    public static final String QUORUM_ELECTION_TIMEOUT_MS_DOC = "Maximum time in milliseconds to wait " +
        "without being able to fetch from the leader before triggering a new election";
    public static final int DEFAULT_QUORUM_ELECTION_TIMEOUT_MS = 1_000;

    /**
     * Fetch超时配置键
     *
     * 【两种角色的不同含义】
     *
     * 对于Follower：
     * - 在此时间内无法成功fetch，转换为Candidate并发起选举
     *
     * 对于Leader：
     * - 在此时间内未收到多数派的有效fetch或fetchSnapshot请求，辞职
     *
     * 【Leader为什么要辞职？】
     *
     * <pre>
     * 场景：Leader被网络分区隔离
     * 1. Leader和多数派分区
     * 2. Leader在2秒内没收到多数派的fetch请求
     * 3. Leader意识到自己可能已经不是Leader了
     * 4. Leader主动辞职（转换为Follower或Unattached）
     * 5. 另一边的多数派选出新Leader
     * </pre>
     *
     * 好处：
     * - 避免脑裂
     * - 快速发现网络分区
     * - 减少不必要的日志写入
     *
     * 【默认值2秒】
     *
     * 为什么是2秒？
     * - 2倍的election.timeout.ms
     * - 给Follower足够时间发起选举
     * - Leader稍晚才辞职，避免过早辞职
     */
    public static final String QUORUM_FETCH_TIMEOUT_MS_CONFIG = QUORUM_PREFIX + "fetch.timeout.ms";
    public static final String QUORUM_FETCH_TIMEOUT_MS_DOC = "Maximum time without a successful fetch from " +
        "the current leader before becoming a candidate and triggering an election for voters; Maximum time " +
        "a leader can go without receiving valid fetch or fetchSnapshot request from a majority of the quorum before resigning.";
    public static final int DEFAULT_QUORUM_FETCH_TIMEOUT_MS = 2_000;

    /**
     * 选举退避最大值配置键
     *
     * 【为什么需要退避机制？】
     *
     * <pre>
     * 场景：没有退避机制
     * 1. 3个节点同时发起选举（epoch=5）
     * 2. 都无法获得多数票
     * 3. 同时进入epoch=6，再次发起选举
     * 4. 仍然无法获得多数票
     * 5. 无限循环（死锁）
     * </pre>
     *
     * 有了二进制指数退避：
     * <pre>
     * 1. 节点1等待 rand(0-1000)ms = 300ms 后发起选举
     * 2. 节点2等待 rand(0-1000)ms = 700ms 后发起选举
     * 3. 节点3等待 rand(0-1000)ms = 500ms 后发起选举
     * 4. 节点1先发起，很可能赢得选举（节点2和3还在等待）
     * </pre>
     *
     * 【二进制指数退避算法】
     *
     * <pre>
     * 第1次失败：等待 rand(0, min(1000, 2^1)) = rand(0, 2)ms
     * 第2次失败：等待 rand(0, min(1000, 2^2)) = rand(0, 4)ms
     * 第3次失败：等待 rand(0, min(1000, 2^3)) = rand(0, 8)ms
     * ...
     * 第10次失败：等待 rand(0, min(1000, 2^10)) = rand(0, 1000)ms
     * 第11次失败：等待 rand(0, min(1000, 2^11)) = rand(0, 1000)ms（达到上限）
     * </pre>
     *
     * 【默认值1000ms】
     *
     * 与election.timeout.ms相同：
     * - 最大退避时间 = 一个选举周期
     * - 在最坏情况下，退避时间不会太长
     */
    public static final String QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG = QUORUM_PREFIX + "election.backoff.max.ms";
    public static final String QUORUM_ELECTION_BACKOFF_MAX_MS_DOC = "Maximum time in milliseconds before starting new elections. " +
        "This is used in the binary exponential backoff mechanism that helps prevent gridlocked elections";
    public static final int DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS = 1_000;

    /**
     * 追加延迟配置键（Linger时间）
     *
     * 【为什么需要linger？】
     *
     * <pre>
     * 没有linger（立即刷新）：
     * 1. 收到记录1，立即写盘
     * 2. 收到记录2，立即写盘
     * 3. 收到记录3，立即写盘
     * 性能：每个记录一次磁盘I/O，吞吐量低
     *
     * 有linger（延迟25ms）：
     * 1. 收到记录1，等待
     * 2. 在25ms内收到记录2、3、...、100
     * 3. 一次性写入100条记录
     * 性能：100条记录一次磁盘I/O，吞吐量高
     * </pre>
     *
     * 【权衡】
     *
     * Linger太短（如0ms）：
     * - 优点：延迟低（立即写入）
     * - 缺点：吞吐量低（频繁I/O）
     *
     * Linger太长（如1000ms）：
     * - 优点：吞吐量高（批量写入）
     * - 缺点：延迟高（写入延迟1秒）
     *
     * 默认25ms：
     * - 在延迟和吞吐量之间平衡
     * - 25ms的额外延迟对大多数应用可接受
     * - 可以积累足够的记录进行批量写入
     *
     * 【使用场景】
     *
     * Leader收到append请求：
     * <pre>
     * 1. 将记录添加到accumulator
     * 2. 如果accumulator未满，等待linger.ms
     * 3. linger.ms到期或accumulator满
     * 4. 刷新到磁盘并复制到Follower
     * </pre>
     */
    public static final String QUORUM_LINGER_MS_CONFIG = QUORUM_PREFIX + "append.linger.ms";
    public static final String QUORUM_LINGER_MS_DOC = "The duration in milliseconds that the leader will " +
        "wait for writes to accumulate before flushing them to disk.";
    public static final int DEFAULT_QUORUM_LINGER_MS = 25;

    /**
     * 请求超时配置键
     *
     * 【用途】
     *
     * 发送Raft请求（Vote, Fetch, BeginQuorumEpoch等）的超时时间。
     *
     * 【示例】
     *
     * <pre>
     * // 发送VoteRequest
     * 1. T0: 发送请求
     * 2. T0+2000ms: 超时（未收到响应）
     * 3. 记录超时事件
     * 4. 重试或放弃
     * </pre>
     *
     * 【默认值2000ms】
     *
     * 与fetch.timeout.ms相同：
     * - 足够长以应对网络延迟
     * - 足够短以快速检测故障
     */
    public static final String QUORUM_REQUEST_TIMEOUT_MS_CONFIG = QUORUM_PREFIX +
        CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final String QUORUM_REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
    public static final int DEFAULT_QUORUM_REQUEST_TIMEOUT_MS = 2_000;

    /**
     * 重试退避配置键
     *
     * 【用途】
     *
     * 请求失败后，重试前的等待时间。
     *
     * 【示例】
     *
     * <pre>
     * 1. 发送FetchRequest，失败（网络错误）
     * 2. 等待20ms
     * 3. 重试FetchRequest
     * </pre>
     *
     * 【为什么需要退避？】
     *
     * 立即重试的问题：
     * - 如果是临时故障（网络抖动），立即重试可能仍然失败
     * - 频繁重试会增加网络和CPU负载
     *
     * 有退避的好处：
     * - 给系统恢复的时间
     * - 减少不必要的重试
     * - 降低资源消耗
     *
     * 【默认值20ms】
     *
     * 相对较短：
     * - KRaft期望快速恢复
     * - 20ms对大多数网络延迟足够
     */
    public static final String QUORUM_RETRY_BACKOFF_MS_CONFIG = QUORUM_PREFIX +
        CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
    public static final String QUORUM_RETRY_BACKOFF_MS_DOC = CommonClientConfigs.RETRY_BACKOFF_MS_DOC;
    public static final int DEFAULT_QUORUM_RETRY_BACKOFF_MS = 20;

    /**
     * 自动加入配置键
     *
     * 【用途】
     *
     * 控制KRaft controller是否自动加入其集群ID的集群元数据分区。
     *
     * 【使用场景】
     *
     * 启用自动加入（auto.join.enable=true）：
     * <pre>
     * 1. 节点启动
     * 2. 检测到cluster.id匹配
     * 3. 自动加入集群作为observer
     * 4. 逐渐同步元数据
     * 5. 可能提升为voter（如果需要）
     * </pre>
     *
     * 禁用自动加入（auto.join.enable=false，默认）：
     * <pre>
     * 1. 节点启动
     * 2. 必须手动添加到集群
     * 3. 管理员执行：kafka-metadata-quorum.sh add-voter ...
     * 4. 节点才能加入集群
     * </pre>
     *
     * 【为什么默认是false？】
     *
     * 安全性考虑：
     * - 防止错误配置的节点自动加入
     * - 管理员可以显式控制集群成员
     * - 避免意外的集群成员变化
     *
     * 【何时启用？】
     *
     * 适合自动伸缩场景：
     * - 云环境自动添加节点
     * - Kubernetes自动扩容
     * - 测试环境快速部署
     *
     * 【默认值false】
     */
    public static final String QUORUM_AUTO_JOIN_ENABLE_CONFIG = QUORUM_PREFIX + "auto.join.enable";
    public static final String QUORUM_AUTO_JOIN_ENABLE_DOC = "Controls whether a KRaft controller should automatically " +
        "join the cluster metadata partition for its cluster id.";
    public static final boolean DEFAULT_QUORUM_AUTO_JOIN_ENABLE = false;

    /**
     * 配置定义
     *
     * 使用Kafka的ConfigDef框架定义所有配置项。
     *
     * 【ConfigDef说明】
     *
     * 每个配置项包含：
     * - 配置键：如QUORUM_VOTERS_CONFIG
     * - 类型：LIST, INT, BOOLEAN等
     * - 默认值：如DEFAULT_QUORUM_VOTERS
     * - 验证器：如ControllerQuorumVotersValidator
     * - 重要性：HIGH, MEDIUM, LOW
     * - 文档：配置说明
     *
     * 【重要性级别】
     *
     * HIGH：必须配置或非常重要
     * - controller.quorum.voters
     * - controller.quorum.election.timeout.ms
     * - controller.quorum.fetch.timeout.ms
     *
     * MEDIUM：影响性能
     * - controller.quorum.append.linger.ms
     * - controller.quorum.request.timeout.ms
     *
     * LOW：高级配置
     * - controller.quorum.retry.backoff.ms
     * - controller.quorum.auto.join.enable
     */
    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
            .define(QUORUM_VOTERS_CONFIG, LIST, DEFAULT_QUORUM_VOTERS, new ControllerQuorumVotersValidator(), HIGH, QUORUM_VOTERS_DOC)
            .define(QUORUM_BOOTSTRAP_SERVERS_CONFIG, LIST, DEFAULT_QUORUM_BOOTSTRAP_SERVERS, new ControllerQuorumBootstrapServersValidator(), HIGH, QUORUM_BOOTSTRAP_SERVERS_DOC)
            .define(QUORUM_ELECTION_TIMEOUT_MS_CONFIG, INT, DEFAULT_QUORUM_ELECTION_TIMEOUT_MS, atLeast(0), HIGH, QUORUM_ELECTION_TIMEOUT_MS_DOC)
            .define(QUORUM_FETCH_TIMEOUT_MS_CONFIG, INT, DEFAULT_QUORUM_FETCH_TIMEOUT_MS, atLeast(0), HIGH, QUORUM_FETCH_TIMEOUT_MS_DOC)
            .define(QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, INT, DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS, atLeast(0), HIGH, QUORUM_ELECTION_BACKOFF_MAX_MS_DOC)
            .define(QUORUM_LINGER_MS_CONFIG, INT, DEFAULT_QUORUM_LINGER_MS, atLeast(0), MEDIUM, QUORUM_LINGER_MS_DOC)
            .define(QUORUM_REQUEST_TIMEOUT_MS_CONFIG, INT, DEFAULT_QUORUM_REQUEST_TIMEOUT_MS, atLeast(0), MEDIUM, QUORUM_REQUEST_TIMEOUT_MS_DOC)
            .define(QUORUM_RETRY_BACKOFF_MS_CONFIG, INT, DEFAULT_QUORUM_RETRY_BACKOFF_MS, atLeast(0), LOW, QUORUM_RETRY_BACKOFF_MS_DOC)
            .define(QUORUM_AUTO_JOIN_ENABLE_CONFIG, BOOLEAN, DEFAULT_QUORUM_AUTO_JOIN_ENABLE, LOW, QUORUM_AUTO_JOIN_ENABLE_DOC);

    // ==================== 配置字段 ====================

    private final List<String> voters;
    private final List<String> bootstrapServers;
    private final int requestTimeoutMs;
    private final int retryBackoffMs;
    private final int electionTimeoutMs;
    private final int electionBackoffMaxMs;
    private final int fetchTimeoutMs;
    private final int appendLingerMs;
    private final boolean autoJoin;

    /**
     * 构造函数
     *
     * 从AbstractConfig中提取QuorumConfig相关的配置项。
     *
     * 【使用示例】
     *
     * <pre>
     * // 从KafkaConfig创建QuorumConfig
     * KafkaConfig kafkaConfig = new KafkaConfig(props);
     * QuorumConfig quorumConfig = new QuorumConfig(kafkaConfig);
     *
     * // 获取配置值
     * int electionTimeout = quorumConfig.electionTimeoutMs();
     * int fetchTimeout = quorumConfig.fetchTimeoutMs();
     * List<String> voters = quorumConfig.voters();
     * </pre>
     *
     * @param abstractConfig Kafka配置对象
     */
    public QuorumConfig(AbstractConfig abstractConfig) {
        this.voters = abstractConfig.getList(QUORUM_VOTERS_CONFIG);
        this.bootstrapServers = abstractConfig.getList(QUORUM_BOOTSTRAP_SERVERS_CONFIG);
        this.requestTimeoutMs = abstractConfig.getInt(QUORUM_REQUEST_TIMEOUT_MS_CONFIG);
        this.retryBackoffMs = abstractConfig.getInt(QUORUM_RETRY_BACKOFF_MS_CONFIG);
        this.electionTimeoutMs = abstractConfig.getInt(QUORUM_ELECTION_TIMEOUT_MS_CONFIG);
        this.electionBackoffMaxMs = abstractConfig.getInt(QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG);
        this.fetchTimeoutMs = abstractConfig.getInt(QUORUM_FETCH_TIMEOUT_MS_CONFIG);
        this.appendLingerMs = abstractConfig.getInt(QUORUM_LINGER_MS_CONFIG);
        this.autoJoin = abstractConfig.getBoolean(QUORUM_AUTO_JOIN_ENABLE_CONFIG);
    }

    // ==================== Getter 方法 ====================

    public List<String> voters() {
        return voters;
    }

    public List<String> bootstrapServers() {
        return bootstrapServers;
    }

    public int requestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int retryBackoffMs() {
        return retryBackoffMs;
    }

    public int electionTimeoutMs() {
        return electionTimeoutMs;
    }

    public int electionBackoffMaxMs() {
        return electionBackoffMaxMs;
    }

    public int fetchTimeoutMs() {
        return fetchTimeoutMs;
    }

    public int appendLingerMs() {
        return appendLingerMs;
    }

    public boolean autoJoin() {
        return autoJoin;
    }

    // ==================== 解析方法 ====================

    /**
     * 解析投票者ID
     *
     * 将字符串转换为整数节点ID。
     *
     * 【示例】
     *
     * <pre>
     * parseVoterId("1") → 1
     * parseVoterId("123") → 123
     * parseVoterId("abc") → 抛出ConfigException
     * </pre>
     *
     * @param idString 节点ID字符串
     * @return 节点ID整数
     * @throws ConfigException 如果无法解析为整数
     */
    private static Integer parseVoterId(String idString) {
        try {
            return Integer.parseInt(idString);
        } catch (NumberFormatException e) {
            throw new ConfigException("Failed to parse voter ID as an integer from " + idString);
        }
    }

    /**
     * 解析投票者连接信息（公共方法）
     *
     * 将投票者字符串列表解析为节点ID到地址的映射。
     *
     * 【格式】
     *
     * 输入：["1@localhost:9092", "2@localhost:9093", "3@localhost:9094"]
     *
     * 输出：
     * <pre>
     * {
     *   1 → localhost:9092,
     *   2 → localhost:9093,
     *   3 → localhost:9094
     * }
     * </pre>
     *
     * 【验证】
     *
     * - 要求可路由地址（不能是0.0.0.0）
     * - 检查格式是否正确
     * - 检查ID是否重复
     *
     * @param voterEntries 投票者字符串列表
     * @return 节点ID到地址的映射
     * @throws ConfigException 如果格式错误或包含非可路由地址
     */
    public static Map<Integer, InetSocketAddress> parseVoterConnections(List<String> voterEntries) {
        return parseVoterConnections(voterEntries, true);
    }

    /**
     * 解析投票者ID集合
     *
     * 只提取节点ID，不解析地址。
     *
     * 【示例】
     *
     * <pre>
     * 输入：["1@localhost:9092", "2@localhost:9093", "3@localhost:9094"]
     * 输出：{1, 2, 3}
     * </pre>
     *
     * 【用途】
     *
     * 当只需要知道有哪些投票者，不需要地址信息时使用。
     *
     * @param voterEntries 投票者字符串列表
     * @return 节点ID集合
     */
    public static Set<Integer> parseVoterIds(List<String> voterEntries) {
        return parseVoterConnections(voterEntries, false).keySet();
    }

    /**
     * 解析投票者连接信息（内部实现）
     *
     * @param voterEntries 投票者字符串列表
     * @param requireRoutableAddresses 是否要求可路由地址
     * @return 节点ID到地址的映射
     * @throws ConfigException 如果解析失败
     */
    private static Map<Integer, InetSocketAddress> parseVoterConnections(
        List<String> voterEntries,
        boolean requireRoutableAddresses
    ) {
        Map<Integer, InetSocketAddress> voterMap = new HashMap<>(voterEntries.size());
        for (String voterMapEntry : voterEntries) {
            // 分割ID和地址：1@localhost:9092 → ["1", "localhost:9092"]
            String[] idAndAddress = voterMapEntry.split("@");
            if (idAndAddress.length != 2) {
                throw new ConfigException("Invalid configuration value for " + QUORUM_VOTERS_CONFIG
                    + ". Each entry should be in the form `{id}@{host}:{port}`.");
            }

            // 解析节点ID
            Integer voterId = parseVoterId(idAndAddress[0]);

            // 解析主机名
            String host = Utils.getHost(idAndAddress[1]);
            if (host == null || !Utils.validHostPattern(host)) {
                throw new ConfigException("Failed to parse host name from entry " + voterMapEntry
                    + " for the configuration " + QUORUM_VOTERS_CONFIG
                    + ". Each entry should be in the form `{id}@{host}:{port}`.");
            }

            // 解析端口
            Integer port = Utils.getPort(idAndAddress[1]);
            if (port == null) {
                throw new ConfigException("Failed to parse host port from entry " + voterMapEntry
                    + " for the configuration " + QUORUM_VOTERS_CONFIG
                    + ". Each entry should be in the form `{id}@{host}:{port}`.");
            }

            // 创建地址
            InetSocketAddress address = new InetSocketAddress(host, port);

            // 验证地址是否可路由
            if (address.getHostString().equals(NON_ROUTABLE_HOST) && requireRoutableAddresses) {
                throw new ConfigException(
                    String.format("Host string (%s) is not routeable", address.getHostString())
                );
            } else {
                voterMap.put(voterId, address);
            }
        }

        return voterMap;
    }

    /**
     * 解析引导服务器列表
     *
     * 将字符串列表转换为InetSocketAddress列表。
     *
     * 【格式】
     *
     * 输入：["localhost:9092", "localhost:9093", "localhost:9094"]
     *
     * 输出：
     * <pre>
     * [
     *   localhost:9092,
     *   localhost:9093,
     *   localhost:9094
     * ]
     * </pre>
     *
     * 【与parseVoterConnections的区别】
     *
     * <pre>
     * parseVoterConnections：
     * - 输入格式：1@localhost:9092
     * - 返回：Map<Integer, InetSocketAddress>
     *
     * parseBootstrapServers：
     * - 输入格式：localhost:9092
     * - 返回：List<InetSocketAddress>
     * </pre>
     *
     * @param bootstrapServers 引导服务器字符串列表
     * @return InetSocketAddress列表
     */
    public static List<InetSocketAddress> parseBootstrapServers(List<String> bootstrapServers) {
        return bootstrapServers
            .stream()
            .map(QuorumConfig::parseBootstrapServer)
            .collect(Collectors.toList());
    }

    /**
     * 解析单个引导服务器
     *
     * 将字符串解析为InetSocketAddress。
     *
     * 【示例】
     *
     * <pre>
     * parseBootstrapServer("localhost:9092") → localhost:9092
     * parseBootstrapServer("192.168.1.1:9093") → 192.168.1.1:9093
     * parseBootstrapServer("invalid") → 抛出ConfigException
     * </pre>
     *
     * @param bootstrapServer 引导服务器字符串
     * @return InetSocketAddress对象
     * @throws ConfigException 如果格式错误
     */
    private static InetSocketAddress parseBootstrapServer(String bootstrapServer) {
        // 解析主机名
        String host = Utils.getHost(bootstrapServer);
        if (host == null || !Utils.validHostPattern(host)) {
            throw new ConfigException(
                String.format(
                    "Failed to parse host name from %s for the configuration %s. Each " +
                    "entry should be in the form \"{host}:{port}\"",
                    bootstrapServer,
                    QUORUM_BOOTSTRAP_SERVERS_CONFIG
                )
            );
        }

        // 解析端口
        Integer port = Utils.getPort(bootstrapServer);
        if (port == null) {
            throw new ConfigException(
                String.format(
                    "Failed to parse host port from %s for the configuration %s. Each " +
                    "entry should be in the form \"{host}:{port}\"",
                    bootstrapServer,
                    QUORUM_BOOTSTRAP_SERVERS_CONFIG
                )
            );
        }

        // 创建未解析的地址（不会触发DNS查询）
        return InetSocketAddress.createUnresolved(host, port);
    }

    /**
     * 将投票者字符串转换为Node列表
     *
     * 便捷方法，直接从字符串列表转换为Node列表。
     *
     * 【示例】
     *
     * <pre>
     * 输入：["1@localhost:9092", "2@localhost:9093"]
     *
     * 输出：
     * [
     *   Node(id=1, host=localhost, port=9092),
     *   Node(id=2, host=localhost, port=9093)
     * ]
     * </pre>
     *
     * @param voters 投票者字符串列表
     * @return Node列表
     */
    public static List<Node> quorumVoterStringsToNodes(List<String> voters) {
        return voterConnectionsToNodes(parseVoterConnections(voters));
    }

    /**
     * 将投票者连接信息转换为Node列表
     *
     * 从Map转换为List，方便网络层使用。
     *
     * 【示例】
     *
     * <pre>
     * 输入：
     * {
     *   1 → localhost:9092,
     *   2 → localhost:9093
     * }
     *
     * 输出：
     * [
     *   Node(id=1, host=localhost, port=9092),
     *   Node(id=2, host=localhost, port=9093)
     * ]
     * </pre>
     *
     * @param voterConnections 投票者连接信息映射
     * @return Node列表
     */
    public static List<Node> voterConnectionsToNodes(Map<Integer, InetSocketAddress> voterConnections) {
        return voterConnections
            .entrySet()
            .stream()
            .filter(Objects::nonNull)
            .map(entry -> new Node(entry.getKey(), entry.getValue().getHostString(), entry.getValue().getPort()))
            .collect(Collectors.toList());
    }

    // ==================== 验证器 ====================

    /**
     * 投票者配置验证器
     *
     * 验证controller.quorum.voters配置的格式是否正确。
     *
     * 【验证内容】
     *
     * 1. 配置不能为null
     * 2. 格式必须是 {id}@{host}:{port}
     * 3. ID必须是有效的整数
     * 4. 主机名和端口必须有效
     * 5. （可选）地址必须可路由
     *
     * 【使用时机】
     *
     * - 读取配置文件时
     * - 验证配置有效性
     * - 防止错误配置导致启动失败
     */
    public static class ControllerQuorumVotersValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, null);
            }

            @SuppressWarnings("unchecked")
            List<String> voterStrings = (List<String>) value;

            // 尝试解析连接字符串（不要求可路由地址，因为可能配置0.0.0.0）
            parseVoterConnections(voterStrings, false);
        }

        @Override
        public String toString() {
            return "non-empty list";
        }
    }

    /**
     * 引导服务器配置验证器
     *
     * 验证controller.quorum.bootstrap.servers配置的格式是否正确。
     *
     * 【验证内容】
     *
     * 1. 配置不能为null
     * 2. 格式必须是 {host}:{port}
     * 3. 主机名和端口必须有效
     *
     * 【与ControllerQuorumVotersValidator的区别】
     *
     * - 不需要节点ID
     * - 格式更简单
     * - 只验证主机名和端口
     */
    public static class ControllerQuorumBootstrapServersValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, null);
            }

            @SuppressWarnings("unchecked")
            List<String> entries = (List<String>) value;

            // 尝试解析连接字符串
            for (String entry : entries) {
                parseBootstrapServer(entry);
            }
        }

        @Override
        public String toString() {
            return "non-empty list";
        }
    }
}
