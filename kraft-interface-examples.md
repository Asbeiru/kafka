# KRaft 核心接口示例

本文档提供 KRaft 核心接口的简化示例，帮助你快速理解和开始实现。

---

## 1. RaftClient 接口

这是最核心的客户端接口，定义了 Raft 客户端的所有操作。

**位置**: `kraft-raft/src/main/java/org/apache/kafka/raft/RaftClient.java`

**参考 Kafka 源码**: `raft/src/main/java/org/apache/kafka/raft/RaftClient.java`

```java
package org.apache.kafka.raft;

import java.io.Closeable;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Kafka Raft 客户端接口
 *
 * 这是 Raft 协议的主要入口点。客户端可以是：
 * - Leader: 接收写入请求，复制日志到 Follower
 * - Follower: 从 Leader 复制日志
 * - Candidate: 参与选举成为 Leader
 *
 * @param <T> 日志记录的类型
 */
public interface RaftClient<T> extends Closeable {

    /**
     * 初始化 Raft 客户端
     *
     * 在调用任何其他方法之前必须先调用此方法。
     */
    void initialize() throws Exception;

    /**
     * 注册监听器以接收 Raft 事件
     *
     * @param listener 事件监听器
     */
    void register(Listener<T> listener);

    /**
     * 追加记录到日志（仅 Leader 可用）
     *
     * @param epoch 当前的 Leader Epoch
     * @param records 要追加的记录
     * @param maxTimestamp 最大时间戳
     * @return 追加后的偏移量
     */
    long append(int epoch, T records, long maxTimestamp);

    /**
     * 读取日志记录
     *
     * @param startOffset 起始偏移量
     * @param isolation 隔离级别（COMMITTED 或 UNCOMMITTED）
     * @return 批次读取器
     */
    BatchReader<T> read(long startOffset, Isolation isolation);

    /**
     * 获取当前的 Leader 信息
     *
     * @return Leader ID 和 Epoch，如果没有 Leader 则返回空
     */
    LeaderAndEpoch currentLeader();

    /**
     * 获取当前节点的 ID
     *
     * @return 节点 ID
     */
    int nodeId();

    /**
     * 获取当前的 Epoch
     *
     * @return 当前 Epoch
     */
    int currentEpoch();

    /**
     * 轮询处理事件（事件循环的核心）
     *
     * 此方法应该在一个循环中被调用：
     * while (true) {
     *     client.poll();
     * }
     */
    void poll();

    /**
     * 优雅关闭（Leader 才需要）
     *
     * Leader 在关闭前会将 Leader 身份转移给其他节点。
     *
     * @param epoch 当前 Epoch
     * @throws Exception 如果关闭失败
     */
    void shutdown(int epoch) throws Exception;

    /**
     * Raft 事件监听器
     *
     * @param <T> 记录类型
     */
    interface Listener<T> {

        /**
         * 当日志记录被提交时调用
         *
         * @param reader 批次读取器
         */
        void handleCommit(BatchReader<T> reader);

        /**
         * 当发生 Leader 变更时调用
         *
         * @param oldLeader 旧 Leader
         * @param newLeader 新 Leader
         */
        void handleLeaderChange(LeaderAndEpoch oldLeader, LeaderAndEpoch newLeader);

        /**
         * 当需要加载快照时调用
         *
         * @param snapshotId 快照 ID
         * @return 快照读取器
         */
        SnapshotReader<T> handleSnapshot(SnapshotId snapshotId);
    }
}
```

**关键点理解：**

1. **泛型 `<T>`**: 表示日志记录的类型，Kafka 中是 `ApiMessageAndVersion`
2. **`poll()`**: 事件循环的核心，处理网络请求、状态转移等
3. **`Listener`**: 回调接口，当有重要事件发生时通知上层应用

---

## 2. EpochState 接口

状态接口，所有 Raft 状态都实现此接口。

**位置**: `kraft-raft/src/main/java/org/apache/kafka/raft/EpochState.java`

**参考 Kafka 源码**: `raft/src/main/java/org/apache/kafka/raft/EpochState.java`

```java
package org.apache.kafka.raft;

import java.util.Optional;
import java.util.Set;

/**
 * Raft 状态接口
 *
 * Raft 协议中的每个节点都处于某个状态：
 * - Unattached: 未附加到任何集群
 * - Follower: 跟随者
 * - Candidate: 候选人（参与选举）
 * - Leader: 领导者
 * - Resigned: 已辞职（Leader 优雅退出）
 */
public interface EpochState {

    /**
     * 获取当前状态的 Epoch
     *
     * Epoch 类似于 Raft 论文中的 Term，每次选举都会递增。
     *
     * @return 当前 Epoch
     */
    int epoch();

    /**
     * 获取当前状态的名称
     *
     * @return 状态名称（如 "Leader", "Follower", "Candidate"）
     */
    ElectionState election();

    /**
     * 获取选举超时时间（毫秒）
     *
     * @param currentTimeMs 当前时间
     * @return 超时时间，如果不适用则返回 Long.MAX_VALUE
     */
    long electionTimeoutMs(long currentTimeMs);

    /**
     * 判断是否可以投票给某个候选人
     *
     * @param candidateId 候选人 ID
     * @param isLogUpToDate 候选人的日志是否是最新的
     * @return true 如果可以投票
     */
    boolean canGrantVote(int candidateId, boolean isLogUpToDate);

    /**
     * 获取当前 Leader 信息（如果有）
     *
     * @return Leader ID，如果当前没有 Leader 则返回空
     */
    Optional<Integer> leaderId();

    /**
     * 获取投票者集合
     *
     * @return 投票者 ID 集合
     */
    Set<Integer> voters();

    /**
     * 状态名称枚举
     */
    enum Name {
        UNATTACHED,    // 未附加
        FOLLOWER,      // 跟随者
        PROSPECTIVE,   // 预选举（Pre-Vote）
        CANDIDATE,     // 候选人
        LEADER,        // 领导者
        RESIGNED       // 已辞职
    }
}
```

**状态转移图：**

```
          选举超时
Unattached -------> Prospective -------> Candidate -------> Leader
    ^                                        |                |
    |                                        |                |
    |                选举失败                 |   成为Follower   |
    +----------------------------------------+                |
    |                                                          |
    |                          辞职                            |
    +<---------------------- Resigned <-----------------------+
```

---

## 3. ReplicatedLog 接口

日志接口，抽象了日志的读写操作。

**位置**: `kraft-raft/src/main/java/org/apache/kafka/raft/ReplicatedLog.java`

**参考 Kafka 源码**: `raft/src/main/java/org/apache/kafka/raft/ReplicatedLog.java`

```java
package org.apache.kafka.raft;

import java.io.Closeable;
import java.util.Optional;

/**
 * 复制日志接口
 *
 * 这是 Raft 日志的抽象接口，负责：
 * - 日志的持久化存储
 * - 日志的读取和写入
 * - 日志的截断
 * - 快照管理
 */
public interface ReplicatedLog extends Closeable {

    /**
     * 作为 Leader 追加记录到日志
     *
     * Leader 接收客户端写入请求后，将记录追加到本地日志。
     *
     * @param epoch 当前 Leader Epoch
     * @param records 要追加的记录
     * @return 追加信息（包括起始偏移量、结束偏移量等）
     */
    LogAppendInfo appendAsLeader(int epoch, T records);

    /**
     * 作为 Follower 追加记录到日志
     *
     * Follower 从 Leader 收到 Fetch 响应后，将记录追加到本地日志。
     *
     * @param epoch Leader Epoch
     * @param startOffset 起始偏移量
     * @param records 要追加的记录
     * @return 追加信息
     */
    LogAppendInfo appendAsFollower(int epoch, long startOffset, T records);

    /**
     * 读取日志记录
     *
     * @param startOffset 起始偏移量
     * @param isolation 隔离级别
     * @return 读取信息（包括批次读取器）
     */
    LogFetchInfo read(long startOffset, Isolation isolation);

    /**
     * 截断日志到指定偏移量
     *
     * 当 Follower 发现日志与 Leader 不一致时，需要截断本地日志。
     *
     * @param endOffset 截断到的偏移量（不包括）
     * @return 是否成功截断
     */
    boolean truncateTo(long endOffset);

    /**
     * 截断完全到指定偏移量（危险操作）
     *
     * 这会删除 >= startOffset 的所有日志。
     *
     * @param startOffset 起始偏移量
     * @return 是否成功截断
     */
    boolean truncateFullyTo(long startOffset);

    /**
     * 获取日志的结束偏移量
     *
     * @return 日志末尾偏移量（下一条记录将写入的位置）
     */
    long endOffset();

    /**
     * 获取日志的起始偏移量
     *
     * @return 日志起始偏移量
     */
    long startOffset();

    /**
     * 获取指定偏移量的 Epoch
     *
     * @param offset 偏移量
     * @return Epoch，如果偏移量无效则返回空
     */
    Optional<OffsetAndEpoch> endOffsetForEpoch(int epoch);

    /**
     * 更新高水位（High Watermark）
     *
     * 高水位是所有副本都已复制的最大偏移量，只有 <= 高水位的记录才能被客户端读取。
     *
     * @param highWatermark 新的高水位
     */
    void updateHighWatermark(LogOffsetMetadata highWatermark);

    /**
     * 获取当前高水位
     *
     * @return 高水位
     */
    Optional<LogOffsetMetadata> highWatermark();

    /**
     * 刷新日志到磁盘
     *
     * @param flush 是否强制刷新
     */
    void flush(boolean flush);
}
```

**关键概念：**

1. **High Watermark（高水位）**:
   - 已经被多数派确认的最大偏移量
   - 只有 <= 高水位的记录才能被客户端读取（COMMITTED 隔离级别）

2. **Epoch**:
   - 类似于 Raft 论文中的 Term
   - 每次选举都会递增
   - 用于检测日志的一致性

3. **Leader vs Follower 追加**:
   - Leader: 生成新记录，分配偏移量
   - Follower: 复制 Leader 的记录，使用 Leader 分配的偏移量

---

## 4. NetworkChannel 接口

网络通道接口，抽象了网络通信。

**位置**: `kraft-raft/src/main/java/org/apache/kafka/raft/NetworkChannel.java`

**参考 Kafka 源码**: `raft/src/main/java/org/apache/kafka/raft/NetworkChannel.java`

```java
package org.apache.kafka.raft;

import java.io.Closeable;
import java.util.List;

/**
 * 网络通道接口
 *
 * 抽象了 Raft 节点之间的网络通信，负责：
 * - 发送请求到其他节点
 * - 接收其他节点的请求
 * - 发送响应
 */
public interface NetworkChannel extends Closeable {

    /**
     * 发送请求到指定节点
     *
     * @param request 出站请求
     */
    void send(RaftRequest.Outbound request);

    /**
     * 轮询网络事件
     *
     * @param timeoutMs 超时时间（毫秒）
     * @return 接收到的入站请求和响应
     */
    List<RaftMessage> poll(long timeoutMs);

    /**
     * 唤醒网络线程
     *
     * 用于中断 poll() 的阻塞。
     */
    void wakeup();

    /**
     * 更新节点的端点信息
     *
     * @param nodeId 节点 ID
     * @param endpoints 端点信息（地址和端口）
     */
    void updateEndpoints(int nodeId, Endpoints endpoints);
}
```

**使用示例：**

```java
// 发送 Vote 请求
VoteRequest voteRequest = new VoteRequest(...);
RaftRequest.Outbound outbound = new RaftRequest.Outbound(
    correlationId,
    voteRequest,
    destinationNodeId,
    currentTimeMs
);
networkChannel.send(outbound);

// 轮询响应
List<RaftMessage> messages = networkChannel.poll(100);
for (RaftMessage message : messages) {
    if (message instanceof RaftResponse.Inbound) {
        RaftResponse.Inbound response = (RaftResponse.Inbound) message;
        // 处理响应
    } else if (message instanceof RaftRequest.Inbound) {
        RaftRequest.Inbound request = (RaftRequest.Inbound) message;
        // 处理请求
    }
}
```

---

## 5. QuorumConfig 类

配置类，包含 Quorum 的所有配置项。

**位置**: `kraft-raft/src/main/java/org/apache/kafka/raft/QuorumConfig.java`

**参考 Kafka 源码**: `raft/src/main/java/org/apache/kafka/raft/QuorumConfig.java`

```java
package org.apache.kafka.raft;

import java.util.Properties;

/**
 * Quorum 配置
 *
 * 包含 Raft 集群的所有配置参数。
 */
public class QuorumConfig {

    // 选举超时时间（毫秒）
    public static final String ELECTION_TIMEOUT_MS_CONFIG = "controller.quorum.election.timeout.ms";
    public static final int ELECTION_TIMEOUT_MS_DEFAULT = 1000;

    // 选举退避时间（毫秒）
    public static final String ELECTION_BACKOFF_MAX_MS_CONFIG = "controller.quorum.election.backoff.max.ms";
    public static final int ELECTION_BACKOFF_MAX_MS_DEFAULT = 1000;

    // Fetch 超时时间（毫秒）
    public static final String FETCH_TIMEOUT_MS_CONFIG = "controller.quorum.fetch.timeout.ms";
    public static final int FETCH_TIMEOUT_MS_DEFAULT = 2000;

    // 追加延迟时间（用于批处理）
    public static final String APPEND_LINGER_MS_CONFIG = "controller.quorum.append.linger.ms";
    public static final int APPEND_LINGER_MS_DEFAULT = 25;

    // 请求超时时间
    public static final String REQUEST_TIMEOUT_MS_CONFIG = "controller.quorum.request.timeout.ms";
    public static final int REQUEST_TIMEOUT_MS_DEFAULT = 2000;

    // 重试退避时间
    public static final String RETRY_BACKOFF_MS_CONFIG = "controller.quorum.retry.backoff.ms";
    public static final int RETRY_BACKOFF_MS_DEFAULT = 20;

    private final int electionTimeoutMs;
    private final int electionBackoffMaxMs;
    private final int fetchTimeoutMs;
    private final int appendLingerMs;
    private final int requestTimeoutMs;
    private final int retryBackoffMs;

    public QuorumConfig(Properties properties) {
        this.electionTimeoutMs = getInt(properties, ELECTION_TIMEOUT_MS_CONFIG, ELECTION_TIMEOUT_MS_DEFAULT);
        this.electionBackoffMaxMs = getInt(properties, ELECTION_BACKOFF_MAX_MS_CONFIG, ELECTION_BACKOFF_MAX_MS_DEFAULT);
        this.fetchTimeoutMs = getInt(properties, FETCH_TIMEOUT_MS_CONFIG, FETCH_TIMEOUT_MS_DEFAULT);
        this.appendLingerMs = getInt(properties, APPEND_LINGER_MS_CONFIG, APPEND_LINGER_MS_DEFAULT);
        this.requestTimeoutMs = getInt(properties, REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS_DEFAULT);
        this.retryBackoffMs = getInt(properties, RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS_DEFAULT);
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

    public int requestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int retryBackoffMs() {
        return retryBackoffMs;
    }

    private int getInt(Properties props, String key, int defaultValue) {
        String value = props.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }
}
```

**配置示例：**

```properties
# 选举超时（如果在此时间内没有收到 Leader 心跳，则发起选举）
controller.quorum.election.timeout.ms=1000

# Fetch 超时（Follower 多久从 Leader 获取一次日志）
controller.quorum.fetch.timeout.ms=2000

# 批处理延迟（Leader 等待多久后批量发送日志）
controller.quorum.append.linger.ms=25
```

---

## 6. 第一个状态实现示例：UnattachedState

最简单的状态，作为学习的起点。

**位置**: `kraft-raft/src/main/java/org/apache/kafka/raft/UnattachedState.java`

**参考 Kafka 源码**: `raft/src/main/java/org/apache/kafka/raft/UnattachedState.java`

```java
package org.apache.kafka.raft;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Unattached 状态
 *
 * 这是节点的初始状态，表示节点尚未附加到任何集群或尚未参与任何 Epoch。
 *
 * 在此状态下：
 * - 节点不会发送或接收任何 Raft 消息
 * - 节点等待选举超时后转移到 Prospective 或 Candidate 状态
 */
public class UnattachedState implements EpochState {

    private final int epoch;
    private final Set<Integer> voters;
    private final long electionTimeoutMs;

    /**
     * 构造 Unattached 状态
     *
     * @param epoch 当前 Epoch
     * @param voters 投票者集合
     * @param electionTimeoutMs 选举超时时间
     */
    public UnattachedState(
        int epoch,
        Set<Integer> voters,
        long electionTimeoutMs
    ) {
        this.epoch = epoch;
        this.voters = voters;
        this.electionTimeoutMs = electionTimeoutMs;
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withUnknownLeader(epoch, voters);
    }

    @Override
    public long electionTimeoutMs(long currentTimeMs) {
        // 返回选举超时时间，当超时后会触发选举
        return electionTimeoutMs;
    }

    @Override
    public boolean canGrantVote(int candidateId, boolean isLogUpToDate) {
        // Unattached 状态下可以投票给任何候选人（如果日志是最新的）
        return voters.contains(candidateId) && isLogUpToDate;
    }

    @Override
    public Optional<Integer> leaderId() {
        // Unattached 状态下没有 Leader
        return Optional.empty();
    }

    @Override
    public Set<Integer> voters() {
        return Collections.unmodifiableSet(voters);
    }

    @Override
    public String toString() {
        return String.format(
            "UnattachedState(epoch=%d, voters=%s, electionTimeoutMs=%d)",
            epoch, voters, electionTimeoutMs
        );
    }
}
```

**状态转移：**

```
UnattachedState
    |
    | (选举超时)
    v
ProspectiveState (Pre-Vote) 或 CandidateState
```

---

## 7. 开始实现的建议步骤

### 步骤 1：创建项目骨架

```bash
# 运行快速启动脚本
cd /home/user/kafka
chmod +x kraft-learning-quickstart.sh
./kraft-learning-quickstart.sh
```

### 步骤 2：从 Kafka 源码复制核心接口

复制以下文件到你的项目：

```bash
# 假设 Kafka 源码在 /home/user/kafka
KAFKA_SRC=/home/user/kafka
YOUR_PROJECT=$HOME/kraft-learning

# 复制核心接口
cp $KAFKA_SRC/raft/src/main/java/org/apache/kafka/raft/RaftClient.java \
   $YOUR_PROJECT/kraft-raft/src/main/java/org/apache/kafka/raft/

cp $KAFKA_SRC/raft/src/main/java/org/apache/kafka/raft/EpochState.java \
   $YOUR_PROJECT/kraft-raft/src/main/java/org/apache/kafka/raft/

cp $KAFKA_SRC/raft/src/main/java/org/apache/kafka/raft/ReplicatedLog.java \
   $YOUR_PROJECT/kraft-raft/src/main/java/org/apache/kafka/raft/

cp $KAFKA_SRC/raft/src/main/java/org/apache/kafka/raft/NetworkChannel.java \
   $YOUR_PROJECT/kraft-raft/src/main/java/org/apache/kafka/raft/

cp $KAFKA_SRC/raft/src/main/java/org/apache/kafka/raft/QuorumConfig.java \
   $YOUR_PROJECT/kraft-raft/src/main/java/org/apache/kafka/raft/
```

### 步骤 3：逐步实现

按照以下顺序实现：

1. **第1天**：复制并理解 `RaftClient` 接口
2. **第2天**：复制并理解 `EpochState` 接口
3. **第3天**：实现 `UnattachedState`（最简单）
4. **第4天**：实现 `FollowerState`
5. **第5天**：编写第一个单元测试

### 步骤 4：添加详细注释

在复制代码时，添加你自己的理解：

```java
/**
 * 高水位（High Watermark）
 *
 * 我的理解：
 * - 高水位是所有副本都已确认的最大偏移量
 * - 类似于"安全点"，只有到达高水位的记录才能被客户端读取
 * - Leader 通过跟踪所有 Follower 的复制进度来计算高水位
 *
 * 计算方式：
 * highWatermark = min(所有 Follower 的 fetchOffset)
 *
 * 为什么需要：
 * - 保证已提交的记录不会丢失（已被多数派确认）
 * - 保证读一致性（客户端不会读到未提交的数据）
 */
private long highWatermark;
```

---

## 8. 调试和验证技巧

### 技巧 1：添加详细日志

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnattachedState implements EpochState {
    private static final Logger log = LoggerFactory.getLogger(UnattachedState.class);

    @Override
    public boolean canGrantVote(int candidateId, boolean isLogUpToDate) {
        boolean canGrant = voters.contains(candidateId) && isLogUpToDate;

        log.debug("canGrantVote: candidateId={}, isLogUpToDate={}, canGrant={}",
                  candidateId, isLogUpToDate, canGrant);

        return canGrant;
    }
}
```

### 技巧 2：单元测试

```java
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;

class UnattachedStateTest {

    @Test
    void testCanGrantVote() {
        Set<Integer> voters = Set.of(1, 2, 3);
        UnattachedState state = new UnattachedState(0, voters, 1000);

        // 可以投票给投票者，如果日志是最新的
        assertTrue(state.canGrantVote(1, true));

        // 不能投票给非投票者
        assertFalse(state.canGrantVote(4, true));

        // 不能投票给日志不是最新的候选人
        assertFalse(state.canGrantVote(1, false));
    }

    @Test
    void testNoLeader() {
        Set<Integer> voters = Set.of(1, 2, 3);
        UnattachedState state = new UnattachedState(0, voters, 1000);

        // Unattached 状态下没有 Leader
        assertFalse(state.leaderId().isPresent());
    }
}
```

---

## 9. 常见问题

### Q: 我需要完全理解每一行代码吗？

**A**: 不需要一开始就完全理解。建议：
1. 先理解接口和抽象
2. 再理解核心流程（选举、日志复制）
3. 最后深入细节（优化、边界情况）

### Q: 应该复制多少代码？

**A**: 建议：
- **接口和简单类**：完全复制（如 QuorumConfig）
- **核心逻辑类**：边复制边添加注释（如 LeaderState）
- **复杂工具类**：先复制，逐步理解（如 RaftUtil）

### Q: 如何确认我的理解是正确的？

**A**: 建议：
1. 编写单元测试验证行为
2. 对比 Kafka 的测试用例
3. 运行集成测试观察实际行为
4. 画状态转移图和流程图

---

## 10. 下一步

1. **立即行动**：运行快速启动脚本创建项目
2. **复制接口**：从 Kafka 源码复制 5 个核心接口
3. **实现第一个状态**：实现 `UnattachedState`
4. **编写测试**：为 `UnattachedState` 编写单元测试
5. **阅读 Raft 论文**：理解 Leader Election（第5章）

祝学习顺利！🚀

---

## 附录：Kafka 源码文件路径

```
Kafka 源码根目录: /home/user/kafka/

核心 Raft 实现:
  /home/user/kafka/raft/src/main/java/org/apache/kafka/raft/

接口文件:
  RaftClient.java        - raft/src/main/java/org/apache/kafka/raft/RaftClient.java
  EpochState.java        - raft/src/main/java/org/apache/kafka/raft/EpochState.java
  ReplicatedLog.java     - raft/src/main/java/org/apache/kafka/raft/ReplicatedLog.java
  NetworkChannel.java    - raft/src/main/java/org/apache/kafka/raft/NetworkChannel.java

核心类:
  KafkaRaftClient.java   - raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java
  QuorumState.java       - raft/src/main/java/org/apache/kafka/raft/QuorumState.java
  LeaderState.java       - raft/src/main/java/org/apache/kafka/raft/LeaderState.java
  FollowerState.java     - raft/src/main/java/org/apache/kafka/raft/FollowerState.java

测试:
  /home/user/kafka/raft/src/test/java/org/apache/kafka/raft/
```
