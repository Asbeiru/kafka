# KRaft 重写快速开始指南

如果你想立即开始实现，这里提供一个最简化的开始方案。

---

## 第一步：30分钟 - 理解核心概念

### Raft 的 5 个核心概念

1. **Leader 选举**: 只有 Leader 可以处理写入
2. **日志复制**: Leader 将日志复制到多数派
3. **安全性**: 日志一旦提交，永远不会丢失
4. **成员变更**: 动态添加/删除节点
5. **日志压缩**: 通过快照压缩旧日志

### 最小实现范围

```
第一版只需要:
1. Leader 选举 (PreVote + Vote)
2. 基本的日志复制 (Follower Fetch)
3. 简单的持久化 (文件存储)
4. 3 节点测试通过

暂不需要:
- 快照
- 动态成员变更
- 性能优化
- Observer
```

---

## 第二步：2小时 - 搭建项目骨架

### 使用 Java/Maven

```bash
# 创建项目
mkdir kraft-rewrite
cd kraft-rewrite

# 创建目录结构
mkdir -p src/main/java/raft/{api,core,storage,network}
mkdir -p src/main/proto
mkdir -p src/test/java/raft

# 创建 pom.xml
cat > pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>kraft-rewrite</artifactId>
    <version>0.1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <grpc.version>1.59.0</grpc.version>
        <protobuf.version>3.24.0</protobuf.version>
    </properties>

    <dependencies>
        <!-- gRPC -->
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-netty-shaded</artifactId>
            <version>${grpc.version}</version>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-protobuf</artifactId>
            <version>${grpc.version}</version>
        </dependency>
        <dependency>
            <groupId>io.grpc</groupId>
            <artifactId>grpc-stub</artifactId>
            <version>${grpc.version}</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>2.0.9</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.4.11</version>
        </dependency>

        <!-- Testing -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.10.0</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>3.24.2</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>4.2.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <extensions>
            <extension>
                <groupId>kr.motd.maven</groupId>
                <artifactId>os-maven-plugin</artifactId>
                <version>1.7.1</version>
            </extension>
        </extensions>
        <plugins>
            <plugin>
                <groupId>org.xolstice.maven.plugins</groupId>
                <artifactId>protobuf-maven-plugin</artifactId>
                <version>0.6.1</version>
                <configuration>
                    <protocArtifact>com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}</protocArtifact>
                    <pluginId>grpc-java</pluginId>
                    <pluginArtifact>io.grpc:protoc-gen-grpc-java:${grpc.version}:exe:${os.detected.classifier}</pluginArtifact>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>compile-custom</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
EOF

# 初始化 Git
git init
cat > .gitignore << 'EOF'
target/
*.iml
.idea/
*.log
data/
EOF
```

### 使用 Go

```bash
# 创建项目
mkdir kraft-rewrite
cd kraft-rewrite

# 初始化 Go 模块
go mod init github.com/yourname/kraft-rewrite

# 创建目录结构
mkdir -p {api,core,storage,network,pb}

# 安装依赖
go get google.golang.org/grpc@latest
go get google.golang.org/protobuf/cmd/protoc-gen-go@latest
go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go get github.com/stretchr/testify@latest
```

---

## 第三步：1天 - 实现最简单的日志存储

**文件**: `src/main/java/raft/storage/SimpleLog.java`

```java
package raft.storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SimpleLog {
    private final ConcurrentNavigableMap<Long, LogEntry> entries = new ConcurrentSkipListMap<>();
    private final File logFile;
    private long nextOffset = 0;

    public SimpleLog(File logFile) {
        this.logFile = logFile;
        loadFromFile();
    }

    public long append(int epoch, byte[] data) {
        long offset = nextOffset++;
        LogEntry entry = new LogEntry(offset, epoch, data);
        entries.put(offset, entry);
        persistEntry(entry);
        return offset;
    }

    public List<LogEntry> read(long startOffset, int maxCount) {
        List<LogEntry> result = new ArrayList<>();
        for (Map.Entry<Long, LogEntry> e : entries.tailMap(startOffset).entrySet()) {
            if (result.size() >= maxCount) break;
            result.add(e.getValue());
        }
        return result;
    }

    public void truncateTo(long offset) {
        entries.tailMap(offset).clear();
        nextOffset = offset;
        rewriteFile();
    }

    public long endOffset() {
        return nextOffset;
    }

    public int lastEpoch() {
        if (entries.isEmpty()) return 0;
        return entries.lastEntry().getValue().epoch();
    }

    private void persistEntry(LogEntry entry) {
        try (FileOutputStream fos = new FileOutputStream(logFile, true);
             DataOutputStream dos = new DataOutputStream(fos)) {
            dos.writeLong(entry.offset());
            dos.writeInt(entry.epoch());
            dos.writeInt(entry.data().length);
            dos.write(entry.data());
            fos.getFD().sync();  // fsync!
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void loadFromFile() {
        if (!logFile.exists()) return;

        try (FileInputStream fis = new FileInputStream(logFile);
             DataInputStream dis = new DataInputStream(fis)) {
            while (dis.available() > 0) {
                long offset = dis.readLong();
                int epoch = dis.readInt();
                int length = dis.readInt();
                byte[] data = new byte[length];
                dis.readFully(data);
                entries.put(offset, new LogEntry(offset, epoch, data));
                nextOffset = offset + 1;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void rewriteFile() {
        // 简单实现：重写整个文件
        File temp = new File(logFile.getAbsolutePath() + ".tmp");
        try (FileOutputStream fos = new FileOutputStream(temp);
             DataOutputStream dos = new DataOutputStream(fos)) {
            for (LogEntry entry : entries.values()) {
                dos.writeLong(entry.offset());
                dos.writeInt(entry.epoch());
                dos.writeInt(entry.data().length);
                dos.write(entry.data());
            }
            fos.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        temp.renameTo(logFile);
    }

    public static class LogEntry {
        private final long offset;
        private final int epoch;
        private final byte[] data;

        public LogEntry(long offset, int epoch, byte[] data) {
            this.offset = offset;
            this.epoch = epoch;
            this.data = data;
        }

        public long offset() { return offset; }
        public int epoch() { return epoch; }
        public byte[] data() { return data; }
    }
}
```

---

## 第四步：1天 - 实现简单的状态机

**文件**: `src/main/java/raft/core/State.java`

```java
package raft.core;

public interface State {
    StateType type();
    int epoch();
}

enum StateType {
    FOLLOWER, CANDIDATE, LEADER
}

class FollowerState implements State {
    private final int epoch;
    private final int leaderId;

    public FollowerState(int epoch, int leaderId) {
        this.epoch = epoch;
        this.leaderId = leaderId;
    }

    public StateType type() { return StateType.FOLLOWER; }
    public int epoch() { return epoch; }
    public int leaderId() { return leaderId; }
}

class CandidateState implements State {
    private final int epoch;
    private final Set<Integer> votesReceived = new HashSet<>();

    public CandidateState(int epoch, int selfId) {
        this.epoch = epoch;
        this.votesReceived.add(selfId);  // 给自己投票
    }

    public StateType type() { return StateType.CANDIDATE; }
    public int epoch() { return epoch; }

    public void addVote(int voterId) {
        votesReceived.add(voterId);
    }

    public boolean hasMajority(int totalVoters) {
        return votesReceived.size() > totalVoters / 2;
    }
}

class LeaderState implements State {
    private final int epoch;
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private long highWatermark = 0;

    public LeaderState(int epoch) {
        this.epoch = epoch;
    }

    public StateType type() { return StateType.LEADER; }
    public int epoch() { return epoch; }

    public void updateMatchIndex(int followerId, long index) {
        matchIndex.put(followerId, index);
    }

    public void updateHighWatermark(int totalVoters) {
        List<Long> indices = new ArrayList<>(matchIndex.values());
        indices.sort(Long::compareTo);
        int quorumIndex = totalVoters / 2;
        if (quorumIndex < indices.size()) {
            highWatermark = indices.get(quorumIndex);
        }
    }

    public long highWatermark() {
        return highWatermark;
    }
}
```

---

## 第五步：2天 - 实现选举逻辑

**文件**: `src/main/java/raft/core/RaftNode.java`

```java
package raft.core;

import raft.storage.SimpleLog;
import java.util.*;
import java.util.concurrent.*;

public class RaftNode {
    private final int nodeId;
    private final Set<Integer> voters;
    private final SimpleLog log;

    private volatile State currentState;
    private final Random random = new Random();

    public RaftNode(int nodeId, Set<Integer> voters, SimpleLog log) {
        this.nodeId = nodeId;
        this.voters = voters;
        this.log = log;
        this.currentState = new FollowerState(0, -1);
    }

    // 处理投票请求
    public VoteResponse handleVoteRequest(VoteRequest request) {
        int currentEpoch = currentState.epoch();

        // 拒绝旧的 epoch
        if (request.epoch() < currentEpoch) {
            return new VoteResponse(currentEpoch, false);
        }

        // 发现更高的 epoch，转为 Follower
        if (request.epoch() > currentEpoch) {
            currentState = new FollowerState(request.epoch(), -1);
            currentEpoch = request.epoch();
        }

        // 检查日志是否足够新
        boolean logOk = request.lastLogEpoch() > log.lastEpoch() ||
                       (request.lastLogEpoch() == log.lastEpoch() &&
                        request.lastLogOffset() >= log.endOffset() - 1);

        boolean voteGranted = logOk;

        return new VoteResponse(currentEpoch, voteGranted);
    }

    // 发起选举
    public void startElection() {
        int newEpoch = currentState.epoch() + 1;
        currentState = new CandidateState(newEpoch, nodeId);

        // 发送投票请求给所有其他节点
        VoteRequest request = new VoteRequest(
            newEpoch,
            nodeId,
            log.endOffset() - 1,
            log.lastEpoch()
        );

        // TODO: 发送网络请求
        System.out.println("Node " + nodeId + " starting election for epoch " + newEpoch);
    }

    // 处理投票响应
    public void handleVoteResponse(VoteResponse response) {
        if (!(currentState instanceof CandidateState)) {
            return;
        }

        CandidateState candidate = (CandidateState) currentState;

        if (response.voteGranted()) {
            // 这里简化了，实际应该记录是哪个节点投的票
            candidate.addVote(response.voterId);

            if (candidate.hasMajority(voters.size())) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        currentState = new LeaderState(currentState.epoch());
        System.out.println("Node " + nodeId + " became leader for epoch " + currentState.epoch());
    }

    public State currentState() {
        return currentState;
    }
}

// 辅助类
record VoteRequest(int epoch, int candidateId, long lastLogOffset, int lastLogEpoch) {}
record VoteResponse(int epoch, boolean voteGranted, int voterId) {
    VoteResponse(int epoch, boolean voteGranted) {
        this(epoch, voteGranted, -1);
    }
}
```

---

## 第六步：1天 - 编写第一个测试

**文件**: `src/test/java/raft/core/RaftNodeTest.java`

```java
package raft.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import raft.storage.SimpleLog;

import java.io.File;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

public class RaftNodeTest {

    @Test
    public void testElection(@TempDir File tempDir) {
        // 创建 3 个节点
        Set<Integer> voters = Set.of(1, 2, 3);

        SimpleLog log1 = new SimpleLog(new File(tempDir, "log1"));
        SimpleLog log2 = new SimpleLog(new File(tempDir, "log2"));
        SimpleLog log3 = new SimpleLog(new File(tempDir, "log3"));

        RaftNode node1 = new RaftNode(1, voters, log1);
        RaftNode node2 = new RaftNode(2, voters, log2);
        RaftNode node3 = new RaftNode(3, voters, log3);

        // Node1 发起选举
        node1.startElection();
        assertThat(node1.currentState().type()).isEqualTo(StateType.CANDIDATE);

        // Node2 和 Node3 投票
        VoteRequest voteReq = new VoteRequest(1, 1, 0, 0);
        VoteResponse vote2 = node2.handleVoteRequest(voteReq);
        VoteResponse vote3 = node3.handleVoteRequest(voteReq);

        assertThat(vote2.voteGranted()).isTrue();
        assertThat(vote3.voteGranted()).isTrue();

        // Node1 收到投票，应该成为 Leader
        node1.handleVoteResponse(vote2);
        node1.handleVoteResponse(vote3);

        assertThat(node1.currentState().type()).isEqualTo(StateType.LEADER);
    }

    @Test
    public void testLogReplication(@TempDir File tempDir) {
        SimpleLog log = new SimpleLog(new File(tempDir, "log"));

        long offset1 = log.append(1, "data1".getBytes());
        long offset2 = log.append(1, "data2".getBytes());

        assertThat(offset1).isEqualTo(0);
        assertThat(offset2).isEqualTo(1);
        assertThat(log.endOffset()).isEqualTo(2);

        var entries = log.read(0, 10);
        assertThat(entries).hasSize(2);
        assertThat(new String(entries.get(0).data())).isEqualTo("data1");
    }
}
```

---

## 下一步

现在你有了一个可以运行的最小原型！接下来：

1. **完善网络通信**: 使用 gRPC 实现真正的网络通信
2. **实现日志复制**: Leader 复制日志到 Followers
3. **添加更多测试**: 特别是集成测试
4. **实现 PreVote**: 防止无效选举
5. **添加快照**: 实现日志压缩

---

## 学习资源

### 推荐阅读顺序
1. Raft 论文前 5 节（核心算法）
2. Raft 作者的博士论文第 3-4 章
3. etcd/raft 源码（Go，很清晰）
4. Kafka KRaft 设计文档 (KIP-500)

### 调试技巧
- 打印每次状态转换
- 打印每次投票决策
- 使用固定的随机种子
- 记录所有 RPC 请求/响应

### 常见问题
1. **为什么需要 PreVote？** 防止网络分区恢复后的无效选举
2. **为什么只能提交当前 epoch 的日志？** 保证安全性
3. **为什么 Follower 主动 Fetch？** Kafka 的设计哲学

---

开始编码吧！记住：先让它工作，再让它正确，最后让它快速。
