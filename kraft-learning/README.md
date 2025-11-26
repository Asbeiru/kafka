# KRaft 学习项目

深入学习和研究 Kafka Raft (KRaft) 实现的学习项目。

## 项目结构

```
kraft-learning/
├── kraft-common/       # 公共基础模块
├── kraft-raft/         # 核心 Raft 实现 ⭐⭐⭐
├── kraft-storage/      # 日志存储模块
├── kraft-network/      # 网络通信模块
└── kraft-example/      # 示例和测试
```

## 快速开始

### 1. 编译项目

```bash
mvn clean compile
```

### 2. 运行测试

```bash
mvn test
```

### 3. 运行示例

```bash
cd kraft-example
mvn exec:java -Dexec.mainClass="example.SimpleRaftCluster"
```

## 学习路线

请参考项目根目录下的 `KRAFT_LEARNING_PROJECT_GUIDE.md` 获取详细的学习指南。

## 模块说明

### kraft-common
公共基础组件，包括：
- 异常类
- 基础数据结构
- 工具类

### kraft-raft ⭐⭐⭐
核心 Raft 协议实现，包括：
- 84 个核心类
- 状态机实现（Leader, Follower, Candidate等）
- 选举和日志复制
- 快照管理
- 动态成员变更

### kraft-storage
日志存储实现，包括：
- 日志段管理
- 索引管理
- 日志清理

### kraft-network
网络通信实现，包括：
- 网络通道
- 请求/响应处理

### kraft-example
示例和测试，包括：
- 3节点集群示例
- 选举演示
- 故障转移测试

## 学习检查清单

### 第一周
- [ ] 搭建项目骨架
- [ ] 实现核心接口
- [ ] 实现 UnattachedState
- [ ] 阅读 Raft 论文

### 第一个月
- [ ] 实现所有状态类
- [ ] 实现 QuorumState
- [ ] 理解状态转移
- [ ] 理解选举过程

## 参考资料

- [Raft 论文](https://raft.github.io/raft.pdf)
- [KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500)
- [KIP-595](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595)
- [Raft 可视化](https://raft.github.io/)

## License

本项目仅用于学习目的。
