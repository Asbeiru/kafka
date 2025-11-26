#!/bin/bash

# KRaft 学习项目快速启动脚本
# 用途：自动创建项目骨架和初始文件

set -e

PROJECT_NAME="kraft-learning"
BASE_DIR="$HOME/$PROJECT_NAME"

echo "🚀 开始创建 KRaft 学习项目..."
echo "项目位置: $BASE_DIR"
echo ""

# 创建项目根目录
mkdir -p "$BASE_DIR"
cd "$BASE_DIR"

echo "📁 创建目录结构..."

# 创建模块目录
mkdir -p kraft-common/src/main/java/org/apache/kafka/common/{utils,record,protocol,errors}
mkdir -p kraft-common/src/main/java/org/apache/kafka/server/common
mkdir -p kraft-common/src/test/java/org/apache/kafka/common

mkdir -p kraft-raft/src/main/java/org/apache/kafka/raft
mkdir -p kraft-raft/src/main/java/org/apache/kafka/raft/internals
mkdir -p kraft-raft/src/main/java/org/apache/kafka/snapshot
mkdir -p kraft-raft/src/main/resources/common/message
mkdir -p kraft-raft/src/test/java/org/apache/kafka/raft

mkdir -p kraft-storage/src/main/java/org/apache/kafka/storage
mkdir -p kraft-storage/src/main/java/org/apache/kafka/storage/internals/log
mkdir -p kraft-storage/src/test/java/org/apache/kafka/storage

mkdir -p kraft-network/src/main/java/org/apache/kafka/network
mkdir -p kraft-network/src/test/java/org/apache/kafka/network

mkdir -p kraft-example/src/main/java/example

mkdir -p docs

echo "✅ 目录结构创建完成"
echo ""

echo "📝 创建父 POM..."

cat > pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.apache.kafka.learning</groupId>
    <artifactId>kraft-learning</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>KRaft Learning Project</name>
    <description>深入学习 Kafka Raft (KRaft) 实现</description>

    <modules>
        <module>kraft-common</module>
        <module>kraft-raft</module>
        <module>kraft-storage</module>
        <module>kraft-network</module>
        <module>kraft-example</module>
    </modules>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

        <kafka.version>4.0.0-SNAPSHOT</kafka.version>
        <slf4j.version>2.0.9</slf4j.version>
        <jackson.version>2.17.2</jackson.version>
        <junit.version>5.10.1</junit.version>
        <mockito.version>5.8.0</mockito.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>${slf4j.version}</version>
            </dependency>

            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-simple</artifactId>
                <version>${slf4j.version}</version>
            </dependency>

            <dependency>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-databind</artifactId>
                <version>${jackson.version}</version>
            </dependency>

            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit.version}</version>
                <scope>test</scope>
            </dependency>

            <dependency>
                <groupId>org.mockito</groupId>
                <artifactId>mockito-core</artifactId>
                <version>${mockito.version}</version>
                <scope>test</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.11.0</version>
                <configuration>
                    <source>17</source>
                    <target>17</target>
                </configuration>
            </plugin>

            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.2.3</version>
            </plugin>
        </plugins>
    </build>
</project>
EOF

echo "✅ 父 POM 创建完成"
echo ""

echo "📝 创建各模块 POM..."

# kraft-common POM
cat > kraft-common/pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.apache.kafka.learning</groupId>
        <artifactId>kraft-learning</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>kraft-common</artifactId>
    <name>KRaft Common</name>

    <dependencies>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
EOF

# kraft-raft POM
cat > kraft-raft/pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.apache.kafka.learning</groupId>
        <artifactId>kraft-learning</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>kraft-raft</artifactId>
    <name>KRaft Raft Implementation</name>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka.learning</groupId>
            <artifactId>kraft-common</artifactId>
            <version>${project.version}</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>

        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
EOF

# kraft-storage POM
cat > kraft-storage/pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.apache.kafka.learning</groupId>
        <artifactId>kraft-learning</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>kraft-storage</artifactId>
    <name>KRaft Storage</name>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka.learning</groupId>
            <artifactId>kraft-common</artifactId>
            <version>${project.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.kafka.learning</groupId>
            <artifactId>kraft-raft</artifactId>
            <version>${project.version}</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
EOF

# kraft-network POM
cat > kraft-network/pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.apache.kafka.learning</groupId>
        <artifactId>kraft-learning</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>kraft-network</artifactId>
    <name>KRaft Network</name>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka.learning</groupId>
            <artifactId>kraft-common</artifactId>
            <version>${project.version}</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
EOF

# kraft-example POM
cat > kraft-example/pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.apache.kafka.learning</groupId>
        <artifactId>kraft-learning</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>kraft-example</artifactId>
    <name>KRaft Examples</name>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka.learning</groupId>
            <artifactId>kraft-raft</artifactId>
            <version>${project.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.kafka.learning</groupId>
            <artifactId>kraft-storage</artifactId>
            <version>${project.version}</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.1.1</version>
                <configuration>
                    <mainClass>example.SimpleRaftCluster</mainClass>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
EOF

echo "✅ 模块 POM 创建完成"
echo ""

echo "📝 创建 README..."

cat > README.md << 'EOF'
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
EOF

echo "✅ README 创建完成"
echo ""

echo "📝 创建 .gitignore..."

cat > .gitignore << 'EOF'
# Maven
target/
pom.xml.tag
pom.xml.releaseBackup
pom.xml.versionsBackup
pom.xml.next
release.properties
dependency-reduced-pom.xml
buildNumber.properties
.mvn/timing.properties

# IntelliJ IDEA
.idea/
*.iml
*.iws
*.ipr

# Eclipse
.classpath
.project
.settings/

# VS Code
.vscode/

# OS
.DS_Store
Thumbs.db

# Logs
*.log

# Data directories
data/
logs/
EOF

echo "✅ .gitignore 创建完成"
echo ""

echo "📝 创建学习笔记模板..."

cat > docs/learning-notes.md << 'EOF'
# KRaft 学习笔记

## 日期：YYYY-MM-DD

### 今日学习内容

- [ ]

### 今日实现的类

- [ ]

### 遇到的问题

1.

### 解决方案

1.

### 理解要点

-

### 明日计划

- [ ]

---
EOF

cat > docs/raft-protocol.md << 'EOF'
# Raft 协议学习笔记

## 1. 基本概念

### 1.1 角色
- Leader
- Follower
- Candidate

### 1.2 任期（Term）
-

### 1.3 日志结构
-

## 2. Leader Election

### 2.1 选举触发条件
-

### 2.2 选举过程
1.
2.
3.

### 2.3 Pre-Vote 机制
-

## 3. Log Replication

### 3.1 日志复制流程
-

### 3.2 一致性检查
-

### 3.3 高水位（High Watermark）
-

## 4. 安全性

### 4.1 选举安全性
-

### 4.2 Leader Append-Only
-

### 4.3 日志匹配
-

## 5. 成员变更

### 5.1 单成员变更
-

### 5.2 联合共识
-

---
EOF

cat > docs/kafka-raft-design.md << 'EOF'
# Kafka KRaft 设计分析

## 1. 整体架构

### 1.1 模块划分
-

### 1.2 依赖关系
-

## 2. 核心类分析

### 2.1 KafkaRaftClient
- 职责：
- 关键方法：
- 状态管理：

### 2.2 QuorumState
- 职责：
- 状态转移：
- 关键字段：

### 2.3 LeaderState
- 职责：
- 日志复制：
- Follower 追踪：

## 3. 关键流程

### 3.1 启动流程
1.
2.
3.

### 3.2 选举流程
1.
2.
3.

### 3.3 日志复制流程
1.
2.
3.

## 4. 优化点

### 4.1 批处理
-

### 4.2 Pre-Vote
-

### 4.3 快照
-

---
EOF

echo "✅ 学习笔记模板创建完成"
echo ""

echo "🎉 项目创建完成！"
echo ""
echo "📍 项目位置: $BASE_DIR"
echo ""
echo "🚀 下一步："
echo "   1. cd $BASE_DIR"
echo "   2. mvn clean compile  # 验证项目结构"
echo "   3. 开始学习：参考 KRAFT_LEARNING_PROJECT_GUIDE.md"
echo ""
echo "📚 重要文档："
echo "   - $BASE_DIR/KRAFT_LEARNING_PROJECT_GUIDE.md  # 学习指南"
echo "   - $BASE_DIR/README.md                         # 项目说明"
echo "   - $BASE_DIR/docs/                             # 学习笔记"
echo ""
echo "💡 提示："
echo "   从 Kafka 源码复制以下接口开始："
echo "   - RaftClient.java"
echo "   - EpochState.java"
echo "   - ReplicatedLog.java"
echo "   - NetworkChannel.java"
echo ""
echo "✨ Happy Learning! ✨"
