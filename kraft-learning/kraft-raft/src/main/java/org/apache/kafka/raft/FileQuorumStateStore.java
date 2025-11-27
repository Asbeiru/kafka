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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.generated.QuorumStateData;
import org.apache.kafka.raft.generated.QuorumStateDataJsonConverter;
import org.apache.kafka.server.common.KRaftVersion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * FileQuorumStateStore - 基于本地文件的quorum状态存储
 *
 * 使用JSON格式存储{@link QuorumStateData}，并额外添加一个数据版本号字段(data_version)。
 *
 * 【为什么使用JSON格式？】
 *
 * 1. **可读性强**
 *    - 管理员可以直接查看状态文件
 *    - 调试时方便检查状态
 *    - 不需要特殊工具解析
 *
 * 2. **灵活性好**
 *    - 容易添加新字段
 *    - 向后兼容旧版本
 *    - 版本升级平滑
 *
 * 3. **简单性**
 *    - 无需复杂的序列化代码
 *    - 复用Jackson库
 *    - 错误处理简单
 *
 * 【支持的版本格式】
 *
 * Version 0格式示例：
 * <pre>
 * {
 *   "clusterId": "",
 *   "leaderId": 1,
 *   "leaderEpoch": 2,
 *   "votedId": -1,
 *   "appliedOffset": 0,
 *   "currentVoters": [],
 *   "data_version": 0
 * }
 * </pre>
 *
 * 字段说明：
 * - clusterId：集群ID（版本0需要）
 * - leaderId：当前Leader的节点ID
 * - leaderEpoch：当前Leader的epoch
 * - votedId：本节点在当前epoch投票给谁（-1表示未投票）
 * - appliedOffset：已应用的偏移量（版本0需要）
 * - currentVoters：当前投票者列表（版本0需要）
 * - data_version：数据格式版本号
 *
 * Version 1格式示例：
 * <pre>
 * {
 *   "leaderId": -1,
 *   "leaderEpoch": 2,
 *   "votedId": 1,
 *   "votedDirectoryId": "J8aAPcfLQt2bqs1JT_rMgQ",
 *   "data_version": 1
 * }
 * </pre>
 *
 * 版本1的改进：
 * - 移除clusterId（从配置获取）
 * - 移除appliedOffset（从日志获取）
 * - 移除currentVoters（从配置获取）
 * - 添加votedDirectoryId（支持多副本目录）
 * - 更简洁，只存储核心选举状态
 *
 * 【文件结构】
 *
 * <pre>
 * /var/lib/kafka/data/
 *   ├─ quorum-state         ← 主文件（当前有效状态）
 *   └─ quorum-state.tmp     ← 临时文件（写入时使用）
 * </pre>
 *
 * 为什么需要临时文件？
 * - 保证原子写入
 * - 防止写入一半时崩溃导致文件损坏
 *
 * 【原子写入机制】
 *
 * 写入步骤（原子操作）：
 * <pre>
 * 1. 写入 quorum-state.tmp（新内容）
 * 2. 调用 fsync() 确保数据落盘
 * 3. 原子重命名：quorum-state.tmp → quorum-state
 * 4. 删除 quorum-state.tmp（清理临时文件）
 * </pre>
 *
 * 为什么这是原子的？
 * - 文件系统的重命名操作是原子的
 * - 要么看到新内容（重命名成功）
 * - 要么看到旧内容（重命名前崩溃）
 * - 永远不会看到半成品（写入一半的内容）
 *
 * 【崩溃恢复场景】
 *
 * 场景1：写入tmp文件时崩溃
 * <pre>
 * 1. 开始写入quorum-state.tmp
 * 2. 写入一半时进程崩溃
 * 3. 重启后：
 *    - quorum-state 仍然完好（旧状态）
 *    - quorum-state.tmp 损坏或不完整
 * 4. readElectionState() 读取 quorum-state（成功）
 * 5. 下次写入时覆盖损坏的tmp文件
 * 结果：安全，使用旧状态继续运行
 * </pre>
 *
 * 场景2：重命名前崩溃
 * <pre>
 * 1. quorum-state.tmp 写入完成，调用fsync()
 * 2. 准备重命名时崩溃
 * 3. 重启后：
 *    - quorum-state 存在（旧状态）
 *    - quorum-state.tmp 存在且完整（新状态）
 * 4. readElectionState() 读取 quorum-state（旧状态）
 * 5. 删除 quorum-state.tmp（清理临时文件）
 * 结果：安全，使用旧状态继续运行
 * </pre>
 *
 * 场景3：重命名后崩溃
 * <pre>
 * 1. quorum-state.tmp 写入完成
 * 2. 重命名成功：quorum-state.tmp → quorum-state
 * 3. 删除tmp文件前崩溃
 * 4. 重启后：
 *    - quorum-state 存在（新状态）
 *    - quorum-state.tmp 可能存在（重命名前的备份）
 * 5. readElectionState() 读取 quorum-state（新状态）
 * 6. 删除 quorum-state.tmp（清理）
 * 结果：安全，使用新状态继续运行
 * </pre>
 *
 * 【典型使用流程】
 *
 * 初始化：
 * <pre>
 * File stateFile = new File("/var/lib/kafka/data/quorum-state");
 * QuorumStateStore store = new FileQuorumStateStore(stateFile);
 * </pre>
 *
 * 读取状态：
 * <pre>
 * Optional<ElectionState> state = store.readElectionState();
 * if (state.isPresent()) {
 *     int epoch = state.get().epoch();
 *     int votedId = state.get().votedId();
 *     int leaderId = state.get().leaderId().orElse(-1);
 * }
 * </pre>
 *
 * 更新状态：
 * <pre>
 * ElectionState newState = new ElectionState(
 *     5,              // epoch
 *     1,              // votedId
 *     1,              // leaderId
 *     voterSet        // voters
 * );
 * store.writeElectionState(newState, KRaftVersion.KRAFT_VERSION_1);
 * </pre>
 *
 * 清除状态：
 * <pre>
 * store.clear();  // 删除所有状态文件
 * </pre>
 *
 * 【版本兼容性】
 *
 * 支持的版本范围：
 * - LOWEST_SUPPORTED_VERSION = 0
 * - HIGHEST_SUPPORTED_VERSION = 1
 *
 * 版本升级路径：
 * <pre>
 * Version 0 → Version 1：
 * 1. 读取Version 0格式的文件
 * 2. 提取核心字段（leaderId, leaderEpoch, votedId）
 * 3. 下次写入时使用Version 1格式
 * 4. 旧文件被新格式覆盖
 * </pre>
 *
 * 版本降级（不支持）：
 * <pre>
 * Version 1 → Version 0：
 * - 不支持降级
 * - 如果尝试用旧版本Kafka读取Version 1文件，会抛出异常
 * - 需要手动格式化或保留备份
 * </pre>
 *
 * 【线程安全性】
 *
 * FileQuorumStateStore本身不是线程安全的：
 * - 不要多线程并发写入
 * - 可以多线程读取（但要注意缓存一致性）
 * - 调用者负责同步（通常由QuorumState保证）
 *
 * 典型调用模式（由QuorumState保证串行化）：
 * <pre>
 * synchronized(quorumStateLock) {
 *     ElectionState state = computeNewState();
 *     stateStore.writeElectionState(state, kraftVersion);
 * }
 * </pre>
 *
 * 【性能特性】
 *
 * - 写入频率：低（只在投票和状态转移时）
 * - 写入延迟：~1-10ms（包括fsync）
 * - 读取频率：低（只在启动和投票时）
 * - 读取延迟：<1ms
 * - 文件大小：<1KB
 * - 不需要索引或缓存
 *
 * 【错误处理】
 *
 * 文件不存在：
 * - readElectionState() 返回 Optional.empty()
 * - 不抛异常（正常场景，第一次启动）
 *
 * 文件损坏：
 * - 抛出 UncheckedIOException
 * - 包含详细错误信息
 * - 需要管理员介入（删除损坏文件或恢复备份）
 *
 * 版本不兼容：
 * - 抛出 IllegalStateException
 * - 说明当前版本和支持的版本范围
 * - 需要升级Kafka或格式化存储
 *
 * @see QuorumStateStore 接口定义
 * @see ElectionState 选举状态数据结构
 * @see QuorumStateData 内部存储格式
 */
public class FileQuorumStateStore implements QuorumStateStore {
    private static final Logger log = LoggerFactory.getLogger(FileQuorumStateStore.class);

    /**
     * 数据版本号字段名
     * 在JSON中额外添加此字段，用于标识数据格式版本
     */
    private static final String DATA_VERSION = "data_version";

    /**
     * 支持的最低数据版本
     * 可以读取 >= LOWEST_SUPPORTED_VERSION 的文件
     */
    static final short LOWEST_SUPPORTED_VERSION = 0;

    /**
     * 支持的最高数据版本
     * 可以写入 <= HIGHEST_SUPPORTED_VERSION 的文件
     */
    static final short HIGHEST_SUPPORTED_VERSION = 1;

    /**
     * 默认状态文件名
     * 用于存储quorum选举状态
     */
    public static final String DEFAULT_FILE_NAME = "quorum-state";

    /**
     * 状态文件
     * 存储当前有效的选举状态
     */
    private final File stateFile;

    /**
     * 构造函数
     *
     * 创建一个基于文件的quorum状态存储。
     *
     * 【参数说明】
     *
     * @param stateFile 状态文件路径
     *                  - 通常是 /var/lib/kafka/data/quorum-state
     *                  - 父目录必须存在
     *                  - 文件本身可以不存在（第一次启动时创建）
     *
     * 【示例】
     *
     * <pre>
     * // 使用默认文件名
     * File stateFile = new File(dataDir, FileQuorumStateStore.DEFAULT_FILE_NAME);
     * FileQuorumStateStore store = new FileQuorumStateStore(stateFile);
     *
     * // 使用自定义文件名
     * File customFile = new File("/custom/path/my-quorum-state");
     * FileQuorumStateStore customStore = new FileQuorumStateStore(customFile);
     * </pre>
     */
    public FileQuorumStateStore(final File stateFile) {
        this.stateFile = stateFile;
    }

    /**
     * 从文件读取quorum状态数据
     *
     * 内部方法，解析JSON文件并转换为QuorumStateData对象。
     *
     * 【读取流程】
     *
     * <pre>
     * 1. 打开文件，创建BufferedReader
     * 2. 读取第一行（整个文件就一行JSON）
     * 3. 使用Jackson解析JSON
     * 4. 提取data_version字段
     * 5. 验证版本号是否在支持范围内
     * 6. 使用QuorumStateDataJsonConverter解析具体字段
     * 7. 返回QuorumStateData对象
     * </pre>
     *
     * 【为什么整个文件只有一行？】
     *
     * - 简化解析逻辑
     * - 原子读取（一次读取整个状态）
     * - 文件很小（<1KB），不需要多行
     *
     * 【版本验证】
     *
     * <pre>
     * 检查 data_version 是否在支持范围内：
     * if (dataVersion < LOWEST_SUPPORTED_VERSION || dataVersion > HIGHEST_SUPPORTED_VERSION) {
     *     throw IllegalStateException
     * }
     * </pre>
     *
     * 【异常情况】
     *
     * EOFException：
     * - 文件为空（没有内容）
     * - 可能是创建了文件但还没写入
     *
     * IOException：
     * - JSON格式错误
     * - data_version字段缺失
     * - 解析失败
     *
     * IllegalStateException：
     * - 版本号超出支持范围
     * - 需要升级Kafka或格式化存储
     *
     * @param file 要读取的状态文件
     * @return 解析后的QuorumStateData对象
     * @throws UncheckedIOException 如果读取或解析失败
     * @throws IllegalStateException 如果数据版本不在支持范围内
     */
    private QuorumStateData readStateFromFile(File file) {
        try (final BufferedReader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
            // 读取第一行（整个文件）
            final String line = reader.readLine();
            if (line == null) {
                throw new EOFException("File ended prematurely.");
            }

            // 解析JSON
            final ObjectMapper objectMapper = new ObjectMapper();
            JsonNode readNode = objectMapper.readTree(line);

            // 验证是否为JSON对象
            if (!(readNode instanceof ObjectNode dataObject)) {
                throw new IOException("Deserialized node " + readNode +
                    " is not an object node");
            }

            // 提取data_version字段
            JsonNode dataVersionNode = dataObject.get(DATA_VERSION);
            if (dataVersionNode == null) {
                throw new IOException("Deserialized node " + readNode +
                    " does not have " + DATA_VERSION + " field");
            }

            // 获取版本号
            final short dataVersion = dataVersionNode.shortValue();

            // 验证版本号
            if (dataVersion < LOWEST_SUPPORTED_VERSION || dataVersion > HIGHEST_SUPPORTED_VERSION) {
                throw new IllegalStateException(
                    String.format(
                        "data_version (%d) is not within the min (%d) and max (%d) supported version",
                        dataVersion,
                        LOWEST_SUPPORTED_VERSION,
                        HIGHEST_SUPPORTED_VERSION
                    )
                );
            }

            // 使用对应版本的转换器解析数据
            return QuorumStateDataJsonConverter.read(dataObject, dataVersion);
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format("Error while reading the Quorum status from the file %s", file), e);
        }
    }

    /**
     * 从本地文件读取选举状态
     *
     * 实现QuorumStateStore接口的方法。
     *
     * 【读取逻辑】
     *
     * <pre>
     * 1. 检查文件是否存在
     *    - 如果不存在：返回Optional.empty()（第一次启动）
     *    - 如果存在：继续读取
     * 2. 调用readStateFromFile()解析文件
     * 3. 将QuorumStateData转换为ElectionState
     * 4. 返回Optional.of(state)
     * </pre>
     *
     * 【使用场景】
     *
     * 启动时恢复状态：
     * <pre>
     * Optional<ElectionState> state = stateStore.readElectionState();
     * if (state.isPresent()) {
     *     // 恢复到之前的状态
     *     currentEpoch = state.get().epoch();
     *     votedFor = state.get().votedId();
     * } else {
     *     // 第一次启动，从epoch=0开始
     *     currentEpoch = 0;
     *     votedFor = -1;
     * }
     * </pre>
     *
     * 投票前检查：
     * <pre>
     * Optional<ElectionState> state = stateStore.readElectionState();
     * if (state.isPresent() && state.get().epoch() == requestEpoch) {
     *     // 已经在这个epoch投票了
     *     if (state.get().votedId() == candidateId) {
     *         return true;  // 可以重复投票给同一候选人
     *     } else {
     *         return false; // 不能改投其他候选人
     *     }
     * }
     * </pre>
     *
     * @return 最新写入的选举状态，如果文件不存在则返回Optional.empty()
     */
    @Override
    public Optional<ElectionState> readElectionState() {
        if (!stateFile.exists()) {
            return Optional.empty();
        }

        return Optional.of(ElectionState.fromQuorumStateData(readStateFromFile(stateFile)));
    }

    /**
     * 持久化选举状态到文件
     *
     * 实现QuorumStateStore接口的方法。
     *
     * 【写入流程】
     *
     * <pre>
     * 1. 将ElectionState转换为QuorumStateData
     * 2. 根据kraftVersion确定数据格式版本
     * 3. 调用writeElectionStateToFile()执行原子写入
     * </pre>
     *
     * 【版本选择】
     *
     * kraftVersion决定存储格式：
     * <pre>
     * KRaftVersion.KRAFT_VERSION_0 → quorumStateVersion = 0
     * KRaftVersion.KRAFT_VERSION_1 → quorumStateVersion = 1
     * </pre>
     *
     * 【使用场景】
     *
     * 投票时保存：
     * <pre>
     * // 收到VoteRequest，决定投票
     * ElectionState newState = new ElectionState(epoch, candidateId, -1, voterSet);
     * stateStore.writeElectionState(newState, KRaftVersion.KRAFT_VERSION_1);
     * // 持久化后才发送VoteResponse
     * sendVoteResponse(candidateId, true);
     * </pre>
     *
     * 成为Leader时保存：
     * <pre>
     * // 赢得选举
     * ElectionState newState = new ElectionState(epoch, myId, myId, voterSet);
     * stateStore.writeElectionState(newState, KRaftVersion.KRAFT_VERSION_1);
     * // 持久化后才开始作为Leader工作
     * becomeLeader();
     * </pre>
     *
     * @param latest 最新的选举状态
     * @param kraftVersion 最终确定的kraft.version，决定数据格式版本
     */
    @Override
    public void writeElectionState(ElectionState latest, KRaftVersion kraftVersion) {
        short quorumStateVersion = kraftVersion.quorumStateVersion();

        writeElectionStateToFile(
            stateFile,
            latest.toQuorumStateData(quorumStateVersion),
            quorumStateVersion
        );
    }

    /**
     * 获取状态文件路径
     *
     * 实现QuorumStateStore接口的方法。
     *
     * 【用途】
     *
     * 日志记录：
     * <pre>
     * log.info("Quorum state store path: {}", stateStore.path());
     * </pre>
     *
     * 调试：
     * <pre>
     * // 管理员可以检查状态文件
     * cat /var/lib/kafka/data/quorum-state
     * </pre>
     *
     * @return 状态文件的路径
     */
    @Override
    public Path path() {
        return stateFile.toPath();
    }

    /**
     * 将选举状态写入文件（原子操作）
     *
     * 内部方法，实现原子写入逻辑。
     *
     * 【原子写入步骤】
     *
     * <pre>
     * 1. 验证版本号
     *    - 确保version <= HIGHEST_SUPPORTED_VERSION
     *
     * 2. 创建临时文件
     *    - 文件名：quorum-state.tmp
     *    - 先删除旧的tmp文件（如果存在）
     *
     * 3. 写入临时文件
     *    - 将QuorumStateData转换为JSON
     *    - 添加data_version字段
     *    - 写入文件
     *    - 调用flush()刷新缓冲区
     *    - 调用fsync()确保数据落盘
     *
     * 4. 原子重命名
     *    - Utils.atomicMoveWithFallback(tmp, stateFile)
     *    - 文件系统保证重命名的原子性
     *    - 成功后，stateFile包含新内容
     *
     * 5. 清理临时文件
     *    - 删除quorum-state.tmp
     *    - 无论成功失败都要清理
     * </pre>
     *
     * 【为什么需要fsync()？】
     *
     * <pre>
     * 不使用fsync()的风险：
     * 1. 写入数据到文件
     * 2. 数据在操作系统缓冲区（还没落盘）
     * 3. 重命名文件（成功）
     * 4. 系统崩溃
     * 5. 重启后，数据丢失（缓冲区内容没写到磁盘）
     *
     * 使用fsync()后：
     * 1. 写入数据到文件
     * 2. 调用fsync()，强制数据落盘
     * 3. fsync()返回后，数据已在磁盘上（持久化）
     * 4. 重命名文件
     * 5. 即使崩溃，数据也不会丢失
     * </pre>
     *
     * 【为什么先写临时文件再重命名？】
     *
     * <pre>
     * 直接写入主文件的问题：
     * 1. 打开quorum-state文件
     * 2. 写入新内容（覆盖旧内容）
     * 3. 写入一半时崩溃
     * 4. 文件损坏（既不是新内容也不是旧内容）
     * 5. 重启后无法恢复
     *
     * 使用临时文件的好处：
     * 1. 写入quorum-state.tmp（新内容）
     * 2. 写入一半时崩溃
     * 3. quorum-state文件完好（旧内容）
     * 4. 重启后读取quorum-state（成功）
     * 5. 删除损坏的tmp文件，继续运行
     * </pre>
     *
     * 【版本验证】
     *
     * 如果尝试写入不支持的版本：
     * <pre>
     * if (version > HIGHEST_SUPPORTED_VERSION) {
     *     throw IllegalArgumentException
     * }
     * </pre>
     *
     * 防止旧版本Kafka写入新格式（可能导致数据损坏）。
     *
     * 【异常处理】
     *
     * IOException：
     * - 文件写入失败
     * - 磁盘空间不足
     * - 权限不足
     * - 转换为UncheckedIOException
     *
     * IllegalArgumentException：
     * - 版本号太高
     * - 需要升级Kafka
     *
     * @param stateFile 目标状态文件
     * @param state 要写入的quorum状态数据
     * @param version 数据格式版本号
     * @throws IllegalArgumentException 如果版本号超出支持范围
     * @throws UncheckedIOException 如果写入失败
     */
    private void writeElectionStateToFile(final File stateFile, QuorumStateData state, short version) {
        // 验证版本号
        if (version > HIGHEST_SUPPORTED_VERSION) {
            throw new IllegalArgumentException(
                String.format(
                    "Quorum state data version (%d) is greater than the supported version (%d)",
                    version,
                    HIGHEST_SUPPORTED_VERSION
                )
            );
        }

        // 创建临时文件路径
        final File temp = new File(stateFile.getAbsolutePath() + ".tmp");
        deleteFileIfExists(temp);

        log.trace("Writing tmp quorum state {}", temp.getAbsolutePath());

        try {
            // 写入临时文件
            try (final FileOutputStream fileOutputStream = new FileOutputStream(temp);
                 final BufferedWriter writer = new BufferedWriter(
                     new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)
                 )
            ) {
                // 将QuorumStateData转换为JSON
                ObjectNode jsonState = (ObjectNode) QuorumStateDataJsonConverter.write(state, version);

                // 添加data_version字段
                jsonState.set(DATA_VERSION, new ShortNode(version));

                // 写入JSON字符串
                writer.write(jsonState.toString());

                // 刷新缓冲区
                writer.flush();

                // 强制数据落盘（持久化）
                fileOutputStream.getFD().sync();
            }

            // 原子重命名：tmp → stateFile
            Utils.atomicMoveWithFallback(temp.toPath(), stateFile.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format(
                    "Error while writing the Quorum status from the file %s",
                    stateFile.getAbsolutePath()
                ),
                e
            );
        } finally {
            // 清理临时文件（无论成功失败）
            deleteFileIfExists(temp);
        }
    }

    /**
     * 清除状态存储（删除本地quorum状态文件）
     *
     * 实现QuorumStateStore接口的方法。
     *
     * 【清除的文件】
     *
     * <pre>
     * 删除：
     * 1. quorum-state        （主文件）
     * 2. quorum-state.tmp    （临时文件，如果存在）
     * </pre>
     *
     * 【使用场景】
     *
     * 格式化存储：
     * <pre>
     * // kafka-storage.sh format
     * stateStore.clear();
     * replicatedLog.deleteAll();
     * snapshotStore.deleteAll();
     * </pre>
     *
     * 测试清理：
     * <pre>
     * @AfterEach
     * void cleanup() {
     *     stateStore.clear();
     * }
     * </pre>
     *
     * 【清除后的行为】
     *
     * <pre>
     * stateStore.clear();
     * Optional<ElectionState> state = stateStore.readElectionState();
     * // state.isEmpty() == true
     * </pre>
     */
    @Override
    public void clear() {
        deleteFileIfExists(stateFile);
        deleteFileIfExists(new File(stateFile.getAbsolutePath() + ".tmp"));
    }

    /**
     * toString方法
     *
     * 返回状态存储的字符串表示，用于日志记录。
     *
     * 【输出示例】
     *
     * <pre>
     * Quorum state filepath: /var/lib/kafka/data/quorum-state
     * </pre>
     *
     * @return 包含文件路径的字符串
     */
    @Override
    public String toString() {
        return "Quorum state filepath: " + stateFile.getAbsolutePath();
    }

    /**
     * 删除文件（如果存在）
     *
     * 内部辅助方法，安全地删除文件。
     *
     * 【行为】
     *
     * <pre>
     * 文件存在：删除文件
     * 文件不存在：什么也不做（不抛异常）
     * 删除失败：抛出UncheckedIOException
     * </pre>
     *
     * 【使用场景】
     *
     * <pre>
     * // 清理临时文件
     * deleteFileIfExists(new File("quorum-state.tmp"));
     *
     * // 格式化存储
     * deleteFileIfExists(stateFile);
     * </pre>
     *
     * @param file 要删除的文件
     * @throws UncheckedIOException 如果删除失败
     */
    private void deleteFileIfExists(File file) {
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format("Error while deleting file %s", file.getAbsoluteFile()), e);
        }
    }
}
